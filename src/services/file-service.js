'use strict';

const { randomUUID } = require('node:crypto');
const { hashKey } = require('../ring');
const { CATALOG_NAME, MAX_HOPS } = require('../chord-config');
const { validateFileName } = require('../validation');

class FileService {
  constructor({ node, rpcClient, repository, routingService, replicationService,
    clock = () => new Date() }) {
    this.node = node;
    this.rpcClient = rpcClient;
    this.repository = repository;
    this.routingService = routingService;
    this.replicationService = replicationService;
    this.clock = clock;
    this.primaryWriteLock = Promise.resolve();
  }

  initializeStorage() {
    return this.repository.initialize();
  }

  async put(fileName, content, { updateCatalog = true } = {}) {
    this.node.assertJoined();
    const name = validateFileName(fileName);
    if (name === CATALOG_NAME) throw new Error(`${CATALOG_NAME} é reservado para o controle da rede`);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const hashId = hashKey(name);
    const uploadedAt = this.clock().toISOString();
    let previousHistory = [];
    try {
      previousHistory = (await this.replicationService().locate(name)).events || [];
    } catch {
      // Arquivo novo ou referências temporariamente indisponíveis.
    }
    const uploadEvent = {
      id: randomUUID(),
      type: 'upload',
      timestamp: uploadedAt,
      sequence: nextSequence(previousHistory),
      node: this.node.reference
    };

    // O nó que recebeu o upload é o primário. O hash continua sendo exibido e
    // usado pelo Chord, mas a localização é resolvida pelo índice distribuído.
    await this.storeLocal(name, bytes, {
      isReplica: false,
      primaryNodeId: this.node.id,
      hashId,
      uploadedBy: this.node.reference,
      uploadedAt,
      history: [...previousHistory, uploadEvent]
    });
    const replicas = await this.replicationService().replicate(name, bytes, hashId);

    if (updateCatalog) await this.addToCatalog(name);
    return {
      name,
      hashId,
      node: this.node.reference,
      primary: this.node.reference,
      replicas,
      uploadedBy: this.node.reference,
      uploadedAt,
      locations: [
        { ...this.node.reference, role: 'primary' },
        ...replicas.map((replica) => ({ ...replica, role: 'replica' }))
      ],
      size: bytes.length
    };
  }

  async get(fileName) {
    this.node.assertJoined();
    const name = validateFileName(fileName);
    if (name === CATALOG_NAME) {
      const names = await this.getNetworkCatalog();
      const content = Buffer.from(names.length ? `${names.join('\n')}\n` : '');
      return {
        name,
        hashId: hashKey(name),
        node: this.node.reference,
        primary: null,
        isReplica: false,
        size: content.length,
        content
      };
    }

    const located = await this.replicationService().locate(name);
    const candidates = [located.primary, ...located.replicas].filter(Boolean);
    let lastError = notFound(name);
    for (const candidate of candidates) {
      try {
        if (candidate.id === this.node.id) {
          const content = await this.repository.read(name);
          const metadata = await this.repository.getMetadata(name);
          return fileResult(name, content, this.node.reference, located.primary, metadata.isReplica);
        }
        const result = await this.rpcClient.request(
          candidate, `/rpc/files?name=${encodeURIComponent(name)}`);
        return fileResult(name, Buffer.from(result.content, 'base64'), candidate,
          located.primary, Boolean(result.isReplica));
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError;
  }

  async getFromKnownCopies(name) {
    const result = await this.get(name);
    return { node: result.node, isReplica: result.isReplica, content: result.content };
  }

  async addToCatalog(fileName) {
    const name = validateFileName(fileName);
    await this.repository.addCatalogEntry(name);
    const visited = new Set([this.node.id]);
    const queue = [this.node.predecessor, this.node.successor,
      ...this.node.fingers.map((finger) => finger.node)].filter(Boolean);

    while (queue.length && visited.size < MAX_HOPS) {
      const current = queue.shift();
      if (!current || visited.has(current.id)) continue;
      visited.add(current.id);
      try {
        await this.rpcClient.request(current, '/rpc/catalog', {
          method: 'PUT',
          body: { names: [name] }
        });
        const state = await this.rpcClient.request(current, '/api/state');
        queue.push(state.predecessor, state.successor,
          ...state.fingerTable.map((finger) => finger.node));
      } catch {
        // O arquivo já está confirmado; outros nós repararão o catálogo ao consultá-lo.
      }
    }
  }

  async getNetworkCatalog() {
    this.node.assertJoined();
    const names = new Set();
    const visited = new Set();
    const queue = [this.node.reference, this.node.predecessor, this.node.successor,
      ...this.node.fingers.map((finger) => finger.node)].filter(Boolean);

    while (queue.length && visited.size < MAX_HOPS) {
      const current = queue.shift();
      if (!current || visited.has(current.id)) continue;
      visited.add(current.id);
      if (current.id === this.node.id) {
        const result = {
          names: [...await this.repository.readCatalogNames(),
            ...await this.repository.listFileNames()]
        };
        for (const name of result.names) names.add(name);
      } else {
        try {
          const [result, state] = await Promise.all([
            this.rpcClient.request(current, '/rpc/catalog'),
            this.rpcClient.request(current, '/api/state')
          ]);
          for (const name of result.names || []) names.add(name);
          queue.push(state.predecessor, state.successor,
            ...state.fingerTable.map((finger) => finger.node));
        } catch {
          // Continua pelas referências restantes se este nó estiver offline.
        }
      }
    }

    const catalog = [...names].sort((left, right) => left.localeCompare(right, 'pt-BR'));
    await this.repository.mergeCatalogEntries(catalog);
    return catalog;
  }

  async syncCatalog(source) {
    await this.repository.initialize();
    if (!source || source.id === this.node.id) return this.repository.readCatalogNames();
    try {
      const result = await this.rpcClient.request(source, '/rpc/catalog');
      return this.repository.mergeCatalogEntries(result.names || []);
    } catch {
      return this.repository.readCatalogNames();
    }
  }

  async storeLocal(fileName, content, options = {}) {
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const { isReplica = false, hashId } = options;
    return this.withPrimaryWriteLock(async () => {
      if (!isReplica && this.node.leaving && this.node._leavePhase === 'forwarding') {
        await this.forwardPrimaryFile(name, bytes, hashId, options);
        return false;
      }
      const stored = await this.repository.store(name, bytes, options);
      if (stored && !isReplica && this.node.leaving && this.node._leaveTarget) {
        await this.forwardPrimaryFile(name, bytes, hashId, options);
      }
      return stored;
    });
  }

  async transferPrimaryFiles(target, { replicate = true, transferId, fromNode } = {}) {
    const files = await this.repository.listPrimaryFiles();
    for (const file of files) {
      const [content, metadata] = await Promise.all([
        this.repository.read(file.name),
        this.repository.getMetadata(file.name)
      ]);
      const timestamp = this.clock().toISOString();
      const event = {
        id: transferId || randomUUID(),
        type: 'primary_transferred',
        timestamp,
        sequence: nextSequence(metadata.history),
        node: target,
        fromNode: fromNode || this.node.reference
      };
      await this.rpcClient.request(target, '/rpc/files', {
        method: 'PUT',
        body: {
          name: file.name,
          content: content.toString('base64'),
          isReplica: false,
          hashId: file.hashId,
          replicate,
          uploadedBy: metadata.uploadedBy,
          uploadedAt: metadata.uploadedAt,
          history: [...(metadata.history || []), event]
        }
      });
    }
  }

  async forwardPrimaryFile(name, content, hashId, metadata = {}) {
    if (!this.node._leaveTarget) throw new Error('Destino da saída não definido');
    await this.rpcClient.request(this.node._leaveTarget, '/rpc/files', {
      method: 'PUT',
      body: {
        name,
        content: content.toString('base64'),
        isReplica: false,
        hashId: hashId ?? null,
        replicate: false,
        uploadedBy: metadata.uploadedBy,
        uploadedAt: metadata.uploadedAt,
        history: metadata.history || []
      }
    });
  }

  withPrimaryWriteLock(operation) {
    const next = this.primaryWriteLock.then(() => operation());
    this.primaryWriteLock = next.catch(() => {});
    return next;
  }
}

function fileResult(name, content, node, primary, isReplica) {
  return {
    name,
    hashId: hashKey(name),
    node,
    primary,
    isReplica,
    size: content.length,
    content
  };
}

function notFound(name) {
  const error = new Error(`Arquivo "${name}" não encontrado na rede`);
  error.code = 'ENOENT';
  return error;
}

function nextSequence(history = []) {
  return history.reduce((highest, event) =>
    Math.max(highest, Number(event.sequence) || 0), 0) + 1;
}

module.exports = { FileService };
