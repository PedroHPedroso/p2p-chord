'use strict';

const { hashKey } = require('../ring');
const { CATALOG_NAME, MAX_HOPS } = require('../chord-config');
const { validateFileName } = require('../validation');

class FileService {
  constructor({ node, rpcClient, repository, routingService, replicationService }) {
    this.node = node;
    this.rpcClient = rpcClient;
    this.repository = repository;
    this.routingService = routingService;
    this.replicationService = replicationService;
    this.primaryWriteLock = Promise.resolve();
  }

  async put(fileName, content, { updateCatalog = true } = {}) {
    this.node.assertJoined();
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const hashId = hashKey(name);
    const owner = await this.routingService.findSuccessor(hashId);

    let replicas = [];
    if (owner.id === this.node.id) {
      await this.storeLocal(name, bytes, {
        isReplica: false,
        primaryNodeId: this.node.id,
        hashId
      });
      replicas = await this.replicationService().replicate(name, bytes, hashId);
    } else {
      const stored = await this.rpcClient.request(owner, '/rpc/files', {
        method: 'PUT',
        body: { name, content: bytes.toString('base64'), hashId }
      });
      replicas = stored.replicas || [];
    }

    if (updateCatalog && name !== CATALOG_NAME) await this.addToCatalog(name);
    return {
      name,
      hashId,
      node: owner,
      primary: owner,
      replicas,
      locations: [
        { ...owner, role: 'primary' },
        ...replicas.map((replica) => ({ ...replica, role: 'replica' }))
      ],
      size: bytes.length
    };
  }

  async get(fileName) {
    this.node.assertJoined();
    const name = validateFileName(fileName);
    const hashId = hashKey(name);
    let owner = null;
    try {
      owner = await this.routingService.findSuccessor(hashId);
      if (owner.id === this.node.id) {
        const content = await this.repository.read(name);
        const metadata = await this.repository.getMetadata(name);
        return {
          name,
          hashId,
          node: this.node.reference,
          primary: owner,
          isReplica: metadata.isReplica,
          size: content.length,
          content
        };
      }
      const result = await this.rpcClient.request(
        owner, `/rpc/files?name=${encodeURIComponent(name)}`);
      const content = Buffer.from(result.content, 'base64');
      return {
        name,
        hashId,
        node: owner,
        primary: owner,
        isReplica: Boolean(result.isReplica),
        size: content.length,
        content
      };
    } catch (error) {
      const recovered = await this.getFromKnownCopies(name);
      if (recovered) {
        return {
          name,
          hashId,
          node: recovered.node,
          primary: owner,
          isReplica: recovered.isReplica,
          size: recovered.content.length,
          content: recovered.content
        };
      }
      throw error;
    }
  }

  async getFromKnownCopies(name) {
    const queue = [this.node.reference, this.node.predecessor, this.node.successor,
      ...this.node.fingers.map((finger) => finger.node)].filter(Boolean);
    const visited = new Set();

    while (queue.length && visited.size < MAX_HOPS) {
      const candidate = queue.shift();
      if (!candidate || visited.has(candidate.id)) continue;
      visited.add(candidate.id);
      try {
        if (candidate.id === this.node.id) {
          const content = await this.repository.read(name);
          const metadata = await this.repository.getMetadata(name);
          return { node: this.node.reference, isReplica: metadata.isReplica, content };
        }
        const result = await this.rpcClient.request(
          candidate, `/rpc/files?name=${encodeURIComponent(name)}`);
        return {
          node: candidate,
          isReplica: Boolean(result.isReplica),
          content: Buffer.from(result.content, 'base64')
        };
      } catch {
        if (candidate.id !== this.node.id) {
          try {
            const state = await this.rpcClient.request(candidate, '/api/state');
            queue.push(state.predecessor, state.successor,
              ...state.fingerTable.map((finger) => finger.node));
          } catch {
            // Indisponível; continua pelas referências restantes.
          }
        }
      }
    }
    return null;
  }

  async addToCatalog(fileName) {
    let names = [];
    try {
      const catalog = await this.get(CATALOG_NAME);
      names = catalog.content.toString('utf8').split(/\r?\n/).filter(Boolean);
    } catch (error) {
      if (error.code !== 'ENOENT' && !/não encontrado/i.test(error.message)) throw error;
    }
    if (!names.includes(fileName)) names.push(fileName);
    names.sort((left, right) => left.localeCompare(right, 'pt-BR'));
    await this.put(CATALOG_NAME, Buffer.from(`${names.join('\n')}\n`), {
      updateCatalog: false
    });
  }

  async storeLocal(fileName, content, options = {}) {
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const { isReplica = false, hashId } = options;
    return this.withPrimaryWriteLock(async () => {
      if (!isReplica && this.node.leaving && this.node._leavePhase === 'forwarding') {
        await this.forwardPrimaryFile(name, bytes, hashId);
        return;
      }
      const stored = await this.repository.store(name, bytes, options);
      if (stored && !isReplica && this.node.leaving && this.node._leaveSuccessor) {
        await this.forwardPrimaryFile(name, bytes, hashId);
      }
    });
  }

  async transferPrimaryFiles(successor, { replicate = true } = {}) {
    const files = await this.repository.listPrimaryFiles({ includeCatalog: true });
    for (const file of files) {
      const content = await this.repository.read(file.name);
      await this.rpcClient.request(successor, '/rpc/files', {
        method: 'PUT',
        body: {
          name: file.name,
          content: content.toString('base64'),
          isReplica: false,
          hashId: file.hashId,
          replicate
        }
      });
    }
  }

  async forwardPrimaryFile(name, content, hashId) {
    if (!this.node._leaveSuccessor) throw new Error('Sucessor de saída não definido');
    await this.rpcClient.request(this.node._leaveSuccessor, '/rpc/files', {
      method: 'PUT',
      body: {
        name,
        content: content.toString('base64'),
        isReplica: false,
        hashId: hashId ?? null,
        replicate: false
      }
    });
  }

  withPrimaryWriteLock(operation) {
    const next = this.primaryWriteLock.then(() => operation());
    this.primaryWriteLock = next.catch(() => {});
    return next;
  }
}

module.exports = { FileService };