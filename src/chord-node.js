'use strict';

const path = require('node:path');
const { FINGER_COUNT, add, hashKey, inInterval, validateId } = require('./ring');
const { FileStore, fileNotFound } = require('./file-store');

const CATALOG_NAME = 'catalogo.txt';
const REPLICA_COUNT = 1;

class ChordNode {
  constructor({ id, host = '127.0.0.1', port = 5000, requestTimeout = 10000,
    storageDirectory } = {}) {
    this.id = validateId(id);
    this.host = String(host || '').trim();
    if (!this.host || this.host === '0.0.0.0' || this.host === '::') {
      throw new Error('Informe o IP ou hostname pelo qual os outros nós acessam esta máquina');
    }
    this.port = Number(port);
    if (!Number.isInteger(this.port) || this.port < 1 || this.port > 65535) {
      throw new Error('A porta deve ser um inteiro entre 1 e 65535');
    }
    this.requestTimeout = requestTimeout;
    this.storageDirectory = storageDirectory || path.join(
      process.cwd(), 'data', `node-${this.id}-${this.port}`);
    this.store = new FileStore(this.storageDirectory);
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();
    this.joined = false;
  }

  get reference() {
    return { id: this.id, host: this.host, port: this.port };
  }

  get primaryFiles() {
    return this.store.primaryFiles;
  }

  get replicaFiles() {
    return this.store.replicaFiles;
  }

  buildEmptyFingerTable() {
    return Array.from({ length: FINGER_COUNT }, (_, index) => ({
      index: index + 1,
      start: add(this.id, 2 ** index),
      node: null
    }));
  }

  get successor() {
    return this.fingers[0].node;
  }

  set successor(node) {
    this.fingers[0].node = node;
  }

  createRing() {
    this.predecessor = this.reference;
    for (const finger of this.fingers) finger.node = this.reference;
    this.joined = true;
  }

  async join(bootstrap) {
    if (this.joined) throw new Error('Este nó já pertence a uma rede Chord');

    // Limpa resíduos de estado de um join anterior para permitir reconexão limpa.
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();

    if (!bootstrap) {
      this.createRing();
      return this.state();
    }

    const contact = normalizeReference(bootstrap);
    if (contact.id === this.id) throw new Error('O nó de entrada não pode ter o mesmo id');

    const successor = await this.rpc(contact, '/rpc/find-successor', {
      method: 'POST',
      body: { id: this.id }
    });
    if (successor.id === this.id) throw new Error(`O id ${this.id} já está em uso`);

    const predecessorResult = await this.rpc(successor, '/rpc/predecessor');
    const predecessor = predecessorResult.node || successor;

    this.successor = successor;
    this.predecessor = predecessor;

    await this.rpc(successor, '/rpc/predecessor', {
      method: 'PUT',
      body: { node: this.reference }
    });
    if (predecessor.id !== successor.id) {
      await this.rpc(predecessor, '/rpc/successor', {
        method: 'PUT',
        body: { node: this.reference }
      });
    } else {
      await this.rpc(successor, '/rpc/successor', {
        method: 'PUT',
        body: { node: this.reference }
      });
    }

    this.joined = true;

    await this.refreshFingerTable();

    await this.rpc(this.successor, '/rpc/refresh-fingers', {
      method: 'POST',
      body: { originId: this.id, hops: 0 }
    });
    return this.state();
  }

  /**
   * Saída graciosa do anel Chord:
   * 1. Transfere todos os arquivos primários para o sucessor (como primários).
   * 2. Ajusta os ponteiros do predecessor e do sucessor para se conectarem diretamente.
   * 3. Dispara a atualização das finger tables no restante do anel.
   * 4. Reseta o estado local para permitir um futuro join() limpo.
   */
  async leave() {
    if (!this.joined) return;

    const successor = this.successor;
    const predecessor = this.predecessor;
    const isSoleNode = !successor || successor.id === this.id;

    // ── 1. Handoff de arquivos primários ────────────────────────────────────
    if (!isSoleNode) {
      await this.store.ensureLoaded();
      const primaryFiles = this.store.listPrimary();
      for (const file of primaryFiles) {
        let content;
        try {
          content = await this.readLocal(file.name);
        } catch (error) {
          console.error(`[leave] Não foi possível ler "${file.name}": ${error.message}`);
          continue;
        }
        try {
          await this.rpc(successor, '/rpc/files', {
            method: 'PUT',
            body: {
              name: file.name,
              content: content.toString('base64'),
              isReplica: false,
              primaryNodeId: successor.id,
              hashId: file.hashId ?? null
            }
          });
          console.log(`[leave] "${file.name}" transferido para o nó ${successor.id}.`);
        } catch (error) {
          console.error(`[leave] Falha ao transferir "${file.name}" para o nó ${successor.id}: ${error.message}`);
        }
      }
    }

    // ── 2. Reajuste de ponteiros do anel ────────────────────────────────────
    if (!isSoleNode && predecessor && successor) {
      // Predecessor deve apontar seu sucessor para o nosso sucessor.
      try {
        await this.rpc(predecessor, '/rpc/successor', {
          method: 'PUT',
          body: { node: successor }
        });
      } catch (error) {
        console.error(`[leave] Falha ao atualizar sucessor do predecessor (nó ${predecessor.id}): ${error.message}`);
      }
      // Sucessor deve apontar seu predecessor para o nosso predecessor.
      try {
        await this.rpc(successor, '/rpc/predecessor', {
          method: 'PUT',
          body: { node: predecessor }
        });
      } catch (error) {
        console.error(`[leave] Falha ao atualizar predecessor do sucessor (nó ${successor.id}): ${error.message}`);
      }

      // ── 3. Propaga atualização de finger tables no anel ─────────────────
      try {
        await this.rpc(successor, '/rpc/refresh-fingers', {
          method: 'POST',
          body: { originId: successor.id, hops: 0 }
        });
      } catch (error) {
        console.error(`[leave] Falha ao propagar refresh de fingers: ${error.message}`);
      }
    }

    // ── 4. Reset do estado local ─────────────────────────────────────────────
    this.joined = false;
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();
  }

  async refreshFingerTable() {
    const nodes = await Promise.all(this.fingers.map((finger) =>
      this.findSuccessor(finger.start)));
    this.fingers.forEach((finger, index) => {
      finger.node = nodes[index];
    });
    setImmediate(() => {
      this.verifyReplicas().catch((error) => {
        console.error(`[replicação] Erro em verifyReplicas após refreshFingerTable: ${error.message}`);
      });
    });
  }

  async refreshRingFingerTables(originId, hops = 0) {
    validateId(originId);
    if (this.id === Number(originId)) return { ok: true };
    if (hops >= 32) throw new Error('Limite de nós excedido ao atualizar finger tables');

    await this.refreshFingerTable();

    const next = this.successor;
    setImmediate(() => {
      this.rpc(next, '/rpc/refresh-fingers', {
        method: 'POST',
        body: { originId: Number(originId), hops: hops + 1 }
      }).catch((error) => {
        console.error(`Não foi possível atualizar as fingers após o nó ${this.id}: ${error.message}`);
      });
    });
    return { ok: true };
  }

  async findSuccessor(rawId, hops = 0, skipIds = []) {
    const id = validateId(rawId);
    if (!this.joined || !this.successor) throw new Error('O nó ainda não entrou em uma rede');
    const skip = new Set((skipIds || []).map(Number));

    if (this.successor.id === this.id) return this.reference;
    if (id === this.id && !skip.has(this.id)) return this.reference;

    const actingSuccessor = this.firstKnownSuccessor(skip) || this.reference;
    if (actingSuccessor.id === this.id) return this.reference;

    if (inInterval(id, this.id, actingSuccessor.id, false, true)) {
      return actingSuccessor;
    }

    if (hops >= 32) throw new Error('Limite de saltos excedido ao procurar sucessor');
    let next = this.closestPrecedingFinger(id, skip);
    if (next.id === this.id || skip.has(next.id)) next = actingSuccessor;

    try {
      return await this.rpc(next, '/rpc/find-successor', {
        method: 'POST',
        body: { id, hops: hops + 1, skipIds: [...skip] }
      });
    } catch (error) {
      if (!isUnreachableError(error)) throw error;
      skip.add(next.id);
      return this.findSuccessor(id, hops, [...skip]);
    }
  }

  closestPrecedingFinger(id, skip = new Set()) {
    for (let i = this.fingers.length - 1; i >= 0; i -= 1) {
      const candidate = this.fingers[i].node;
      if (candidate && candidate.id !== this.id
        && !skip.has(candidate.id)
        && inInterval(candidate.id, this.id, id, false, false)) {
        return candidate;
      }
    }
    return this.reference;
  }

  /**
   * Sucessores únicos conhecidos pela finger table, em ordem crescente de distância.
   * O primeiro elemento é o sucessor imediato.
   */
  getSuccessorList() {
    const seen = new Set([this.id]);
    const successors = [];
    for (const finger of this.fingers) {
      const node = finger.node;
      if (node && !seen.has(node.id)) {
        seen.add(node.id);
        successors.push(node);
      }
    }
    return successors;
  }

  firstKnownSuccessor(skip = new Set()) {
    return this.getSuccessorList().find((node) => !skip.has(node.id)) || null;
  }

  /** Insere bytes na rede, grava no dono primário e replica no sucessor imediato. */
  async put(fileName, content, { updateCatalog = true } = {}) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const hashId = hashKey(name);
    const owner = await this.findSuccessor(hashId);
    let replicaNode = null;

    if (owner.id === this.id) {
      await this.storeLocal(name, bytes, { isReplica: false, primaryNodeId: this.id, hashId });
      replicaNode = await this.replicateFile(name, bytes, hashId);
    } else {
      const result = await this.rpc(owner, '/rpc/files', {
        method: 'PUT',
        body: { name, content: bytes.toString('base64'), hashId }
      });
      replicaNode = result.replicaNode || null;
    }

    if (updateCatalog && name !== CATALOG_NAME) await this.addToCatalog(name);
    return { name, hashId, node: owner, replicaNode, size: bytes.length };
  }

  /**
   * Recupera um arquivo pelo Chord. Se o dono primário estiver inacessível,
   * busca automaticamente a réplica no sucessor.
   */
  async get(fileName) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const hashId = hashKey(name);
    const owner = await this.findSuccessor(hashId);

    try {
      return await this._readFromNode(name, hashId, owner, 'primary', owner);
    } catch (error) {
      if (!shouldFallbackToReplica(error)) throw error;

      if (await this.store.has(name)) {
        const content = await this.store.read(name);
        return this._fileResult(name, hashId, this.reference, content, {
          source: this.store.isPrimary(name) ? 'primary' : 'replica',
          owner
        });
      }

      const replicaHolder = await this.findReplicaHolder(owner);
      return this._readFromNode(name, hashId, replicaHolder, 'replica', owner);
    }
  }

  async _readFromNode(name, hashId, node, source, owner) {
    if (node.id === this.id) {
      const content = await this.readLocal(name);
      return this._fileResult(name, hashId, this.reference, content, { source, owner });
    }
    const result = await this.rpc(node, `/rpc/files?name=${encodeURIComponent(name)}`);
    const content = Buffer.from(result.content, 'base64');
    return this._fileResult(name, hashId, node, content, {
      source: result.source || source,
      owner: owner || node
    });
  }

  _fileResult(name, hashId, servedBy, content, { source, owner } = {}) {
    return {
      name,
      hashId,
      node: servedBy,
      owner: owner || servedBy,
      source: source || 'primary',
      size: content.length,
      content
    };
  }

  async findReplicaHolder(owner) {
    if (!owner || owner.id === this.id) {
      return this.firstKnownSuccessor(new Set()) || this.reference;
    }
    try {
      const result = await this.rpc(owner, '/rpc/successor', { timeout: 3000 });
      if (result.node) return normalizeReference(result.node);
    } catch (error) {
      if (!isUnreachableError(error)) throw error;
    }
    return this.findSuccessor(add(owner.id, 1), 0, [owner.id]);
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
    names.sort((a, b) => a.localeCompare(b, 'pt-BR'));
    await this.put(CATALOG_NAME, Buffer.from(`${names.join('\n')}\n`), {
      updateCatalog: false
    });
  }

  /**
   * Grava bytes no diretório local do nó, separando primários e réplicas.
   */
  async storeLocal(fileName, content, { isReplica = false, primaryNodeId, hashId } = {}) {
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const key = hashId ?? hashKey(name);
    if (isReplica) {
      await this.store.writeReplica(name, bytes, {
        hashId: key,
        primaryNodeId: primaryNodeId ?? null
      });
    } else {
      await this.store.writePrimary(name, bytes, { hashId: key });
    }
  }

  async readLocal(fileName) {
    const name = validateFileName(fileName);
    return this.store.read(name);
  }

  async getReplicaMeta(fileName) {
    const name = validateFileName(fileName);
    await this.store.ensureLoaded();
    const meta = this.store.getMeta(name);
    if (!meta) throw fileNotFound(name);
    return {
      hashId: meta.hashId ?? null,
      primaryNodeId: meta.primaryNodeId ?? (this.store.isPrimary(name) ? this.id : null),
      isReplica: this.store.isReplica(name)
    };
  }

  async getAllPrimaryFiles() {
    await this.store.ensureLoaded();
    return this.store.listPrimary();
  }

  /**
   * Até REPLICA_COUNT sucessores únicos (em geral o sucessor imediato).
   */
  getReplicationTargets() {
    return this.getSuccessorList().slice(0, REPLICA_COUNT);
  }

  /**
   * Envia a 2ª cópia obrigatória ao sucessor imediato. Falhas são logadas
   * e não impedem que o primário já tenha sido gravado.
   */
  async replicateFile(fileName, content, hashId) {
    const targets = this.getReplicationTargets();
    if (targets.length === 0) return null;

    let replicaNode = null;
    for (const target of targets) {
      try {
        const check = await this.rpc(target,
          `/rpc/replica-check?name=${encodeURIComponent(fileName)}`);
        if (check.exists) {
          console.log(`[replicação] "${fileName}" já existe no nó ${target.id}, pulando.`);
          replicaNode = target;
          continue;
        }
        await this.rpc(target, '/rpc/files', {
          method: 'PUT',
          body: {
            name: fileName,
            content: content.toString('base64'),
            isReplica: true,
            primaryNodeId: this.id,
            hashId
          }
        });
        console.log(`[replicação] "${fileName}" replicado com sucesso no nó ${target.id}.`);
        replicaNode = target;
      } catch (error) {
        console.error(
          `[replicação] Falha ao replicar "${fileName}" no nó ${target.id}: ${error.message}`);
      }
    }
    return replicaNode;
  }

  async verifyReplicas() {
    if (!this.joined) return;
    let primaryFiles;
    try {
      primaryFiles = await this.getAllPrimaryFiles();
    } catch (error) {
      console.error(`[replicação] Não foi possível ler metadados: ${error.message}`);
      return;
    }
    for (const file of primaryFiles) {
      let content;
      try {
        content = await this.readLocal(file.name);
      } catch {
        continue;
      }
      await this.replicateFile(file.name, content, file.hashId);
    }
  }

  assertJoined() {
    if (!this.joined) throw new Error('O nó ainda não entrou em uma rede');
  }

  async rpc(node, path, { method = 'GET', body, timeout } = {}) {
    const target = normalizeReference(node);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout ?? this.requestTimeout);
    try {
      const response = await fetch(`http://${target.host}:${target.port}${path}`, {
        method,
        headers: body ? { 'content-type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal
      });
      const data = await response.json();
      if (!response.ok) {
        const error = new Error(data.error || `Erro HTTP ${response.status}`);
        error.code = data.code;
        error.status = response.status;
        throw error;
      }
      return data;
    } catch (error) {
      if (error.name === 'AbortError') {
        const timeoutError = new Error(
          `Tempo limite ao acessar o nó ${target.id} em ${target.host}:${target.port}`);
        timeoutError.code = 'ETIMEDOUT';
        throw timeoutError;
      }
      if (isUnreachableError(error) && error.code !== 'ETIMEDOUT') {
        const unreachable = new Error(
          `Nó ${target.id} inacessível em ${target.host}:${target.port}`);
        unreachable.code = 'ECONNREFUSED';
        unreachable.cause = error;
        throw unreachable;
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  state() {
    return {
      node: this.reference,
      joined: this.joined,
      predecessor: this.predecessor,
      successor: this.successor,
      successors: this.getSuccessorList(),
      fingerTable: this.fingers,
      primaryFiles: this.store.listPrimary(),
      replicaFiles: this.store.listReplica()
    };
  }

  async nodeStatus() {
    await this.store.ensureLoaded();
    return {
      id: this.id,
      host: this.host,
      port: this.port,
      joined: this.joined,
      predecessor: this.predecessor,
      successor: this.successor,
      successors: this.getSuccessorList(),
      primaryFiles: this.store.listPrimary().map((file) => ({
        name: file.name,
        hashId: file.hashId,
        size: file.size
      })),
      replicaFiles: this.store.listReplica().map((file) => ({
        name: file.name,
        hashId: file.hashId,
        size: file.size,
        primaryNodeId: file.primaryNodeId ?? null
      }))
    };
  }
}

function shouldFallbackToReplica(error) {
  return isUnreachableError(error)
    || error.code === 'ENOENT'
    || error.status === 404
    || /não encontrado/i.test(error.message || '');
}

function isUnreachableError(error) {
  if (!error) return false;
  if (error.code === 'ETIMEDOUT' || error.code === 'ECONNREFUSED' || error.code === 'ECONNRESET') {
    return true;
  }
  if (error.name === 'AbortError') return true;
  if (error.name === 'TypeError' && /fetch failed/i.test(String(error.message || ''))) return true;
  const message = String(error.message || '');
  if (/Tempo limite|inacessível|fetch failed|ECONNREFUSED|ECONNRESET|network/i.test(message)) {
    return true;
  }
  return isUnreachableError(error.cause);
}

function validateFileName(fileName) {
  if (typeof fileName !== 'string' || !fileName.trim()) {
    throw new Error('O nome do arquivo é obrigatório');
  }
  const name = fileName.trim();
  if (name === '.' || name === '..' || path.basename(name) !== name
    || name.includes('/') || name.includes('\\') || name.includes('\0')) {
    throw new Error('Nome de arquivo inválido');
  }
  return name;
}

function normalizeReference(node) {
  if (!node || typeof node !== 'object') throw new Error('Referência de nó inválida');
  return {
    id: validateId(node.id),
    host: String(node.host || '127.0.0.1'),
    port: Number(node.port || 5000)
  };
}

module.exports = {
  ChordNode,
  normalizeReference,
  validateFileName,
  CATALOG_NAME,
  REPLICA_COUNT
};
