'use strict';

const path = require('node:path');
const { FINGER_COUNT, add, hashKey, inInterval, validateId } = require('./ring');
const { FileStore, fileNotFound } = require('./file-store');

const CATALOG_NAME = 'catalogo.txt';
const REPLICA_COUNT = 2;

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
    this.leaving = false;
    this._leavePhase = 'active';
    this._leaveSuccessor = null;
    this._leavePromise = null;
    this._leaveDetached = false;
    this._leaveSuccessorRewired = false;
    this._primaryWriteLock = Promise.resolve();
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
   * Retira voluntariamente este nó do anel. O servidor deve permanecer aberto
   * até esta operação terminar para que fingers antigas ainda possam usá-lo
   * como ponte durante a reparação.
   */
  async leave() {
    if (this._leavePromise) {
      const conflict = new Error(`O nó ${this.id} já está saindo da rede`);
      conflict.code = 'ELEAVEINPROGRESS';
      throw conflict;
    }
    this._leavePromise = this._performLeave();
    try {
      return await this._leavePromise;
    } finally {
      this._leavePromise = null;
    }
  }

  async _performLeave() {
    this.assertJoined();

    const successor = this._leaveSuccessor || this.successor;
    const predecessor = this.predecessor;
    if (!successor || !predecessor) throw new Error('Topologia incompleta para sair da rede');

    if (successor.id === this.id && predecessor.id === this.id) {
      this.joined = false;
      this.leaving = false;
      this._leavePhase = 'left';
      this.predecessor = null;
      this.fingers = this.buildEmptyFingerTable();
      return this.state();
    }

    this.leaving = true;
    this._leavePhase = 'draining';
    this._leaveSuccessor = successor;

    if (!this._leaveDetached) {
      try {
        await this._withPrimaryWriteLock(() => this._transferPrimaryFiles(successor, {
          replicate: false
        }));
      } catch (error) {
        this._resetLeaveState();
        throw error;
      }

      let successorChanged = this._leaveSuccessorRewired;
      try {
        if (!successorChanged) {
          await this.rpc(successor, '/rpc/predecessor', {
            method: 'PUT',
            body: { node: predecessor, expectedId: this.id }
          });
          successorChanged = true;
          this._leaveSuccessorRewired = true;
        }
        await this.rpc(predecessor, '/rpc/successor', {
          method: 'PUT',
          body: { node: successor, expectedId: this.id }
        });
        this._leaveDetached = true;
        this._leaveSuccessorRewired = false;
      } catch (error) {
        if (successorChanged) {
          try {
            await this.rpc(successor, '/rpc/predecessor', {
              method: 'PUT',
              body: { node: this.reference, expectedId: predecessor.id }
            });
            successorChanged = false;
            this._leaveSuccessorRewired = false;
          } catch (rollbackError) {
            error.message += `; tambem falhou o rollback: ${rollbackError.message}`;
          }
        }
        if (!successorChanged) this._resetLeaveState();
        throw error;
      }
    }

    await this.rpc(successor, '/rpc/repair-fingers', {
      method: 'POST',
      body: { originId: successor.id, hops: 0 }
    });

    await this._withPrimaryWriteLock(async () => {
      await this._transferPrimaryFiles(successor);
      this._leavePhase = 'forwarding';
    });

    this.joined = false;
    return this.state();
  }

  _resetLeaveState() {
    this.leaving = false;
    this._leavePhase = 'active';
    this._leaveSuccessor = null;
    this._leaveDetached = false;
    this._leaveSuccessorRewired = false;
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

  /** Recalcula as fingers do anel inteiro e só responde ao concluir a volta. */
  async repairRingFingerTables(originId, hops = 0) {
    const origin = validateId(originId);
    if (hops > 0 && this.id === origin) return { ok: true };
    if (hops >= 32) throw new Error('Limite de nós excedido ao reparar finger tables');

    await this.refreshFingerTable();
    const next = this.successor;
    if (next.id === origin) return { ok: true };
    return this.rpc(next, '/rpc/repair-fingers', {
      method: 'POST',
      body: { originId: origin, hops: hops + 1 }
    });
  }

  async findSuccessor(rawId, hops = 0, skipIds = []) {
    const id = validateId(rawId);
    if ((!this.joined && !this.leaving) || !this.successor) {
      throw new Error('O nó ainda não entrou em uma rede');
    }
    const skip = new Set((skipIds || []).map(Number));

    if (this.successor.id === this.id) return this.reference;
    if (id === this.id) {
      if (this.leaving) return this.successor;
      if (!skip.has(this.id)) return this.reference;
    }

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
    let replicas = [];
    if (owner.id === this.id) {
      await this.storeLocal(name, bytes, { isReplica: false, primaryNodeId: this.id, hashId });
      replicas = await this.replicateFile(name, bytes, hashId);
    } else {
      const stored = await this.rpc(owner, '/rpc/files', {
        method: 'PUT',
        body: { name, content: bytes.toString('base64'), hashId }
      });
      replicas = stored.replicas || (stored.replicaNode ? [stored.replicaNode] : []);
    }

    if (updateCatalog && name !== CATALOG_NAME) await this.addToCatalog(name);
    return {
      name,
      hashId,
      node: owner,
      replicaNode: replicas[0] || null,
      primary: owner,
      replicas,
      locations: [
        { ...owner, role: 'primary' },
        ...replicas.map((replica) => ({ ...replica, role: 'replica' }))
      ],
      size: bytes.length
    };
  }

  /**
   * Recupera um arquivo pelo Chord. Se o dono primário estiver inacessível,
   * busca automaticamente a réplica no sucessor.
   */
  async get(fileName) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const hashId = hashKey(name);
    let owner = null;

    try {
      owner = await this.findSuccessor(hashId);
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

      try {
        const replicaHolder = await this.findReplicaHolder(owner);
        return await this._readFromNode(name, hashId, replicaHolder, 'replica', owner);
      } catch {
        const recovered = await this._getFromKnownCopies(name);
        if (recovered) {
          return this._fileResult(name, hashId, recovered.node, recovered.content, {
            source: recovered.isReplica ? 'replica' : 'primary',
            owner
          });
        }
        throw error;
      }
    }
  }

  async _readFromNode(name, hashId, node, source, owner) {
    if (node.id === this.id) {
      const content = await this.readLocal(name);
      const role = this.store.isPrimary(name) ? 'primary' : 'replica';
      return this._fileResult(name, hashId, this.reference, content, {
        source: source || role,
        owner
      });
    }
    const result = await this.rpc(node, `/rpc/files?name=${encodeURIComponent(name)}`);
    const content = Buffer.from(result.content, 'base64');
    const role = result.source || (result.isReplica ? 'replica' : source || 'primary');
    return this._fileResult(name, hashId, node, content, {
      source: role,
      owner: owner || node
    });
  }

  _fileResult(name, hashId, servedBy, content, { source, owner } = {}) {
    const role = source || 'primary';
    return {
      name,
      hashId,
      node: servedBy,
      owner: owner || servedBy,
      primary: owner || servedBy,
      source: role,
      isReplica: role === 'replica',
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

  async _getFromKnownCopies(name) {
    const queue = [this.reference, this.predecessor, this.successor,
      ...this.fingers.map((finger) => finger.node)].filter(Boolean);
    const visited = new Set();

    while (queue.length && visited.size < 32) {
      const candidate = queue.shift();
      if (!candidate || visited.has(candidate.id)) continue;
      visited.add(candidate.id);

      try {
        if (candidate.id === this.id) {
          const content = await this.readLocal(name);
          const meta = await this.getReplicaMeta(name);
          return { node: this.reference, isReplica: meta.isReplica, content };
        }
        const result = await this.rpc(candidate,
          `/rpc/files?name=${encodeURIComponent(name)}`);
        return {
          node: candidate,
          isReplica: Boolean(result.isReplica),
          content: Buffer.from(result.content, 'base64')
        };
      } catch {
        if (candidate.id !== this.id) {
          try {
            const state = await this.rpc(candidate, '/api/state');
            queue.push(state.predecessor, state.successor,
              ...state.fingerTable.map((finger) => finger.node));
          } catch {
            // Nó indisponível; prossegue pelas demais referências conhecidas.
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

    const write = async () => {
      if (!isReplica && this.leaving && this._leavePhase === 'forwarding') {
        await this._forwardPrimaryFile(name, bytes, hashId);
        return;
      }

      if (isReplica) {
        await this.store.ensureLoaded();
        if (this.store.isPrimary(name)) return;
        await this.store.writeReplica(name, bytes, {
          hashId: key,
          primaryNodeId: primaryNodeId ?? null
        });
      } else {
        await this.store.writePrimary(name, bytes, { hashId: key });
      }

      if (!isReplica && this.leaving && this._leaveSuccessor) {
        await this._forwardPrimaryFile(name, bytes, hashId);
      }
    };

    return this._withPrimaryWriteLock(write);
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

  async getAllPrimaryFiles({ includeCatalog = false } = {}) {
    await this.store.ensureLoaded();
    return this.store.listPrimary()
      .filter((file) => includeCatalog || file.name !== CATALOG_NAME);
  }

  async _transferPrimaryFiles(successor, { replicate = true } = {}) {
    const files = await this.getAllPrimaryFiles({ includeCatalog: true });
    for (const file of files) {
      const content = await this.readLocal(file.name);
      await this.rpc(successor, '/rpc/files', {
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

  async _forwardPrimaryFile(name, content, hashId) {
    if (!this._leaveSuccessor) throw new Error('Sucessor de saída não definido');
    await this.rpc(this._leaveSuccessor, '/rpc/files', {
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

  /**
   * Até REPLICA_COUNT sucessores únicos (em geral o sucessor imediato).
   */
  async getReplicationTargets() {
    const seen = new Set([this.id]);
    const targets = [];
    let next = this.successor;
    while (next && !seen.has(next.id) && targets.length < REPLICA_COUNT) {
      seen.add(next.id);
      targets.push(next);
      if (targets.length >= REPLICA_COUNT) break;
      try {
        const result = await this.rpc(next, '/rpc/successor');
        next = result.node;
      } catch (error) {
        console.error(`[replicação] Não foi possível consultar o sucessor do nó ${next.id}: ${error.message}`);
        break;
      }
    }
    return targets;
  }

  /**
   * Envia a 2ª cópia obrigatória ao sucessor imediato. Falhas são logadas
   * e não impedem que o primário já tenha sido gravado.
   */
  async replicateFile(fileName, content, hashId) {
    const targets = await this.getReplicationTargets();
    const replicas = [];
    for (const target of targets) {
      try {
        const check = await this.rpc(target,
          `/rpc/replica-check?name=${encodeURIComponent(fileName)}`);
        if (check.exists) {
          console.log(`[replicação] "${fileName}" já existe no nó ${target.id}, pulando.`);
          if (check.isReplica !== false) replicas.push(target);
          continue;
        }
        const stored = await this.rpc(target, '/rpc/files', {
          method: 'PUT',
          body: {
            name: fileName,
            content: content.toString('base64'),
            isReplica: true,
            primaryNodeId: this.id,
            hashId
          }
        });
        if (stored.isReplica !== false) replicas.push(target);
        console.log(`[replicação] "${fileName}" replicado com sucesso no nó ${target.id}.`);
      } catch (error) {
        console.error(
          `[replicação] Falha ao replicar "${fileName}" no nó ${target.id}: ${error.message}`);
      }
    }
    return replicas;
  }

  /** Lista todas as cópias confirmadas percorrendo o anel a partir deste nó. */
  async locateFile(fileName) {
    const name = validateFileName(fileName);
    const locations = [];
    const visited = new Set();
    let current = this.reference;

    while (current && !visited.has(current.id) && visited.size < 32) {
      visited.add(current.id);
      try {
        let check;
        if (current.id === this.id) {
          const meta = await this.getReplicaMeta(name);
          check = { exists: true, ...meta };
        } else {
          check = await this.rpc(current,
            `/rpc/replica-check?name=${encodeURIComponent(name)}`);
        }
        if (check.exists) {
          locations.push({ ...current, role: check.isReplica ? 'replica' : 'primary' });
        }
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
      }

      if (current.id === this.id) {
        current = this.successor;
      } else {
        const result = await this.rpc(current, '/rpc/successor');
        current = result.node;
      }
    }
    return { name, hashId: hashKey(name), locations };
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

  _withPrimaryWriteLock(fn) {
    const next = this._primaryWriteLock.then(() => fn());
    this._primaryWriteLock = next.catch(() => {});
    return next;
  }

  assertJoined() {
    if (!this.joined && !this.leaving) throw new Error('O nó ainda não entrou em uma rede');
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
