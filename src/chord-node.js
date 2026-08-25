'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');
const { FINGER_COUNT, add, hashKey, inInterval, validateId } = require('./ring');

const CATALOG_NAME = 'catalogo.txt';
const REPLICA_META_NAME = 'replicas.json'; // arquivo de metadados de replicação (reservado)
const REPLICA_COUNT = 2; // número de sucessores diretos que receberão réplicas

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
    // Mutex via cadeia de Promises para serializar leituras/escritas do replicas.json.
    this._replicaMetaLock = Promise.resolve();
  }

  get reference() {
    return { id: this.id, host: this.host, port: this.port };
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
    if (!bootstrap) {
      this.createRing();
      return this.state();
    }

    const contact = normalizeReference(bootstrap);
    if (contact.id === this.id) throw new Error('O nó de entrada não pode ter o mesmo id');

    // Localiza a posição do novo nó no anel usando o nó de entrada.
    const successor = await this.rpc(contact, '/rpc/find-successor', {
      method: 'POST',
      body: { id: this.id }
    });
    if (successor.id === this.id) throw new Error(`O id ${this.id} já está em uso`);

    const predecessorResult = await this.rpc(successor, '/rpc/predecessor');
    const predecessor = predecessorResult.node || successor;

    this.successor = successor;
    this.predecessor = predecessor;

    // Faz o novo nó entrar entre predecessor e sucessor.
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
      // A rede possuía apenas um nó.
      await this.rpc(successor, '/rpc/successor', {
        method: 'PUT',
        body: { node: this.reference }
      });
    }

    this.joined = true;

    await this.refreshFingerTable();

    // A entrada altera também as fingers dos nós que já estavam no anel.
    await this.rpc(this.successor, '/rpc/refresh-fingers', {
      method: 'POST',
      body: { originId: this.id, hops: 0 }
    });
    return this.state();
  }

  /**
   * Retira voluntariamente este no do anel. O servidor deve permanecer aberto
   * ate esta operacao terminar para que fingers antigas ainda possam usa-lo
   * como ponte durante a reparacao.
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
    // Após atualizar a Finger Table, verificar e sincronizar réplicas em background.
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

    // Cada nó responde após atualizar a própria tabela. O próximo salto ocorre
    // fora da requisição atual para o tempo total não crescer com o anel.
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

  /** Recalcula as fingers do anel inteiro e so responde ao concluir a volta. */
  async repairRingFingerTables(originId, hops = 0) {
    const origin = validateId(originId);
    if (hops > 0 && this.id === origin) return { ok: true };
    if (hops >= 32) throw new Error('Limite de nos excedido ao reparar finger tables');

    await this.refreshFingerTable();
    const next = this.successor;
    if (next.id === origin) return { ok: true };
    return this.rpc(next, '/rpc/repair-fingers', {
      method: 'POST',
      body: { originId: origin, hops: hops + 1 }
    });
  }

  async findSuccessor(rawId, hops = 0) {
    const id = validateId(rawId);
    if ((!this.joined && !this.leaving) || !this.successor) {
      throw new Error('O nó ainda não entrou em uma rede');
    }
    if (this.successor.id === this.id) return this.reference;
    if (id === this.id) return this.leaving ? this.successor : this.reference;

    if (inInterval(id, this.id, this.successor.id, false, true)) {
      return this.successor;
    }

    if (hops >= 32) throw new Error('Limite de saltos excedido ao procurar sucessor');
    let next = this.closestPrecedingFinger(id);
    // Uma finger table ainda desatualizada não deve interromper a busca:
    // caminhar pelo sucessor sempre encontra a posição correta no anel.
    if (next.id === this.id) next = this.successor;

    return this.rpc(next, '/rpc/find-successor', {
      method: 'POST',
      body: { id, hops: hops + 1 }
    });
  }

  closestPrecedingFinger(id) {
    for (let i = this.fingers.length - 1; i >= 0; i -= 1) {
      const candidate = this.fingers[i].node;
      if (candidate && candidate.id !== this.id
        && inInterval(candidate.id, this.id, id, false, false)) {
        return candidate;
      }
    }
    return this.reference;
  }

  /** Insere bytes na rede e devolve a posição do hash e o nó responsável. */
  async put(fileName, content, { updateCatalog = true } = {}) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const hashId = hashKey(name);
    const owner = await this.findSuccessor(hashId);

    let replicas = [];
    if (owner.id === this.id) {
      await this.storeLocal(name, bytes, { isReplica: false, primaryNodeId: this.id, hashId });
      // O upload só é confirmado depois das cópias: assim o cliente recebe
      // exatamente os nós que efetivamente armazenaram o arquivo.
      replicas = await this.replicateFile(name, bytes, hashId);
    } else {
      const stored = await this.rpc(owner, '/rpc/files', {
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

  /** Busca os bytes de um arquivo a partir de qualquer nó da rede. */
  async get(fileName) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const hashId = hashKey(name);
    let owner = null;
    try {
      owner = await this.findSuccessor(hashId);
      if (owner.id === this.id) {
        const content = await this.readLocal(name);
        const meta = await this.getReplicaMeta(name);
        return { name, hashId, node: this.reference, primary: owner,
          isReplica: meta.isReplica,
          size: content.length, content };
      }
      const result = await this.rpc(owner, `/rpc/files?name=${encodeURIComponent(name)}`);
      const content = Buffer.from(result.content, 'base64');
      return { name, hashId, node: owner, primary: owner,
        isReplica: Boolean(result.isReplica), size: content.length, content };
    } catch (error) {
      // O primário pode ter saído ou estar temporariamente inacessível. As
      // referências conhecidas permitem localizar uma das cópias sobreviventes.
      const recovered = await this._getFromKnownCopies(name);
      if (recovered) {
        return { name, hashId, node: recovered.node, primary: owner,
          isReplica: recovered.isReplica, size: recovered.content.length,
          content: recovered.content };
      }
      throw error;
    }
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
        // Mesmo sem o arquivo, um nó acessível pode revelar outras rotas.
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
   * Grava bytes no diretório local do nó e registra metadados de replicação.
   *
   * @param {string} fileName - Nome do arquivo.
   * @param {Buffer} content  - Conteúdo do arquivo.
   * @param {{ isReplica?: boolean, primaryNodeId?: number|null, hashId?: number|null }} [opts]
   *   `isReplica`: true indica que este é uma cópia do arquivo original.
   *   `primaryNodeId`: id do nó que detém o arquivo primário.
   *   `hashId`: hash/chave do arquivo no anel Chord.
   */
  async storeLocal(fileName, content, { isReplica = false, primaryNodeId, hashId } = {}) {
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const write = async () => {
      if (!isReplica && this.leaving && this._leavePhase === 'forwarding') {
        await this._forwardPrimaryFile(name, bytes, hashId);
        return;
      }

      // Uma réplica atrasada nunca pode rebaixar nem sobrescrever um primário
      // que já foi armazenado neste nó.
      if (isReplica) {
        const currentMeta = await this._readReplicaMeta();
        if (currentMeta[name] && !currentMeta[name].isReplica) return;
      }

      await fs.mkdir(this.storageDirectory, { recursive: true });
      await fs.writeFile(path.join(this.storageDirectory, name), bytes);
      // Registrar metadados; ignorar o próprio arquivo de metadados para evitar recursão.
      if (name !== REPLICA_META_NAME) {
        await this._withReplicaLock(async () => {
          const meta = await this._readReplicaMeta();
          meta[name] = {
            hashId: hashId ?? null,
            primaryNodeId: isReplica ? (primaryNodeId ?? null) : this.id,
            isReplica: Boolean(isReplica)
          };
          await this._writeReplicaMeta(meta);
        });
      }

      if (!isReplica && this.leaving && this._leaveSuccessor) {
        await this._forwardPrimaryFile(name, bytes, hashId);
      }
    };

    return this._withPrimaryWriteLock(write);
  }

  async readLocal(fileName) {
    const name = validateFileName(fileName);
    try {
      return await fs.readFile(path.join(this.storageDirectory, name));
    } catch (error) {
      if (error.code === 'ENOENT') {
        const notFound = new Error(`Arquivo "${name}" não encontrado na rede`);
        notFound.code = 'ENOENT';
        throw notFound;
      }
      throw error;
    }
  }

  // ─── Replicação ─────────────────────────────────────────────────────────────

  /**
   * Retorna os metadados de replicação de um arquivo armazenado localmente.
   * Lança erro com code 'ENOENT' se o arquivo não estiver registrado em replicas.json.
   *
   * @param {string} fileName
   * @returns {Promise<{ hashId: number|null, primaryNodeId: number|null, isReplica: boolean }>}
   */
  async getReplicaMeta(fileName) {
    const name = validateFileName(fileName);
    const meta = await this._readReplicaMeta();
    if (!meta[name]) {
      const notFound = new Error(`Arquivo "${name}" não encontrado na rede`);
      notFound.code = 'ENOENT';
      throw notFound;
    }
    return meta[name];
  }

  /**
   * Retorna todos os arquivos primários (não réplicas) armazenados localmente,
   * excluindo o catálogo e o próprio arquivo de metadados.
   *
   * @returns {Promise<Array<{ name: string, hashId: number|null }>>}
   */
  async getAllPrimaryFiles({ includeCatalog = false } = {}) {
    const meta = await this._readReplicaMeta();
    return Object.entries(meta)
      .filter(([name, info]) => !info.isReplica && (includeCatalog || name !== CATALOG_NAME))
      .map(([name, info]) => ({ name, hashId: info.hashId }));
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
   * Retorna até REPLICA_COUNT nós únicos da Finger Table que não sejam o próprio nó,
   * em ordem crescente de distância (finger[0] = sucessor imediato).
   *
   * @returns {Array<{ id: number, host: string, port: number }>}
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
   * Garante que `fileName` está replicado nos nós alvo.
   * Para cada nó, consulta `/rpc/replica-check` antes de transferir; só envia se ausente.
   * Falhas individuais por nó são capturadas e logadas sem interromper os demais.
   *
   * @param {string} fileName
   * @param {Buffer} content
   * @param {number|null} hashId
   */
  async replicateFile(fileName, content, hashId) {
    const targets = await this.getReplicationTargets();
    const replicas = [];
    for (const target of targets) {
      try {
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

  /**
   * Percorre todos os arquivos primários locais e garante que suas réplicas
   * existam nos nós alvo atuais da Finger Table.
   * Chamado automaticamente via setImmediate após cada atualização da Finger Table.
   */
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
        // Arquivo pode ter sido removido do disco; ignora silenciosamente.
        continue;
      }
      await this.replicateFile(file.name, content, file.hashId);
    }
  }

  // ─── Infraestrutura de metadados de réplica ──────────────────────────────

  /**
   * Mutex via cadeia de Promises: serializa todas as leituras/escritas do replicas.json,
   * evitando condições de corrida em operações concorrentes de armazenamento.
   */
  _withReplicaLock(fn) {
    const next = this._replicaMetaLock.then(() => fn());
    // Prevenir que rejeições de `fn` quebrem a cadeia para chamadas futuras.
    this._replicaMetaLock = next.catch(() => {});
    return next;
  }

  _withPrimaryWriteLock(fn) {
    const next = this._primaryWriteLock.then(() => fn());
    this._primaryWriteLock = next.catch(() => {});
    return next;
  }

  async _readReplicaMeta() {
    const metaPath = path.join(this.storageDirectory, REPLICA_META_NAME);
    try {
      const data = await fs.readFile(metaPath, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      if (error.code === 'ENOENT') return {};
      throw error;
    }
  }

  async _writeReplicaMeta(meta) {
    await fs.mkdir(this.storageDirectory, { recursive: true });
    const metaPath = path.join(this.storageDirectory, REPLICA_META_NAME);
    await fs.writeFile(metaPath, JSON.stringify(meta, null, 2));
  }

  // ─── Utilitários ─────────────────────────────────────────────────────────

  assertJoined() {
    if (!this.joined && !this.leaving) throw new Error('O nó ainda não entrou em uma rede');
  }

  async rpc(node, path, { method = 'GET', body } = {}) {
    const target = normalizeReference(node);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.requestTimeout);
    try {
      const response = await fetch(`http://${target.host}:${target.port}${path}`, {
        method,
        headers: body ? { 'content-type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal
      });
      const data = await response.json();
      if (!response.ok) {
        const requestError = new Error(data.error || `Erro HTTP ${response.status}`);
        requestError.status = response.status;
        throw requestError;
      }
      return data;
    } catch (error) {
      if (error.name === 'AbortError') {
        const timeout = new Error(
          `Tempo limite ao acessar o nó ${target.id} em ${target.host}:${target.port}`);
        timeout.code = 'ETIMEDOUT';
        throw timeout;
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
      fingerTable: this.fingers
    };
  }
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
  REPLICA_META_NAME
};
