'use strict';

const { randomUUID } = require('node:crypto');
const { hashKey } = require('../ring');
const { REPLICA_COUNT, MAX_HOPS } = require('../chord-config');
const { validateFileName } = require('../validation');

class ReplicationService {
  constructor({ node, rpcClient, repository, logger = console }) {
    this.node = node;
    this.rpcClient = rpcClient;
    this.repository = repository;
    this.logger = logger;
    this.replicationLocks = new Map();
  }

  async getTargets() {
    const seen = new Set([this.node.id]);
    const targets = [];
    let next = this.node.successor;
    while (next && !seen.has(next.id) && targets.length < REPLICA_COUNT) {
      seen.add(next.id);
      targets.push(next);
      if (targets.length >= REPLICA_COUNT) break;
      try {
        const result = await this.rpcClient.request(next, '/rpc/successor');
        next = result.node;
      } catch (error) {
        this.logger.error(
          `[replicação] Não foi possível consultar o sucessor do nó ${next.id}: ${error.message}`);
        break;
      }
    }
    return targets;
  }

  async replicate(fileName, content, hashId) {
    const previous = this.replicationLocks.get(fileName) || Promise.resolve();
    const next = previous.then(() => this.performReplication(fileName, content, hashId));
    this.replicationLocks.set(fileName, next);
    return next.finally(() => {
      if (this.replicationLocks.get(fileName) === next) {
        this.replicationLocks.delete(fileName);
      }
    });
  }

  async performReplication(fileName, content, hashId) {
    const targets = await this.getTargets();
    const replicas = [];
    let metadata = null;
    try {
      metadata = await this.repository.getMetadata(fileName);
    } catch {
      // Chamadas de compatibilidade podem não ter metadados adicionais.
    }
    let currentHistory = metadata?.history || [];
    let existingLocations = [];
    try {
      existingLocations = (await this.locate(fileName)).locations;
    } catch {
      // A gravação local recém-confirmada continua sendo a fonte da replicação.
    }
    const existingById = new Map(existingLocations.map((location) => [location.id, location]));

    for (const target of targets) {
      const existing = existingById.get(target.id);
      const createsReplica = !existing || existing.role !== 'replica'
        || existing.primaryNodeId !== this.node.id;
      const transferEvent = createsReplica ? {
        id: randomUUID(),
        type: 'replica_created',
        timestamp: new Date().toISOString(),
        sequence: nextSequence(currentHistory),
        fromNode: this.node.reference,
        node: target
      } : null;
      const history = mergeEvents([...currentHistory, transferEvent].filter(Boolean));
      try {
        const stored = await this.rpcClient.request(target, '/rpc/files', {
          method: 'PUT',
          body: {
            name: fileName,
            content: content.toString('base64'),
            isReplica: true,
            primaryNodeId: this.node.id,
            hashId,
            uploadedBy: metadata?.uploadedBy || this.node.reference,
            uploadedAt: metadata?.uploadedAt,
            history,
            allowDemotion: true
          }
        });
        if (stored.isReplica !== false) {
          replicas.push(target);
          if (transferEvent) {
            metadata = await this.repository.appendHistory(fileName, transferEvent);
            currentHistory = metadata.history || history;
          }
        }
        this.logger.log(`[replicação] "${fileName}" replicado com sucesso no nó ${target.id}.`);
      } catch (error) {
        this.logger.error(
          `[replicação] Falha ao replicar "${fileName}" no nó ${target.id}: ${error.message}`);
      }
    }

    await this.removeObsoleteReplicas(fileName, new Set(targets.map((target) => target.id)));
    return replicas;
  }

  async removeObsoleteReplicas(fileName, expectedIds) {
    let locations;
    try {
      ({ locations } = await this.locate(fileName));
    } catch (error) {
      this.logger.error(`[replicação] Falha ao reconciliar "${fileName}": ${error.message}`);
      return;
    }

    for (const location of locations) {
      if (location.id === this.node.id || expectedIds.has(location.id)) continue;
      try {
        await this.rpcClient.request(location,
          `/rpc/files?name=${encodeURIComponent(fileName)}&force=true`, { method: 'DELETE' });
        const metadata = await this.repository.getMetadata(fileName);
        await this.repository.appendHistory(fileName, {
          id: randomUUID(),
          type: 'replica_removed',
          timestamp: new Date().toISOString(),
          sequence: nextSequence(metadata.history),
          fromNode: this.node.reference,
          node: { id: location.id, host: location.host, port: location.port }
        });
      } catch (error) {
        this.logger.error(
          `[replicação] Não foi possível remover a réplica excedente do nó ${location.id}: ${error.message}`);
      }
    }
  }

  async locate(fileName) {
    const name = validateFileName(fileName);
    const locations = [];
    const visited = new Set();
    const queue = [this.node.reference, this.node.predecessor, this.node.successor,
      ...this.node.fingers.map((finger) => finger.node)].filter(Boolean);

    while (queue.length && visited.size < MAX_HOPS) {
      const current = queue.shift();
      if (!current || visited.has(current.id)) continue;
      visited.add(current.id);
      try {
        const check = current.id === this.node.id
          ? { exists: true, ...await this.repository.getMetadata(name) }
          : await this.rpcClient.request(current,
            `/rpc/replica-check?name=${encodeURIComponent(name)}`);
        if (check.exists) {
          locations.push({
            ...current,
            role: check.isReplica ? 'replica' : 'primary',
            primaryNodeId: check.primaryNodeId,
            storedAt: check.storedAt,
            uploadedBy: check.uploadedBy || null,
            uploadedAt: check.uploadedAt || null,
            history: check.history || []
          });
        }
      } catch (error) {
        if (current.id === this.node.id && error.code !== 'ENOENT') throw error;
      }

      if (current.id !== this.node.id) {
        try {
          const state = await this.rpcClient.request(current, '/api/state');
          queue.push(state.predecessor, state.successor,
            ...state.fingerTable.map((finger) => finger.node));
        } catch {
          // Um nó offline não impede a busca pelas outras referências conhecidas.
        }
      }
    }

    locations.sort((left, right) => left.id - right.id);

    const primary = locations.find((location) => location.role === 'primary') || null;
    const replicas = locations.filter((location) => location.role === 'replica');
    const source = primary || locations[0] || {};
    const events = mergeEvents(locations.flatMap((location) => location.history || []));
    return {
      name,
      hashId: hashKey(name),
      uploadedBy: source.uploadedBy || null,
      uploadedAt: source.uploadedAt || null,
      primary,
      replicas,
      replicaLimit: REPLICA_COUNT,
      locations,
      events
    };
  }

  async verify() {
    if (!this.node.joined) return;
    let primaryFiles;
    try {
      primaryFiles = await this.repository.listPrimaryFiles();
    } catch (error) {
      this.logger.error(`[replicação] Não foi possível ler metadados: ${error.message}`);
      return;
    }
    for (const file of primaryFiles) {
      try {
        const content = await this.repository.read(file.name);
        await this.replicate(file.name, content, file.hashId);
      } catch (error) {
        this.logger.error(`[replicação] Falha ao verificar "${file.name}": ${error.message}`);
      }
    }
  }
}

function mergeEvents(events) {
  const unique = new Map();
  for (const event of events) {
    const id = event.id || `${event.type}-${event.timestamp}-${event.node?.id || ''}`;
    unique.set(id, { ...event, id });
  }
  return [...unique.values()].sort(compareEvents);
}

function compareEvents(left, right) {
  const leftSequence = Number(left.sequence);
  const rightSequence = Number(right.sequence);
  if (leftSequence > 0 && rightSequence > 0 && leftSequence !== rightSequence) {
    return leftSequence - rightSequence;
  }
  return String(left.timestamp || '').localeCompare(String(right.timestamp || ''));
}

function nextSequence(history = []) {
  return history.reduce((highest, event) =>
    Math.max(highest, Number(event.sequence) || 0), 0) + 1;
}

module.exports = { ReplicationService };
