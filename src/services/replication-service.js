'use strict';

const { hashKey } = require('../ring');
const { REPLICA_COUNT, MAX_HOPS } = require('../chord-config');
const { validateFileName } = require('../validation');

class ReplicationService {
  constructor({ node, rpcClient, repository, logger = console }) {
    this.node = node;
    this.rpcClient = rpcClient;
    this.repository = repository;
    this.logger = logger;
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
    const targets = await this.getTargets();
    const replicas = [];
    for (const target of targets) {
      try {
        const stored = await this.rpcClient.request(target, '/rpc/files', {
          method: 'PUT',
          body: {
            name: fileName,
            content: content.toString('base64'),
            isReplica: true,
            primaryNodeId: this.node.id,
            hashId
          }
        });
        if (stored.isReplica !== false) replicas.push(target);
        this.logger.log(
          `[replicação] "${fileName}" replicado com sucesso no nó ${target.id}.`);
      } catch (error) {
        this.logger.error(
          `[replicação] Falha ao replicar "${fileName}" no nó ${target.id}: ${error.message}`);
      }
    }
    return replicas;
  }

  async locate(fileName) {
    const name = validateFileName(fileName);
    const locations = [];
    const visited = new Set();
    let current = this.node.reference;

    while (current && !visited.has(current.id) && visited.size < MAX_HOPS) {
      visited.add(current.id);
      try {
        const check = current.id === this.node.id
          ? { exists: true, ...await this.repository.getMetadata(name) }
          : await this.rpcClient.request(current,
            `/rpc/replica-check?name=${encodeURIComponent(name)}`);
        if (check.exists) {
          locations.push({ ...current, role: check.isReplica ? 'replica' : 'primary' });
        }
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
      }

      if (current.id === this.node.id) {
        current = this.node.successor;
      } else {
        const result = await this.rpcClient.request(current, '/rpc/successor');
        current = result.node;
      }
    }
    return { name, hashId: hashKey(name), locations };
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
      let content;
      try {
        content = await this.repository.read(file.name);
      } catch {
        continue;
      }
      await this.replicate(file.name, content, file.hashId);
    }
  }
}

module.exports = { ReplicationService };
