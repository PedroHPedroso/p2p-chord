'use strict';

const { FINGER_COUNT, add, inInterval, validateId } = require('../ring');
const { MAX_HOPS } = require('../chord-config');

class RoutingService {
  constructor({ node, rpcClient, schedule = setImmediate, onTableRefreshed = () => {} }) {
    this.node = node;
    this.rpcClient = rpcClient;
    this.schedule = schedule;
    this.onTableRefreshed = onTableRefreshed;
  }

  buildEmptyFingerTable() {
    return Array.from({ length: FINGER_COUNT }, (_, index) => ({
      index: index + 1,
      start: add(this.node.id, 2 ** index),
      node: null
    }));
  }

  async refreshFingerTable() {
    const nodes = await Promise.all(this.node.fingers.map((finger) =>
      this.findSuccessor(finger.start)));
    this.node.fingers.forEach((finger, index) => {
      finger.node = nodes[index];
    });
    this.schedule(() => {
      Promise.resolve(this.onTableRefreshed()).catch((error) => {
        console.error(`[replicação] Erro ao verificar réplicas após refreshFingerTable: ${error.message}`);
      });
    });
  }

  async refreshRingFingerTables(originId, hops = 0) {
    validateId(originId);
    if (this.node.id === Number(originId)) return { ok: true };
    if (hops >= MAX_HOPS) throw new Error('Limite de nós excedido ao atualizar finger tables');

    await this.refreshFingerTable();
    const next = this.node.successor;
    this.schedule(() => {
      this.rpcClient.request(next, '/rpc/refresh-fingers', {
        method: 'POST',
        body: { originId: Number(originId), hops: hops + 1 }
      }).catch((error) => {
        console.error(
          `Não foi possível atualizar as fingers após o nó ${this.node.id}: ${error.message}`);
      });
    });
    return { ok: true };
  }

  async repairRingFingerTables(originId, hops = 0) {
    const origin = validateId(originId);
    if (hops > 0 && this.node.id === origin) return { ok: true };
    if (hops >= MAX_HOPS) throw new Error('Limite de nós excedido ao reparar finger tables');

    await this.refreshFingerTable();
    const next = this.node.successor;
    if (next.id === origin) return { ok: true };
    return this.rpcClient.request(next, '/rpc/repair-fingers', {
      method: 'POST',
      body: { originId: origin, hops: hops + 1 }
    });
  }

  async findSuccessor(rawId, hops = 0) {
    const id = validateId(rawId);
    if ((!this.node.joined && !this.node.leaving) || !this.node.successor) {
      throw new Error('O nó ainda não entrou em uma rede');
    }
    if (this.node.successor.id === this.node.id) return this.node.reference;
    if (id === this.node.id) {
      return this.node.leaving ? this.node.successor : this.node.reference;
    }
    if (inInterval(id, this.node.id, this.node.successor.id, false, true)) {
      return this.node.successor;
    }
    if (hops >= MAX_HOPS) throw new Error('Limite de saltos excedido ao procurar sucessor');

    let next = this.closestPrecedingFinger(id);
    if (next.id === this.node.id) next = this.node.successor;
    return this.rpcClient.request(next, '/rpc/find-successor', {
      method: 'POST',
      body: { id, hops: hops + 1 }
    });
  }

  closestPrecedingFinger(id) {
    for (let index = this.node.fingers.length - 1; index >= 0; index -= 1) {
      const candidate = this.node.fingers[index].node;
      if (candidate && candidate.id !== this.node.id
        && inInterval(candidate.id, this.node.id, id, false, false)) {
        return candidate;
      }
    }
    return this.node.reference;
  }
}

module.exports = { RoutingService };
