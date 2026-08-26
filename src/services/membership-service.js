'use strict';

const { normalizeReference } = require('../validation');

class MembershipService {
  constructor({ node, rpcClient, routingService, fileService }) {
    this.node = node;
    this.rpcClient = rpcClient;
    this.routingService = routingService;
    this.fileService = fileService;
    this.leavePromise = null;
  }

  async createRing() {
    this.node.predecessor = this.node.reference;
    for (const finger of this.node.fingers) finger.node = this.node.reference;
    this.node.joined = true;
    await this.node.fileRepository.initialize();
    return this.node.state();
  }

  async join(bootstrap) {
    if (this.node.joined) throw new Error('Este nó já pertence a uma rede Chord');
    if (!bootstrap) {
      return this.createRing();
    }

    const contact = normalizeReference(bootstrap);
    if (contact.id === this.node.id) throw new Error('O nó de entrada não pode ter o mesmo id');
    const successor = await this.rpcClient.request(contact, '/rpc/find-successor', {
      method: 'POST',
      body: { id: this.node.id }
    });
    if (successor.id === this.node.id) throw new Error(`O id ${this.node.id} já está em uso`);

    const predecessorResult = await this.rpcClient.request(successor, '/rpc/predecessor');
    const predecessor = predecessorResult.node || successor;
    this.node.successor = successor;
    this.node.predecessor = predecessor;

    await this.rpcClient.request(successor, '/rpc/predecessor', {
      method: 'PUT',
      body: { node: this.node.reference }
    });
    await this.rpcClient.request(
      predecessor.id !== successor.id ? predecessor : successor,
      '/rpc/successor',
      { method: 'PUT', body: { node: this.node.reference } }
    );

    this.node.joined = true;
    await this.routingService.refreshFingerTable();
    await this.fileService.syncCatalog(this.node.successor);
    await this.rpcClient.request(this.node.successor, '/rpc/refresh-fingers', {
      method: 'POST',
      body: { originId: this.node.id, hops: 0 }
    });
    return this.node.state();
  }

  async leave() {
    if (this.leavePromise) {
      const conflict = new Error(`O nó ${this.node.id} já está saindo da rede`);
      conflict.code = 'ELEAVEINPROGRESS';
      throw conflict;
    }
    this.leavePromise = this.performLeave();
    try {
      return await this.leavePromise;
    } finally {
      this.leavePromise = null;
    }
  }

  async performLeave() {
    this.node.assertJoined();
    const successor = this.node.successor;
    const predecessor = this.node.predecessor;
    if (!successor || !predecessor) throw new Error('Topologia incompleta para sair da rede');

    if (successor.id === this.node.id && predecessor.id === this.node.id) {
      this.node.joined = false;
      this.node.leaving = false;
      this.node._leavePhase = 'left';
      this.node.predecessor = null;
      this.node.fingers = this.routingService.buildEmptyFingerTable();
      return this.node.state();
    }

    this.node.leaving = true;
    this.node._leavePhase = 'draining';
    this.node._leaveTarget = predecessor;
    this.node._leaveTransferId ||= `leave-${this.node.id}-${Date.now()}`;

    if (!this.node._leaveDetached) {
      try {
        await this.fileService.withPrimaryWriteLock(() =>
          this.fileService.transferPrimaryFiles(predecessor, {
            replicate: false,
            transferId: this.node._leaveTransferId,
            fromNode: this.node.reference
          }));
      } catch (error) {
        this.resetLeaveState();
        throw error;
      }
      await this.detachNeighbors(successor, predecessor);
    }

    await this.rpcClient.request(successor, '/rpc/repair-fingers', {
      method: 'POST',
      body: { originId: successor.id, hops: 0 }
    });
    await this.fileService.withPrimaryWriteLock(async () => {
      await this.fileService.transferPrimaryFiles(predecessor, {
        transferId: this.node._leaveTransferId,
        fromNode: this.node.reference
      });
      this.node._leavePhase = 'forwarding';
    });
    await this.reconcileReplicas(successor);
    this.node.joined = false;
    return this.node.state();
  }

  async reconcileReplicas(start) {
    const visited = new Set();
    let current = start;
    while (current && !visited.has(current.id) && visited.size < 32) {
      visited.add(current.id);
      await this.rpcClient.request(current, '/rpc/verify-replicas', { method: 'POST' });
      const result = await this.rpcClient.request(current, '/rpc/successor');
      current = result.node;
    }
  }

  async detachNeighbors(successor, predecessor) {
    let successorChanged = this.node._leaveSuccessorRewired;
    try {
      if (!successorChanged) {
        await this.rpcClient.request(successor, '/rpc/predecessor', {
          method: 'PUT',
          body: { node: predecessor, expectedId: this.node.id }
        });
        successorChanged = true;
        this.node._leaveSuccessorRewired = true;
      }
      await this.rpcClient.request(predecessor, '/rpc/successor', {
        method: 'PUT',
        body: { node: successor, expectedId: this.node.id }
      });
      this.node._leaveDetached = true;
      this.node._leaveSuccessorRewired = false;
    } catch (error) {
      if (successorChanged) {
        try {
          await this.rpcClient.request(successor, '/rpc/predecessor', {
            method: 'PUT',
            body: { node: this.node.reference, expectedId: predecessor.id }
          });
          successorChanged = false;
          this.node._leaveSuccessorRewired = false;
        } catch (rollbackError) {
          error.message += `; também falhou o rollback: ${rollbackError.message}`;
        }
      }
      if (!successorChanged) this.resetLeaveState();
      throw error;
    }
  }

  resetLeaveState() {
    this.node.leaving = false;
    this.node._leavePhase = 'active';
    this.node._leaveTarget = null;
    this.node._leaveTransferId = null;
    this.node._leaveDetached = false;
    this.node._leaveSuccessorRewired = false;
  }
}

module.exports = { MembershipService };
