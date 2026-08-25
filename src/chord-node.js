'use strict';

const path = require('node:path');
const { validateId } = require('./ring');
const { CATALOG_NAME, REPLICA_META_NAME } = require('./chord-config');
const { normalizeReference, validateFileName } = require('./validation');
const { HttpRpcClient } = require('./infrastructure/http-rpc-client');
const { LocalFileRepository } = require('./infrastructure/local-file-repository');
const { RoutingService } = require('./services/routing-service');
const { MembershipService } = require('./services/membership-service');
const { FileService } = require('./services/file-service');
const { ReplicationService } = require('./services/replication-service');

/**
 * Fachada pública de um nó Chord.
 *
 * Estado e API permanecem aqui por compatibilidade com o servidor HTTP. As
 * regras de roteamento, associação, arquivos, replicação e infraestrutura são
 * implementadas por colaboradores coesos e injetáveis.
 */
class ChordNode {
  constructor({ id, host = '127.0.0.1', port = 5000, requestTimeout = 10000,
    storageDirectory, dependencies = {} } = {}) {
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
    this.joined = false;
    this.leaving = false;
    this._leavePhase = 'active';
    this._leaveSuccessor = null;
    this._leaveDetached = false;
    this._leaveSuccessorRewired = false;

    this.rpcClient = dependencies.rpcClient || new HttpRpcClient({ timeout: requestTimeout });
    this.fileRepository = dependencies.fileRepository || new LocalFileRepository({
      directory: this.storageDirectory,
      nodeId: this.id
    });
    this.routingService = dependencies.routingService || new RoutingService({
      node: this,
      rpcClient: this.rpcClient,
      schedule: dependencies.schedule,
      onTableRefreshed: () => this.replicationService.verify()
    });
    this.fingers = this.routingService.buildEmptyFingerTable();
    this.fileService = dependencies.fileService || new FileService({
      node: this,
      rpcClient: this.rpcClient,
      repository: this.fileRepository,
      routingService: this.routingService,
      replicationService: () => this.replicationService
    });
    this.replicationService = dependencies.replicationService || new ReplicationService({
      node: this,
      rpcClient: this.rpcClient,
      repository: this.fileRepository,
      logger: dependencies.logger
    });
    this.membershipService = dependencies.membershipService || new MembershipService({
      node: this,
      rpcClient: this.rpcClient,
      routingService: this.routingService,
      fileService: this.fileService
    });
  }

  get reference() {
    return { id: this.id, host: this.host, port: this.port };
  }

  get successor() {
    return this.fingers[0].node;
  }

  set successor(node) {
    this.fingers[0].node = node;
  }

  buildEmptyFingerTable() {
    return this.routingService.buildEmptyFingerTable();
  }

  createRing() {
    return this.membershipService.createRing();
  }

  join(bootstrap) {
    return this.membershipService.join(bootstrap);
  }

  leave() {
    return this.membershipService.leave();
  }

  _performLeave() {
    return this.membershipService.performLeave();
  }

  _resetLeaveState() {
    return this.membershipService.resetLeaveState();
  }

  refreshFingerTable() {
    return this.routingService.refreshFingerTable();
  }

  refreshRingFingerTables(originId, hops = 0) {
    return this.routingService.refreshRingFingerTables(originId, hops);
  }

  repairRingFingerTables(originId, hops = 0) {
    return this.routingService.repairRingFingerTables(originId, hops);
  }

  findSuccessor(id, hops = 0) {
    return this.routingService.findSuccessor(id, hops);
  }

  closestPrecedingFinger(id) {
    return this.routingService.closestPrecedingFinger(id);
  }

  put(fileName, content, options) {
    return this.fileService.put(fileName, content, options);
  }

  get(fileName) {
    return this.fileService.get(fileName);
  }

  _getFromKnownCopies(fileName) {
    return this.fileService.getFromKnownCopies(fileName);
  }

  addToCatalog(fileName) {
    return this.fileService.addToCatalog(fileName);
  }

  storeLocal(fileName, content, options) {
    return this.fileService.storeLocal(fileName, content, options);
  }

  readLocal(fileName) {
    return this.fileRepository.read(fileName);
  }

  getReplicaMeta(fileName) {
    return this.fileRepository.getMetadata(fileName);
  }

  getAllPrimaryFiles(options) {
    return this.fileRepository.listPrimaryFiles(options);
  }

  _transferPrimaryFiles(successor, options) {
    return this.fileService.transferPrimaryFiles(successor, options);
  }

  _forwardPrimaryFile(name, content, hashId) {
    return this.fileService.forwardPrimaryFile(name, content, hashId);
  }

  getReplicationTargets() {
    return this.replicationService.getTargets();
  }

  replicateFile(fileName, content, hashId) {
    return this.replicationService.replicate(fileName, content, hashId);
  }

  locateFile(fileName) {
    return this.replicationService.locate(fileName);
  }

  verifyReplicas() {
    return this.replicationService.verify();
  }

  _withReplicaLock(operation) {
    return this.fileRepository.withMetadataLock(operation);
  }

  _withPrimaryWriteLock(operation) {
    return this.fileService.withPrimaryWriteLock(operation);
  }

  _readReplicaMeta() {
    return this.fileRepository.readMetadata();
  }

  _writeReplicaMeta(metadata) {
    return this.fileRepository.writeMetadata(metadata);
  }

  assertJoined() {
    if (!this.joined && !this.leaving) throw new Error('O nó ainda não entrou em uma rede');
  }

  rpc(node, requestPath, options) {
    return this.rpcClient.request(node, requestPath, options);
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

module.exports = {
  ChordNode,
  normalizeReference,
  validateFileName,
  CATALOG_NAME,
  REPLICA_META_NAME
};
