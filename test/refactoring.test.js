'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { LocalFileRepository } = require('../src/infrastructure/local-file-repository');
const { HttpRpcClient, httpStatusForError } = require('../src/infrastructure/http-rpc-client');
const { RoutingService } = require('../src/services/routing-service');

test('erros HTTP seguem os códigos públicos definidos na especificação', () => {
  assert.equal(httpStatusForError({ code: 'ETIMEDOUT' }), 504);
  assert.equal(httpStatusForError({ name: 'AbortError' }), 504);
  assert.equal(httpStatusForError({ code: 'ESTALE_TOPOLOGY' }), 409);
  assert.equal(httpStatusForError({ code: 'ELEAVEINPROGRESS' }), 409);
  assert.equal(httpStatusForError({ code: 'ENODENOTFOUND' }, {
    notFoundCodes: ['ENODENOTFOUND']
  }), 404);
  assert.equal(httpStatusForError({ status: 422 }), 422);
});

test('cliente RPC preserva conflito HTTP e converte aborto em timeout', async () => {
  const target = { id: 8, host: '127.0.0.1', port: 5008 };
  const conflictClient = new HttpRpcClient({
    fetchImplementation: async () => ({
      ok: false,
      status: 409,
      json: async () => ({ error: 'topologia alterada' })
    })
  });
  await assert.rejects(conflictClient.request(target, '/rpc/test'), (error) =>
    error.status === 409 && error.message === 'topologia alterada');

  const timeoutClient = new HttpRpcClient({
    timeout: 5,
    fetchImplementation: async (_url, { signal }) => new Promise((_resolve, reject) => {
      signal.addEventListener('abort', () => {
        const error = new Error('aborted');
        error.name = 'AbortError';
        reject(error);
      });
    })
  });
  await assert.rejects(timeoutClient.request(target, '/rpc/test'), (error) =>
    error.code === 'ETIMEDOUT');
});

test('repositório local impede que réplica atrasada sobrescreva o primário', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-repository-test-'));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const repository = new LocalFileRepository({ directory, nodeId: 20 });

  await repository.store('documento.txt', Buffer.from('primário'), { hashId: 14 });
  const stored = await repository.store('documento.txt', Buffer.from('réplica antiga'), {
    isReplica: true,
    primaryNodeId: 8,
    hashId: 14
  });

  assert.equal(stored, false);
  assert.equal((await repository.read('documento.txt')).toString(), 'primário');
  assert.deepEqual(await repository.getMetadata('documento.txt').then(({ hashId,
    primaryNodeId, isReplica }) => ({ hashId, primaryNodeId, isReplica })), {
      hashId: 14,
      primaryNodeId: 20,
      isReplica: false
    });
});

test('repositório migra primary/replica e index.json sem apagar dados antigos', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-legacy-test-'));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  await fs.mkdir(path.join(directory, 'replica'));
  await fs.writeFile(path.join(directory, 'replica', 'legado.txt'), 'preservado');
  await fs.writeFile(path.join(directory, 'replica', 'catalogo.txt'), 'legado.txt\n');
  await fs.writeFile(path.join(directory, 'index.json'), JSON.stringify({
    primary: {},
    replica: { 'legado.txt': { hashId: 7, primaryNodeId: 3 } }
  }));

  const repository = new LocalFileRepository({ directory, nodeId: 22 });
  await repository.initialize();

  assert.equal((await repository.read('legado.txt')).toString(), 'preservado');
  assert.equal((await repository.getMetadata('legado.txt')).isReplica, true);
  assert.deepEqual(await repository.readCatalogNames(), ['legado.txt']);
});

test('serviço de roteamento usa cliente RPC injetado sem depender de HTTP real', async () => {
  const calls = [];
  const node = {
    id: 8,
    joined: true,
    leaving: false,
    reference: { id: 8, host: 'node-8', port: 5008 },
    successor: { id: 20, host: 'node-20', port: 5020 },
    fingers: []
  };
  const rpcClient = {
    async request(target, requestPath, options) {
      calls.push({ target, requestPath, options });
      return { id: 28, host: 'node-28', port: 5028 };
    }
  };
  const routing = new RoutingService({ node, rpcClient });
  node.fingers = routing.buildEmptyFingerTable();
  for (const finger of node.fingers) finger.node = node.successor;

  const successor = await routing.findSuccessor(30);

  assert.equal(successor.id, 28);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].target.id, 20);
  assert.equal(calls[0].requestPath, '/rpc/find-successor');
  assert.deepEqual(calls[0].options.body, { id: 30, hops: 1 });
});
