'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { LocalFileRepository } = require('../src/infrastructure/local-file-repository');
const { RoutingService } = require('../src/services/routing-service');

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
  assert.deepEqual(await repository.getMetadata('documento.txt'), {
    hashId: 14,
    primaryNodeId: 20,
    isReplica: false
  });
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
