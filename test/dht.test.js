'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { hashKey } = require('../src/ring');
const { startNodeServer } = require('../src/node-server');

test('put grava o primário no dono e a réplica no sucessor imediato', async (t) => {
  const { first, second, directory, name, bytes } = await twoNodeNetwork(t);
  const result = await first.node.put(name, bytes);

  assert.equal(result.node.id, 20);
  assert.equal(result.replicaNode.id, 8);
  assert.equal(first.node.store.isReplica(name), true);
  assert.equal(second.node.store.isPrimary(name), true);
  assert.deepEqual(await second.node.readLocal(name), bytes);
  assert.deepEqual(await first.node.readLocal(name), bytes);

  const status = await first.node.nodeStatus();
  assert.equal(status.id, 8);
  assert.ok(status.successors.some((node) => node.id === 20));
  assert.ok(status.replicaFiles.some((file) => file.name === name));
  assert.ok(status.replicaFiles.some((file) => file.primaryNodeId === 20));

  const ownerStatus = await second.node.nodeStatus();
  assert.ok(ownerStatus.primaryFiles.some((file) => file.name === name));
});

test('GET /node-status descreve id, sucessores, primários e réplicas', async (t) => {
  const { first, second, name, bytes } = await twoNodeNetwork(t);
  await first.node.put(name, bytes);

  const response = await fetch(`http://127.0.0.1:${second.node.port}/node-status`);
  assert.equal(response.ok, true);
  const status = await response.json();
  assert.equal(status.id, 20);
  assert.ok(Array.isArray(status.successors));
  assert.ok(status.primaryFiles.some((file) => file.name === name));
  assert.ok(status.successors.some((node) => node.id === 8));
});

test('get recupera pela réplica quando o dono primário está fora do ar', async (t) => {
  const { first, second, name, bytes } = await twoNodeNetwork(t, { requestTimeout: 1500 });
  await first.node.put(name, bytes);
  await second.close();

  const recovered = await first.node.get(name);
  assert.deepEqual(recovered.content, bytes);
  assert.equal(recovered.source, 'replica');
  assert.equal(recovered.node.id, 8);
});

test('get recupera a réplica remota com três nós após a queda do dono', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-dht-3-'));
  const [portA, portB, portC] = await Promise.all([freePort(), freePort(), freePort()]);
  const first = await startNodeServer({
    id: 8, host: '127.0.0.1', port: portA, requestTimeout: 1500,
    storageDirectory: path.join(directory, '8')
  });
  const second = await startNodeServer({
    id: 20, host: '127.0.0.1', port: portB, requestTimeout: 1500,
    storageDirectory: path.join(directory, '20')
  });
  const third = await startNodeServer({
    id: 28, host: '127.0.0.1', port: portC, requestTimeout: 1500,
    storageDirectory: path.join(directory, '28')
  });
  t.after(async () => {
    await Promise.allSettled([first.close(), second.close(), third.close()]);
    await fs.rm(directory, { recursive: true, force: true });
  });

  await first.node.join(null);
  await second.node.join(first.node.reference);
  await third.node.join(first.node.reference);

  const name = await fileOwnedBy(20, 8);
  const bytes = Buffer.from('conteúdo replicado');
  const stored = await first.node.put(name, bytes);
  assert.equal(stored.node.id, 20);
  assert.equal(stored.replicaNode.id, 28);
  assert.equal(third.node.store.isReplica(name), true);
  assert.equal(first.node.store.isReplica(name), true);

  await second.close();

  const recovered = await first.node.get(name);
  assert.deepEqual(recovered.content, bytes);
  assert.equal(recovered.source, 'replica');
  assert.ok([8, 28].includes(recovered.node.id));
});

async function twoNodeNetwork(t, extra = {}) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-dht-'));
  const [portA, portB] = await Promise.all([freePort(), freePort()]);
  const first = await startNodeServer({
    id: 8, host: '127.0.0.1', port: portA, storageDirectory: path.join(directory, '8'), ...extra
  });
  const second = await startNodeServer({
    id: 20, host: '127.0.0.1', port: portB, storageDirectory: path.join(directory, '20'), ...extra
  });
  t.after(async () => {
    await Promise.allSettled([first.close(), second.close()]);
    await fs.rm(directory, { recursive: true, force: true });
  });
  await first.node.join(null);
  await second.node.join(first.node.reference);
  const name = await fileOwnedBy(20, 8);
  return { first, second, directory, name, bytes: Buffer.from([9, 8, 7, 6]) };
}

async function fileOwnedBy(ownerId, predecessorId) {
  for (let index = 0; index < 2000; index += 1) {
    const candidate = `arquivo-${index}.bin`;
    const position = hashKey(candidate);
    if (position > predecessorId && position <= ownerId) return candidate;
  }
  throw new Error(`Não foi possível achar um nome com dono ${ownerId}`);
}

async function freePort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address();
  await new Promise((resolve, reject) =>
    server.close((error) => error ? reject(error) : resolve()));
  return port;
}
