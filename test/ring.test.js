'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { add, hashKey, inInterval } = require('../src/ring');
const { ChordNode } = require('../src/chord-node');
const { startNodeServer } = require('../src/node-server');

test('aritmética circular usa ids públicos de 1 a 32', () => {
  assert.equal(add(31, 1), 32);
  assert.equal(add(32, 1), 1);
  assert.equal(add(30, 4), 2);
});

test('intervalos circulares atravessam o fim do anel', () => {
  assert.equal(inInterval(32, 30, 3, false, true), true);
  assert.equal(inInterval(2, 30, 3, false, true), true);
  assert.equal(inInterval(20, 30, 3, false, true), false);
});

test('primeiro nó cria anel e preenche cinco fingers', async () => {
  const node = new ChordNode({ id: 8 });
  await node.join(null);
  assert.equal(node.fingers.length, 5);
  assert.equal(node.predecessor.id, 8);
  assert.ok(node.fingers.every((finger) => finger.node.id === 8));
  assert.deepEqual(node.fingers.map((finger) => finger.start), [9, 10, 12, 16, 24]);
});

test('cada nó aceita uma porta própria e rejeita portas inválidas', () => {
  assert.equal(new ChordNode({ id: 2, port: 5001 }).port, 5001);
  assert.throws(() => new ChordNode({ id: 2, port: 70000 }), /porta/);
  assert.throws(() => new ChordNode({ id: 2, host: '0.0.0.0', port: 5001 }),
    /IP ou hostname/);
});

test('hash de arquivo é determinístico e sempre aponta para uma das 32 posições', () => {
  assert.equal(hashKey('relatorio.pdf'), hashKey('relatorio.pdf'));
  assert.ok(hashKey('relatorio.pdf') >= 1 && hashKey('relatorio.pdf') <= 32);
});

test('put e get armazenam arquivo e catálogo no sucessor ativo', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-test-'));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const node = new ChordNode({ id: 8, storageDirectory: directory });
  await node.join(null);

  const stored = await node.put('aula.txt', Buffer.from('Chord distribuído'));
  assert.equal(stored.node.id, 8);
  assert.equal((await node.get('aula.txt')).content.toString(), 'Chord distribuído');
  assert.equal((await node.get('catalogo.txt')).content.toString(), 'aula.txt\n');
});

test('nomes de arquivo não podem escapar do diretório do nó', async () => {
  const node = new ChordNode({ id: 1 });
  await node.join(null);
  await assert.rejects(node.put('../segredo.txt', 'x'), /inválido/);
});

test('posição sem nó armazena no próximo nó ativo através de HTTP', async (t) => {
  const [portA, portB] = await Promise.all([freePort(), freePort()]);
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-network-test-'));
  const first = await startNodeServer({
    id: 8, port: portA, storageDirectory: path.join(directory, '8')
  });
  const second = await startNodeServer({
    id: 20, port: portB, storageDirectory: path.join(directory, '20')
  });
  t.after(async () => {
    await Promise.all([first.close(), second.close()]);
    await fs.rm(directory, { recursive: true, force: true });
  });
  await first.node.join(null);
  await second.node.join(first.node.reference);

  let name;
  for (let index = 0; index < 1000; index += 1) {
    const candidate = `arquivo-${index}.bin`;
    const position = hashKey(candidate);
    if (position > 8 && position <= 20) {
      name = candidate;
      break;
    }
  }
  assert.ok(name);
  const bytes = Buffer.from([0, 1, 2, 253, 254, 255]);
  const result = await first.node.put(name, bytes);

  assert.notEqual(result.hashId, 20, 'a posição virtual escolhida não deve ter nó');
  assert.equal(result.node.id, 20);
  assert.deepEqual((await first.node.get(name)).content, bytes);
  assert.deepEqual(await second.node.readLocal(name), bytes);
  assert.match((await second.node.get('catalogo.txt')).content.toString(), new RegExp(name));
});

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
