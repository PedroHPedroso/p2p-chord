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
  assert.equal(node.state().leavePhase, 'active');
  assert.equal(node.state().leaving, false);
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

test('put torna o nó que recebeu o upload o primário e cria o catálogo', async (t) => {
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

test('upload permanece primário no nó de origem e pode ser lido por outro nó', async (t) => {
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
  assert.equal(result.node.id, 8);
  assert.deepEqual((await first.node.get(name)).content, bytes);
  assert.deepEqual(await second.node.readLocal(name), bytes);
  assert.match((await second.node.get('catalogo.txt')).content.toString(), new RegExp(name));
  assert.match((await first.node.get('catalogo.txt')).content.toString(), new RegExp(name));
});

test('upload confirma o nó primário e as réplicas nos sucessores imediatos', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-replica-locations-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  const name = findNameInInterval('replicas', 8, 20);
  const bytes = Buffer.from('três cópias confirmadas');
  const stored = await running[0].node.put(name, bytes);

  assert.equal(stored.primary.id, 8);
  assert.deepEqual(stored.replicas.map((node) => node.id), [20, 28]);
  assert.deepEqual(stored.locations.map(({ id, role }) => ({ id, role })), [
    { id: 8, role: 'primary' },
    { id: 20, role: 'replica' },
    { id: 28, role: 'replica' }
  ]);
  for (const entry of running) assert.deepEqual(await entry.node.readLocal(name), bytes);
  for (const entry of running) {
    assert.match((await entry.node.get('catalogo.txt')).content.toString(), new RegExp(name));
    assert.ok((await entry.node.fileRepository.readCatalogNames()).includes(name));
  }
  const catalogResponse = await fetch(
    `http://127.0.0.1:${running[1].node.port}/api/catalog`);
  assert.equal(catalogResponse.status, 200);
  assert.ok((await catalogResponse.json()).names.includes(name));
  const panelResponse = await fetch(`http://127.0.0.1:${running[1].node.port}/`);
  const panel = await panelResponse.text();
  assert.match(panel, /id="file-trace"/);
  assert.match(panel, /id="trace-path"/);
  assert.match(panel, /Caminho completo do arquivo/);

  const locationsResponse = await fetch(
    `http://127.0.0.1:${running[0].node.port}/api/files/locations?name=${encodeURIComponent(name)}`);
  assert.equal(locationsResponse.status, 200);
  const located = await locationsResponse.json();
  assert.deepEqual(located.locations.map(({ id, role }) => ({ id, role })), [
    { id: 8, role: 'primary' },
    { id: 20, role: 'replica' },
    { id: 28, role: 'replica' }
  ]);
  assert.equal(located.uploadedBy.id, 8);
  assert.deepEqual(located.events.map((event) => event.type), [
    'upload',
    'replica_created',
    'replica_created'
  ]);
  assert.deepEqual(located.events.slice(1).map((event) => [
    event.fromNode.id,
    event.node.id
  ]), [[8, 20], [8, 28]]);
});

test('download usa uma réplica quando o nó primário fica offline abruptamente', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-replica-fallback-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  const name = findNameInInterval('fallback', 8, 20);
  const bytes = Buffer.from('disponível enquanto houver uma cópia online');
  const stored = await running[1].node.put(name, bytes);
  assert.equal(stored.primary.id, 20);

  await running[1].close();
  const response = await fetch(
    `http://127.0.0.1:${running[0].node.port}/api/files?name=${encodeURIComponent(name)}`);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('x-chord-node-id'), '8');
  assert.equal(response.headers.get('x-chord-copy-role'), 'replica');
  assert.deepEqual(Buffer.from(await response.arrayBuffer()), bytes);
});

test('novo upload do mesmo nome transfere a primazia para o nó que enviou', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-reupload-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  await running[0].node.put('mesmo-nome.txt', Buffer.from('primeira versão'));
  await running[2].node.put('mesmo-nome.txt', Buffer.from('segunda versão'));

  const located = await running[1].node.locateFile('mesmo-nome.txt');
  assert.equal(located.primary.id, 28);
  assert.deepEqual(located.replicas.map((node) => node.id), [8, 20]);
  assert.equal(located.locations.filter((node) => node.role === 'primary').length, 1);
  assert.equal(located.uploadedBy.id, 28);
  assert.equal((await running[0].node.get('mesmo-nome.txt')).content.toString(), 'segunda versão');
});

test('saída controlada religa os vizinhos e remove o nó das finger tables', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-leave-ring-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  const leaving = running[1].node.leave();
  await assert.rejects(running[1].node.leave(), /já está saindo/);
  await leaving;
  await running[1].close();

  assert.equal(running[0].node.predecessor.id, 28);
  assert.equal(running[0].node.successor.id, 28);
  assert.equal(running[2].node.predecessor.id, 8);
  assert.equal(running[2].node.successor.id, 8);
  assert.ok(running[0].node.fingers.every((finger) => finger.node.id !== 20));
  assert.ok(running[2].node.fingers.every((finger) => finger.node.id !== 20));
});

test('saída promove o predecessor e replica nos sucessores dele', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-leave-files-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  let name;
  for (let index = 0; index < 1000; index += 1) {
    const candidate = `saida-${index}.bin`;
    const position = hashKey(candidate);
    if (position > 8 && position <= 20) {
      name = candidate;
      break;
    }
  }
  const bytes = Buffer.from('arquivo preservado na saída');
  assert.equal((await running[1].node.put(name, bytes)).node.id, 20);

  await running[1].node.leave();
  await running[1].close();

  assert.deepEqual((await running[0].node.get(name)).content, bytes);
  assert.match((await running[2].node.get('catalogo.txt')).content.toString(), new RegExp(name));
  assert.equal((await running[0].node.getReplicaMeta(name)).isReplica, false);
  assert.equal((await running[2].node.getReplicaMeta(name)).isReplica, true);
  await running[0].node.storeLocal(name, Buffer.from('réplica atrasada'), {
    isReplica: true,
    primaryNodeId: 20,
    hashId: hashKey(name)
  });
  assert.equal((await running[0].node.getReplicaMeta(name)).isReplica, false);
  assert.deepEqual(await running[0].node.readLocal(name), bytes);
});

test('saída com quatro nós mantém o predecessor como primário e só dois sucessores', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-leave-three-copies-'));
  const running = await startRing([4, 12, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  const name = 'promocao-controlada.bin';
  await running[2].node.put(name, Buffer.from('primário e duas réplicas'));
  await running[2].node.leave();
  await running[2].close();

  const located = await running[1].node.locateFile(name);
  assert.equal(located.primary.id, 12);
  assert.deepEqual(located.replicas.map((node) => node.id), [4, 28]);
  assert.equal(located.locations.length, 3);
  const transfer = located.events.find((event) => event.type === 'primary_transferred');
  assert.equal(transfer.fromNode.id, 20);
  assert.equal(transfer.node.id, 12);
  const sequences = located.events.map((event) => event.sequence);
  assert.deepEqual(sequences, [...sequences].sort((left, right) => left - right));
});

test('histórico registra criação e remoção ao mudar o caminho das réplicas', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-file-path-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  const name = 'caminho-completo.txt';
  await running[0].node.put(name, Buffer.from('trilha persistida'));
  const inserted = await startNodeServer({
    id: 12,
    port: await freePort(),
    storageDirectory: path.join(directory, '12')
  });
  await inserted.node.join(running[0].node.reference);
  running.push(inserted);
  await running[0].node.refreshFingerTable();
  await running[0].node.verifyReplicas();

  const firstTrace = await running[0].node.locateFile(name);
  assert.deepEqual(firstTrace.replicas.map((node) => node.id), [12, 20]);
  assert.ok(firstTrace.events.some((event) =>
    event.type === 'replica_created' && event.node.id === 12));
  assert.ok(firstTrace.events.some((event) =>
    event.type === 'replica_removed' && event.node.id === 28));

  const eventCount = firstTrace.events.length;
  await running[0].node.verifyReplicas();
  assert.equal((await running[0].node.locateFile(name)).events.length, eventCount);
});

test('saída de um nó réplica recompõe as duas cópias antes de encerrar', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-leave-replica-'));
  const running = await startRing([4, 12, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  const name = 'replica-que-sai.bin';
  await running[0].node.put(name, Buffer.from('recompor antes de fechar'));
  await running[1].node.leave();
  await running[1].close();

  const located = await running[0].node.locateFile(name);
  assert.equal(located.primary.id, 4);
  assert.deepEqual(located.replicas.map((node) => node.id), [20, 28]);
  assert.equal(located.locations.length, 3);
});

test('saída de dois nós para um faz o sobrevivente apontar para si mesmo', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-leave-two-'));
  const running = await startRing([8, 20], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  await running[1].node.leave();
  await running[1].close();

  assert.equal(running[0].node.predecessor.id, 8);
  assert.equal(running[0].node.successor.id, 8);
  assert.ok(running[0].node.fingers.every((finger) => finger.node.id === 8));
});

test('nó usado como bootstrap pode sair sem encerrar o anel', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-leave-bootstrap-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  await running[0].node.leave();
  await running[0].close();

  assert.equal(running[1].node.predecessor.id, 28);
  assert.equal(running[1].node.successor.id, 28);
  assert.equal(running[2].node.predecessor.id, 20);
  assert.equal(running[2].node.successor.id, 20);
  assert.ok(running[1].node.fingers.every((finger) => finger.node.id !== 8));
  assert.ok(running[2].node.fingers.every((finger) => finger.node.id !== 8));
});

test('falha ao religar vizinhos desfaz a alteração e mantém o nó ativo', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-leave-rollback-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  await running[0].close();
  await assert.rejects(running[1].node.leave());

  assert.equal(running[1].node.joined, true);
  assert.equal(running[1].node.leaving, false);
  assert.equal(running[2].node.predecessor.id, 20);
});

test('falha na migração mantém o nó e a topologia original ativos', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'chord-leave-transfer-failure-'));
  const running = await startRing([8, 20, 28], directory);
  t.after(async () => {
    await Promise.allSettled(running.map((entry) => entry.close()));
    await fs.rm(directory, { recursive: true, force: true });
  });

  await running[1].node.storeLocal('migracao.bin', Buffer.from('conteúdo'), {
    hashId: 14
  });
  await running[2].close();
  await assert.rejects(running[1].node.leave());

  assert.equal(running[1].node.joined, true);
  assert.equal(running[1].node.leaving, false);
  assert.equal(running[1].node.predecessor.id, 8);
  assert.equal(running[1].node.successor.id, 28);
  assert.equal(running[0].node.successor.id, 20);
});

test('único nó pode sair sem consultar a rede', async () => {
  const node = new ChordNode({ id: 8 });
  await node.join(null);
  const state = await node.leave();
  assert.equal(state.joined, false);
  assert.equal(state.leavePhase, 'left');
  assert.equal(state.leaving, false);
  assert.equal(state.predecessor, null);
  assert.equal(state.successor, null);
  assert.ok(state.fingerTable.every((finger) => finger.node === null));
});

async function startRing(ids, directory) {
  const running = [];
  for (const id of ids) {
    const entry = await startNodeServer({
      id,
      port: await freePort(),
      storageDirectory: path.join(directory, String(id))
    });
    await entry.node.join(running[0]?.node.reference || null);
    running.push(entry);
  }
  return running;
}

function findNameInInterval(prefix, lowerExclusive, upperInclusive) {
  for (let index = 0; index < 1000; index += 1) {
    const candidate = `${prefix}-${index}.bin`;
    const position = hashKey(candidate);
    if (position > lowerExclusive && position <= upperInclusive) return candidate;
  }
  throw new Error('Não foi possível encontrar um nome no intervalo solicitado');
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
