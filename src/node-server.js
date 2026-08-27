'use strict';

const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');
const { URL } = require('node:url');
const { ChordNode, normalizeReference, CATALOG_NAME, REPLICA_META_NAME } = require('./chord-node');
const { httpStatusForError } = require('./infrastructure/http-rpc-client');

const PUBLIC_DIRECTORY = path.join(__dirname, '..', 'public');
const STATIC_FILES = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/app.js': ['app.js', 'text/javascript; charset=utf-8'],
  '/styles.css': ['styles.css', 'text/css; charset=utf-8']
};

async function startNodeServer(options) {
  const node = new ChordNode(options);
  const server = http.createServer((request, response) =>
    handleNodeRequest(node, request, response));

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(node.port, '0.0.0.0', resolve);
  });

  return {
    node,
    server,
    close: () => new Promise((resolve, reject) => {
      if (!server.listening) return resolve();
      server.close((error) => error ? reject(error) : resolve());
    })
  };
}

async function handleNodeRequest(node, request, response) {
  try {
    const url = new URL(request.url, `http://${request.headers.host}`);
    if (request.method === 'GET' && STATIC_FILES[url.pathname]) {
      const [file, contentType] = STATIC_FILES[url.pathname];
      return sendFile(response, path.join(PUBLIC_DIRECTORY, file), contentType);
    }
    if (request.method === 'GET' && url.pathname === '/api/state') {
      return json(response, 200, node.state());
    }
    if (request.method === 'POST' && url.pathname === '/api/files') {
      const body = await readJson(request);
      if (body.name === CATALOG_NAME) {
        throw new Error('catalogo.txt é reservado para o controle da rede');
      }
      if (body.name === REPLICA_META_NAME) {
        throw new Error(`${REPLICA_META_NAME} é reservado para o controle da rede`);
      }
      const content = Buffer.from(body.content || '', body.encoding === 'base64' ? 'base64' : 'utf8');
      return json(response, 201, await node.put(body.name, content));
    }
    if (request.method === 'GET' && url.pathname === '/api/catalog') {
      return json(response, 200, { names: await node.getCatalog() });
    }
    if (request.method === 'GET' && url.pathname === '/api/files/locations') {
      return json(response, 200, await node.locateFile(url.searchParams.get('name')));
    }
    if (request.method === 'GET' && url.pathname === '/api/files') {
      const result = await node.get(url.searchParams.get('name'));
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': `attachment; filename="${encodeURIComponent(result.name)}"`,
        'x-chord-hash-id': String(result.hashId),
        'x-chord-node-id': String(result.node.id),
        'x-chord-copy-role': result.isReplica ? 'replica' : 'primary'
      });
      return response.end(result.content);
    }
    if (request.method === 'POST' && url.pathname === '/join') {
      const { bootstrap = null } = await readJson(request);
      return json(response, 200, await node.join(bootstrap));
    }
    if (request.method === 'POST' && url.pathname === '/rpc/find-successor') {
      const body = await readJson(request);
      return json(response, 200, await node.findSuccessor(body.id, body.hops || 0));
    }
    if (request.method === 'GET' && url.pathname === '/rpc/predecessor') {
      return json(response, 200, { node: node.predecessor });
    }
    if (request.method === 'PUT' && url.pathname === '/rpc/predecessor') {
      const body = await readJson(request);
      assertExpectedNeighbor(node.predecessor, body.expectedId, 'predecessor');
      node.predecessor = normalizeReference(body.node);
      return json(response, 200, { ok: true });
    }
    if (request.method === 'PUT' && url.pathname === '/rpc/successor') {
      const body = await readJson(request);
      assertExpectedNeighbor(node.successor, body.expectedId, 'sucessor');
      node.successor = normalizeReference(body.node);
      return json(response, 200, { ok: true });
    }
    if (request.method === 'GET' && url.pathname === '/rpc/successor') {
      return json(response, 200, { node: node.successor });
    }
    if (request.method === 'POST' && url.pathname === '/rpc/refresh-fingers') {
      const body = await readJson(request);
      return json(response, 200,
        await node.refreshRingFingerTables(body.originId, body.hops || 0));
    }
    if (request.method === 'POST' && url.pathname === '/rpc/repair-fingers') {
      const body = await readJson(request);
      return json(response, 200,
        await node.repairRingFingerTables(body.originId, body.hops || 0));
    }
    if (request.method === 'POST' && url.pathname === '/rpc/verify-replicas') {
      await node.verifyReplicas();
      return json(response, 200, { ok: true });
    }
    if (request.method === 'PUT' && url.pathname === '/rpc/files') {
      const body = await readJson(request);
      const content = Buffer.from(body.content || '', 'base64');
      const isReplica = Boolean(body.isReplica);
      await node.storeLocal(body.name, content, {
        isReplica,
        primaryNodeId: body.primaryNodeId ?? null,
        hashId: body.hashId ?? null,
        uploadedBy: body.uploadedBy || null,
        uploadedAt: body.uploadedAt || null,
        history: body.history || [],
        allowDemotion: Boolean(body.allowDemotion)
      });
      const replicas = !isReplica && body.replicate !== false
        ? await node.replicateFile(body.name, content, body.hashId ?? null)
        : [];
      const meta = await node.getReplicaMeta(body.name);
      return json(response, 200, {
        ok: true,
        size: content.length,
        isReplica: meta.isReplica,
        replicas
      });
    }
    if (request.method === 'GET' && url.pathname === '/rpc/files') {
      const name = url.searchParams.get('name');
      const content = await node.readLocal(name);
      const meta = await node.getReplicaMeta(name);
      return json(response, 200, {
        name,
        content: content.toString('base64'),
        isReplica: meta.isReplica,
        primaryNodeId: meta.primaryNodeId
      });
    }
    if (request.method === 'DELETE' && url.pathname === '/rpc/files') {
      return json(response, 200, {
        ok: true,
        removed: url.searchParams.get('force') === 'true'
          ? await node.fileRepository.removeFile(url.searchParams.get('name'))
          : await node.fileRepository.removeReplica(url.searchParams.get('name'))
      });
    }
    if (request.method === 'GET' && url.pathname === '/rpc/catalog') {
      const names = new Set([
        ...await node.fileRepository.readCatalogNames(),
        ...await node.fileRepository.listFileNames()
      ]);
      return json(response, 200, { names: [...names] });
    }
    if (request.method === 'PUT' && url.pathname === '/rpc/catalog') {
      const body = await readJson(request);
      return json(response, 200, {
        ok: true,
        names: await node.fileRepository.mergeCatalogEntries(body.names || [])
      });
    }
    // Verifica se uma réplica existe antes de transferi-la, evitando envios desnecessários.
    if (request.method === 'GET' && url.pathname === '/rpc/replica-check') {
      const name = url.searchParams.get('name');
      try {
        const meta = await node.getReplicaMeta(name);
        return json(response, 200, { exists: true, ...meta });
      } catch (error) {
        if (error.code === 'ENOENT') {
          return json(response, 200, { exists: false, isReplica: false });
        }
        throw error;
      }
    }
    return json(response, 404, { error: 'Rota não encontrada' });
  } catch (error) {
    const status = httpStatusForError(error, { notFoundCodes: ['ENOENT'] });
    return json(response, status, { error: error.message });
  }
}

function assertExpectedNeighbor(current, expectedId, label) {
  if (expectedId === undefined || expectedId === null) return;
  if (current?.id === Number(expectedId)) return;
  const conflict = new Error(
    `O ${label} mudou: esperado ${expectedId}, atual ${current?.id ?? 'nenhum'}`);
  conflict.code = 'ESTALE_TOPOLOGY';
  throw conflict;
}

function json(response, status, value) {
  response.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(value, null, 2));
}

async function sendFile(response, file, contentType) {
  const content = await fs.readFile(file);
  response.writeHead(200, {
    'content-type': contentType,
    'cache-control': 'no-cache'
  });
  response.end(content);
}

async function readJson(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

module.exports = { startNodeServer, handleNodeRequest };
