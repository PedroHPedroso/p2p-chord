'use strict';

const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');
const { URL } = require('node:url');
const { ChordNode, normalizeReference, CATALOG_NAME } = require('./chord-node');
const { INDEX_NAME } = require('./file-store');

const PUBLIC_DIRECTORY = path.join(__dirname, '..', 'public');
const STATIC_FILES = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/app.js': ['app.js', 'text/javascript; charset=utf-8'],
  '/styles.css': ['styles.css', 'text/css; charset=utf-8']
};

async function startNodeServer(options) {
  const node = new ChordNode(options);
  await node.store.ensureLoaded();
  const server = http.createServer((request, response) =>
    handleNodeRequest(node, request, response));

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(node.port, '0.0.0.0', resolve);
  });

  return {
    node,
    server,
    close: () => new Promise((resolve, reject) =>
      server.close((error) => error ? reject(error) : resolve()))
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
      await node.store.ensureLoaded();
      return json(response, 200, node.state());
    }
    if (request.method === 'GET' && url.pathname === '/node-status') {
      return json(response, 200, await node.nodeStatus());
    }
    if (request.method === 'POST' && url.pathname === '/api/files') {
      const body = await readJson(request);
      if (body.name === CATALOG_NAME) {
        throw new Error('catalogo.txt é reservado para o controle da rede');
      }
      if (body.name === INDEX_NAME) {
        throw new Error(`${INDEX_NAME} é reservado para o controle da rede`);
      }
      const content = Buffer.from(body.content || '', body.encoding === 'base64' ? 'base64' : 'utf8');
      return json(response, 201, await node.put(body.name, content));
    }
    if (request.method === 'GET' && url.pathname === '/api/files') {
      const result = await node.get(url.searchParams.get('name'));
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': `attachment; filename="${encodeURIComponent(result.name)}"`,
        'x-chord-hash-id': String(result.hashId),
        'x-chord-node-id': String(result.node.id),
        'x-chord-owner-id': String((result.owner || result.node).id),
        'x-chord-source': result.source || 'primary'
      });
      return response.end(result.content);
    }
    if (request.method === 'POST' && url.pathname === '/join') {
      const { bootstrap = null } = await readJson(request);
      return json(response, 200, await node.join(bootstrap));
    }
    if (request.method === 'POST' && url.pathname === '/rpc/find-successor') {
      const body = await readJson(request);
      return json(response, 200,
        await node.findSuccessor(body.id, body.hops || 0, body.skipIds || []));
    }
    if (request.method === 'GET' && url.pathname === '/rpc/ping') {
      return json(response, 200, { ok: true, id: node.id });
    }
    if (request.method === 'GET' && url.pathname === '/rpc/predecessor') {
      return json(response, 200, { node: node.predecessor });
    }
    if (request.method === 'GET' && url.pathname === '/rpc/successor') {
      return json(response, 200, { node: node.successor });
    }
    if (request.method === 'PUT' && url.pathname === '/rpc/predecessor') {
      node.predecessor = normalizeReference((await readJson(request)).node);
      return json(response, 200, { ok: true });
    }
    if (request.method === 'PUT' && url.pathname === '/rpc/successor') {
      node.successor = normalizeReference((await readJson(request)).node);
      return json(response, 200, { ok: true });
    }
    if (request.method === 'POST' && url.pathname === '/rpc/refresh-fingers') {
      const body = await readJson(request);
      return json(response, 200,
        await node.refreshRingFingerTables(body.originId, body.hops || 0));
    }
    if (request.method === 'PUT' && url.pathname === '/rpc/files') {
      const body = await readJson(request);
      const content = Buffer.from(body.content || '', 'base64');
      const isReplica = Boolean(body.isReplica);
      await node.storeLocal(body.name, content, {
        isReplica,
        primaryNodeId: body.primaryNodeId ?? null,
        hashId: body.hashId ?? null
      });
      let replicaNode = null;
      if (!isReplica) {
        replicaNode = await node.replicateFile(body.name, content, body.hashId ?? null);
      }
      return json(response, 200, { ok: true, size: content.length, replicaNode });
    }
    if (request.method === 'GET' && url.pathname === '/rpc/files') {
      const name = url.searchParams.get('name');
      const content = await node.readLocal(name);
      await node.store.ensureLoaded();
      const source = node.store.isPrimary(name) ? 'primary' : 'replica';
      return json(response, 200, { name, content: content.toString('base64'), source });
    }
    // Verifica se uma réplica existe antes de transferi-la, evitando envios desnecessários.
    if (request.method === 'GET' && url.pathname === '/rpc/replica-check') {
      const name = url.searchParams.get('name');
      try {
        const meta = await node.getReplicaMeta(name);
        return json(response, 200, { exists: true, isReplica: meta.isReplica });
      } catch (error) {
        if (error.code === 'ENOENT') {
          return json(response, 200, { exists: false, isReplica: false });
        }
        throw error;
      }
    }
    return json(response, 404, { error: 'Rota não encontrada' });
  } catch (error) {
    const status = error.code === 'ENOENT' ? 404
      : error.name === 'AbortError' || error.code === 'ETIMEDOUT' || error.code === 'ECONNREFUSED'
        ? 504 : 400;
    return json(response, status, { error: error.message, code: error.code });
  }
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
