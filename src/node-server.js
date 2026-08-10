'use strict';

const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');
const { URL } = require('node:url');
const { ChordNode, normalizeReference } = require('./chord-node');

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
      return json(response, 200, node.state());
    }
    if (request.method === 'POST' && url.pathname === '/api/files') {
      const body = await readJson(request);
      if (body.name === 'catalogo.txt') {
        throw new Error('catalogo.txt é reservado para o controle da rede');
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
        'x-chord-node-id': String(result.node.id)
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
      await node.storeLocal(body.name, content);
      return json(response, 200, { ok: true, size: content.length });
    }
    if (request.method === 'GET' && url.pathname === '/rpc/files') {
      const name = url.searchParams.get('name');
      const content = await node.readLocal(name);
      return json(response, 200, { name, content: content.toString('base64') });
    }
    return json(response, 404, { error: 'Rota não encontrada' });
  } catch (error) {
    const status = error.name === 'AbortError' || error.code === 'ETIMEDOUT'
      ? 504 : error.code === 'ENOENT' ? 404 : 400;
    return json(response, status, { error: error.message });
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
