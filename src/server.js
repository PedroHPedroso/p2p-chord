'use strict';

const http = require('node:http');
const os = require('node:os');
const fs = require('node:fs/promises');
const path = require('node:path');
const { URL } = require('node:url');
const { startNodeServer } = require('./node-server');

const CONTROL_PORT = Number(process.env.PORT || 5000);
const PUBLIC_DIRECTORY = path.join(__dirname, '..', 'public');
const nodes = new Map();

const STATIC_FILES = {
  '/': ['manager.html', 'text/html; charset=utf-8'],
  '/manager.js': ['manager.js', 'text/javascript; charset=utf-8'],
  '/styles.css': ['styles.css', 'text/css; charset=utf-8']
};

const server = http.createServer(async (request, response) => {
  try {
    const url = new URL(request.url, `http://${request.headers.host}`);

    if (request.method === 'GET' && STATIC_FILES[url.pathname]) {
      const [file, contentType] = STATIC_FILES[url.pathname];
      return sendFile(response, path.join(PUBLIC_DIRECTORY, file), contentType);
    }
    if (request.method === 'GET' && url.pathname === '/api/nodes') {
      await Promise.all(Array.from(nodes.values(), ({ node }) => node.store.ensureLoaded()));
      return json(response, 200, Array.from(nodes.values(), ({ node }) => node.state()));
    }
    if (request.method === 'GET' && url.pathname === '/api/network') {
      const addresses = localIPv4Addresses();
      const requestedHost = request.headers.host?.replace(/:\d+$/, '');
      const suggestedHost = isLoopback(requestedHost) ? addresses[0] : requestedHost;
      return json(response, 200, {
        addresses,
        suggestedHost: suggestedHost || addresses[0] || '127.0.0.1'
      });
    }
    const deleteNodeMatch = url.pathname.match(/^\/api\/nodes\/(\d+)$/);
    if (request.method === 'DELETE' && deleteNodeMatch) {
      const port = Number(deleteNodeMatch[1]);
      const running = nodes.get(port);
      if (!running) {
        const notFound = new Error(`Não existe nó local na porta ${port}`);
        notFound.code = 'ENODENOTFOUND';
        throw notFound;
      }

      const reference = running.node.reference;
      await running.node.leave();
      await running.close();
      nodes.delete(port);
      return json(response, 200, { ok: true, node: reference });
    }
    if (request.method === 'POST' && url.pathname === '/api/nodes') {
      const body = await readJson(request);
      const port = Number(body.port);
      if (port === CONTROL_PORT) {
        throw new Error(`A porta ${CONTROL_PORT} pertence ao painel; escolha outra porta`);
      }
      if (nodes.has(port)) throw new Error(`Já existe um nó local na porta ${port}`);
      if (Array.from(nodes.values()).some(({ node }) => node.id === Number(body.id))) {
        throw new Error(`O ID ${body.id} já está sendo usado localmente`);
      }

      const running = await startNodeServer({
        id: body.id,
        host: body.host,
        port
      });
      try {
        await running.node.join(body.bootstrap || null);
        nodes.set(port, running);
        return json(response, 201, running.node.state());
      } catch (error) {
        await running.close();
        throw error;
      }
    }
    if (request.method === 'DELETE' && url.pathname === '/api/nodes') {
      const port = Number(url.searchParams.get('port'));
      if (!port) throw new Error('Parâmetro "port" é obrigatório');
      const running = nodes.get(port);
      if (!running) throw new Error(`Nenhum nó local encontrado na porta ${port}`);
      await running.node.leave();
      await running.close();
      nodes.delete(port);
      return json(response, 200, { ok: true, port });
    }
    return json(response, 404, { error: 'Rota não encontrada' });
  } catch (error) {
    const status = error.code === 'ENODENOTFOUND' ? 404
      : error.code === 'ELEAVEINPROGRESS' || error.status === 409 ? 409
        : error.status || 400;
    return json(response, status, { error: error.message });
  }
});

server.listen(CONTROL_PORT, '0.0.0.0', () => {
  const addresses = localIPv4Addresses();
  console.log(`Painel Chord local: http://127.0.0.1:${CONTROL_PORT}`);
  for (const address of addresses) {
    console.log(`Painel Chord na rede: http://${address}:${CONTROL_PORT}`);
  }
});

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n${signal}: retirando nós locais da rede...`);

  for (const [port, running] of Array.from(nodes.entries())) {
    try {
      await running.node.leave();
    } catch (error) {
      console.error(`Não foi possível retirar o nó ${running.node.id}: ${error.message}`);
      process.exitCode = 1;
    }
    try {
      await running.close();
      nodes.delete(port);
    } catch (error) {
      console.error(`Não foi possível fechar o nó ${running.node.id}: ${error.message}`);
      process.exitCode = 1;
    }
  }

  server.close((error) => {
    if (error) {
      console.error(`Não foi possível fechar o painel: ${error.message}`);
      process.exitCode = 1;
    }
  });
}

process.once('SIGINT', () => shutdown('SIGINT'));
process.once('SIGTERM', () => shutdown('SIGTERM'));

function localIPv4Addresses() {
  return Object.values(os.networkInterfaces())
    .flat()
    .filter((entry) => entry && entry.family === 'IPv4' && !entry.internal)
    .map((entry) => entry.address)
    .sort((left, right) => Number(!left.startsWith('172.16.'))
      - Number(!right.startsWith('172.16.')));
}

function isLoopback(host) {
  return !host || host === 'localhost' || host === '127.0.0.1' || host === '[::1]';
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
