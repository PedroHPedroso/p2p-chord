'use strict';

const { normalizeReference } = require('../validation');

class HttpRpcClient {
  constructor({ timeout = 10000, fetchImplementation = globalThis.fetch } = {}) {
    if (typeof fetchImplementation !== 'function') {
      throw new Error('Uma implementação de fetch é obrigatória');
    }
    this.timeout = timeout;
    this.fetch = fetchImplementation;
  }

  async request(node, path, { method = 'GET', body } = {}) {
    const target = normalizeReference(node);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const response = await this.fetch(`http://${target.host}:${target.port}${path}`, {
        method,
        headers: body ? { 'content-type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal
      });
      const data = await response.json();
      if (!response.ok) {
        const requestError = new Error(data.error || `Erro HTTP ${response.status}`);
        requestError.status = response.status;
        throw requestError;
      }
      return data;
    } catch (error) {
      if (error.name === 'AbortError') {
        const timeout = new Error(
          `Tempo limite ao acessar o nó ${target.id} em ${target.host}:${target.port}`);
        timeout.code = 'ETIMEDOUT';
        throw timeout;
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
}

module.exports = { HttpRpcClient };
