'use strict';

const path = require('node:path');
const { validateId } = require('./ring');

function validateFileName(fileName) {
  if (typeof fileName !== 'string' || !fileName.trim()) {
    throw new Error('O nome do arquivo é obrigatório');
  }
  const name = fileName.trim();
  if (name === '.' || name === '..' || path.basename(name) !== name
    || name.includes('/') || name.includes('\\') || name.includes('\0')) {
    throw new Error('Nome de arquivo inválido');
  }
  return name;
}

function normalizeReference(node) {
  if (!node || typeof node !== 'object') throw new Error('Referência de nó inválida');
  return {
    id: validateId(node.id),
    host: String(node.host || '127.0.0.1'),
    port: Number(node.port || 5000)
  };
}

module.exports = { validateFileName, normalizeReference };
