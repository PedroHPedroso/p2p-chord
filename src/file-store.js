'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');

const INDEX_NAME = 'index.json';

class FileStore {
  constructor(directory) {
    this.directory = directory;
    this.primaryDir = path.join(directory, 'primary');
    this.replicaDir = path.join(directory, 'replica');
    this.indexPath = path.join(directory, INDEX_NAME);
    this.primaryFiles = new Map();
    this.replicaFiles = new Map();
    this._loaded = false;
    this._lock = Promise.resolve();
  }

  async ensureLoaded() {
    if (this._loaded) return;
    await fs.mkdir(this.primaryDir, { recursive: true });
    await fs.mkdir(this.replicaDir, { recursive: true });
    try {
      const index = JSON.parse(await fs.readFile(this.indexPath, 'utf8'));
      for (const [name, meta] of Object.entries(index.primary || {})) {
        this.primaryFiles.set(name, normalizeMeta(meta));
      }
      for (const [name, meta] of Object.entries(index.replica || {})) {
        this.replicaFiles.set(name, normalizeMeta(meta));
      }
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    this._loaded = true;
  }

  async writePrimary(name, content, { hashId = null } = {}) {
    await this.ensureLoaded();
    return this._withLock(async () => {
      await fs.mkdir(this.primaryDir, { recursive: true });
      await fs.writeFile(path.join(this.primaryDir, name), content);
      await this._removeFile(this.replicaDir, name);
      this.replicaFiles.delete(name);
      const meta = { hashId, size: content.length };
      this.primaryFiles.set(name, meta);
      await this._persist();
      return meta;
    });
  }

  async writeReplica(name, content, { hashId = null, primaryNodeId = null } = {}) {
    await this.ensureLoaded();
    return this._withLock(async () => {
      await fs.mkdir(this.replicaDir, { recursive: true });
      await fs.writeFile(path.join(this.replicaDir, name), content);
      await this._removeFile(this.primaryDir, name);
      this.primaryFiles.delete(name);
      const meta = { hashId, size: content.length, primaryNodeId };
      this.replicaFiles.set(name, meta);
      await this._persist();
      return meta;
    });
  }

  async read(name) {
    await this.ensureLoaded();
    try {
      return await fs.readFile(path.join(this.primaryDir, name));
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    try {
      return await fs.readFile(path.join(this.replicaDir, name));
    } catch (error) {
      if (error.code === 'ENOENT') throw fileNotFound(name);
      throw error;
    }
  }

  async has(name) {
    await this.ensureLoaded();
    return this.primaryFiles.has(name) || this.replicaFiles.has(name);
  }

  isPrimary(name) {
    return this.primaryFiles.has(name);
  }

  isReplica(name) {
    return this.replicaFiles.has(name);
  }

  listPrimary() {
    return serializeMap(this.primaryFiles);
  }

  listReplica() {
    return serializeMap(this.replicaFiles);
  }

  getMeta(name) {
    return this.primaryFiles.get(name) || this.replicaFiles.get(name) || null;
  }

  _withLock(fn) {
    const next = this._lock.then(() => fn());
    this._lock = next.catch(() => {});
    return next;
  }

  async _persist() {
    const index = {
      primary: Object.fromEntries(this.primaryFiles),
      replica: Object.fromEntries(this.replicaFiles)
    };
    await fs.mkdir(this.directory, { recursive: true });
    await fs.writeFile(this.indexPath, JSON.stringify(index, null, 2));
  }

  async _removeFile(directory, name) {
    try {
      await fs.unlink(path.join(directory, name));
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }
}

function serializeMap(map) {
  return Array.from(map, ([name, meta]) => ({ name, ...meta }));
}

function normalizeMeta(meta = {}) {
  return {
    hashId: meta.hashId ?? null,
    size: meta.size ?? 0,
    ...(meta.primaryNodeId != null ? { primaryNodeId: meta.primaryNodeId } : {})
  };
}

function fileNotFound(name) {
  const error = new Error(`Arquivo "${name}" não encontrado na rede`);
  error.code = 'ENOENT';
  return error;
}

module.exports = { FileStore, INDEX_NAME, fileNotFound };
