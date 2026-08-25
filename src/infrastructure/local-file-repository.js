'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');
const { CATALOG_NAME, REPLICA_META_NAME } = require('../chord-config');
const { validateFileName } = require('../validation');

class LocalFileRepository {
  constructor({ directory, nodeId, fileSystem = fs } = {}) {
    this.directory = directory;
    this.nodeId = nodeId;
    this.fs = fileSystem;
    this.metadataLock = Promise.resolve();
  }

  async store(fileName, content, { isReplica = false, primaryNodeId, hashId } = {}) {
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);

    return this.withMetadataLock(async () => {
      const metadata = await this.readMetadata();
      if (isReplica && metadata[name] && !metadata[name].isReplica) return false;

      await this.fs.mkdir(this.directory, { recursive: true });
      await this.fs.writeFile(path.join(this.directory, name), bytes);
      if (name !== REPLICA_META_NAME) {
        metadata[name] = {
          hashId: hashId ?? null,
          primaryNodeId: isReplica ? (primaryNodeId ?? null) : this.nodeId,
          isReplica: Boolean(isReplica)
        };
        await this.writeMetadata(metadata);
      }
      return true;
    });
  }

  async read(fileName) {
    const name = validateFileName(fileName);
    try {
      return await this.fs.readFile(path.join(this.directory, name));
    } catch (error) {
      if (error.code === 'ENOENT') {
        const notFound = new Error(`Arquivo "${name}" não encontrado na rede`);
        notFound.code = 'ENOENT';
        throw notFound;
      }
      throw error;
    }
  }

  async getMetadata(fileName) {
    const name = validateFileName(fileName);
    const metadata = await this.readMetadata();
    if (!metadata[name]) {
      const notFound = new Error(`Arquivo "${name}" não encontrado na rede`);
      notFound.code = 'ENOENT';
      throw notFound;
    }
    return metadata[name];
  }

  async listPrimaryFiles({ includeCatalog = false } = {}) {
    const metadata = await this.readMetadata();
    return Object.entries(metadata)
      .filter(([name, info]) => !info.isReplica && (includeCatalog || name !== CATALOG_NAME))
      .map(([name, info]) => ({ name, hashId: info.hashId }));
  }

  withMetadataLock(operation) {
    const next = this.metadataLock.then(() => operation());
    this.metadataLock = next.catch(() => {});
    return next;
  }

  async readMetadata() {
    const metadataPath = path.join(this.directory, REPLICA_META_NAME);
    try {
      return JSON.parse(await this.fs.readFile(metadataPath, 'utf8'));
    } catch (error) {
      if (error.code === 'ENOENT') return {};
      throw error;
    }
  }

  async writeMetadata(metadata) {
    await this.fs.mkdir(this.directory, { recursive: true });
    const metadataPath = path.join(this.directory, REPLICA_META_NAME);
    await this.fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2));
  }
}

module.exports = { LocalFileRepository };
