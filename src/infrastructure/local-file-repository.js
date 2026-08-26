'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');
const { CATALOG_NAME, REPLICA_META_NAME } = require('../chord-config');
const { validateFileName } = require('../validation');

const LEGACY_INDEX_NAME = 'index.json';
const RESERVED_NAMES = new Set([CATALOG_NAME, REPLICA_META_NAME, LEGACY_INDEX_NAME]);

class LocalFileRepository {
  constructor({ directory, nodeId, nodeReference, fileSystem = fs, clock = () => new Date() } = {}) {
    this.directory = directory;
    this.nodeId = nodeId;
    this.nodeReference = nodeReference || { id: nodeId };
    this.fs = fileSystem;
    this.clock = clock;
    this.metadataLock = Promise.resolve();
    this.migrationPromise = null;
  }

  async initialize() {
    if (!this.migrationPromise) {
      this.migrationPromise = this.migrateLegacyStorage().catch((error) => {
        this.migrationPromise = null;
        throw error;
      });
    }
    return this.migrationPromise;
  }

  async store(fileName, content, options = {}) {
    const name = validateFileName(fileName);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);
    await this.initialize();

    return this.withMetadataLock(async () => {
      const metadata = await this.readMetadataFile();
      const current = metadata[name];
      const isReplica = Boolean(options.isReplica);
      if (isReplica && current && !current.isReplica && !options.allowDemotion) return false;

      const now = this.clock().toISOString();
      const history = mergeHistory(current?.history, options.history, options.event);
      const uploadedBy = normalizeActor(options.uploadedBy || current?.uploadedBy);
      const uploadedAt = options.uploadedAt || current?.uploadedAt
        || history.find((event) => event.type === 'upload')?.timestamp || now;

      await this.fs.mkdir(this.directory, { recursive: true });
      await this.fs.writeFile(path.join(this.directory, name), bytes);
      if (name !== REPLICA_META_NAME) {
        metadata[name] = {
          hashId: options.hashId ?? current?.hashId ?? null,
          primaryNodeId: isReplica
            ? (options.primaryNodeId ?? current?.primaryNodeId ?? null)
            : this.nodeId,
          isReplica,
          uploadedBy,
          uploadedAt,
          storedAt: now,
          history
        };
        await this.writeMetadataFile(metadata);
      }
      return true;
    });
  }

  async read(fileName) {
    const name = validateFileName(fileName);
    await this.initialize();
    try {
      return await this.fs.readFile(path.join(this.directory, name));
    } catch (error) {
      if (error.code === 'ENOENT') throw notFound(name);
      throw error;
    }
  }

  async getMetadata(fileName) {
    const name = validateFileName(fileName);
    const metadata = await this.readMetadata();
    if (!metadata[name]) throw notFound(name);
    return metadata[name];
  }

  async listPrimaryFiles({ includeCatalog = false } = {}) {
    const metadata = await this.readMetadata();
    return Object.entries(metadata)
      .filter(([name, info]) => !info.isReplica && (includeCatalog || name !== CATALOG_NAME))
      .map(([name, info]) => ({ name, hashId: info.hashId }));
  }

  async listFileNames() {
    const metadata = await this.readMetadata();
    return Object.keys(metadata)
      .filter((name) => !RESERVED_NAMES.has(name))
      .sort((left, right) => left.localeCompare(right, 'pt-BR'));
  }

  async removeReplica(fileName) {
    return this.removeFile(fileName, { replicaOnly: true });
  }

  async removeFile(fileName, { replicaOnly = false } = {}) {
    const name = validateFileName(fileName);
    await this.initialize();
    return this.withMetadataLock(async () => {
      const metadata = await this.readMetadataFile();
      if (!metadata[name] || (replicaOnly && !metadata[name].isReplica)) return false;
      delete metadata[name];
      await this.fs.rm(path.join(this.directory, name), { force: true });
      await this.writeMetadataFile(metadata);
      return true;
    });
  }

  async readCatalogNames() {
    await this.initialize();
    return this.readCatalogNamesWithoutInitialize();
  }

  async mergeCatalogEntries(names) {
    await this.initialize();
    return this.withMetadataLock(async () => {
      const current = await this.readCatalogNamesWithoutInitialize();
      const merged = uniqueNames([...current, ...names]);
      await this.fs.mkdir(this.directory, { recursive: true });
      await this.fs.writeFile(
        path.join(this.directory, CATALOG_NAME),
        merged.length ? `${merged.join('\n')}\n` : ''
      );
      return merged;
    });
  }

  addCatalogEntry(name) {
    return this.mergeCatalogEntries([validateFileName(name)]);
  }

  withMetadataLock(operation) {
    const next = this.metadataLock.then(() => operation());
    this.metadataLock = next.catch(() => {});
    return next;
  }

  async readMetadata() {
    await this.initialize();
    return this.readMetadataFile();
  }

  async writeMetadata(metadata) {
    await this.initialize();
    return this.writeMetadataFile(metadata);
  }

  async readMetadataFile() {
    const metadataPath = path.join(this.directory, REPLICA_META_NAME);
    try {
      return JSON.parse(await this.fs.readFile(metadataPath, 'utf8'));
    } catch (error) {
      if (error.code === 'ENOENT') return {};
      throw error;
    }
  }

  async writeMetadataFile(metadata) {
    await this.fs.mkdir(this.directory, { recursive: true });
    const metadataPath = path.join(this.directory, REPLICA_META_NAME);
    await this.fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2));
  }

  async readCatalogNamesWithoutInitialize() {
    try {
      const content = await this.fs.readFile(path.join(this.directory, CATALOG_NAME), 'utf8');
      return uniqueNames(content.split(/\r?\n/));
    } catch (error) {
      if (error.code === 'ENOENT') return [];
      throw error;
    }
  }

  async migrateLegacyStorage() {
    await this.fs.mkdir(this.directory, { recursive: true });
    const existing = await this.readMetadataFile();
    const legacy = await this.readLegacyIndex();
    let changed = false;

    for (const [role, isReplica] of [['primary', false], ['replica', true]]) {
      for (const [name, info] of Object.entries(legacy[role] || {})) {
        if (RESERVED_NAMES.has(name) && name !== CATALOG_NAME) continue;
        const source = path.join(this.directory, role, name);
        const target = path.join(this.directory, name);
        if (await this.exists(source) && !(await this.exists(target))) {
          await this.fs.copyFile(source, target);
        }
        if (name === CATALOG_NAME) continue;
        if (!existing[name] || (!isReplica && existing[name].isReplica)) {
          existing[name] = legacyMetadata(info, isReplica, this.nodeId, this.nodeReference);
          changed = true;
        }
      }
    }

    let entries = [];
    try {
      entries = await this.fs.readdir(this.directory, { withFileTypes: true });
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    for (const entry of entries) {
      if (!entry.isFile() || RESERVED_NAMES.has(entry.name)) continue;
      if (!existing[entry.name]) {
        existing[entry.name] = legacyMetadata({}, false, this.nodeId, this.nodeReference);
        changed = true;
      }
    }

    const catalogNames = await this.readCatalogNamesWithoutInitialize();
    for (const catalogPath of [
      path.join(this.directory, 'primary', CATALOG_NAME),
      path.join(this.directory, 'replica', CATALOG_NAME)
    ]) {
      try {
        catalogNames.push(...(await this.fs.readFile(catalogPath, 'utf8')).split(/\r?\n/));
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
      }
    }
    catalogNames.push(...Object.keys(existing));
    const mergedCatalog = uniqueNames(catalogNames);
    await this.fs.writeFile(
      path.join(this.directory, CATALOG_NAME),
      mergedCatalog.length ? `${mergedCatalog.join('\n')}\n` : ''
    );

    if (changed || !(await this.exists(path.join(this.directory, REPLICA_META_NAME)))) {
      await this.writeMetadataFile(existing);
    }
  }

  async readLegacyIndex() {
    try {
      const parsed = JSON.parse(await this.fs.readFile(
        path.join(this.directory, LEGACY_INDEX_NAME), 'utf8'));
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch (error) {
      if (error.code === 'ENOENT') return {};
      throw error;
    }
  }

  async exists(target) {
    try {
      await this.fs.access(target);
      return true;
    } catch (error) {
      if (error.code === 'ENOENT') return false;
      throw error;
    }
  }
}

function legacyMetadata(info, isReplica, nodeId, nodeReference) {
  const timestamp = new Date(0).toISOString();
  return {
    hashId: info.hashId ?? null,
    primaryNodeId: isReplica ? (info.primaryNodeId ?? null) : nodeId,
    isReplica,
    uploadedBy: null,
    uploadedAt: null,
    storedAt: timestamp,
    history: [{
      id: `legacy-${nodeId}-${info.hashId ?? 'unknown'}-${isReplica ? 'replica' : 'primary'}`,
      type: 'legacy_import',
      timestamp,
      node: normalizeActor(nodeReference)
    }]
  };
}

function mergeHistory(...groups) {
  const byId = new Map();
  for (const event of groups.flat().filter(Boolean)) {
    const normalized = { ...event };
    normalized.id ||= `${normalized.type || 'event'}-${normalized.timestamp || ''}-${normalized.node?.id || ''}`;
    byId.set(normalized.id, normalized);
  }
  return [...byId.values()].sort((left, right) =>
    String(left.timestamp || '').localeCompare(String(right.timestamp || '')));
}

function normalizeActor(actor) {
  if (!actor) return null;
  return {
    id: Number(actor.id),
    ...(actor.host ? { host: String(actor.host) } : {}),
    ...(actor.port ? { port: Number(actor.port) } : {})
  };
}

function uniqueNames(names) {
  return [...new Set(names.map((name) => String(name || '').trim())
    .filter((name) => name && !RESERVED_NAMES.has(name)))]
    .sort((left, right) => left.localeCompare(right, 'pt-BR'));
}

function notFound(name) {
  const error = new Error(`Arquivo "${name}" não encontrado na rede`);
  error.code = 'ENOENT';
  return error;
}

module.exports = { LocalFileRepository };
