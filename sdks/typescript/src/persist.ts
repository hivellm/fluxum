// Durable client state (SPEC-021 CS-040/CS-041) — the TypeScript mirror of
// the Rust SDK's `persist.rs`: an opt-in local store that lets a client
// render instantly after a reload and replay queued mutations exactly-once.
//
// The browser backend is IndexedDB (CS-040); `MemoryBackend` is the test
// double and the parity twin of the Rust `MemoryBackend`. State is keyed by
// `(server, clientId)` on disk with the LAST session's identity stored
// inside — a fresh session authenticating as a different identity discards
// the queued mutations rather than replaying them as someone else, and the
// reconcile against the new identity's `InitialData` removes any rows the
// new user may not see.
//
// Optimistic OVERLAYS are not persisted: an updater is a closure, not data.
// A queued call restored from disk replays its server-side effect, and the
// authoritative `TxUpdate` delivers the resulting rows.

import { decode, encode } from '@msgpack/msgpack';

import type { TableSnapshot } from './cache.ts';
import type { QueueSnapshot } from './optimistic.ts';

/**
 * A platform local store (CS-040): IndexedDB in the browser, anything
 * key-value on other hosts. Keys are opaque strings; values opaque bytes.
 */
export interface PersistenceBackend {
  /** Store `value` under `key`, replacing any previous value. */
  put(key: string, value: Uint8Array): Promise<void>;
  /** The value under `key`, or `null`. */
  get(key: string): Promise<Uint8Array | null>;
  /** Remove `key`. Removing an absent key is not an error. */
  delete(key: string): Promise<void>;
  /** Every stored key starting with `prefix`. */
  list(prefix: string): Promise<string[]>;
}

/** An in-memory {@link PersistenceBackend} — the test double. */
export class MemoryBackend implements PersistenceBackend {
  readonly #map = new Map<string, Uint8Array>();

  put(key: string, value: Uint8Array): Promise<void> {
    this.#map.set(key, Uint8Array.from(value));
    return Promise.resolve();
  }

  get(key: string): Promise<Uint8Array | null> {
    const value = this.#map.get(key);
    return Promise.resolve(value === undefined ? null : Uint8Array.from(value));
  }

  delete(key: string): Promise<void> {
    this.#map.delete(key);
    return Promise.resolve();
  }

  list(prefix: string): Promise<string[]> {
    return Promise.resolve([...this.#map.keys()].filter((k) => k.startsWith(prefix)).sort());
  }
}

/**
 * The browser {@link PersistenceBackend} over IndexedDB (CS-040): one
 * database, one object store, string key → bytes. Every call opens lazily,
 * so constructing the backend never touches storage.
 */
export class IndexedDbBackend implements PersistenceBackend {
  readonly #dbName: string;
  #db: Promise<IDBDatabase> | null = null;

  constructor(dbName = 'fluxum-client') {
    this.#dbName = dbName;
  }

  #open(): Promise<IDBDatabase> {
    this.#db ??= new Promise((resolve, reject) => {
      const request = indexedDB.open(this.#dbName, 1);
      request.onupgradeneeded = () => {
        request.result.createObjectStore('kv');
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error ?? new Error('indexedDB open failed'));
    });
    return this.#db;
  }

  async #tx<T>(
    mode: IDBTransactionMode,
    run: (store: IDBObjectStore) => IDBRequest<T>,
  ): Promise<T> {
    const db = await this.#open();
    return new Promise<T>((resolve, reject) => {
      const request = run(db.transaction('kv', mode).objectStore('kv'));
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error ?? new Error('indexedDB request failed'));
    });
  }

  async put(key: string, value: Uint8Array): Promise<void> {
    await this.#tx('readwrite', (store) => store.put(Uint8Array.from(value), key));
  }

  async get(key: string): Promise<Uint8Array | null> {
    const value = await this.#tx<unknown>('readonly', (store) => store.get(key));
    return value instanceof Uint8Array ? value : null;
  }

  async delete(key: string): Promise<void> {
    await this.#tx('readwrite', (store) => store.delete(key));
  }

  async list(prefix: string): Promise<string[]> {
    const keys = await this.#tx<IDBValidKey[]>('readonly', (store) => store.getAllKeys());
    return keys
      .filter((k): k is string => typeof k === 'string' && k.startsWith(prefix))
      .sort();
  }
}

// --- The persisted state ------------------------------------------------------

/** The client-level blob: last session's identity plus the offline queue. */
export interface PersistedMeta {
  /** The identity the persisted state belongs to (CS-040's identity key). */
  identity: Uint8Array;
  /** The offline queue, keys included (CS-032). */
  queue: QueueSnapshot;
}

/** One subscription's persisted state. */
export interface PersistedQuery {
  /** The subscription's SQL, replayed on startup. */
  sql: string;
  /** The highest resume offset applied before shutdown (informational in
   *  this SDK — reconnection is a full re-establishment). */
  txOffset: number;
  /** Table name → held rows, wire bytes. */
  tables: [string, Uint8Array[]][];
}

/** The rows of a {@link PersistedQuery} as cache snapshots. */
export function querySnapshots(query: PersistedQuery): TableSnapshot[] {
  return query.tables.map(([table, rows]) => ({ table, rows }));
}

/**
 * The client's view of its slice of a backend: key construction plus
 * serialize/deserialize, namespaced by `(server, clientId)`. Write errors
 * are swallowed by design — persistence is an optimization, and a full
 * store must not take the live session down; a corrupt blob hydrates as
 * nothing, which is always safe.
 */
export class ClientStore {
  readonly #backend: PersistenceBackend;
  readonly #prefix: string;

  constructor(backend: PersistenceBackend, server: string, clientId: string) {
    this.#backend = backend;
    this.#prefix = `fluxum|${server}|${clientId}|`;
  }

  #metaKey(): string {
    return `${this.#prefix}meta`;
  }

  #queryKey(sql: string): string {
    return `${this.#prefix}query|${fnv1a64(sql).toString(16).padStart(16, '0')}`;
  }

  async loadMeta(): Promise<PersistedMeta | null> {
    try {
      const bytes = await this.#backend.get(this.#metaKey());
      if (bytes === null) return null;
      const raw = decode(bytes) as { identity?: unknown; queue?: unknown };
      if (!(raw.identity instanceof Uint8Array) || typeof raw.queue !== 'object') return null;
      return { identity: raw.identity, queue: raw.queue as QueueSnapshot };
    } catch {
      return null;
    }
  }

  async saveMeta(meta: PersistedMeta): Promise<void> {
    try {
      await this.#backend.put(this.#metaKey(), encode(meta));
    } catch {
      // best-effort by design
    }
  }

  async loadQueries(): Promise<PersistedQuery[]> {
    try {
      const keys = await this.#backend.list(`${this.#prefix}query|`);
      const queries: PersistedQuery[] = [];
      for (const key of keys) {
        const bytes = await this.#backend.get(key);
        if (bytes === null) continue;
        try {
          const raw = decode(bytes) as PersistedQuery;
          if (typeof raw.sql === 'string' && Array.isArray(raw.tables)) queries.push(raw);
        } catch {
          // corrupt blob: cold-start this query
        }
      }
      queries.sort((a, b) => (a.sql < b.sql ? -1 : a.sql > b.sql ? 1 : 0));
      return queries;
    } catch {
      return [];
    }
  }

  async saveQuery(query: PersistedQuery): Promise<void> {
    try {
      await this.#backend.put(this.#queryKey(query.sql), encode(query));
    } catch {
      // best-effort by design
    }
  }

  async deleteQuery(sql: string): Promise<void> {
    try {
      await this.#backend.delete(this.#queryKey(sql));
    } catch {
      // best-effort by design
    }
  }

  /** Drop everything under this `(server, clientId)` namespace. */
  async clear(): Promise<void> {
    try {
      for (const key of await this.#backend.list(this.#prefix)) {
        await this.#backend.delete(key);
      }
    } catch {
      // best-effort by design
    }
  }
}

/** FNV-1a, 64-bit — a tiny stable hash for key derivation (not security). */
function fnv1a64(text: string): bigint {
  const bytes = new TextEncoder().encode(text);
  let hash = 0xcbf29ce484222325n;
  for (const b of bytes) {
    hash ^= BigInt(b);
    hash = (hash * 0x00000100000001b3n) & 0xffffffffffffffffn;
  }
  return hash;
}
