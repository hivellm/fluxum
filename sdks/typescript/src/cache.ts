// The local row cache (SPEC-011 SDK-040, SDK-042, SDK-044, SDK-045).
//
// Row identity is the row's full FluxBIN bytes as received on the wire
// (SDK-040). Byte-keying gives map semantics even for column types the host
// language cannot hash — an F64 column is a perfectly good part of a row and a
// terrible map key — and makes row equality a byte comparison rather than a
// field-by-field decode.
//
// The cache is schema-agnostic on purpose: it never decodes a row. Generated
// code supplies the primary-key projection per table (SDK-040), which is the
// only schema knowledge the diff algorithm needs. That keeps this file inside
// the SDK-083 size budget and out of the codegen's way.

/** Per-table hooks the generated bindings supply. */
export interface TableSchema {
  /** Table name as it appears in `TableUpdate`. */
  name: string;
  /**
   * Stable primary-key string for a full row.
   *
   * A string rather than a value because primary keys are not always
   * hashable: composite keys are tuples and `Identity` keys are byte arrays,
   * and both must collapse to something a `Map` can key on (SDK-040).
   */
  pkOfRow(row: Uint8Array): string;
  /**
   * Stable primary-key string for a delete entry.
   *
   * Separate from `pkOfRow` because the wire carries **primary-key fields
   * only** for deletes (SPEC-006), so the two decode different layouts.
   */
  pkOfDelete(entry: Uint8Array): string;
}

/** One table's inserts and deletes within a `TxUpdate`. */
export interface TableDiff {
  table: string;
  inserts: Uint8Array[];
  /** Primary-key-only entries (SPEC-006). */
  deletes: Uint8Array[];
}

/** A full table image, as carried by `InitialData`. */
export interface TableSnapshot {
  table: string;
  rows: Uint8Array[];
}

/** A semantic row event. `update` is the primary-key-coalesced pair (SDK-042). */
export type RowEvent =
  | { kind: 'insert'; table: string; row: Uint8Array }
  | { kind: 'delete'; table: string; row: Uint8Array }
  | { kind: 'update'; table: string; old: Uint8Array; row: Uint8Array };

/** Thrown when a diff names a table the cache was not built for. */
export class UnknownTableError extends Error {
  readonly table: string;
  constructor(table: string) {
    super(`no schema registered for table "${table}"`);
    this.name = 'UnknownTableError';
    this.table = table;
  }
}

interface Entry {
  bytes: Uint8Array;
  /** How many active subscription queries currently see this row (SDK-044). */
  refs: number;
}

interface TableState {
  schema: TableSchema;
  /** Byte key → entry. The authoritative store. */
  byKey: Map<string, Entry>;
  /** Primary key → byte key. The projection deletes and updates resolve through. */
  byPk: Map<string, string>;
}

/**
 * Byte identity as a map key.
 *
 * Latin-1 rather than hex: one character per byte instead of two, which halves
 * the key memory on a hot path that holds one key per cached row. Every byte
 * 0–255 maps to a distinct code unit, so this is injective — unlike UTF-8,
 * where it would not be.
 */
function byteKey(bytes: Uint8Array): string {
  let key = '';
  // Chunked to stay clear of the argument-count limit on very wide rows,
  // which `String.fromCharCode(...all)` would hit as a stack overflow.
  const CHUNK = 4096;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    key += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return key;
}

/**
 * The byte-keyed, reference-counted row store.
 *
 * Mutation and notification are separated by construction: `applyTxUpdate`
 * and `reconcile` finish every cache change and then *return* the events.
 * They never invoke a callback themselves. That is what makes SDK-045's
 * "callbacks always observe the full post-commit state" a property of the
 * shape of the code rather than a rule someone has to remember.
 */
export class RowCache {
  readonly #tables = new Map<string, TableState>();
  #stale = false;
  /**
   * Which cached rows each subscription query currently holds (SDK-044):
   * `queryId → table → byte-keys`. This is what makes unsubscribe correct
   * under overlap — dropping a query releases exactly the rows it
   * contributed, and a row still held by another query survives with no
   * callback. Populated only when a caller applies a diff WITH a query id
   * ({@link applyQueryDiff}); the untracked {@link applyTxUpdate} path leaves
   * it empty.
   */
  readonly #queryKeys = new Map<number, Map<string, Set<string>>>();

  constructor(schemas: Iterable<TableSchema>) {
    for (const schema of schemas) {
      this.#tables.set(schema.name, { schema, byKey: new Map(), byPk: new Map() });
    }
  }

  /**
   * True while the connection is down (SDK-047).
   *
   * The cache is retained rather than cleared — an application should keep
   * rendering the last known state — but it is known to be behind the server.
   */
  get stale(): boolean {
    return this.#stale;
  }

  /** Mark the cache as behind the server. No callbacks fire while stale. */
  markStale(): void {
    this.#stale = true;
  }

  /** Rows currently cached for `table`, in insertion order. */
  rows(table: string): Uint8Array[] {
    return [...this.#table(table).byKey.values()].map((entry) => entry.bytes);
  }

  /** How many subscriptions currently see this exact row. 0 when absent. */
  refcount(table: string, row: Uint8Array): number {
    return this.#table(table).byKey.get(byteKey(row))?.refs ?? 0;
  }

  /** Total cached rows across every table. */
  get size(): number {
    let total = 0;
    for (const state of this.#tables.values()) total += state.byKey.size;
    return total;
  }

  /** The registered table names, sorted (deterministic iteration order). */
  tableNames(): string[] {
    return [...this.#tables.keys()].sort();
  }

  /**
   * Project a full row to its primary key through the table's registered
   * schema. `null` for an unregistered table.
   */
  projectPk(table: string, row: Uint8Array): string | null {
    const state = this.#tables.get(table);
    return state === undefined ? null : state.schema.pkOfRow(row);
  }

  /**
   * The `[primary key, row]` pairs cached for `table`, in insertion order.
   * Where several byte-distinct rows share a primary key (transiently
   * possible under join semantics), only the canonical row — the one the pk
   * projection resolves to — is listed. Empty for an unknown table.
   */
  pkRows(table: string): [string, Uint8Array][] {
    const state = this.#tables.get(table);
    if (state === undefined) return [];
    const out: [string, Uint8Array][] = [];
    for (const [key, entry] of state.byKey) {
      const pk = state.schema.pkOfRow(entry.bytes);
      if (state.byPk.get(pk) === key) out.push([pk, entry.bytes]);
    }
    return out;
  }

  /**
   * Apply one `TxUpdate` and return the semantic events it produced.
   *
   * Inserts are applied before deletes per table (SDK-045). That ordering is
   * not cosmetic: a byte-identical delete+insert pair — legitimate under join
   * semantics — would otherwise drive the refcount transiently to zero and
   * fire a spurious delete/insert pair for a row that never actually left.
   */
  applyTxUpdate(diffs: Iterable<TableDiff>): RowEvent[] {
    const inserts: RowEvent[] = [];
    const deletes: RowEvent[] = [];

    for (const diff of diffs) {
      const state = this.#table(diff.table);

      // Deletes carry primary keys only, so they must be resolved to the
      // rows they name BEFORE inserts run — an insert under the same primary
      // key overwrites the projection this lookup depends on.
      const doomed: { pk: string; key: string; bytes: Uint8Array }[] = [];
      for (const entry of diff.deletes) {
        const pk = state.schema.pkOfDelete(entry);
        const key = state.byPk.get(pk);
        if (key === undefined) continue; // never cached; nothing to remove
        const cached = state.byKey.get(key);
        if (cached !== undefined) doomed.push({ pk, key, bytes: cached.bytes });
      }

      for (const row of diff.inserts) {
        const key = byteKey(row);
        const existing = state.byKey.get(key);
        if (existing !== undefined) {
          // Already visible through another query: one more reference, and
          // deliberately no callback (SDK-044).
          existing.refs += 1;
          continue;
        }
        state.byKey.set(key, { bytes: row, refs: 1 });
        state.byPk.set(state.schema.pkOfRow(row), key);
        inserts.push({ kind: 'insert', table: diff.table, row });
      }

      for (const { pk, key, bytes } of doomed) {
        const entry = state.byKey.get(key);
        if (entry === undefined) continue;
        entry.refs -= 1;
        if (entry.refs > 0) continue; // still visible elsewhere: no event
        state.byKey.delete(key);
        // Only drop the projection if it still points at the row being
        // removed; an insert in this same update may have repointed it.
        if (state.byPk.get(pk) === key) state.byPk.delete(pk);
        deletes.push({ kind: 'delete', table: diff.table, row: bytes });
      }
    }

    return coalesce(inserts, deletes, (table) => this.#table(table).schema);
  }

  /**
   * Apply a `TxUpdate` (or `InitialData`) that belongs to a KNOWN
   * subscription query, tracking which rows the query holds so a later
   * {@link releaseQuery} can drop exactly them (SDK-044).
   *
   * The refcount is still by byte identity across queries: a row two queries
   * both deliver is cached once at refcount 2, and each query records it. The
   * difference from {@link applyTxUpdate} is the gate — a query increments a
   * row's refcount only the FIRST time it delivers that row, so a query
   * re-sending a row it already holds (a reconnect replay) is idempotent
   * rather than an inflated count.
   */
  applyQueryDiff(queryId: number, diffs: Iterable<TableDiff>): RowEvent[] {
    const inserts: RowEvent[] = [];
    const deletes: RowEvent[] = [];
    const held = this.#heldByQuery(queryId);

    for (const diff of diffs) {
      const state = this.#table(diff.table);
      const heldKeys = held.get(diff.table) ?? new Set<string>();
      held.set(diff.table, heldKeys);

      // Resolve deletes to their rows before inserts run — an insert under
      // the same PK repoints the projection this lookup depends on.
      const doomed: { pk: string; key: string; bytes: Uint8Array }[] = [];
      for (const entry of diff.deletes) {
        const pk = state.schema.pkOfDelete(entry);
        const key = state.byPk.get(pk);
        if (key === undefined) continue;
        const cached = state.byKey.get(key);
        if (cached !== undefined) doomed.push({ pk, key, bytes: cached.bytes });
      }

      for (const row of diff.inserts) {
        const key = byteKey(row);
        if (heldKeys.has(key)) continue; // this query already holds it: idempotent
        heldKeys.add(key);
        const existing = state.byKey.get(key);
        if (existing !== undefined) {
          existing.refs += 1; // visible through another query too (SDK-044)
          continue;
        }
        state.byKey.set(key, { bytes: row, refs: 1 });
        state.byPk.set(state.schema.pkOfRow(row), key);
        inserts.push({ kind: 'insert', table: diff.table, row });
      }

      for (const { pk, key, bytes } of doomed) {
        if (!heldKeys.has(key)) continue; // this query never held it
        heldKeys.delete(key);
        const entry = state.byKey.get(key);
        if (entry === undefined) continue;
        entry.refs -= 1;
        if (entry.refs > 0) continue;
        state.byKey.delete(key);
        if (state.byPk.get(pk) === key) state.byPk.delete(pk);
        deletes.push({ kind: 'delete', table: diff.table, row: bytes });
      }
    }

    return coalesce(inserts, deletes, (table) => this.#table(table).schema);
  }

  /**
   * Drop a subscription: release every row the query held (SDK-044) and
   * return the net-difference events. A row still held by another query
   * survives at a lower refcount and fires nothing; a row only this query
   * held reaches refcount 0, leaves the cache, and fires one `delete`.
   */
  releaseQuery(queryId: number): RowEvent[] {
    const held = this.#queryKeys.get(queryId);
    if (held === undefined) return [];
    this.#queryKeys.delete(queryId);

    const deletes: RowEvent[] = [];
    for (const [table, keys] of held) {
      const state = this.#tables.get(table);
      if (state === undefined) continue;
      for (const key of keys) {
        const entry = state.byKey.get(key);
        if (entry === undefined) continue;
        entry.refs -= 1;
        if (entry.refs > 0) continue;
        state.byKey.delete(key);
        const pk = state.schema.pkOfRow(entry.bytes);
        if (state.byPk.get(pk) === key) state.byPk.delete(pk);
        deletes.push({ kind: 'delete', table, row: entry.bytes });
      }
    }
    return deletes;
  }

  /**
   * Forget all per-query membership without touching cached rows or their
   * refcounts. Used on reconnect, where {@link reconcile} rebuilds refcounts
   * from the fresh snapshot and {@link trackQuery} then re-establishes which
   * query holds what (the server reassigns query ids on resubscribe).
   */
  resetQueries(): void {
    this.#queryKeys.clear();
  }

  /**
   * Record that `queryId` holds these rows, by byte identity, WITHOUT
   * changing refcounts — the rows are already cached (post-reconcile). The
   * companion of {@link resetQueries} on the reconnect path.
   */
  trackQuery(queryId: number, snapshots: Iterable<TableSnapshot>): void {
    const held = this.#heldByQuery(queryId);
    for (const snapshot of snapshots) {
      const keys = held.get(snapshot.table) ?? new Set<string>();
      held.set(snapshot.table, keys);
      for (const row of snapshot.rows) keys.add(byteKey(row));
    }
  }

  #heldByQuery(queryId: number): Map<string, Set<string>> {
    let held = this.#queryKeys.get(queryId);
    if (held === undefined) {
      held = new Map();
      this.#queryKeys.set(queryId, held);
    }
    return held;
  }

  /**
   * Rebuild from a fresh `InitialData` and return only the net difference
   * (SDK-047).
   *
   * The naive reconnect — clear the cache, reinsert everything — is a callback
   * storm that tells the application every row it already had was deleted and
   * recreated. What an application actually needs to know is what *changed*
   * while it was disconnected, which is what this computes.
   */
  reconcile(snapshots: Iterable<TableSnapshot>): RowEvent[] {
    const inserts: RowEvent[] = [];
    const deletes: RowEvent[] = [];
    const seen = new Set<string>();

    for (const snapshot of snapshots) {
      const state = this.#table(snapshot.table);
      seen.add(snapshot.table);

      const fresh = new Map<string, Uint8Array>();
      for (const row of snapshot.rows) fresh.set(byteKey(row), row);

      for (const [key, entry] of state.byKey) {
        if (!fresh.has(key)) {
          deletes.push({ kind: 'delete', table: snapshot.table, row: entry.bytes });
        }
      }
      for (const [key, row] of fresh) {
        if (!state.byKey.has(key)) {
          inserts.push({ kind: 'insert', table: snapshot.table, row });
        }
      }

      // Refcounts are rebuilt from the fresh data rather than carried over
      // (SDK-047): the old counts describe subscriptions from a session that
      // no longer exists. Duplicate rows within one snapshot are the overlap.
      const rebuilt = new Map<string, Entry>();
      const rebuiltPk = new Map<string, string>();
      for (const row of snapshot.rows) {
        const key = byteKey(row);
        const entry = rebuilt.get(key);
        if (entry !== undefined) {
          entry.refs += 1;
          continue;
        }
        rebuilt.set(key, { bytes: row, refs: 1 });
        rebuiltPk.set(state.schema.pkOfRow(row), key);
      }
      state.byKey = rebuilt;
      state.byPk = rebuiltPk;
    }

    // A table with an active subscription that came back empty sends an empty
    // snapshot; a table absent from InitialData entirely is no longer
    // subscribed, and its rows are gone rather than merely unmentioned.
    for (const [name, state] of this.#tables) {
      if (seen.has(name)) continue;
      for (const entry of state.byKey.values()) {
        deletes.push({ kind: 'delete', table: name, row: entry.bytes });
      }
      state.byKey = new Map();
      state.byPk = new Map();
    }

    this.#stale = false;
    return coalesce(inserts, deletes, (table) => this.#table(table).schema);
  }

  #table(name: string): TableState {
    const state = this.#tables.get(name);
    if (state === undefined) throw new UnknownTableError(name);
    return state;
  }
}

/**
 * Fold delete/insert pairs sharing a primary key into single `update` events
 * (SDK-042), then order the result: inserts, deletes, updates (SDK-045).
 */
function coalesce(
  inserts: RowEvent[],
  deletes: RowEvent[],
  schemaOf: (table: string) => TableSchema,
): RowEvent[] {
  if (inserts.length === 0 || deletes.length === 0) {
    return [...inserts, ...deletes];
  }

  // Index the deletes by table and primary key so each insert can ask whether
  // its key also left in this transaction.
  const pending = new Map<string, RowEvent>();
  for (const event of deletes) {
    if (event.kind !== 'delete') continue;
    pending.set(`${event.table} ${schemaOf(event.table).pkOfRow(event.row)}`, event);
  }

  const finalInserts: RowEvent[] = [];
  const updates: RowEvent[] = [];
  const matched = new Set<RowEvent>();

  for (const event of inserts) {
    if (event.kind !== 'insert') continue;
    const id = `${event.table} ${schemaOf(event.table).pkOfRow(event.row)}`;
    const paired = pending.get(id);
    if (paired !== undefined && paired.kind === 'delete') {
      matched.add(paired);
      updates.push({ kind: 'update', table: event.table, old: paired.row, row: event.row });
      continue;
    }
    finalInserts.push(event);
  }

  const finalDeletes = deletes.filter((event) => !matched.has(event));
  return [...finalInserts, ...finalDeletes, ...updates];
}
