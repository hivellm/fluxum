// Optimistic mutations with server reconciliation (SPEC-021 CS-010..012) and
// the offline replay queue (CS-032) — the TypeScript mirror of the Rust SDK's
// `optimistic.rs` + `idempotency.rs`, with the same observable behaviour so
// the two clients reconcile identically.
//
// The design keeps two worlds separate: the **base** is the authoritative
// `RowCache`, mutated only by server data; each in-flight optimistic call is
// an **overlay layer** — an ordered list of upserts/deletes by primary key,
// applied over the base in submission order (CS-012). What the application
// sees is the *effective view* (base + layers), and every transition emits
// the net difference as one atomic event batch. That construction is the
// CS-011 guarantee: the optimistic→authoritative swap is an `update` (or
// nothing, when the bytes match), never a delete/insert flicker, and a
// rolled-back layer is gone — no later update can resurrect its rows.
//
// A layer is dropped when its own commit's `TxUpdate` applies (matched
// client-side: `caller` = this identity, `reducer_name` FIFO per reducer —
// commits on one connection happen in submission order), in the SAME
// transition that applies the authoritative rows. The `Ok` ack drops a layer
// early only when holding it would be pointless: its ops are already
// shadowed by the base, or the client holds no subscriptions at all. `Err`
// rolls the layer back on the spot.

import { RowCache } from './cache.ts';
import type { RowEvent, TableDiff, TableSchema, TableSnapshot } from './cache.ts';

/** One local mutation recorded by an optimistic updater (CS-010). */
export type OptimisticOp =
  | { kind: 'insert'; table: string; row: Uint8Array }
  | { kind: 'delete'; table: string; pk: string };

/**
 * The local store handed to an optimistic updater (CS-010): read the
 * effective view, record upserts and deletes. The recorded ops become one
 * overlay layer, applied atomically.
 */
export class OptimisticStore {
  readonly #cache: SyncedCache;
  readonly ops: OptimisticOp[] = [];

  constructor(cache: SyncedCache) {
    this.#cache = cache;
  }

  /** The effective rows of `table` as the mutation begins. */
  rows(table: string): Uint8Array[] {
    return this.#cache.rows(table);
  }

  /** Record an insert-or-replace of `row` (upsert by primary key). */
  insert(table: string, row: Uint8Array): void {
    this.ops.push({ kind: 'insert', table, row });
  }

  /** Record a delete of the row under `pk` (a `TableSchema.pkOfRow` string). */
  delete(table: string, pk: string): void {
    this.ops.push({ kind: 'delete', table, pk });
  }
}

/** One in-flight optimistic call's overlay. */
interface Layer {
  id: number;
  reducer: string;
  ops: OptimisticOp[];
  /** `ReducerResult` Ok received; held only until its update applies. */
  confirmed: boolean;
}

/** The effective `pk → row` view of one table. */
type View = Map<string, Uint8Array>;

/**
 * The authoritative {@link RowCache} plus the ordered optimistic overlay
 * (CS-010/CS-012) — what a resilient client holds as its cache. Read surface
 * (`rows`, `size`, `stale`) reflects the EFFECTIVE view; the raw base stays
 * reachable via {@link SyncedCache.authoritative}. When no layers are in
 * flight every path short-circuits to the plain `RowCache` behaviour.
 */
export class SyncedCache {
  readonly #base: RowCache;
  #layers: Layer[] = [];
  #nextLayer = 1;

  constructor(schemas: Iterable<TableSchema>) {
    this.#base = new RowCache(schemas);
  }

  /** The authoritative cache, untouched by any overlay. */
  get authoritative(): RowCache {
    return this.#base;
  }

  /** True while the connection is down (SDK-047). */
  get stale(): boolean {
    return this.#base.stale;
  }

  /** Mark the cache as behind the server. */
  markStale(): void {
    this.#base.markStale();
  }

  /** How many optimistic layers are currently in flight. */
  get optimisticLength(): number {
    return this.#layers.length;
  }

  /**
   * Effective rows for `table`: base rows in insertion order with the
   * overlay applied — replaced rows stay in place, overlay-new rows append
   * in submission order (CS-012).
   */
  rows(table: string): Uint8Array[] {
    if (this.#layers.length === 0) return this.#base.rows(table);
    const view: ({ pk: string; row: Uint8Array } | null)[] = this.#base
      .pkRows(table)
      .map(([pk, row]) => ({ pk, row }));
    const index = new Map<string, number>();
    view.forEach((slot, i) => {
      if (slot !== null) index.set(slot.pk, i);
    });
    for (const layer of this.#layers) {
      for (const op of layer.ops) {
        if (op.table !== table) continue;
        if (op.kind === 'insert') {
          const pk = this.#base.projectPk(table, op.row);
          if (pk === null) continue; // unregistered table: nothing to show
          const at = index.get(pk);
          if (at !== undefined) view[at] = { pk, row: op.row };
          else {
            index.set(pk, view.length);
            view.push({ pk, row: op.row });
          }
        } else {
          const at = index.get(op.pk);
          if (at !== undefined) {
            view[at] = null;
            index.delete(op.pk);
          }
        }
      }
    }
    return view.filter((slot) => slot !== null).map((slot) => slot.row);
  }

  /** Total effective rows across every registered table. */
  get size(): number {
    if (this.#layers.length === 0) return this.#base.size;
    let total = 0;
    for (const table of this.#base.tableNames()) total += this.rows(table).length;
    return total;
  }

  /**
   * Apply per-query authoritative diffs (a `TxUpdate` or `InitialData`).
   * When the update is this client's own commit, pass the reducer name as
   * `ownReducer` so the matching layer drops **in the same event batch** —
   * that single-transition diff is the no-flicker guarantee (CS-011).
   */
  applyTx(byQuery: Iterable<[number, TableDiff[]]>, ownReducer: string | null): RowEvent[] {
    if (this.#layers.length === 0) {
      const events: RowEvent[] = [];
      for (const [queryId, diffs] of byQuery) {
        events.push(...this.#base.applyQueryDiff(queryId, diffs));
      }
      return events;
    }
    const tables = this.#touchedTables();
    const before = this.#snapshotViews(tables);
    const baseEvents: RowEvent[] = [];
    for (const [queryId, diffs] of byQuery) {
      baseEvents.push(...this.#base.applyQueryDiff(queryId, diffs));
    }
    if (ownReducer !== null) this.#noteOwnTx(ownReducer);
    return this.#finish(before, tables, baseEvents);
  }

  /** Drop a subscription, translated through the overlay. */
  releaseQuery(queryId: number): RowEvent[] {
    if (this.#layers.length === 0) return this.#base.releaseQuery(queryId);
    const tables = this.#touchedTables();
    const before = this.#snapshotViews(tables);
    const baseEvents = this.#base.releaseQuery(queryId);
    return this.#finish(before, tables, baseEvents);
  }

  /**
   * Rebuild from a fresh `InitialData`, translated through the overlay:
   * queued optimistic rows stay visible on top of the reconciled base until
   * their calls resolve.
   */
  reconcile(snapshots: Iterable<TableSnapshot>): RowEvent[] {
    if (this.#layers.length === 0) return this.#base.reconcile(snapshots);
    const tables = this.#touchedTables();
    const before = this.#snapshotViews(tables);
    const baseEvents = this.#base.reconcile(snapshots);
    return this.#finish(before, tables, baseEvents);
  }

  /**
   * Forward of {@link RowCache.querySnapshot} — the authoritative rows a
   * subscription holds, the unit the durable client state persists per
   * query (CS-040). Optimistic overlays are deliberately absent.
   */
  querySnapshot(queryId: number): TableSnapshot[] {
    return this.#base.querySnapshot(queryId);
  }

  /** Forward of {@link RowCache.resetQueries} (no visible rows change). */
  resetQueries(): void {
    this.#base.resetQueries();
  }

  /** Forward of {@link RowCache.trackQuery} (no visible rows change). */
  trackQuery(queryId: number, snapshots: Iterable<TableSnapshot>): void {
    this.#base.trackQuery(queryId, snapshots);
  }

  /**
   * Run an optimistic updater and apply its ops as a new overlay layer
   * (CS-010). Returns the layer id — the handle {@link confirm} /
   * {@link rollback} take — and the events of the transition.
   */
  applyOptimistic(
    reducer: string,
    updater: (store: OptimisticStore) => void,
  ): { layer: number; events: RowEvent[] } {
    const store = new OptimisticStore(this);
    updater(store);

    const tables = this.#touchedTables();
    for (const op of store.ops) tables.add(op.table);
    const before = this.#snapshotViews(tables);
    const id = this.#nextLayer++;
    this.#layers.push({ id, reducer, ops: store.ops, confirmed: false });
    return { layer: id, events: this.#finish(before, tables, []) };
  }

  /**
   * Roll a layer back (CS-011, the `Err` path): remove it and return the
   * net events. Unknown ids are a no-op — the layer already resolved.
   */
  rollback(layerId: number): RowEvent[] {
    const at = this.#layers.findIndex((l) => l.id === layerId);
    if (at < 0) return [];
    const tables = this.#touchedTables();
    const before = this.#snapshotViews(tables);
    this.#layers.splice(at, 1);
    return this.#finish(before, tables, []);
  }

  /**
   * Record the `Ok` ack for a layer (CS-011). The layer drops now if its
   * authoritative update can never arrive (`noSubscriptions`) or the base
   * already shadows every op; otherwise it holds until {@link applyTx}
   * matches its own `TxUpdate`.
   */
  confirm(layerId: number, noSubscriptions: boolean): RowEvent[] {
    const layer = this.#layers.find((l) => l.id === layerId);
    if (layer === undefined) return [];
    layer.confirmed = true;
    if (!noSubscriptions && !this.#fullyShadowed(layer)) return [];
    const tables = this.#touchedTables();
    const before = this.#snapshotViews(tables);
    this.#dropLayerAndOlder(layerId);
    return this.#finish(before, tables, []);
  }

  // --- Internals ------------------------------------------------------------

  /**
   * FIFO-attribute one own-commit `TxUpdate` to the oldest live layer for
   * `reducer` and drop it: its authoritative rows are in the base as of this
   * very transition, so the drop diffs clean (CS-011). The ack arriving
   * later finds the layer gone and is a no-op.
   */
  #noteOwnTx(reducer: string): void {
    const layer = this.#layers.find((l) => l.reducer === reducer);
    if (layer === undefined) return; // not one of ours
    this.#dropLayerAndOlder(layer.id);
  }

  /**
   * Remove `layerId` plus every *older* confirmed layer: same-shard commits
   * are ordered, so once a newer call's update has applied an older
   * confirmed call's update is provably not still in flight.
   */
  #dropLayerAndOlder(layerId: number): void {
    const at = this.#layers.findIndex((l) => l.id === layerId);
    if (at < 0) return;
    this.#layers = this.#layers.filter(
      (layer, i) => !(i === at || (i < at && layer.confirmed)),
    );
  }

  /**
   * Whether the base already reflects every op of `layer`: each inserted
   * primary key resolves to a base row (bytes may differ — dropping then
   * yields an update, not a flicker) and each deleted key is absent.
   */
  #fullyShadowed(layer: Layer): boolean {
    return layer.ops.every((op) => {
      const basePks = new Set(this.#base.pkRows(op.table).map(([pk]) => pk));
      if (op.kind === 'insert') {
        const pk = this.#base.projectPk(op.table, op.row);
        return pk === null ? true : basePks.has(pk);
      }
      return !basePks.has(op.pk);
    });
  }

  /** Every table any live layer touches, sorted. */
  #touchedTables(): Set<string> {
    const tables = new Set<string>();
    for (const layer of this.#layers) {
      for (const op of layer.ops) tables.add(op.table);
    }
    return new Set([...tables].sort());
  }

  /** The effective `pk → row` view of each named table. */
  #snapshotViews(tables: Set<string>): Map<string, View> {
    const views = new Map<string, View>();
    for (const table of tables) {
      const view: View = new Map(this.#base.pkRows(table));
      for (const layer of this.#layers) {
        for (const op of layer.ops) {
          if (op.table !== table) continue;
          if (op.kind === 'insert') {
            const pk = this.#base.projectPk(table, op.row);
            if (pk !== null) view.set(pk, op.row);
          } else {
            view.delete(op.pk);
          }
        }
      }
      views.set(table, view);
    }
    return views;
  }

  /**
   * Close a transition: diff the touched tables' effective views against
   * `before`, pass base events for untouched tables through unchanged, and
   * order the batch inserts → deletes → updates (SDK-045).
   */
  #finish(before: Map<string, View>, tables: Set<string>, baseEvents: RowEvent[]): RowEvent[] {
    const after = this.#snapshotViews(tables);
    const inserts: RowEvent[] = [];
    const deletes: RowEvent[] = [];
    const updates: RowEvent[] = [];
    for (const table of tables) {
      const old = before.get(table) ?? new Map<string, Uint8Array>();
      const fresh = after.get(table) ?? new Map<string, Uint8Array>();
      for (const pk of [...fresh.keys()].sort()) {
        // Non-null: pk was just taken from the map's own keys.
        const row = fresh.get(pk) as Uint8Array;
        const prev = old.get(pk);
        if (prev === undefined) inserts.push({ kind: 'insert', table, row });
        else if (!bytesEqual(prev, row)) updates.push({ kind: 'update', table, old: prev, row });
      }
      for (const pk of [...old.keys()].sort()) {
        if (!fresh.has(pk)) {
          deletes.push({ kind: 'delete', table, row: old.get(pk) as Uint8Array });
        }
      }
    }
    for (const event of baseEvents) {
      if (tables.has(event.table)) continue;
      if (event.kind === 'insert') inserts.push(event);
      else if (event.kind === 'delete') deletes.push(event);
      else updates.push(event);
    }
    return [...inserts, ...deletes, ...updates];
  }
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

// --- The offline replay queue (CS-032) ---------------------------------------

/** A call queued for submission, carrying its stable idempotency key. */
export interface QueuedCall {
  /** The reducer to run. */
  reducer: string;
  /** Its positional arguments, as the wire encoder takes them. */
  args: readonly unknown[];
  /** The key minted at enqueue. Stable across every resend — that stability
   *  IS the exactly-once guarantee (CS-032). */
  idempotencyKey: string;
  /** How many times it has been handed to the transport. */
  attempts: number;
}

/** A serializable image of an {@link OfflineQueue} (CS-040). */
export interface QueueSnapshot {
  clientId: string;
  nextSeq: number;
  pending: QueuedCall[];
}

/**
 * An offline replay queue that mints a stable `idempotency_key` per call
 * (CS-032), so reconnect replay is safe. Keys are namespaced by `clientId`;
 * a DURABLE queue must reuse the id it persisted (CS-040) or a call queued
 * before a restart would replay under a fresh key and double-apply.
 */
export class OfflineQueue {
  #clientId: string;
  #nextSeq = 0;
  #pending: QueuedCall[] = [];

  constructor(clientId: string) {
    this.#clientId = clientId;
  }

  /** Rebuild a queue from a persisted snapshot: pending calls keep their
   *  minted keys and the counter resumes where it left off. */
  static restore(snapshot: QueueSnapshot): OfflineQueue {
    const queue = new OfflineQueue(snapshot.clientId);
    queue.#nextSeq = snapshot.nextSeq;
    queue.#pending = snapshot.pending.map((call) => ({ ...call }));
    return queue;
  }

  /** Enqueue a call, minting its stable key now (CS-032). Returns the key. */
  enqueue(reducer: string, args: readonly unknown[]): string {
    const key = `${this.#clientId}:${this.#nextSeq}`;
    this.#nextSeq += 1;
    this.#pending.push({ reducer, args, idempotencyKey: key, attempts: 0 });
    return key;
  }

  /** The calls awaiting acknowledgement, oldest first. */
  get pending(): readonly QueuedCall[] {
    return this.#pending;
  }

  /** Whether anything is awaiting acknowledgement. */
  get isEmpty(): boolean {
    return this.#pending.length === 0;
  }

  /**
   * Hand a specific queued call to the transport, bumping its attempt count.
   * The key is untouched — that is the point. `null` if the key is not
   * queued (already acknowledged).
   */
  attempt(idempotencyKey: string): QueuedCall | null {
    const call = this.#pending.find((c) => c.idempotencyKey === idempotencyKey);
    if (call === undefined) return null;
    call.attempts += 1;
    return call;
  }

  /**
   * Drop a call once the server has acknowledged it. An ack for a
   * deduplicated replay is an ack like any other. Returns whether the key
   * was still queued.
   */
  acknowledge(idempotencyKey: string): boolean {
    const before = this.#pending.length;
    this.#pending = this.#pending.filter((c) => c.idempotencyKey !== idempotencyKey);
    return this.#pending.length !== before;
  }

  /** A point-in-time image of the whole queue, for durable persistence. */
  snapshot(): QueueSnapshot {
    return {
      clientId: this.#clientId,
      nextSeq: this.#nextSeq,
      pending: this.#pending.map((call) => ({ ...call })),
    };
  }
}
