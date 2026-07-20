// Row cache semantics (SDK-040, SDK-042, SDK-044, SDK-045, SDK-047).
//
// The scenarios here are the ones SPEC-011 acceptance 8 and 13 name directly,
// because they are the ones where a plausible-looking cache is wrong: refcount
// dedupe across overlapping queries, delete+insert coalescing, and reconnect
// reconciliation that must not turn into a callback storm.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { RowCache, UnknownTableError } from '../src/cache.ts';
import type { RowEvent, TableSchema } from '../src/cache.ts';

// A row is [pk, payload]; a delete entry is [pk]. Enough structure to exercise
// the projection without pulling a schema or FluxBIN into these tests.
function row(pk: number, payload = 0): Uint8Array {
  return new Uint8Array([pk, payload]);
}
function del(pk: number): Uint8Array {
  return new Uint8Array([pk]);
}

const TASK: TableSchema = {
  name: 'Task',
  pkOfRow: (r) => String(r[0]),
  pkOfDelete: (e) => String(e[0]),
};

function cache(): RowCache {
  return new RowCache([TASK]);
}

function kinds(events: RowEvent[]): string[] {
  return events.map((e) => e.kind);
}

test('a first insert fires one insert event and caches the row', () => {
  const c = cache();
  const events = c.applyTxUpdate([{ table: 'Task', inserts: [row(1)], deletes: [] }]);

  assert.deepEqual(kinds(events), ['insert']);
  assert.equal(c.refcount('Task', row(1)), 1);
  assert.equal(c.size, 1);
});

test('overlapping subscriptions dedupe into one row with a refcount', () => {
  // SDK-044 / acceptance 8: two queries covering the same row produce one
  // cached row with refcount 2 and a single insert callback.
  const c = cache();
  const first = c.applyTxUpdate([{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  const second = c.applyTxUpdate([{ table: 'Task', inserts: [row(1)], deletes: [] }]);

  assert.deepEqual(kinds(first), ['insert']);
  assert.deepEqual(kinds(second), [], 'the second query fires nothing');
  assert.equal(c.refcount('Task', row(1)), 2);
  assert.equal(c.size, 1, 'one cached row, not two');
});

test('dropping one of two overlapping queries fires no callback', () => {
  // The 1→0 transition is what fires a delete; 2→1 is invisible to the app.
  const c = cache();
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1)], deletes: [] }]);

  const events = c.applyTxUpdate([{ table: 'Task', inserts: [], deletes: [del(1)] }]);
  assert.deepEqual(kinds(events), [], 'still visible through the other query');
  assert.equal(c.refcount('Task', row(1)), 1);

  const last = c.applyTxUpdate([{ table: 'Task', inserts: [], deletes: [del(1)] }]);
  assert.deepEqual(kinds(last), ['delete'], 'the 1→0 transition fires');
  assert.equal(c.size, 0);
});

test('a byte-identical delete+insert pair in one update fires nothing', () => {
  // SDK-045 / acceptance 8. This is precisely what applying inserts before
  // deletes buys: the refcount goes 1→2→1 instead of 1→0→1, so the row never
  // appears to leave.
  const c = cache();
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1, 7)], deletes: [] }]);

  const events = c.applyTxUpdate([
    { table: 'Task', inserts: [row(1, 7)], deletes: [del(1)] },
  ]);

  assert.deepEqual(kinds(events), [], 'nothing actually changed');
  assert.equal(c.refcount('Task', row(1, 7)), 1);
  assert.equal(c.size, 1);
});

test('delete+insert of the same pk with different bytes is one update', () => {
  // SDK-042 / acceptance 8: exactly one `:update`, never a delete/insert pair.
  const c = cache();
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1, 1)], deletes: [] }]);

  const events = c.applyTxUpdate([
    { table: 'Task', inserts: [row(1, 2)], deletes: [del(1)] },
  ]);

  assert.deepEqual(kinds(events), ['update']);
  const [event] = events;
  assert.ok(event !== undefined && event.kind === 'update');
  assert.deepEqual([...event.old], [1, 1], 'old carries the previously cached bytes');
  assert.deepEqual([...event.row], [1, 2]);

  assert.equal(c.size, 1, 'the old row is gone, not lingering');
  assert.equal(c.refcount('Task', row(1, 2)), 1);
  assert.equal(c.refcount('Task', row(1, 1)), 0);
});

test('events are ordered inserts, then deletes, then updates', () => {
  // SDK-045 fixes the dispatch order so applications can rely on it.
  const c = cache();
  c.applyTxUpdate([
    { table: 'Task', inserts: [row(1, 1), row(2, 1)], deletes: [] },
  ]);

  const events = c.applyTxUpdate([
    // pk 3 is new (insert), pk 2 leaves (delete), pk 1 changes (update).
    { table: 'Task', inserts: [row(3, 1), row(1, 9)], deletes: [del(2), del(1)] },
  ]);

  assert.deepEqual(kinds(events), ['insert', 'delete', 'update']);
});

test('a delete for a row that was never cached is ignored', () => {
  const c = cache();
  const events = c.applyTxUpdate([{ table: 'Task', inserts: [], deletes: [del(99)] }]);
  assert.deepEqual(kinds(events), []);
  assert.equal(c.size, 0);
});

test('an unknown table is a typed error, not a silent no-op', () => {
  const c = cache();
  assert.throws(
    () => c.applyTxUpdate([{ table: 'Ghost', inserts: [row(1)], deletes: [] }]),
    (err: unknown) => err instanceof UnknownTableError && err.table === 'Ghost',
  );
});

test('a wide row keys correctly', () => {
  // The byte-key builder chunks its input; a row past one chunk must still
  // produce a distinct, stable key rather than blowing the argument limit.
  const c = cache();
  const wide = new Uint8Array(10_000);
  wide.fill(7);
  wide[0] = 1;
  const other = new Uint8Array(10_000);
  other.fill(7);
  other[0] = 2;

  const schema: TableSchema = {
    name: 'Wide',
    pkOfRow: (r) => String(r[0]),
    pkOfDelete: (e) => String(e[0]),
  };
  const wideCache = new RowCache([schema]);
  wideCache.applyTxUpdate([{ table: 'Wide', inserts: [wide, other], deletes: [] }]);

  assert.equal(wideCache.size, 2, 'two distinct wide rows');
  assert.equal(wideCache.refcount('Wide', wide), 1);
});

// --- Reconnect reconciliation (SDK-047, acceptance 13) ----------------------

test('reconciliation emits only the net difference', () => {
  // The whole point: no delete-everything/reinsert-everything storm. Of three
  // cached rows, one is untouched, one changed, one left, and one is new.
  const c = cache();
  c.applyTxUpdate([
    { table: 'Task', inserts: [row(1, 1), row(2, 1), row(3, 1)], deletes: [] },
  ]);
  c.markStale();
  assert.equal(c.stale, true);

  const events = c.reconcile([
    { table: 'Task', rows: [row(1, 1), row(2, 9), row(4, 1)] },
  ]);

  // pk1 unchanged → nothing. pk2 changed → update. pk3 gone → delete.
  // pk4 new → insert.
  assert.deepEqual(kinds(events), ['insert', 'delete', 'update']);
  assert.equal(events.filter((e) => e.kind === 'insert').length, 1);
  assert.equal(c.stale, false, 'reconciling clears the stale mark');
  assert.equal(c.size, 3);
});

test('an unchanged cache reconciles to silence', () => {
  const c = cache();
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1, 1), row(2, 1)], deletes: [] }]);
  c.markStale();

  const events = c.reconcile([{ table: 'Task', rows: [row(1, 1), row(2, 1)] }]);
  assert.deepEqual(kinds(events), [], 'nothing changed while disconnected');
  assert.equal(c.size, 2);
});

test('reconciliation rebuilds refcounts from the fresh data', () => {
  // The old counts describe a session that no longer exists. Carrying them
  // over would leave a row that now has one query looking like it has two,
  // and its eventual delete would never fire.
  const c = cache();
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  assert.equal(c.refcount('Task', row(1)), 2);

  c.markStale();
  c.reconcile([{ table: 'Task', rows: [row(1)] }]);
  assert.equal(c.refcount('Task', row(1)), 1, 'rebuilt from one occurrence');
});

test('a table missing from InitialData has its rows removed', () => {
  // Absent means no longer subscribed — the rows are gone, not merely
  // unmentioned. Leaving them would be exactly the "stale rows surviving
  // unreconciled" SDK-047 forbids.
  const c = cache();
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  c.markStale();

  const events = c.reconcile([]);
  assert.deepEqual(kinds(events), ['delete']);
  assert.equal(c.size, 0);
});

test('an empty snapshot for a subscribed table clears it', () => {
  const c = cache();
  c.applyTxUpdate([{ table: 'Task', inserts: [row(1), row(2)], deletes: [] }]);
  c.markStale();

  const events = c.reconcile([{ table: 'Task', rows: [] }]);
  assert.deepEqual(kinds(events), ['delete', 'delete']);
  assert.equal(c.size, 0);
});

// --- Per-query attribution and unsubscribe (SDK-044) ------------------------

test('two queries hold one refcounted row; dropping one keeps it, dropping both frees it', () => {
  // The whole point of per-query tracking: an overlapping row survives the
  // loss of one subscription and fires nothing, then leaves on the last.
  const c = cache();
  const a = c.applyQueryDiff(1, [{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  const b = c.applyQueryDiff(2, [{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  assert.deepEqual(kinds(a), ['insert'], 'first query caches and fires');
  assert.deepEqual(kinds(b), [], 'second query dedupes, fires nothing');
  assert.equal(c.refcount('Task', row(1)), 2);

  const dropA = c.releaseQuery(1);
  assert.deepEqual(kinds(dropA), [], 'still held by query 2: no callback');
  assert.equal(c.refcount('Task', row(1)), 1);
  assert.equal(c.size, 1);

  const dropB = c.releaseQuery(2);
  assert.deepEqual(kinds(dropB), ['delete'], 'last holder gone: one delete');
  assert.equal(c.size, 0);
});

test('unsubscribing a query drops the rows only it held', () => {
  const c = cache();
  c.applyQueryDiff(1, [{ table: 'Task', inserts: [row(1), row(2)], deletes: [] }]);
  c.applyQueryDiff(2, [{ table: 'Task', inserts: [row(2), row(3)], deletes: [] }]);
  assert.equal(c.size, 3, 'rows 1,2,3 with 2 shared');

  const events = c.releaseQuery(1);
  // Row 1 was query 1's alone (delete); row 2 is still held by query 2 (kept).
  assert.deepEqual(kinds(events), ['delete']);
  assert.equal(c.refcount('Task', row(1)), 0, 'row 1 gone');
  assert.equal(c.refcount('Task', row(2)), 1, 'row 2 survives on query 2');
  assert.equal(c.refcount('Task', row(3)), 1, 'row 3 untouched');
});

test('a query re-delivering a row it already holds is idempotent', () => {
  // A reconnect replay must not inflate the refcount: the same query
  // sending the same row twice holds it once.
  const c = cache();
  c.applyQueryDiff(1, [{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  const again = c.applyQueryDiff(1, [{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  assert.deepEqual(kinds(again), [], 'no second insert');
  assert.equal(c.refcount('Task', row(1)), 1, 'held once, not twice');
});

test('a delete under a query releases that query’s hold', () => {
  const c = cache();
  c.applyQueryDiff(1, [{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  c.applyQueryDiff(2, [{ table: 'Task', inserts: [row(1)], deletes: [] }]);
  // Query 1 sees the row deleted (PK-only entry).
  const ev = c.applyQueryDiff(1, [{ table: 'Task', inserts: [], deletes: [del(1)] }]);
  assert.deepEqual(kinds(ev), [], 'query 2 still holds it: no callback');
  assert.equal(c.refcount('Task', row(1)), 1);
  // Now releasing query 2 frees it — query 1 no longer holds it, so no
  // double-release.
  assert.deepEqual(kinds(c.releaseQuery(1)), [], 'query 1 already released its hold');
  assert.deepEqual(kinds(c.releaseQuery(2)), ['delete']);
  assert.equal(c.size, 0);
});
