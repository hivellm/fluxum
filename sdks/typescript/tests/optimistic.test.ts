// Optimistic mutations + offline queue (SPEC-021 CS-010..012, CS-032) — the
// TypeScript mirror of the Rust SDK's `optimistic.rs`/`idempotency.rs` unit
// suite, scenario for scenario, so the two clients reconcile identically.

import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { TableDiff, TableSchema } from '../src/cache.ts';
import { FluxumClient } from '../src/client.ts';
import { OfflineQueue, SyncedCache } from '../src/optimistic.ts';
import type { OptimisticOp } from '../src/optimistic.ts';
import { decodeMessage, encodeMessage } from '../src/protocol.ts';
import { SESSION_HEADER } from '../src/transport/http.ts';

/** Row is `[pk, payload]`; a delete entry is `[pk]`. */
function taskSchema(name = 'Task'): TableSchema {
  return {
    name,
    pkOfRow: (row) => String(row[0]),
    pkOfDelete: (entry) => String(entry[0]),
  };
}

function cache(): SyncedCache {
  return new SyncedCache([taskSchema()]);
}

function row(pk: number, payload: number): Uint8Array {
  return Uint8Array.from([pk, payload]);
}

function del(pk: number): Uint8Array {
  return Uint8Array.from([pk]);
}

function tx(inserts: Uint8Array[], deletes: Uint8Array[]): [number, TableDiff[]][] {
  return [[1, [{ table: 'Task', inserts, deletes }]]];
}

function titles(c: SyncedCache): number[][] {
  return c.rows('Task').map((r) => [...r]);
}

test('an optimistic insert is visible immediately', () => {
  const c = cache();
  const { events } = c.applyOptimistic('add', (s) => s.insert('Task', row(1, 7)));
  assert.deepEqual(events, [{ kind: 'insert', table: 'Task', row: row(1, 7) }]);
  assert.deepEqual(titles(c), [[1, 7]]);
  assert.equal(c.authoritative.rows('Task').length, 0, 'base untouched');
});

test('confirm after own tx swaps without flicker', () => {
  const c = cache();
  const { layer } = c.applyOptimistic('add', (s) => s.insert('Task', row(1, 7)));
  const events = c.applyTx(tx([row(1, 7)], []), 'add');
  assert.deepEqual(events, [], 'same bytes, no re-render');
  assert.equal(c.optimisticLength, 0, 'matched tx dropped the layer');
  assert.deepEqual(c.confirm(layer, false), [], 'late ack is a no-op');
  assert.deepEqual(titles(c), [[1, 7]]);
});

test('differing authoritative bytes arrive as one update', () => {
  const c = cache();
  c.applyOptimistic('add', (s) => s.insert('Task', row(1, 0)));
  const events = c.applyTx(tx([row(1, 9)], []), 'add');
  assert.deepEqual(events, [{ kind: 'update', table: 'Task', old: row(1, 0), row: row(1, 9) }]);
  assert.deepEqual(titles(c), [[1, 9]]);
});

test('ack before tx holds the overlay until the update lands', () => {
  const c = cache();
  const { layer } = c.applyOptimistic('add', (s) => s.insert('Task', row(1, 7)));
  assert.deepEqual(c.confirm(layer, false), []);
  assert.equal(c.optimisticLength, 1, 'confirmed but held');
  assert.deepEqual(titles(c), [[1, 7]], 'still rendered');

  assert.deepEqual(c.applyTx(tx([row(1, 7)], []), 'add'), []);
  assert.equal(c.optimisticLength, 0);
});

test('a rejected mutation rolls back to the exact prior state', () => {
  const c = cache();
  c.applyTx(tx([row(1, 1)], []), null);
  const { layer } = c.applyOptimistic('edit', (s) => {
    s.insert('Task', row(1, 9));
    s.insert('Task', row(2, 2));
  });
  assert.deepEqual(titles(c), [
    [1, 9],
    [2, 2],
  ]);

  const events = c.rollback(layer);
  assert.deepEqual(events, [
    { kind: 'delete', table: 'Task', row: row(2, 2) },
    { kind: 'update', table: 'Task', old: row(1, 9), row: row(1, 1) },
  ]);
  assert.deepEqual(titles(c), [[1, 1]]);
});

test('a rolled-back row is never resurrected', () => {
  const c = cache();
  const { layer } = c.applyOptimistic('add', (s) => s.insert('Task', row(5, 5)));
  c.rollback(layer);
  const events = c.applyTx(tx([row(1, 1)], []), null);
  assert.deepEqual(events, [{ kind: 'insert', table: 'Task', row: row(1, 1) }]);
  assert.deepEqual(titles(c), [[1, 1]], 'row 5 stays gone');
});

test('concurrent layers reconcile in submission order', () => {
  const c = cache();
  c.applyTx(tx([row(1, 0)], []), null);
  c.applyOptimistic('edit', (s) => s.insert('Task', row(1, 5)));
  const { layer: b } = c.applyOptimistic('edit', (s) => s.insert('Task', row(1, 8)));
  assert.deepEqual(titles(c), [[1, 8]], 'submission order');

  const events = c.rollback(b);
  assert.deepEqual(events, [{ kind: 'update', table: 'Task', old: row(1, 8), row: row(1, 5) }]);
  assert.deepEqual(titles(c), [[1, 5]]);
});

test('an optimistic delete hides the base row until confirmed', () => {
  const c = cache();
  c.applyTx(tx([row(1, 1)], []), null);
  const { events } = c.applyOptimistic('remove', (s) => s.delete('Task', '1'));
  assert.deepEqual(events, [{ kind: 'delete', table: 'Task', row: row(1, 1) }]);
  assert.equal(c.rows('Task').length, 0);

  assert.deepEqual(c.applyTx(tx([], [del(1)]), 'remove'), []);
  assert.equal(c.optimisticLength, 0);
});

test('confirm drops immediately when the base already shadows the ops', () => {
  const c = cache();
  const { layer } = c.applyOptimistic('add', (s) => s.insert('Task', row(1, 7)));
  c.applyTx(tx([row(1, 7)], []), null); // no attribution
  assert.deepEqual(c.confirm(layer, false), []);
  assert.equal(c.optimisticLength, 0);
});

test('confirm with no subscriptions drops the layer', () => {
  const c = cache();
  const { layer } = c.applyOptimistic('add', (s) => s.insert('Task', row(1, 7)));
  const events = c.confirm(layer, true);
  assert.deepEqual(events, [{ kind: 'delete', table: 'Task', row: row(1, 7) }]);
  assert.equal(c.optimisticLength, 0);
});

test('dropping a layer drops older confirmed layers too', () => {
  const c = cache();
  const { layer: a } = c.applyOptimistic('a', (s) => s.insert('Task', row(1, 1)));
  c.applyOptimistic('b', (s) => s.insert('Task', row(2, 2)));
  assert.deepEqual(c.confirm(a, false), [], 'A held');

  const events = c.applyTx(tx([row(2, 2)], []), 'b');
  assert.equal(c.optimisticLength, 0, 'B matched, A force-dropped');
  assert.deepEqual(events, [{ kind: 'delete', table: 'Task', row: row(1, 1) }]);
});

test('fifo attribution matches same-reducer calls in order', () => {
  const c = cache();
  c.applyOptimistic('add', (s) => s.insert('Task', row(1, 1)));
  c.applyOptimistic('add', (s) => s.insert('Task', row(2, 2)));

  c.applyTx(tx([row(1, 1)], []), 'add');
  assert.equal(c.optimisticLength, 1, 'only the first layer matched');
  c.applyTx(tx([row(2, 2)], []), 'add');
  assert.equal(c.optimisticLength, 0);
  assert.deepEqual(titles(c), [
    [1, 1],
    [2, 2],
  ]);
});

test('unregistered tables are ignored, not fatal', () => {
  const c = cache();
  const { layer, events } = c.applyOptimistic('add', (s) => s.insert('Ghost', row(1, 1)));
  assert.deepEqual(events, []);
  assert.deepEqual(c.confirm(layer, false), []);
  assert.equal(c.optimisticLength, 0, 'shadowed-by-vacuity drops it');
});

test('reconcile keeps pending optimistic rows on top', () => {
  const c = cache();
  c.applyTx(tx([row(1, 1)], []), null);
  c.applyOptimistic('add', (s) => s.insert('Task', row(9, 9)));

  const events = c.reconcile([{ table: 'Task', rows: [row(1, 1), row(2, 2)] }]);
  assert.deepEqual(events, [{ kind: 'insert', table: 'Task', row: row(2, 2) }]);
  assert.deepEqual(titles(c), [
    [1, 1],
    [2, 2],
    [9, 9],
  ]);
});

// --- The offline queue (CS-032/CS-040) ---------------------------------------

test('every queued call gets its own stable key, namespaced per client', () => {
  const a = new OfflineQueue('client-a');
  const b = new OfflineQueue('client-b');
  const k1 = a.enqueue('transfer', []);
  const k2 = a.enqueue('transfer', []);
  assert.notEqual(k1, k2, 'distinct calls are distinct submissions');
  assert.notEqual(a.enqueue('x', []), b.enqueue('x', []), 'namespaced');
});

test('a retry reuses the key it was enqueued with', () => {
  const queue = new OfflineQueue('client-a');
  const key = queue.enqueue('transfer', []);
  const seen = [queue.attempt(key), queue.attempt(key), queue.attempt(key)];
  assert.deepEqual(
    seen.map((c) => c?.idempotencyKey),
    [key, key, key],
    'CS-032: the key is stable across retries',
  );
  assert.equal(queue.pending[0]?.attempts, 3);
});

test('acknowledging removes the call; duplicate acks are no-ops', () => {
  const queue = new OfflineQueue('client-a');
  const k1 = queue.enqueue('transfer', []);
  const k2 = queue.enqueue('refund', []);
  assert.equal(queue.acknowledge(k1), true);
  assert.equal(queue.pending.length, 1);
  assert.equal(queue.acknowledge(k1), false);
  assert.equal(queue.attempt(k1), null, 'acknowledged: gone');
  assert.equal(queue.acknowledge(k2), true);
  assert.equal(queue.isEmpty, true);
});

test('a snapshot round-trips with original keys and counter', () => {
  const queue = new OfflineQueue('client-a');
  const k1 = queue.enqueue('transfer', []);
  const restored = OfflineQueue.restore(JSON.parse(JSON.stringify(queue.snapshot())));
  assert.equal(restored.pending[0]?.idempotencyKey, k1);
  assert.notEqual(restored.enqueue('transfer', []), k1, 'the counter resumed');
});

// --- Offline replay through the client (CS-032) ------------------------------

test('a call queued while the send fails replays on reconnect under its key', async (t) => {
  // The fake Streamable-HTTP server: session 1 accepts auth + subscribe but
  // FAILS the reducer POST (an outage mid-call); closing the push stream
  // then drives the client's reconnect, whose reconcile step must replay
  // the queued call — same idempotency key, exactly once.
  let failCalls = false;
  // Behind an object property: TS's flow analysis cannot see assignments
  // made inside the stream's start() callback.
  const push: { controller: ReadableStreamDefaultController<Uint8Array> | null } = {
    controller: null,
  };
  const calls: { reducer: string; key: unknown }[] = [];
  let session = 0;

  const streaming = (frames: Uint8Array[], init: ResponseInit = {}): Response =>
    new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          for (const frame of frames) controller.enqueue(frame);
          controller.close();
        },
      }),
      init,
    );

  const fetchImpl = (async (url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    if (init?.method !== 'POST') {
      return new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            push.controller = controller;
          },
        }),
      );
    }
    const frame = init.body as Uint8Array;
    const { tag, payload } = decodeMessage(frame.subarray(4));
    const id = payload[0];
    if (tag === 'Authenticate') {
      session += 1;
      return streaming([encodeMessage('AuthResult', [id, new Uint8Array(32)])], {
        headers: { [SESSION_HEADER]: `session-${session}` },
      });
    }
    if (tag === 'Subscribe') {
      const queries = payload[1] as string[];
      const empty = [0, ['Fixed', 0], new Uint8Array(0)];
      const replies = queries.map(() =>
        encodeMessage('InitialData', [id, 1, [[1, 'Task', 1, empty, empty]]]),
      );
      return streaming(replies);
    }
    if (tag === 'ReducerCall') {
      if (failCalls) throw new Error('network down');
      calls.push({ reducer: String(payload[1]), key: payload[4] });
      return streaming([encodeMessage('ReducerResult', [id, ['Ok', null]])]);
    }
    throw new Error(`fake server got an unexpected ${tag}`);
  }) as typeof globalThis.fetch;

  const db = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [taskSchema()],
    fetch: fetchImpl,
    reconnect: { initialMs: 1, maxMs: 5, jitter: 0 },
  });
  t.after(async () => {
    await db.close();
  });
  await db.subscribe(['SELECT * FROM Task']);

  failCalls = true;
  const key = await db.callOptimistic('add_task', ['queued offline'], (s) => {
    s.insert('Task', row(1, 7));
  });
  assert.equal(db.pendingMutations, 1, 'the failed send leaves the call queued');
  assert.equal(db.cache.rows('Task').length, 1, 'and rendered');
  assert.equal(calls.length, 0, 'nothing reached the server yet');

  // The outage ends; the push stream dies, the client reconnects and the
  // reconcile step replays the queue.
  failCalls = false;
  push.controller?.close();

  const deadline = Date.now() + 5000;
  while ((calls.length === 0 || db.pendingMutations > 0) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.equal(calls.length, 1, 'replayed exactly once');
  assert.equal(calls[0]?.reducer, 'add_task');
  assert.equal(calls[0]?.key, key, 'CS-032: the replay reuses the enqueue-time key');
  assert.equal(db.pendingMutations, 0, 'the ack drained the queue');
  assert.equal(db.cache.rows('Task').length, 1, 'the optimistic row never flickered');
});

// --- Property test (task 1.8): random interleavings converge ----------------

/** Deterministic xorshift32 — reproducible failures, no dependency. */
function rng(seed: number): () => number {
  let x = seed;
  return () => {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    x >>>= 0;
    return x;
  };
}

test('property: random optimistic interleavings converge bit-identical', () => {
  for (let seed = 1; seed <= 25; seed += 1) {
    const next = rng(seed * 2654435761);
    const c = cache();
    const server = new Map<string, Uint8Array>();
    const inflight: { layer: number; ops: OptimisticOp[] }[] = [];

    // Commit ops and return the NET wire diff, as the server broadcasts it.
    const commit = (ops: OptimisticOp[]): { inserts: Uint8Array[]; deletes: Uint8Array[] } => {
      const before = new Map(server);
      for (const op of ops) {
        if (op.kind === 'insert') server.set(String(op.row[0]), op.row);
        else server.delete(op.pk);
      }
      const inserts: Uint8Array[] = [];
      const deletes: Uint8Array[] = [];
      for (const [pk, fresh] of server) {
        const prev = before.get(pk);
        if (prev === undefined) inserts.push(fresh);
        else if (!prev.every((b, i) => b === fresh[i]) || prev.length !== fresh.length) {
          deletes.push(Uint8Array.from([Number(pk)]));
          inserts.push(fresh);
        }
      }
      for (const pk of before.keys()) {
        if (!server.has(pk)) deletes.push(Uint8Array.from([Number(pk)]));
      }
      return { inserts, deletes };
    };

    const resolveOldest = (): void => {
      const call = inflight.shift();
      if (call === undefined) return;
      if (next() % 4 === 0) {
        c.rollback(call.layer); // rejected: nothing commits
        return;
      }
      const { inserts, deletes } = commit(call.ops);
      if (inserts.length === 0 && deletes.length === 0) {
        c.confirm(call.layer, false);
        return;
      }
      if (next() % 2 === 0) {
        c.applyTx(tx(inserts, deletes), 'mutate');
        c.confirm(call.layer, false);
      } else {
        c.confirm(call.layer, false);
        c.applyTx(tx(inserts, deletes), 'mutate');
      }
    };

    for (let step = 0; step < 50; step += 1) {
      const dice = next() % 3;
      if (dice === 0) {
        const ops: OptimisticOp[] = [];
        for (let i = 0; i <= next() % 2; i += 1) {
          const pk = next() % 6;
          if (next() % 4 === 0) ops.push({ kind: 'delete', table: 'Task', pk: String(pk) });
          else ops.push({ kind: 'insert', table: 'Task', row: row(pk, next() % 250) });
        }
        const { layer } = c.applyOptimistic('mutate', (s) => {
          for (const op of ops) {
            if (op.kind === 'insert') s.insert(op.table, op.row);
            else s.delete(op.table, op.pk);
          }
        });
        inflight.push({ layer, ops });
      } else if (dice === 1 && inflight.length > 0) {
        resolveOldest();
      } else {
        const pk = next() % 6;
        const { inserts, deletes } = commit([
          { kind: 'insert', table: 'Task', row: row(pk, next() % 250) },
        ]);
        if (inserts.length > 0 || deletes.length > 0) c.applyTx(tx(inserts, deletes), null);
      }
    }
    while (inflight.length > 0) resolveOldest();

    assert.equal(c.optimisticLength, 0, `seed ${seed}: layers must drain`);
    const got = titles(c).sort((a, b) => a[0]! - b[0]!);
    const want = [...server.values()].map((r) => [...r]).sort((a, b) => a[0]! - b[0]!);
    assert.deepEqual(got, want, `seed ${seed}: cache != server state`);
  }
});
