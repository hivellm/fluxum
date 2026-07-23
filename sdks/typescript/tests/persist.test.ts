// Durable client state (SPEC-021 CS-040/CS-041) — store round-trips plus the
// client-level hydrate → resubscribe → reconcile → replay flow, driven
// through a fake Streamable-HTTP server. The scenarios mirror the Rust SDK's
// persistence e2e suite so the two clients behave identically across a
// "reload".

import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { TableSchema } from '../src/cache.ts';
import { FluxumClient } from '../src/client.ts';
import { ClientStore, MemoryBackend } from '../src/persist.ts';
import { decodeMessage, encodeMessage } from '../src/protocol.ts';
import { SESSION_HEADER } from '../src/transport/http.ts';

const TASK: TableSchema = {
  name: 'Task',
  pkOfRow: (row) => String(row[0]),
  pkOfDelete: (entry) => String(entry[0]),
};

function row(pk: number, payload: number): Uint8Array {
  return Uint8Array.from([pk, payload]);
}

// --- Store round-trips --------------------------------------------------------

test('the client store round-trips meta and queries, namespaced', async () => {
  const backend = new MemoryBackend();
  const a = new ClientStore(backend, 'http://h:1', 'cli-a');
  const b = new ClientStore(backend, 'http://h:1', 'cli-b');

  assert.equal(await a.loadMeta(), null, 'cold start hydrates nothing');
  const meta = {
    identity: Uint8Array.from({ length: 32 }, () => 7),
    queue: { clientId: 'cli-a', nextSeq: 3, pending: [] },
  };
  await a.saveMeta(meta);
  assert.deepEqual(await a.loadMeta(), meta);
  assert.equal(await b.loadMeta(), null, 'another client id');

  const query = {
    sql: 'SELECT * FROM Task',
    txOffset: 42,
    tables: [['Task', [row(1, 7), row(2, 9)]]] as [string, Uint8Array[]][],
  };
  await a.saveQuery(query);
  await a.saveQuery(query); // replace, not append
  assert.deepEqual(await a.loadQueries(), [query]);
  assert.deepEqual(await b.loadQueries(), []);

  await a.deleteQuery(query.sql);
  assert.deepEqual(await a.loadQueries(), []);
  assert.notEqual(await a.loadMeta(), null, 'meta untouched by query delete');

  await a.clear();
  assert.equal(await a.loadMeta(), null, 'clear drops the namespace');
});

test('a corrupt blob hydrates as nothing', async () => {
  const backend = new MemoryBackend();
  const store = new ClientStore(backend, 'http://h:1', 'cli-a');
  await store.saveMeta({
    identity: new Uint8Array(32),
    queue: { clientId: 'c', nextSeq: 0, pending: [] },
  });
  await store.saveQuery({ sql: 'SELECT * FROM Task', txOffset: 0, tables: [] });
  for (const key of await backend.list('')) {
    await backend.put(key, Uint8Array.from([0xc1, 0xff]));
  }
  assert.equal(await store.loadMeta(), null, 'corrupt meta = cold start');
  assert.deepEqual(await store.loadQueries(), [], 'corrupt query dropped');
});

// --- The client-level reload flow ----------------------------------------------

interface FakeServer {
  fetch: typeof globalThis.fetch;
  calls: { reducer: string; key: unknown }[];
  subscribes: () => number;
}

/**
 * A fake Streamable-HTTP server whose `Task` table content and session
 * identity are set per scenario phase. Reducer calls are recorded and acked.
 */
function fakeServer(state: { rows: () => Uint8Array[]; identity: () => Uint8Array }): FakeServer {
  const calls: { reducer: string; key: unknown }[] = [];
  let subscribes = 0;

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

  const rowList = (rows: Uint8Array[]): unknown[] => {
    const size = 2;
    const data = new Uint8Array(rows.length * size);
    rows.forEach((r, i) => data.set(r, i * size));
    return [rows.length, ['Fixed', rows.length === 0 ? 0 : size], data];
  };

  const fetchImpl = (async (url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    if (init?.method !== 'POST') {
      return new Response(new ReadableStream<Uint8Array>({ start() {} }));
    }
    const frame = init.body as Uint8Array;
    const { tag, payload } = decodeMessage(frame.subarray(4));
    const id = payload[0];
    if (tag === 'Authenticate') {
      return streaming([encodeMessage('AuthResult', [id, state.identity()])], {
        headers: { [SESSION_HEADER]: `session-${Math.random().toString(16).slice(2)}` },
      });
    }
    if (tag === 'Subscribe') {
      subscribes += 1;
      const queries = payload[1] as string[];
      const replies = queries.map(() =>
        encodeMessage('InitialData', [
          id,
          1,
          [[1, 'Task', 1, rowList(state.rows()), rowList([])]],
        ]),
      );
      return streaming(replies);
    }
    if (tag === 'ReducerCall') {
      calls.push({ reducer: String(payload[1]), key: payload[4] });
      return streaming([encodeMessage('ReducerResult', [id, ['Ok', null]])]);
    }
    throw new Error(`fake server got an unexpected ${tag}`);
  }) as typeof globalThis.fetch;

  return { fetch: fetchImpl, calls, subscribes: () => subscribes };
}

test('a reload hydrates, resubscribes, and reconciles to the net difference', async (t) => {
  const backend = new MemoryBackend();
  const identity = Uint8Array.from({ length: 32 }, () => 9);

  // Session 1: subscribe over rows [1,7] — state is written through.
  const phase = { rows: [row(1, 7)] };
  const server = fakeServer({ rows: () => phase.rows, identity: () => identity });
  const db1 = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [TASK],
    fetch: server.fetch,
    reconnect: false,
    persistence: { backend, clientId: 'cli-1' },
  });
  await db1.subscribe(['SELECT * FROM Task']);
  assert.equal(db1.cache.rows('Task').length, 1);
  await db1.close();

  // The server state moves while "the page is closed": row 1 changed, row 2
  // appeared.
  phase.rows = [row(1, 8), row(2, 5)];

  // Session 2, same backend: NO explicit subscribe — hydration replays the
  // persisted query and reconciles. The net difference fires as events.
  const events: string[] = [];
  const db2 = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [TASK],
    fetch: server.fetch,
    reconnect: false,
    persistence: { backend, clientId: 'cli-1' },
  });
  t.after(async () => {
    await db2.close();
  });
  db2.on('Task:insert', () => events.push('insert'));

  const rows = db2.cache.rows('Task').map((r) => [...r]);
  rows.sort((a, b) => a[0]! - b[0]!);
  assert.deepEqual(
    rows,
    [
      [1, 8],
      [2, 5],
    ],
    'hydrated, resubscribed, reconciled — without an explicit subscribe',
  );
  assert.ok(server.subscribes() >= 2, 'the persisted query was replayed');
});

test('a queued call survives the reload and replays under its original key', async (t) => {
  const backend = new MemoryBackend();
  const identity = Uint8Array.from({ length: 32 }, () => 9);

  // Session 1: the reducer POST fails (offline) — the call stays queued and
  // the queue is persisted.
  let offline = true;
  const server = fakeServer({ rows: () => [], identity: () => identity });
  const failing = (async (url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    if (init?.method === 'POST') {
      const { tag } = decodeMessage((init.body as Uint8Array).subarray(4));
      if (tag === 'ReducerCall' && offline) throw new Error('network down');
    }
    return server.fetch(url, init);
  }) as typeof globalThis.fetch;

  const db1 = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [TASK],
    fetch: failing,
    reconnect: false,
    persistence: { backend, clientId: 'cli-1' },
  });
  await db1.subscribe(['SELECT * FROM Task']);
  const key = await db1.callOptimistic('add_task', ['queued'], (s) => {
    s.insert('Task', row(1, 1));
  });
  assert.equal(db1.pendingMutations, 1, 'queued, unacknowledged');
  await db1.close();

  // Session 2: back online. The restored queue replays during connect.
  offline = false;
  const db2 = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [TASK],
    fetch: failing,
    reconnect: false,
    persistence: { backend, clientId: 'cli-1' },
  });
  t.after(async () => {
    await db2.close();
  });
  assert.equal(server.calls.length, 1, 'replayed exactly once');
  assert.equal(server.calls[0]?.reducer, 'add_task');
  assert.equal(server.calls[0]?.key, key, 'CS-032: the enqueue-time key survived the reload');
  assert.equal(db2.pendingMutations, 0, 'acknowledged and drained');
});

test('a different identity discards the hydrated queue instead of replaying it', async (t) => {
  const backend = new MemoryBackend();
  const who = { identity: Uint8Array.from({ length: 32 }, () => 1) };
  const server = fakeServer({ rows: () => [], identity: () => who.identity });
  const failing = (async (url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    if (init?.method === 'POST') {
      const { tag } = decodeMessage((init.body as Uint8Array).subarray(4));
      if (tag === 'ReducerCall' && who.identity[0] === 1) throw new Error('network down');
    }
    return server.fetch(url, init);
  }) as typeof globalThis.fetch;

  // Alice queues offline on the shared device.
  const alice = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [TASK],
    fetch: failing,
    reconnect: false,
    persistence: { backend, clientId: 'shared-device' },
  });
  await alice.subscribe(['SELECT * FROM Task']);
  await alice.callOptimistic('add_task', ['alice offline'], (s) => {
    s.insert('Task', row(1, 1));
  });
  assert.equal(alice.pendingMutations, 1);
  await alice.close();

  // Bob logs in on the same device: alice's queue must not replay as him.
  who.identity = Uint8Array.from({ length: 32 }, () => 2);
  const bob = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [TASK],
    fetch: failing,
    reconnect: false,
    persistence: { backend, clientId: 'shared-device' },
  });
  t.after(async () => {
    await bob.close();
  });
  assert.equal(bob.pendingMutations, 0, "alice's queue was discarded");
  assert.equal(server.calls.length, 0, 'nothing ever replayed as bob');
  // The store now belongs to bob: alice's slice was cleared, and bob's own
  // write-throughs re-seeded it under HIS identity with an empty queue.
  const meta = await new ClientStore(backend, 'http://localhost:15800', 'shared-device').loadMeta();
  assert.deepEqual(meta?.identity, who.identity, 'the store re-keyed to bob');
  assert.deepEqual(meta?.queue.pending, [], "alice's queued call is gone from disk");
});
