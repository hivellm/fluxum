// The schema-mismatch drill (SDK-043, SPEC-011 acceptance 9).
//
// Driven through an injected `fetch` because the drill's interesting paths
// need a server whose schema_version CHANGES between connections — a real
// server cannot be migrated mid-test, but a fake one answers whatever the
// scenario needs: transient mismatch (healed by the reconnect), confirmed
// mismatch with /schema reachable (refused before any reconnect), and
// confirmed mismatch with /schema guarded (refused after the one retry).
//
// The invariant every case asserts: a mismatched InitialData NEVER reaches
// the cache. Mistyped rows are the one outcome SDK-043 exists to prevent.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { FluxumClient, SchemaMismatchError } from '../src/client.ts';
import type { TableSchema } from '../src/cache.ts';
import { decodeMessage, encodeMessage } from '../src/protocol.ts';
import { SESSION_HEADER } from '../src/transport/http.ts';

const T: TableSchema = {
  name: 'T',
  pkOfRow: (row) => String(row[0]),
  pkOfDelete: (entry) => String(entry[0]),
};

/** A finite streaming response carrying `frames` back to back. */
function streaming(frames: Uint8Array[], init: ResponseInit = {}): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const frame of frames) controller.enqueue(frame);
      controller.close();
    },
  });
  return new Response(stream, init);
}

/** A push stream that stays open and quiet, like an idle GET /rpc. */
function idlePushStream(): Response {
  return new Response(new ReadableStream<Uint8Array>({ start() {} }));
}

/** One 4-byte row per RowList, or none. */
function rowList(rows: number[][]): unknown[] {
  const size = 4;
  const data = new Uint8Array(rows.length * size);
  rows.forEach((row, i) => data.set(row, i * size));
  return [rows.length, ['Fixed', rows.length === 0 ? 0 : size], data];
}

interface FakeServerScenario {
  /** schema_version answered by the Nth Subscribe (last entry repeats). */
  versions: number[];
  /** Rows the table carries once the version finally matches. */
  rows?: number[][];
  /** The GET /schema endpoint. Defaults to the SEC-054 guard refusing. */
  schema?: () => Response;
}

/** A fake Streamable-HTTP server good enough for the drill's choreography. */
function fakeServer(scenario: FakeServerScenario): {
  fetch: typeof globalThis.fetch;
  subscribes: () => number;
  schemaFetches: () => number;
} {
  let subscribes = 0;
  let schemaFetches = 0;

  const fetchImpl = (async (url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    if (String(url).endsWith('/schema')) {
      schemaFetches += 1;
      return scenario.schema?.() ?? new Response('{"success":false}', { status: 403 });
    }
    if (init?.method !== 'POST') return idlePushStream();

    const frame = init.body as Uint8Array;
    const { tag, payload } = decodeMessage(frame.subarray(4));
    const id = payload[0];

    if (tag === 'Authenticate') {
      return streaming([encodeMessage('AuthResult', [id, new Uint8Array(32)])], {
        headers: { [SESSION_HEADER]: 'session-1' },
      });
    }
    if (tag === 'Subscribe') {
      const version = scenario.versions[Math.min(subscribes, scenario.versions.length - 1)];
      subscribes += 1;
      // Every test embeds schemaVersion 1 in the client. An InitialData under
      // any other version carries a poison row — if it ever shows up in the
      // cache, the "never mistyped rows" invariant broke.
      const rows = version === 1 ? (scenario.rows ?? []) : [[9, 9, 9, 9]];
      const queries = payload[1] as string[];
      const replies = queries.map(() =>
        encodeMessage('InitialData', [id, version, [[1, 'T', 1, rowList(rows), rowList([])]]]),
      );
      return streaming(replies);
    }
    throw new Error(`fake server got an unexpected ${tag}`);
  }) as typeof globalThis.fetch;

  return { fetch: fetchImpl, subscribes: () => subscribes, schemaFetches: () => schemaFetches };
}

const FAST_RECONNECT = { initialMs: 1, maxMs: 5, jitter: 0 };

test('a transient mismatch heals through refresh + reconnect, silently', async (t) => {
  // The server answers the first Subscribe with version 2 (a migration
  // window), and version 1 from then on. /schema already reports 1 — the
  // version the bindings embed — so the drill reconnects instead of failing.
  const server = fakeServer({
    versions: [2, 1],
    rows: [[7, 0, 0, 0]],
    schema: () =>
      new Response(JSON.stringify({ success: true, payload: { schema_version: 1 } })),
  });
  const db = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [T],
    schemaVersion: 1,
    fetch: server.fetch,
    reconnect: FAST_RECONNECT,
  });
  t.after(() => db.close());

  const inserted: Uint8Array[] = [];
  db.on('T:insert', (row) => inserted.push(row));

  await db.subscribe(['SELECT * FROM T']);

  assert.equal(server.schemaFetches(), 1, 'the drill refreshed the schema');
  assert.equal(server.subscribes(), 2, 'one reconnect, one resubscribe');
  assert.equal(db.cache.size, 1, 'the matching InitialData was applied');
  assert.equal(inserted.length, 1, 'the reconciled row fired its callback');
  assert.equal(inserted[0]?.[0], 7, 'and it is the post-migration row, not the mistyped one');
});

test('a reachable /schema confirming the mismatch fails fast, without reconnecting', async (t) => {
  // Refresh says the server is on 2 and the bindings embed 1: reconnecting
  // cannot regenerate types, so the error surfaces before any retry.
  const server = fakeServer({
    versions: [2],
    schema: () =>
      new Response(JSON.stringify({ success: true, payload: { schema_version: 2 } })),
  });
  const db = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [T],
    schemaVersion: 1,
    fetch: server.fetch,
    reconnect: FAST_RECONNECT,
  });
  t.after(() => db.close());

  await assert.rejects(db.subscribe(['SELECT * FROM T']), (err: unknown) => {
    assert.ok(err instanceof SchemaMismatchError, `expected SchemaMismatchError, got ${String(err)}`);
    assert.equal(err.expected, 1);
    assert.equal(err.actual, 2);
    return true;
  });

  assert.equal(server.subscribes(), 1, 'no reconnect was attempted');
  assert.equal(db.cache.size, 0, 'the mismatched rows never reached the cache');
});

test('with /schema unreachable, one reconnect confirms and the loop stops', async (t) => {
  // A remote browser client: the admin guard (SEC-054) refuses /schema, so
  // InitialData is the only witness. The drill reconnects exactly once; the
  // second sighting confirms, surfaces the typed error, and does NOT leave a
  // reconnect loop hammering a server that will answer 2 forever.
  const server = fakeServer({ versions: [2] });
  const db = await FluxumClient.connect({
    url: 'http://localhost:15800',
    tables: [T],
    schemaVersion: 1,
    fetch: server.fetch,
    reconnect: FAST_RECONNECT,
  });
  t.after(() => db.close());

  const surfaced: Error[] = [];
  db.onError((err) => surfaced.push(err));

  await assert.rejects(db.subscribe(['SELECT * FROM T']), (err: unknown) => {
    assert.ok(err instanceof SchemaMismatchError);
    assert.equal(err.actual, 2, 'the actual version comes from InitialData');
    return true;
  });

  assert.equal(server.schemaFetches(), 1, 'the refresh was attempted');
  assert.equal(server.subscribes(), 2, 'exactly one reconnect-and-recheck');
  assert.equal(db.cache.size, 0, 'no mistyped row was ever applied');

  // The failure is terminal: give the loop a beat and confirm it stayed dead.
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.equal(server.subscribes(), 2, 'no further reconnect attempts after confirmation');
  assert.equal(surfaced.length, 0, 'the error went to the awaiting subscribe, not onError');
});
