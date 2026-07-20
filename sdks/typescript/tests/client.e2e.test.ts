// `FluxumClient` against the real server (SPEC-011 acceptance 8).
//
// The companion of e2e.test.ts, one layer up: that one proves the wire by
// hand-building envelopes, this one proves the client — id correlation,
// reducer outcomes, cache application, typed callbacks.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { FluxumClient, ReducerError, SchemaMismatchError } from '../src/client.ts';
import type { TableSchema } from '../src/cache.ts';
import { RowReader } from '../src/fluxbin.ts';
import { BINARY, serverAvailable, startServer } from './support/server.ts';

// ChatMessage is (id: u64, sender: Identity, channel: u32, content: Str,
// sent_at: Timestamp). The cache only needs a stable key per row, and the
// primary key is the leading u64 — so the projection reads just that.
const CHAT: TableSchema = {
  name: 'ChatMessage',
  pkOfRow: (row) => String(new RowReader(row).read('U64')),
  pkOfDelete: (entry) => String(new RowReader(entry).read('U64')),
};

const TASK: TableSchema = {
  name: 'Task',
  pkOfRow: (row) => String(new RowReader(row).read('U64')),
  pkOfDelete: (entry) => String(new RowReader(entry).read('U64')),
};

const skip = serverAvailable
  ? false
  : `no server binary at ${BINARY} — run: cargo build -p fluxum-server`;

test('the client drives a real session end to end', { skip }, async (t) => {
  const server = await startServer('client-e2e');
  const db = await FluxumClient.connect({
    url: server.httpUrl,
    tables: [CHAT, TASK],
  });
  // Ordering matters: the client goes down first. Killing the server while a
  // client is live starts its reconnect loop, which by default never gives up
  // and would hold the process open past the end of the run.
  t.after(async () => {
    await db.close();
    await server.stop();
  });

  assert.ok(db.identity, 'the server derived an identity for the session');

  // A typed callback, registered before the rows exist.
  const inserted: string[] = [];
  db.on('ChatMessage:insert', (row) => {
    // Columns in declaration order — FluxBIN is positional, so reading them
    // out of order silently yields plausible garbage rather than an error.
    const reader = new RowReader(row);
    reader.read('U64'); // id
    reader.read('Identity'); // sender
    reader.read('U32'); // channel
    inserted.push(String(reader.read('Str')));
  });

  await db.subscribe(['SELECT * FROM ChatMessage']);
  assert.equal(db.cache.size, 0, 'a fresh database starts empty');

  await db.callReducer('send_chat', [1, 'hello from the client']);

  // The TxUpdate arrives on the push stream, independently of the reducer's
  // own reply — so wait for the cache rather than assuming ordering.
  const deadline = Date.now() + 5000;
  while (db.cache.size === 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }

  assert.equal(db.cache.size, 1, 'the row reached the local cache');
  assert.deepEqual(inserted, ['hello from the client'], 'the typed callback fired with the row');
});

test('a rejected reducer surfaces as a typed error', { skip }, async (t) => {
  const server = await startServer('client-reject');
  const db = await FluxumClient.connect({ url: server.httpUrl, tables: [CHAT] });
  t.after(async () => {
    await db.close();
    await server.stop();
  });

  // The demo module rejects an empty message.
  await assert.rejects(db.callReducer('send_chat', [1, '']), (err: unknown) => {
    assert.ok(err instanceof ReducerError, `expected ReducerError, got ${String(err)}`);
    assert.match(err.message, /empty/);
    return true;
  });
});

test('concurrent reducer calls are correlated by id, not by arrival', { skip }, async (t) => {
  // RPC-002: responses may come back out of order. A client that assumed
  // request/response pairing by arrival would hand each caller someone else's
  // outcome — and would look correct whenever the server happened to be
  // sequential.
  const server = await startServer('client-mux');
  const db = await FluxumClient.connect({ url: server.httpUrl, tables: [CHAT] });
  t.after(async () => {
    await db.close();
    await server.stop();
  });

  await db.subscribe(['SELECT * FROM ChatMessage']);

  const results = await Promise.allSettled([
    db.callReducer('send_chat', [1, 'first']),
    db.callReducer('send_chat', [1, '']), // rejected
    db.callReducer('send_chat', [1, 'third']),
  ]);

  assert.equal(results[0]?.status, 'fulfilled');
  assert.equal(results[1]?.status, 'rejected', 'the empty one, and only it, failed');
  assert.equal(results[2]?.status, 'fulfilled');
});

const ONLINE: TableSchema = {
  name: 'OnlineUser',
  pkOfRow: (row) => new RowReader(row).read('Identity') as string,
  pkOfDelete: (entry) => new RowReader(entry).read('Identity') as string,
};

test('a multi-query subscribe populates every table, not just the first', { skip }, async (t) => {
  // The server answers a batched Subscribe with one InitialData PER QUERY,
  // each echoing the same request id (RPC-032). A client that resolves its
  // pending id on the first reply drops the rest silently — the first table
  // fills and the others look simply empty, with no error to notice.
  const server = await startServer('client-multiquery');
  const db = await FluxumClient.connect({
    url: server.httpUrl,
    tables: [CHAT, TASK, ONLINE],
  });
  // One hook, registered once `batched` exists below, so every client is
  // closed before the server dies — otherwise the survivors start reconnect
  // loops that never give up and hold the process open.
  let batched: FluxumClient | null = null;
  t.after(async () => {
    await Promise.all([db.close(), batched?.close()]);
    await server.stop();
  });

  // Give each of the three tables a row: chat and task by calling reducers,
  // OnlineUser by simply being connected (the on_connect hook).
  await db.subscribe(['SELECT * FROM ChatMessage']);
  await db.callReducer('send_chat', [1, 'row for the batch test']);
  await db.callReducer('add_task', ['task for the batch test']);

  // A second client subscribes to all three at once — the case under test.
  batched = await FluxumClient.connect({
    url: server.httpUrl,
    tables: [CHAT, TASK, ONLINE],
    token: new TextEncoder().encode('batched'),
  });

  await batched.subscribe([
    'SELECT * FROM ChatMessage',
    'SELECT * FROM Task',
    'SELECT * FROM OnlineUser',
  ]);

  assert.ok(batched.cache.rows('ChatMessage').length > 0, 'first query populated');
  assert.ok(
    batched.cache.rows('OnlineUser').length > 0,
    'the LAST query populated too — this is what silently broke',
  );
});

test('stale bindings surface a typed SchemaMismatchError, not mistyped rows', { skip }, async (t) => {
  // SPEC-011 acceptance 9 against the real server: a client whose embedded
  // schema_version does not match detects it on InitialData, refreshes the
  // REAL GET /schema (loopback passes the SEC-054 guard), and — since the
  // refreshed document confirms the bindings are stale — surfaces the typed
  // error without ever applying a row. A matching client on the same server
  // proves the gate does not false-positive.
  const server = await startServer('client-schema-drill');
  const stale = await FluxumClient.connect({
    url: server.httpUrl,
    tables: [CHAT],
    schemaVersion: 999,
  });
  const current = await FluxumClient.connect({
    url: server.httpUrl,
    tables: [CHAT],
    schemaVersion: 1,
    token: new TextEncoder().encode('current'),
  });
  t.after(async () => {
    await Promise.all([stale.close(), current.close()]);
    await server.stop();
  });

  const inserted: Uint8Array[] = [];
  stale.on('ChatMessage:insert', (row) => inserted.push(row));

  await assert.rejects(stale.subscribe(['SELECT * FROM ChatMessage']), (err: unknown) => {
    assert.ok(err instanceof SchemaMismatchError, `expected SchemaMismatchError, got ${String(err)}`);
    assert.equal(err.expected, 999);
    assert.equal(err.actual, 1, 'the refreshed /schema document named the real version');
    assert.match(err.message, /fluxum generate/, 'the error tells the user the way out');
    return true;
  });
  assert.equal(stale.cache.size, 0, 'nothing was applied under the wrong schema');
  assert.equal(inserted.length, 0, 'no callback fired with a mistyped row');

  await current.subscribe(['SELECT * FROM ChatMessage']);
  assert.equal(current.cache.size, 0, 'the matching client subscribed cleanly');
});

test('owner_only rows are filtered by the server, not the client', { skip }, async (t) => {
  // DM-060: two identities subscribing to the same query get different rows.
  // The demo's Task table carries #[visibility(owner_only(owner))].
  const server = await startServer('client-visibility');
  // Distinct tokens, or this test proves nothing: the dev `none` provider
  // derives Identity = SHA-256(token), so two clients authenticating with the
  // same (empty) token are literally the same user, and owner_only would
  // correctly show them the same rows.
  const alice = await FluxumClient.connect({
    url: server.httpUrl,
    tables: [TASK],
    token: new TextEncoder().encode('alice'),
  });
  const bob = await FluxumClient.connect({
    url: server.httpUrl,
    tables: [TASK],
    token: new TextEncoder().encode('bob'),
  });
  assert.notDeepEqual(alice.identity, bob.identity, 'the two clients are different identities');
  t.after(async () => {
    await Promise.all([alice.close(), bob.close()]);
    await server.stop();
  });

  await alice.subscribe(['SELECT * FROM Task']);
  await bob.subscribe(['SELECT * FROM Task']);

  await alice.callReducer('add_task', ['alice writes a test']);

  const deadline = Date.now() + 5000;
  while (alice.cache.size === 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }

  assert.equal(alice.cache.size, 1, 'alice sees her own task');
  assert.equal(bob.cache.size, 0, "bob never receives alice's task");
});
