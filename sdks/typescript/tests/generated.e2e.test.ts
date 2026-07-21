// The demo scenario end-to-end on the GENERATED SDK (T6.5 1.6 — the gate-G6
// input, PRD 12.1).
//
// Everything schema-shaped here comes from `fluxum generate --lang typescript`
// output committed under `./generated/` — the cache hooks (`allTableSchemas`),
// the typed reducer wrappers (`Reducers`), and the typed row decoders
// (`decodeChatMessage`, `decodeTask`). No hand-written column list anywhere in
// this file: that gap is exactly what this test exists to close, and two sync
// gates pin the bindings to the served module (`demo_schema_golden` pins the
// demo module's /schema; `typescript_generated_golden` pins these files to a
// fresh generation from that golden).
//
// The generated files import the packaged runtime ('@hivehub/fluxum' →
// dist/), exactly as an application would — so like the vanilla smoke test
// this skips (visibly) until `npm run build` has produced it.
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { FluxumClient } from '../src/client.ts';
import { toHex } from '../src/fluxbin.ts';
import { BINARY, serverAvailable, startServer } from './support/server.ts';

const DIST = path.resolve(import.meta.dirname, '../dist/index.js');

const skip = !existsSync(DIST)
  ? `no runtime at ${DIST} — run: npm run build`
  : !serverAvailable
    ? `no server binary at ${BINARY} — run: cargo build -p fluxum-server`
    : false;

/** Poll until `check` holds, or fail after five seconds. */
async function until(check: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!check()) {
    assert.ok(Date.now() < deadline, `timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

test('the demo runs end-to-end on the generated SDK (T6.5 1.6)', { skip }, async (t) => {
  // Dynamic so the skip above runs before the generated files (and through
  // them the dist runtime) are resolved.
  const gen = await import('./generated/index.ts');
  assert.equal(typeof gen.SCHEMA_VERSION, 'number');

  const server = await startServer('generated-e2e');
  const db = await FluxumClient.connect({
    url: server.httpUrl,
    // The generated cache hooks — the `tables` option, with zero hand-rolled
    // pk projections (SDK-040 via codegen).
    tables: gen.allTableSchemas(),
    token: new TextEncoder().encode('gen-user'),
  });
  t.after(async () => {
    await db.close();
    await server.stop();
  });

  // A typed insert callback through the generated decoder (SPEC-011
  // acceptance 8): the callback sees a `ChatMessage`, not bytes.
  const chats: Array<{ id: bigint; channel: number; content: string }> = [];
  db.on('ChatMessage:insert', (row) => {
    chats.push(gen.decodeChatMessage(row));
  });

  await db.subscribe(['SELECT * FROM ChatMessage', 'SELECT * FROM Task']);

  // The typed wrappers: argument names, types and arity come from the schema.
  const reducers = new gen.Reducers(db);
  await reducers.send_chat(7, 'typed, end to end');

  await until(() => chats.length === 1, 'the ChatMessage TxUpdate');
  const chat = chats[0]!;
  assert.equal(chat.channel, 7);
  assert.equal(chat.content, 'typed, end to end');
  assert.equal(typeof chat.id, 'bigint', 'a u64 pk decodes as bigint, never number');

  // The per-user task loop: add, decode the cached row (owner_only — visible
  // because this session owns it), then complete it by its server-assigned
  // bigint id. The id crossing back through `callReducer` proves 64-bit
  // arguments survive the wire, not merely the decode.
  await reducers.add_task('ship the generated bindings');
  await until(() => db.cache.rows('Task').length === 1, 'the Task row');
  const task = gen.decodeTask(db.cache.rows('Task')[0]!);
  assert.equal(task.done, false);
  assert.ok(db.identity, 'authenticated');
  assert.equal(task.owner, toHex(db.identity), 'the owner is this session, typed as Identity');

  await reducers.complete_task(task.id);
  await until(() => {
    const rows = db.cache.rows('Task');
    return rows.length === 1 && gen.decodeTask(rows[0]!).done;
  }, 'the completed Task update');
});
