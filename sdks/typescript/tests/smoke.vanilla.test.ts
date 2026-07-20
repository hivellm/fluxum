// The vanilla-JS smoke test (SPEC-011 SDK-081), scripted.
//
// SDK-081's promise is that a plain `<script type="module">` page can use the
// SDK with no build step. What makes that true is the artifact: a single-file
// ESM bundle with every dependency inlined, loadable from a URL with no
// package resolution behind it. That is exactly how this test consumes it —
// `import(fileURL)` of `dist/fluxum.min.js`, never `import '@hivehub/fluxum'`
// — and then drives the full loop the demo page performs: connect, subscribe,
// call a reducer, receive the resulting TxUpdate as a typed callback.
//
// Runs in Node rather than Chromium: the browser-engine half of the story is
// the conformance corpus's job (phase6_sdk-conformance-corpus, run in Node AND
// headless Chromium). What THIS pins is the packaging — the bundle stands
// alone and its public exports carry a real session.
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { pathToFileURL } from 'node:url';

import { BINARY, serverAvailable, startServer } from './support/server.ts';

const BUNDLE = path.resolve(import.meta.dirname, '../dist/fluxum.min.js');

const skip = !existsSync(BUNDLE)
  ? `no bundle at ${BUNDLE} — run: npm run build`
  : !serverAvailable
    ? `no server binary at ${BINARY} — run: cargo build -p fluxum-server`
    : false;

test('the packaged bundle drives a session with no build step (SDK-081)', { skip }, async (t) => {
  // The import the demo page performs, byte for byte the same artifact.
  const sdk = (await import(pathToFileURL(BUNDLE).href)) as typeof import('../src/index.ts');

  const server = await startServer('vanilla-smoke');
  const db = await sdk.FluxumClient.connect({
    url: server.httpUrl,
    tables: [
      {
        name: 'ChatMessage',
        pkOfRow: (row) => String(new sdk.RowReader(row).read('U64')),
        pkOfDelete: (entry) => String(new sdk.RowReader(entry).read('U64')),
      },
    ],
  });
  t.after(async () => {
    await db.close();
    await server.stop();
  });

  const contents: string[] = [];
  db.on('ChatMessage:insert', (row) => {
    const reader = new sdk.RowReader(row);
    reader.read('U64'); // id
    reader.read('Identity'); // sender
    reader.read('U32'); // channel
    contents.push(String(reader.read('Str')));
  });

  await db.subscribe(['SELECT * FROM ChatMessage']);
  await db.callReducer('send_chat', [1, 'no build step']);

  // The TxUpdate rides the push stream, so wait for it rather than assume.
  const deadline = Date.now() + 5000;
  while (contents.length === 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }

  assert.deepEqual(contents, ['no build step'], 'the TxUpdate reached a typed callback');
  assert.equal(db.cache.size, 1, 'and the row is in the local cache');
});
