// End-to-end against a REAL running server, at the wire level.
//
// The companion of client.e2e.test.ts, one layer down: that one drives
// `FluxumClient`, this one hand-builds envelopes so a client bug and a wire
// bug cannot hide each other.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { decodeMessage, encodeMessage } from '../src/protocol.ts';
import { HttpTransport } from '../src/transport/http.ts';

import { BINARY, serverAvailable, startServer } from './support/server.ts';

test(
  'the full loop against a live server: auth, subscribe, reduce, receive',
  {
    skip: serverAvailable
      ? false
      : `no server binary at ${BINARY} — run: cargo build -p fluxum-server`,
  },
  async (t) => {
    const server = await startServer('wire-e2e');
    t.after(() => server.stop());

    const transport = new HttpTransport(server.httpUrl);

    const messages: { tag: string; payload: unknown[] }[] = [];
    transport.onFrame((body) => messages.push(decodeMessage(body)));

    // 1. Authenticate. The development profile runs the `none` provider, so
    //    the credential fields are empty — the point here is the handshake and
    //    the Fluxum-Session the server hands back (RPC-007).
    // Fields are positional (compact MessagePack writes a struct as an array):
    // id, token, compression, tx_updates, namespace. The token is `bin`, not
    // nil — an empty one under the `none` provider.
    await transport.send(
      encodeMessage('Authenticate', [1, new Uint8Array(0), null, null, null]),
    );

    const auth = messages.find((m) => m.tag === 'AuthResult');
    assert.ok(auth, `expected AuthResult, got ${JSON.stringify(messages.map((m) => m.tag))}`);
    assert.ok(transport.sessionId, 'the server issued a session id');

    // 2. Subscribe to the demo module's public chat table.
    messages.length = 0;
    await transport.send(encodeMessage('Subscribe', [2, ['SELECT * FROM ChatMessage']]));

    const initial = messages.find((m) => m.tag === 'InitialData');
    assert.ok(initial, `expected InitialData, got ${JSON.stringify(messages.map((m) => m.tag))}`);

    // 3. Open the push stream, then call the reducer. The TxUpdate arrives on
    //    the stream rather than the POST response — that split is the whole
    //    reason GET /rpc exists (RPC-006).
    await transport.openPushStream();
    messages.length = 0;

    // id, reducer, version, args, idempotency_key — `version` sits BEFORE the
    // arguments, which is easy to get backwards and lands as a 400.
    await transport.send(
      encodeMessage('ReducerCall', [3, 'send_chat', null, [1, 'hello from the SDK'], null]),
    );

    // 4. Wait for the TxUpdate the commit fans out.
    const deadline = Date.now() + 5000;
    while (!messages.some((m) => m.tag === 'TxUpdate') && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }

    const tags = messages.map((m) => m.tag);
    assert.ok(
      tags.includes('TxUpdate'),
      `expected a TxUpdate after send_chat, got ${JSON.stringify(tags)}`,
    );
  },
);
