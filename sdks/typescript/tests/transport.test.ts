// Transport tests (SPEC-006 RPC-004..RPC-007, SPEC-011 SDK-082).
//
// The HTTP transport is driven through an injected `fetch`, so these exercise
// the real streaming path — chunk boundaries, session capture, keep-alives —
// without a server.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { encodeMessage, decodeMessage, KEEPALIVE_FRAME } from '../src/protocol.ts';
import { HttpTransport, SESSION_HEADER, FLUXUM_CONTENT_TYPE } from '../src/transport/http.ts';
import { connect } from '../src/transport/connect.ts';
import { SessionExpiredError, TransportError } from '../src/transport/types.ts';

/** A response whose body streams `chunks` one read at a time. */
function streaming(chunks: Uint8Array[], init: ResponseInit = {}): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  return new Response(stream, init);
}

function collect(transport: HttpTransport): string[] {
  const tags: string[] = [];
  transport.onFrame((body) => tags.push(decodeMessage(body).tag));
  return tags;
}


/**
 * Wait for the transport to surface `count` frames.
 *
 * `send` resolves once the request is on the wire, not once its response has
 * been fully read — the frames arrive through `onFrame` afterwards. Asserting
 * straight after `await send(...)` only ever passed by accident of timing.
 */
async function settle(tags: string[], count: number, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (tags.length < count && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

test('a POST response is decoded into message frames', async () => {
  const body = encodeMessage('AuthResult', [true, null]);
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async () => streaming([body]),
  });
  const tags = collect(transport);

  await transport.send(encodeMessage('Authenticate', [1, null, null, null, null]));
  await settle(tags, 1);
  assert.deepEqual(tags, ['AuthResult']);
});

test('the session id is captured from the response and echoed afterwards', async () => {
  // RPC-007: the server issues the id on the first AuthResult; every later
  // request must carry it, or the server sees an unknown session and 404s.
  const seen: (string | null)[] = [];
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async (_url, init) => {
      const headers = new Headers(init?.headers);
      seen.push(headers.get(SESSION_HEADER));
      return streaming([encodeMessage('AuthResult', [true, null])], {
        headers: { [SESSION_HEADER]: 'sess-42' },
      });
    },
  });

  assert.equal(transport.sessionId, null);
  await transport.send(encodeMessage('Authenticate', [1, null, null, null, null]));
  assert.equal(transport.sessionId, 'sess-42');

  await transport.send(encodeMessage('Subscribe', [2, ['SELECT * FROM t']]));
  assert.deepEqual(seen, [null, 'sess-42'], 'the first request has no session, the second does');
});

test('every request declares the binary content type', async () => {
  // RPC-004: the server answers 415 to anything else, so getting this wrong
  // fails every call with a status that looks like a routing problem.
  let contentType: string | null = null;
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async (_url, init) => {
      contentType = new Headers(init?.headers).get('Content-Type');
      return streaming([]);
    },
  });
  await transport.send(encodeMessage('Ping', []));
  assert.equal(contentType, FLUXUM_CONTENT_TYPE);
});

test('a frame split across chunk boundaries still arrives whole', async () => {
  // The property that makes streaming work: chunk boundaries carry no meaning,
  // and a frame may span any number of them.
  const body = encodeMessage('TxUpdate', [7, null]);
  const chunks = [body.subarray(0, 2), body.subarray(2, 5), body.subarray(5)];
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async () => streaming(chunks),
  });
  const tags = collect(transport);

  await transport.send(encodeMessage('Ping', []));
  await settle(tags, 1);
  assert.deepEqual(tags, ['TxUpdate']);
});

test('several frames arriving in one chunk all surface', async () => {
  const a = encodeMessage('InitialData', [1]);
  const b = encodeMessage('TxUpdate', [2, null]);
  const merged = new Uint8Array(a.length + b.length);
  merged.set(a, 0);
  merged.set(b, a.length);

  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async () => streaming([merged]),
  });
  const tags = collect(transport);

  await transport.send(encodeMessage('Ping', []));
  await settle(tags, 2);
  assert.deepEqual(tags, ['InitialData', 'TxUpdate']);
});

test('keep-alives on the stream are consumed, not surfaced', async () => {
  // RPC-006: the server ticks every http_keepalive_s on an idle stream. If one
  // reached the message layer it would decode as a malformed envelope and tear
  // down a perfectly healthy subscription.
  const real = encodeMessage('TxUpdate', [1, null]);
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async () => streaming([KEEPALIVE_FRAME, KEEPALIVE_FRAME, real]),
  });
  const tags = collect(transport);

  await transport.send(encodeMessage('Ping', []));
  await settle(tags, 1);
  assert.deepEqual(tags, ['TxUpdate']);
});

test('HTTP 404 is a typed session-expiry, not a generic failure', async () => {
  // RPC-007/RPC-062: the recovery is re-auth + resubscribe. A caller that saw
  // only "request failed" would retry against a session that is never coming
  // back.
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async () => new Response(null, { status: 404 }),
  });
  await assert.rejects(
    transport.send(encodeMessage('Subscribe', [1, []])),
    (err: unknown) => err instanceof SessionExpiredError && err.code === 404,
  );
});

test('HTTP 415 names the content-type mismatch', async () => {
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async () => new Response(null, { status: 415 }),
  });
  await assert.rejects(transport.send(encodeMessage('Ping', [])), (err: unknown) => {
    assert.ok(err instanceof TransportError);
    assert.equal(err.code, 415);
    assert.match(err.message, /not FluxRPC/);
    return true;
  });
});

test('the push stream refuses to open before authentication', async () => {
  // RPC-007: GET /rpc is identified by the session header, so opening one
  // without a session can only ever 404.
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async () => streaming([]),
  });
  await assert.rejects(transport.openPushStream(), /before authenticating/);
});

test('the push stream delivers server-initiated frames', async () => {
  const push = encodeMessage('TxUpdate', [9, null]);
  const transport = new HttpTransport('http://localhost:15800', {
    fetch: async (_url, init) => {
      if (init?.method === 'GET') return streaming([push]);
      return streaming([encodeMessage('AuthResult', [true, null])], {
        headers: { [SESSION_HEADER]: 'sess-1' },
      });
    },
  });
  const tags = collect(transport);

  await transport.send(encodeMessage('Authenticate', [1, null, null, null, null]));
  await transport.openPushStream();
  // The stream is drained off the call; let its microtasks settle.
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.ok(tags.includes('TxUpdate'), 'a pushed frame reached the handler');
});

test('fluxum:// in a browser fails fast and names the HTTP endpoint', async () => {
  // SDK-082. Simulated by removing `process`, which is how the runtime check
  // decides whether Node's TCP stack exists.
  const saved = (globalThis as { process?: unknown }).process;
  delete (globalThis as { process?: unknown }).process;
  try {
    await assert.rejects(connect('fluxum://db.example.com:15801'), (err: unknown) => {
      assert.ok(err instanceof TransportError);
      assert.match(err.message, /browsers cannot open TCP/);
      assert.match(err.message, /http:\/\/db\.example\.com:15800/);
      return true;
    });
  } finally {
    (globalThis as { process?: unknown }).process = saved;
  }
});

test('an unsupported scheme names the two that work', async () => {
  await assert.rejects(connect('ws://localhost:15800'), (err: unknown) => {
    assert.ok(err instanceof TransportError);
    assert.match(err.message, /fluxum:\/\//);
    assert.match(err.message, /http\(s\):\/\//);
    return true;
  });
});

test('an http(s) URL already ending in /rpc is not doubled', async () => {
  // Otherwise the request lands on /rpc/rpc — a 404 that reads as a dead
  // server rather than a doubled path.
  let requested = '';
  const transport = await connect('http://localhost:15800/rpc', {
    fetch: async (url) => {
      requested = String(url);
      return streaming([]);
    },
  });
  await transport.send(encodeMessage('Ping', []));
  assert.equal(requested, 'http://localhost:15800/rpc');
});
