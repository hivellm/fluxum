// End-to-end against a REAL running server (SPEC-011 acceptance 8).
//
// Everything else in this suite mocks the far side. This one does not: it
// spawns `fluxum-server` with the demo module linked in and drives the full
// loop — authenticate, subscribe, call a reducer, receive the TxUpdate, decode
// the row — over the same Streamable HTTP path a browser uses.
//
// Skipped, loudly, when the binary is absent (`cargo build -p fluxum-server`),
// so a checkout without a compiled server does not report a false pass.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { test } from 'node:test';

import { decodeMessage, encodeMessage } from '../src/protocol.ts';
import { HttpTransport } from '../src/transport/http.ts';

const REPO = path.resolve(import.meta.dirname, '../../..');
const BINARY = path.join(
  REPO,
  'target',
  'debug',
  process.platform === 'win32' ? 'fluxum-server.exe' : 'fluxum-server',
);

/** An unused port, so parallel runs and a stray dev server cannot collide. */
async function freePort(): Promise<number> {
  const server = net.createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address !== null && typeof address === 'object');
  const { port } = address;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}

async function waitForPort(port: number, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const open = await new Promise<boolean>((resolve) => {
      const socket = net.connect({ port, host: '127.0.0.1' });
      socket.once('connect', () => {
        socket.destroy();
        resolve(true);
      });
      socket.once('error', () => resolve(false));
    });
    if (open) return;
    if (Date.now() > deadline) throw new Error(`server did not bind ${port} in ${timeoutMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

interface Running {
  process: ChildProcess;
  httpPort: number;
  stop(): Promise<void>;
}

async function startServer(dataDir: string): Promise<Running> {
  const httpPort = await freePort();
  const tcpPort = await freePort();
  const child = spawn(BINARY, [], {
    env: {
      ...process.env,
      FLUXUM_PROFILE: 'development',
      FLUXUM_SERVER_HTTP_PORT: String(httpPort),
      FLUXUM_SERVER_TCP_PORT: String(tcpPort),
      FLUXUM_STORAGE_COMMIT_LOG_DIR: path.join(dataDir, 'log'),
      FLUXUM_STORAGE_DATA_DIR: dataDir,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  // Kept for the failure message: a server that dies during startup otherwise
  // shows up only as a connection timeout.
  let output = '';
  child.stdout?.on('data', (chunk: Buffer) => (output += chunk.toString()));
  child.stderr?.on('data', (chunk: Buffer) => (output += chunk.toString()));

  try {
    await waitForPort(httpPort);
  } catch (err) {
    child.kill();
    throw new Error(`${(err as Error).message}\nserver output:\n${output}`);
  }

  return {
    process: child,
    httpPort,
    stop: async () => {
      child.kill();
      await new Promise((resolve) => child.once('exit', resolve));
    },
  };
}

const available = existsSync(BINARY);

test(
  'the full loop against a live server: auth, subscribe, reduce, receive',
  { skip: available ? false : `no server binary at ${BINARY} — run: cargo build -p fluxum-server` },
  async (t) => {
    const dataDir = path.join(
      process.env.TMPDIR ?? process.env.TEMP ?? '/tmp',
      `fluxum-e2e-${process.pid}`,
    );
    const server = await startServer(dataDir);
    t.after(() => server.stop());

    const url = `http://127.0.0.1:${server.httpPort}`;
    const transport = new HttpTransport(url);

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
