// Spawning a real `fluxum-server` for end-to-end tests.
//
// Each test gets its own process, its own ports and its own data directory:
// the demo module's tables are global to a server, so a shared instance would
// let one test's rows show up in another's cache assertions.

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { existsSync, mkdtempSync } from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';

const REPO = path.resolve(import.meta.dirname, '../../../..');

/** The compiled reference server. */
export const BINARY = path.join(
  REPO,
  'target',
  'debug',
  process.platform === 'win32' ? 'fluxum-server.exe' : 'fluxum-server',
);

/** False when the binary has not been built; tests skip loudly rather than pass. */
export const serverAvailable = existsSync(BINARY);

/** An unused port, so a stray dev server cannot collide with a test run. */
async function freePort(): Promise<number> {
  const probe = net.createServer();
  await new Promise<void>((resolve) => probe.listen(0, '127.0.0.1', resolve));
  const address = probe.address();
  assert.ok(address !== null && typeof address === 'object');
  const { port } = address;
  await new Promise<void>((resolve) => probe.close(() => resolve()));
  return port;
}

async function waitForPort(port: number, timeoutMs: number): Promise<void> {
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

export interface RunningServer {
  /** `http://127.0.0.1:<port>` — the Streamable HTTP endpoint. */
  httpUrl: string;
  /** `fluxum://127.0.0.1:<port>` — the raw TCP endpoint. */
  tcpUrl: string;
  process: ChildProcess;
  stop(): Promise<void>;
  /**
   * Kill the process and start it again on the SAME ports and data
   * directory — a crash-and-recover, not a fresh server. Committed rows come
   * back through commit-log recovery, and clients reconnect to the same
   * address.
   */
  restart(): Promise<void>;
}

/** Start a server with the demo module, on fresh ports and a fresh data dir. */
export async function startServer(label: string, timeoutMs = 20_000): Promise<RunningServer> {
  const httpPort = await freePort();
  const tcpPort = await freePort();
  const dataDir = mkdtempSync(path.join(os.tmpdir(), `fluxum-${label}-`));

  const env = {
    ...process.env,
    // Config keys are FLUXUM_<PATH> with SINGLE underscores. A doubled
    // separator is not an error — it is silently ignored, and the server
    // quietly starts on the default ports.
    FLUXUM_PROFILE: 'development',
    FLUXUM_SERVER_HTTP_PORT: String(httpPort),
    FLUXUM_SERVER_TCP_PORT: String(tcpPort),
    FLUXUM_STORAGE_DATA_DIR: dataDir,
    FLUXUM_STORAGE_COMMIT_LOG_DIR: path.join(dataDir, 'log'),
  };

  const launch = async (): Promise<ChildProcess> => {
    const child = spawn(BINARY, [], { env, stdio: ['ignore', 'pipe', 'pipe'] });

    // Retained for the failure message: a server that dies during startup
    // otherwise appears only as a connection timeout.
    let output = '';
    child.stdout?.on('data', (chunk: Buffer) => (output += chunk.toString()));
    child.stderr?.on('data', (chunk: Buffer) => (output += chunk.toString()));

    try {
      await waitForPort(httpPort, timeoutMs);
    } catch (err) {
      child.kill();
      throw new Error(`${(err as Error).message}\nserver output:\n${output}`);
    }
    return child;
  };

  let child = await launch();
  const stopChild = async (): Promise<void> => {
    if (child.exitCode !== null) return;
    child.kill();
    await new Promise((resolve) => child.once('exit', resolve));
  };

  return {
    httpUrl: `http://127.0.0.1:${httpPort}`,
    tcpUrl: `fluxum://127.0.0.1:${tcpPort}`,
    get process() {
      return child;
    },
    stop: stopChild,
    restart: async () => {
      await stopChild();
      child = await launch();
    },
  };
}
