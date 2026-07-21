// Headless Chromium for the browser conformance runner, driven over raw CDP.
//
// No puppeteer/playwright dependency: the runner needs exactly three verbs —
// open a page, evaluate an expression, close — and the Chrome DevTools
// Protocol speaks JSON over a WebSocket, which Node ≥22 has built in. A
// hundred lines here buys freedom from a browser-download postinstall step
// that every `npm ci` of this SDK would otherwise pay.
//
// The binary is discovered, not downloaded: an explicit `FLUXUM_CHROMIUM`
// override, then a Playwright cache if one exists, then the OS's own
// Chrome/Edge/Chromium. Tests skip loudly when none is found, mirroring the
// `serverAvailable` pattern.

import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { existsSync, globSync, mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function playwrightCache(): string {
  if (process.platform === 'win32') {
    return path.join(process.env['LOCALAPPDATA'] ?? '', 'ms-playwright');
  }
  if (process.platform === 'darwin') {
    return path.join(os.homedir(), 'Library', 'Caches', 'ms-playwright');
  }
  return path.join(os.homedir(), '.cache', 'ms-playwright');
}

/** Newest matching binary from a Playwright-style versioned cache. */
function newest(pattern: string): string | null {
  const matches = globSync(pattern).sort();
  return matches.length > 0 ? (matches[matches.length - 1] as string) : null;
}

/** A Chromium-family binary to run headless, or null (skip the suite). */
export function findChromium(): string | null {
  const override = process.env['FLUXUM_CHROMIUM'];
  if (override !== undefined && override !== '') return existsSync(override) ? override : null;

  const cache = playwrightCache();
  const sub =
    process.platform === 'win32'
      ? ['chrome-win', 'headless_shell.exe']
      : process.platform === 'darwin'
        ? ['chrome-mac', 'headless_shell']
        : ['chrome-linux', 'headless_shell'];
  const shell = newest(path.join(cache, 'chromium_headless_shell-*', ...sub));
  if (shell !== null) return shell;

  const fixed =
    process.platform === 'win32'
      ? [
          'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
          'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
          'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
          'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
        ]
      : process.platform === 'darwin'
        ? ['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome']
        : ['/usr/bin/google-chrome', '/usr/bin/chromium', '/usr/bin/chromium-browser'];
  return fixed.find((candidate) => existsSync(candidate)) ?? null;
}

interface CdpResponse {
  id?: number;
  result?: Record<string, unknown>;
  error?: { message: string };
}

interface RemoteObject {
  description?: string;
}

interface EvaluateResult {
  result?: { value?: unknown };
  exceptionDetails?: { text?: string; exception?: RemoteObject };
}

/** One page (CDP target) in the running browser. */
export interface BrowserPage {
  /**
   * Evaluate `expression` in the page, awaiting a returned promise. A thrown
   * (or rejected) page-side error becomes a thrown Node-side `Error` carrying
   * the page's message, so an interpreter assertion fails the test with its
   * own words.
   */
  evaluate(expression: string): Promise<unknown>;
  close(): Promise<void>;
}

export interface Browser {
  open(url: string): Promise<BrowserPage>;
  close(): Promise<void>;
}

/** Launch `binary` headless and attach over CDP. */
export async function launchChromium(binary: string, timeoutMs = 20_000): Promise<Browser> {
  const profile = mkdtempSync(path.join(os.tmpdir(), 'fluxum-chromium-'));
  const child: ChildProcess = spawn(
    binary,
    [
      '--headless',
      '--remote-debugging-port=0',
      `--user-data-dir=${profile}`,
      '--no-first-run',
      '--no-default-browser-check',
      '--disable-extensions',
      '--disable-background-networking',
      '--disable-sync',
      'about:blank',
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );

  // The chosen ws endpoint is announced on stderr, port 0 having let the OS
  // pick: `DevTools listening on ws://127.0.0.1:PORT/devtools/browser/UUID`.
  const wsUrl = await new Promise<string>((resolve, reject) => {
    let output = '';
    const timer = setTimeout(() => {
      child.kill();
      reject(new Error(`no DevTools endpoint after ${timeoutMs}ms; output:\n${output}`));
    }, timeoutMs);
    const sniff = (chunk: Buffer): void => {
      output += chunk.toString();
      const match = /DevTools listening on (ws:\/\/\S+)/.exec(output);
      if (match) {
        clearTimeout(timer);
        resolve(match[1] as string);
      }
    };
    child.stderr?.on('data', sniff);
    child.stdout?.on('data', sniff);
    child.once('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`browser exited with ${String(code)} before announcing CDP:\n${output}`));
    });
  });

  const socket = new WebSocket(wsUrl);
  await new Promise<void>((resolve, reject) => {
    socket.onopen = () => resolve();
    socket.onerror = () => reject(new Error(`cannot attach to ${wsUrl}`));
  });

  let nextId = 1;
  const pending = new Map<number, { resolve: (v: Record<string, unknown>) => void; reject: (e: Error) => void }>();
  socket.onmessage = (event) => {
    const message = JSON.parse(String(event.data)) as CdpResponse;
    if (message.id === undefined) return; // an event; the driver polls instead
    const waiter = pending.get(message.id);
    if (!waiter) return;
    pending.delete(message.id);
    if (message.error) waiter.reject(new Error(message.error.message));
    else waiter.resolve(message.result ?? {});
  };

  const send = (
    method: string,
    params: Record<string, unknown> = {},
    sessionId?: string,
  ): Promise<Record<string, unknown>> => {
    const id = nextId;
    nextId += 1;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      socket.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    });
  };

  const open = async (url: string): Promise<BrowserPage> => {
    const { targetId } = (await send('Target.createTarget', { url: 'about:blank' })) as {
      targetId: string;
    };
    const { sessionId } = (await send('Target.attachToTarget', { targetId, flatten: true })) as {
      sessionId: string;
    };

    const evaluate = async (expression: string): Promise<unknown> => {
      const outcome = (await send(
        'Runtime.evaluate',
        { expression, awaitPromise: true, returnByValue: true },
        sessionId,
      )) as EvaluateResult;
      const details = outcome.exceptionDetails;
      if (details !== undefined) {
        throw new Error(details.exception?.description ?? details.text ?? 'page-side exception');
      }
      return outcome.result?.value;
    };

    await send('Page.navigate', { url }, sessionId);
    // Poll instead of wiring Page.loadEventFired: readiness is "the bridge
    // exists", which only the page can answer anyway.
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const ready = await evaluate(`typeof window.conformance === 'object'`);
      if (ready === true) break;
      if (Date.now() > deadline) throw new Error(`${url}: bridge not ready after ${timeoutMs}ms`);
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    return {
      evaluate,
      close: async () => {
        await send('Target.closeTarget', { targetId });
      },
    };
  };

  const close = async (): Promise<void> => {
    await send('Browser.close').catch(() => child.kill());
    await new Promise<void>((resolve) => {
      if (child.exitCode !== null) resolve();
      else child.once('exit', () => resolve());
    });
    // Best-effort: on Windows the profile dir can lag behind the exit.
    try {
      rmSync(profile, { recursive: true, force: true });
    } catch {
      /* a leaked temp profile is not a test failure */
    }
  };

  return { open, close };
}
