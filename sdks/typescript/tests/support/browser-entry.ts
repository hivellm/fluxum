// The in-page half of the Chromium conformance runner (SPEC-013 TST-052).
//
// Bundled by `tests/conformance.chromium.test.ts` (esbuild, browser platform)
// and served BY THE FLUXUM SERVER ITSELF via `server.static_dir`: `/rpc`
// sends no CORS headers, so the page must share its origin — which also makes
// this the honest test, since that is exactly how a real browser app reaches
// Fluxum. The Node driver drives one step at a time through
// `window.conformance`, keeping the session state (clients, handles) in the
// page where the actual browser SDK runs.

import { FluxumClient } from '../../src/client.ts';
import { ScenarioRunner } from './interpreter.ts';
import type { Corpus } from './interpreter.ts';

interface ConformanceBridge {
  init(corpus: Corpus): void;
  step(step: Record<string, Record<string, unknown>>): Promise<void>;
  close(): Promise<void>;
}

declare global {
  interface Window {
    conformance: ConformanceBridge;
  }
}

let runner: ScenarioRunner | undefined;

window.conformance = {
  init(corpus: Corpus): void {
    runner = new ScenarioRunner(corpus, {
      // Same origin as the page — the whole point of the static_dir setup.
      connect: (options) => FluxumClient.connect({ url: location.origin, ...options }),
      // `restart_server` steps are intercepted by the Node driver before they
      // reach the page; only a driver bug lands here.
      restartServer: () => Promise.reject(new Error('restart_server is driven from Node')),
    });
  },
  async step(step): Promise<void> {
    if (runner === undefined) throw new Error('step before init');
    await runner.runStep(step);
  },
  async close(): Promise<void> {
    await runner?.close();
    runner = undefined;
  },
};
