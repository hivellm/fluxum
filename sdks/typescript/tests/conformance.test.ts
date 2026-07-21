// The Node runner for the shared SDK conformance corpus (SPEC-013 TST-052;
// tests/conformance/ at the repo root).
//
// The interpreter itself lives in `tests/support/interpreter.ts`, shared with
// the Chromium runner (`tests/conformance.chromium.test.ts`): this file only
// supplies what Node has and a page does not — a spawned server per scenario
// and the ability to crash-and-recover it.
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { FluxumClient } from '../src/client.ts';
import { ScenarioRunner } from './support/interpreter.ts';
import type { Corpus, Scenario } from './support/interpreter.ts';
import { BINARY, serverAvailable, startServer } from './support/server.ts';

const CORPUS_DIR = path.resolve(import.meta.dirname, '../../../tests/conformance');

const corpus = JSON.parse(readFileSync(path.join(CORPUS_DIR, 'corpus.json'), 'utf8')) as Corpus;

const skip = serverAvailable
  ? false
  : `no server binary at ${BINARY} — run: cargo build -p fluxum-server`;

for (const name of corpus.scenarios) {
  const scenario = JSON.parse(
    readFileSync(path.join(CORPUS_DIR, 'scenarios', `${name}.json`), 'utf8'),
  ) as Scenario;

  test(`conformance: ${name}`, { skip }, async (t) => {
    const server = await startServer(`conf-${name}`);
    const runner = new ScenarioRunner(corpus, {
      connect: (options) => FluxumClient.connect({ url: server.httpUrl, ...options }),
      restartServer: () => server.restart(),
    });
    t.after(async () => {
      await runner.close();
      await server.stop();
    });

    for (const step of scenario.steps) await runner.runStep(step);
  });
}
