// The headless-Chromium runner for the shared SDK conformance corpus
// (SPEC-013 TST-052) — the browser half of the T6.2 exit test.
//
// Same corpus bytes, same interpreter as the Node runner; what changes is
// WHERE the SDK runs. The interpreter + SDK are bundled for the browser and
// served BY THE FLUXUM SERVER under test (`server.static_dir`), because
// `/rpc` sends no CORS headers and the page must share its origin — exactly
// the deployment shape a real browser app uses. Node keeps only what a page
// cannot do: spawn/restart servers and drive Chromium over CDP.
//
// One browser process serves the whole suite; each scenario still gets a
// fresh server (fresh ports, fresh data dir) and a fresh page, since the demo
// tables are global to a server.

import { build } from 'esbuild';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import type { Corpus, Scenario } from './support/interpreter.ts';
import { findChromium, launchChromium } from './support/chromium.ts';
import { BINARY, serverAvailable, startServer } from './support/server.ts';

const CORPUS_DIR = path.resolve(import.meta.dirname, '../../../tests/conformance');

const corpus = JSON.parse(readFileSync(path.join(CORPUS_DIR, 'corpus.json'), 'utf8')) as Corpus;

const chromium = findChromium();
const skip = !serverAvailable
  ? `no server binary at ${BINARY} — run: cargo build -p fluxum-server`
  : chromium === null
    ? 'no Chromium-family browser found — set FLUXUM_CHROMIUM to a binary'
    : false;

test('conformance corpus in headless Chromium', { skip }, async (t) => {
  // The served page: index.html + the bundled interpreter/SDK. Built once —
  // it is the same artifact for every scenario, whatever port serves it.
  const siteDir = mkdtempSync(path.join(os.tmpdir(), 'fluxum-conf-site-'));
  writeFileSync(
    path.join(siteDir, 'index.html'),
    '<!doctype html><meta charset="utf-8"><title>fluxum conformance</title>' +
      '<script type="module" src="./runner.js"></script>',
  );
  await build({
    entryPoints: [path.resolve(import.meta.dirname, 'support', 'browser-entry.ts')],
    bundle: true,
    format: 'esm',
    platform: 'browser',
    target: 'es2022',
    outfile: path.join(siteDir, 'runner.js'),
    // Imported lazily by the TCP transport, which a browser never takes.
    external: ['node:net'],
    logLevel: 'silent',
  });

  const browser = await launchChromium(chromium as string);
  t.after(async () => {
    await browser.close();
    rmSync(siteDir, { recursive: true, force: true });
  });

  for (const name of corpus.scenarios) {
    const scenario = JSON.parse(
      readFileSync(path.join(CORPUS_DIR, 'scenarios', `${name}.json`), 'utf8'),
    ) as Scenario;

    await t.test(`chromium: ${name}`, async () => {
      const server = await startServer(`chrome-${name}`, { staticDir: siteDir });
      const page = await browser.open(`${server.httpUrl}/`);
      try {
        await page.evaluate(`window.conformance.init(${JSON.stringify(corpus)})`);
        for (const step of scenario.steps) {
          // Server lifecycle is Node's half of the split; everything else
          // runs where the SDK does.
          if (Object.keys(step)[0] === 'restart_server') await server.restart();
          else await page.evaluate(`window.conformance.step(${JSON.stringify(step)})`);
        }
      } finally {
        await page.evaluate('window.conformance.close()').catch(() => {});
        await page.close();
        await server.stop();
      }
    });
  }
});
