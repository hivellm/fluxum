// Package the runtime (SPEC-011 SDK-081, SDK-083).
//
// Three outputs, each for a real consumer:
//   dist/index.js      ESM for npm and bundlers
//   dist/index.cjs     CJS for `require`
//   dist/fluxum.min.js browser ESM, dependencies inlined
//
// The browser build inlines `@hivehub/thunder/wire` and `@msgpack/msgpack`
// because a browser cannot resolve a bare specifier — a `<script
// type="module">` page with no build step (SDK-081) would fail on the import,
// not on anything the SDK does. The npm builds leave them external so a
// consuming bundler can dedupe them against its own copy.
//
// No Node builtins need aliasing away: `protocol.ts` imports Thunder's
// `/wire` subpath, whose graph is the codec alone.
//
// The size gate is asserted here rather than trusted: SDK-083 caps the
// hand-written runtime at 50 KB min+gzip, and a budget nobody measures is a
// budget that has already been exceeded.

import { build } from 'esbuild';
import { execFileSync } from 'node:child_process';
import { copyFileSync, existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { globSync } from 'node:fs';
import { gzipSync } from 'node:zlib';

const BUDGET_BYTES = 50 * 1024;
const ENTRY = 'src/index.ts';

rmSync('dist', { recursive: true, force: true });

const shared = {
  entryPoints: [ENTRY],
  bundle: true,
  target: 'es2022',
  sourcemap: true,
  logLevel: 'warning',
};

await build({
  ...shared,
  format: 'esm',
  outfile: 'dist/index.js',
  packages: 'external',
});

await build({
  ...shared,
  format: 'cjs',
  outfile: 'dist/index.cjs',
  packages: 'external',
});

// Typings come from tsc, not esbuild — esbuild does not emit declarations.
// Resolved through require rather than `npx` so the build uses the workspace's
// pinned compiler and never a network fetch.
const require = createRequire(import.meta.url);
execFileSync(
  process.execPath,
  [require.resolve('typescript/bin/tsc'), '-p', 'tsconfig.build.json'],
  { stdio: 'inherit' },
);
if (!existsSync('dist/index.d.ts')) {
  console.error('tsc emitted no dist/index.d.ts — the package.json "types" entry would dangle');
  process.exit(1);
}

// `rewriteRelativeImportExtensions` rewrites specifiers in emitted JavaScript
// but leaves them as `.ts` inside declaration files, where they would point at
// sources this package does not ship. Rewritten here; relative specifiers
// only, so bare imports (`@hivehub/thunder`) are untouched.
for (const file of globSync('dist/**/*.d.ts')) {
  const source = readFileSync(file, 'utf8');
  const rewritten = source.replace(/(from\s+['"])(\.\.?\/[^'"]+)\.ts(['"])/g, '$1$2.js$3');
  if (rewritten !== source) writeFileSync(file, rewritten);
}
console.log('declarations emitted (dist/*.d.ts)');

await build({
  ...shared,
  format: 'esm',
  outfile: 'dist/fluxum.min.js',
  minify: true,
  platform: 'browser',
  // `node:net` is imported lazily by the TCP transport, which a browser never
  // takes; its dynamic import sits in a try/catch that already reports the
  // actionable error.
  external: ['node:net'],
});

const minified = readFileSync('dist/fluxum.min.js');
const gzipped = gzipSync(minified, { level: 9 });
const kb = (n) => `${(n / 1024).toFixed(1)} KB`;

console.log(`browser bundle: ${kb(minified.length)} min, ${kb(gzipped.length)} min+gzip`);
console.log(`SDK-083 budget: ${kb(BUDGET_BYTES)}`);

if (gzipped.length > BUDGET_BYTES) {
  console.error(
    `\nSDK-083 violated: the runtime is ${kb(gzipped.length)} min+gzip, over the ` +
      `${kb(BUDGET_BYTES)} budget. This is what browser users pay before your app ` +
      `loads — trim it rather than raising the cap.`,
  );
  process.exit(1);
}

console.log(`within budget by ${kb(BUDGET_BYTES - gzipped.length)}`);

// The demo page loads the runtime with a plain relative import and no build
// step of its own (SDK-081), so the bundle has to sit beside it. Copied rather
// than symlinked: this has to work on a fresh clone on Windows too.
const DEMO = '../../demo/fluxum.min.js';
copyFileSync('dist/fluxum.min.js', DEMO);
copyFileSync('dist/fluxum.min.js.map', `${DEMO}.map`);
console.log(`copied the browser bundle to demo/`);
