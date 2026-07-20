// The TypeScript runner for the shared SDK conformance corpus
// (SPEC-013 TST-052; tests/conformance/ at the repo root).
//
// This file is an INTERPRETER, not a test author: every scenario, step and
// expected value lives in the corpus, where the other SDK runners read the
// same bytes. If an assertion here can only be phrased in terms of this SDK's
// API, it belongs in the SDK's own suite instead — the corpus asserts what any
// correct client must observe.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { FluxumClient } from '../src/client.ts';
import type { TableSchema } from '../src/cache.ts';
import { RowReader, decodeRow, toHex } from '../src/fluxbin.ts';
import type { FluxType, FluxValue } from '../src/fluxbin.ts';
import { BINARY, serverAvailable, startServer } from './support/server.ts';

const CORPUS_DIR = path.resolve(import.meta.dirname, '../../../tests/conformance');

interface Corpus {
  corpus_version: number;
  tables: Record<string, { primary_key: string; columns: [string, FluxType][] }>;
  scenarios: string[];
}

interface Scenario {
  name: string;
  description: string;
  steps: Record<string, Record<string, unknown>>[];
}

const corpus = JSON.parse(readFileSync(path.join(CORPUS_DIR, 'corpus.json'), 'utf8')) as Corpus;

/** Canonical value forms per the corpus README: 64-bit → decimal string. */
function canonical(value: FluxValue): unknown {
  return typeof value === 'bigint' ? String(value) : value;
}

/** Decode a cached row into its canonical comparison form. */
function canonicalRow(table: string, bytes: Uint8Array): Record<string, unknown> {
  const spec = corpus.tables[table];
  assert.ok(spec, `corpus.json defines no table ${table}`);
  const columns = spec.columns.map(([name, type]) => ({ name, type }));
  const decoded = decodeRow(bytes, columns);
  const row: Record<string, unknown> = {};
  for (const [name] of spec.columns) row[name] = canonical(decoded[name] as FluxValue);
  return row;
}

/** The cache's per-table hooks, derived from the manifest schema. */
function tableSchemas(): TableSchema[] {
  return Object.entries(corpus.tables).map(([name, spec]) => {
    const pkIndex = spec.columns.findIndex(([column]) => column === spec.primary_key);
    assert.ok(pkIndex >= 0, `${name}: primary_key ${spec.primary_key} is not a column`);
    const types = spec.columns.map(([, type]) => type);
    const pkType = types[pkIndex] as FluxType;
    return {
      name,
      pkOfRow: (row) => {
        const reader = new RowReader(row);
        let value: FluxValue = null as unknown as FluxValue;
        for (let i = 0; i <= pkIndex; i += 1) value = reader.read(types[i] as FluxType);
        return String(value);
      },
      // Deletes carry the primary key alone (SPEC-006).
      pkOfDelete: (entry) => String(new RowReader(entry).read(pkType)),
    };
  });
}

/** Interpreter state: the named sessions a scenario builds up. */
import type { RunningServer } from './support/server.ts';

class Session {
  readonly clients = new Map<string, FluxumClient>();
  /** Query-id handles a `subscribe` step bound via `as`, for `unsubscribe`. */
  readonly handles = new Map<string, number[]>();
  readonly server: RunningServer;
  constructor(server: RunningServer) {
    this.server = server;
  }

  get httpUrl(): string {
    return this.server.httpUrl;
  }

  client(name: unknown): FluxumClient {
    const client = this.clients.get(String(name));
    assert.ok(client, `step names client "${String(name)}" before its connect step`);
    return client;
  }

  /** Resolve "$identity:NAME" / "*" escapes; everything else is literal. */
  resolve(expected: unknown): unknown {
    if (typeof expected === 'string' && expected.startsWith('$identity:')) {
      const identity = this.client(expected.slice('$identity:'.length)).identity;
      assert.ok(identity, `${expected}: that session has no identity`);
      return toHex(identity);
    }
    return expected;
  }

  matches(expected: unknown, actual: unknown): boolean {
    if (expected === '*') return true;
    return this.resolve(expected) === actual;
  }

  rowMatches(expected: Record<string, unknown>, actual: Record<string, unknown>): boolean {
    return Object.entries(expected).every(([column, value]) => this.matches(value, actual[column]));
  }

  async close(): Promise<void> {
    await Promise.all([...this.clients.values()].map((client) => client.close()));
  }
}

const AWAIT_MS = 5000;

/** What a scenario asserts about a failure — any subset. */
interface ExpectError {
  /** Substring of the human message (SDK-specific wording, use sparingly). */
  contains?: string;
  /** The stable SPEC-028 catalog code — the portable assertion. */
  code?: number;
  /** The canonical SCREAMING_SNAKE catalog name, when the SDK exposes it. */
  catalog?: string;
}

/** Assert a caught error matches `expect`; returns true so `assert.rejects` passes. */
function matchError(err: unknown, expect: ExpectError): boolean {
  const message = err instanceof Error ? err.message : String(err);
  const code = (err as { code?: unknown })?.code;
  const catalog = (err as { catalog?: unknown })?.catalog;
  if (expect.contains !== undefined) {
    assert.ok(message.includes(expect.contains), `error "${message}" lacks "${expect.contains}"`);
  }
  if (expect.code !== undefined) {
    assert.equal(code, expect.code, `error code ${String(code)} != ${expect.code} ("${message}")`);
  }
  if (expect.catalog !== undefined) {
    assert.equal(catalog, expect.catalog, `error catalog ${String(catalog)} != ${expect.catalog}`);
  }
  return true;
}

async function runStep(session: Session, step: Record<string, Record<string, unknown>>): Promise<void> {
  const [kind, body] = Object.entries(step)[0] as [string, Record<string, unknown>];

  switch (kind) {
    case 'connect': {
      const name = String(body['client']);
      assert.ok(!session.clients.has(name), `client "${name}" connected twice`);
      const token = body['token'];
      session.clients.set(
        name,
        await FluxumClient.connect({
          url: session.httpUrl,
          tables: tableSchemas(),
          ...(token === undefined ? {} : { token: new TextEncoder().encode(String(token)) }),
        }),
      );
      return;
    }
    case 'close': {
      await session.client(body['client']).close();
      return;
    }
    case 'restart_server': {
      await session.server.restart();
      return;
    }
    case 'subscribe': {
      const ids = await session.client(body['client']).subscribe(body['queries'] as string[]);
      if (typeof body['as'] === 'string') session.handles.set(body['as'], ids);
      return;
    }
    case 'unsubscribe': {
      const label = String(body['handles']);
      const ids = session.handles.get(label);
      assert.ok(ids, `unsubscribe names handle "${label}" before its subscribe bound it`);
      await session.client(body['client']).unsubscribe(ids);
      return;
    }
    case 'call': {
      const client = session.client(body['client']);
      const call = client.callReducer(String(body['reducer']), body['args'] as unknown[]);
      const expectError = body['expect_error'] as ExpectError | undefined;
      if (expectError === undefined) {
        await call;
        return;
      }
      await assert.rejects(call, (err: unknown) => matchError(err, expectError));
      return;
    }
    case 'subscribe_error': {
      // A subscription the server refuses (unknown table, non-public table):
      // the error arrives as an `Error` frame, surfaced as a typed rejection.
      const expectError = body['expect_error'] as ExpectError;
      await assert.rejects(
        session.client(body['client']).subscribe(body['queries'] as string[]),
        (err: unknown) => matchError(err, expectError),
      );
      return;
    }
    case 'call_until_error': {
      const client = session.client(body['client']);
      const attempts = Number(body['attempts']);
      const expectError = body['expect_error'] as ExpectError;
      for (let i = 0; i < attempts; i += 1) {
        try {
          await client.callReducer(String(body['reducer']), body['args'] as unknown[]);
        } catch (err) {
          matchError(err, expectError);
          return;
        }
      }
      assert.fail(`all ${attempts} calls succeeded; expected "${expectError.contains}"`);
      return;
    }
    case 'await_row':
    case 'await_gone':
    case 'await_count': {
      const client = session.client(body['client']);
      const table = String(body['table']);
      const where = (body['where'] ?? {}) as Record<string, unknown>;
      const want = kind === 'await_row' ? 1 : kind === 'await_gone' ? 0 : Number(body['count']);
      const atLeast = kind === 'await_row'; // one matching row is enough; more is not a failure
      const deadline = Date.now() + AWAIT_MS;
      for (;;) {
        const matching = client.cache
          .rows(table)
          .filter((bytes) => session.rowMatches(where, canonicalRow(table, bytes))).length;
        if (atLeast ? matching >= want : matching === want) return;
        assert.ok(
          Date.now() < deadline,
          `${kind} ${table} ${JSON.stringify(where)}: ${matching} matching row(s), wanted ${want} after ${AWAIT_MS}ms`,
        );
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    }
    case 'expect_cache': {
      const client = session.client(body['client']);
      const table = String(body['table']);
      const expected = body['rows'] as Record<string, unknown>[];
      const actual = client.cache.rows(table).map((bytes) => canonicalRow(table, bytes));

      // Set equality: every expected row consumes exactly one actual row.
      const remaining = [...actual];
      for (const want of expected) {
        const index = remaining.findIndex((row) => session.rowMatches(want, row));
        assert.ok(
          index >= 0,
          `${table}: no cached row matches ${JSON.stringify(want)}; cache: ${JSON.stringify(remaining)}`,
        );
        remaining.splice(index, 1);
      }
      assert.deepEqual(remaining, [], `${table}: unexpected extra rows in the cache`);
      return;
    }
    case 'expect_distinct_identities': {
      const names = body['clients'] as string[];
      const identities = names.map((name) => {
        const identity = session.client(name).identity;
        assert.ok(identity, `client "${name}" has no identity`);
        return toHex(identity);
      });
      assert.equal(new Set(identities).size, names.length, `identities collide: ${identities.join(', ')}`);
      return;
    }
    default:
      assert.fail(`unknown step "${kind}" — runner and corpus_version disagree`);
  }
}

const skip = serverAvailable
  ? false
  : `no server binary at ${BINARY} — run: cargo build -p fluxum-server`;

for (const name of corpus.scenarios) {
  const scenario = JSON.parse(
    readFileSync(path.join(CORPUS_DIR, 'scenarios', `${name}.json`), 'utf8'),
  ) as Scenario;

  test(`conformance: ${name}`, { skip }, async (t) => {
    const server = await startServer(`conf-${name}`);
    const session = new Session(server);
    t.after(async () => {
      await session.close();
      await server.stop();
    });

    for (const step of scenario.steps) await runStep(session, step);
  });
}
