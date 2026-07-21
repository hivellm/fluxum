// The shared corpus interpreter behind both TypeScript runners (SPEC-013
// TST-052): the Node runner (`tests/conformance.test.ts`) and the Chromium
// runner's in-page half (`tests/support/browser-entry.ts`).
//
// This module is an INTERPRETER, not a test author: every scenario, step and
// expected value lives in the corpus (`tests/conformance/` at the repo root),
// where the other SDK runners read the same bytes. If an assertion here can
// only be phrased in terms of this SDK's API, it belongs in the SDK's own
// suite instead — the corpus asserts what any correct client must observe.
//
// Deliberately environment-free — no `node:*` import anywhere in its graph —
// because the SAME code must run inside a browser page, where the only
// difference is how a client reaches the server (same-origin `/rpc`) and who
// restarts it (the Node driver, via the `restartServer` hook). Assertions are
// therefore plain thrown `Error`s, not `node:assert`.

import type { FluxumClient } from '../../src/client.ts';
import type { TableSchema } from '../../src/cache.ts';
import { RowReader, decodeRow, toHex } from '../../src/fluxbin.ts';
import type { FluxType, FluxValue } from '../../src/fluxbin.ts';

export interface Corpus {
  corpus_version: number;
  tables: Record<string, { primary_key: string; columns: [string, FluxType][] }>;
  scenarios: string[];
}

export interface Scenario {
  name: string;
  description: string;
  steps: Record<string, Record<string, unknown>>[];
}

/** What a scenario asserts about a failure — any subset. */
export interface ExpectError {
  /** Substring of the human message (SDK-specific wording, use sparingly). */
  contains?: string;
  /** The stable SPEC-028 catalog code — the portable assertion. */
  code?: number;
  /** The canonical SCREAMING_SNAKE catalog name, when the SDK exposes it. */
  catalog?: string;
}

/** How a runner reaches the world outside the interpreter. */
export interface RunnerHooks {
  /**
   * Open a client to the scenario's server. The interpreter supplies the
   * cache hooks and token; the environment supplies the URL (a spawned
   * server's address in Node, `location.origin` in a page).
   */
  connect(options: { tables: TableSchema[]; token?: Uint8Array }): Promise<FluxumClient>;
  /**
   * Crash-and-recover the scenario's server (same ports, same data dir).
   * Only Node can do this; the Chromium driver intercepts `restart_server`
   * steps before they reach the page, so the browser hook never fires.
   */
  restartServer(): Promise<void>;
}

// Minimal assertions, so the graph stays free of `node:assert`.

function ok(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function fail(message: string): never {
  throw new Error(message);
}

async function rejects(promise: Promise<unknown>, check: (err: unknown) => void): Promise<void> {
  let failed = false;
  try {
    await promise;
  } catch (err) {
    failed = true;
    check(err);
  }
  ok(failed, 'operation succeeded; the scenario expected an error');
}

/** Assert a caught error matches `expect`. */
function matchError(err: unknown, expect: ExpectError): void {
  const message = err instanceof Error ? err.message : String(err);
  const code = (err as { code?: unknown })?.code;
  const catalog = (err as { catalog?: unknown })?.catalog;
  if (expect.contains !== undefined) {
    ok(message.includes(expect.contains), `error "${message}" lacks "${expect.contains}"`);
  }
  if (expect.code !== undefined) {
    ok(code === expect.code, `error code ${String(code)} != ${expect.code} ("${message}")`);
  }
  if (expect.catalog !== undefined) {
    ok(catalog === expect.catalog, `error catalog ${String(catalog)} != ${expect.catalog}`);
  }
}

const AWAIT_MS = 5000;

/** Canonical value forms per the corpus README: 64-bit → decimal string. */
function canonical(value: FluxValue): unknown {
  return typeof value === 'bigint' ? String(value) : value;
}

/** The interpreter for one scenario run: named sessions, handles, steps. */
export class ScenarioRunner {
  readonly #corpus: Corpus;
  readonly #hooks: RunnerHooks;
  readonly #clients = new Map<string, FluxumClient>();
  /** Query-id handles a `subscribe` step bound via `as`, for `unsubscribe`. */
  readonly #handles = new Map<string, number[]>();

  constructor(corpus: Corpus, hooks: RunnerHooks) {
    this.#corpus = corpus;
    this.#hooks = hooks;
  }

  /** The cache's per-table hooks, derived from the manifest schema. */
  tableSchemas(): TableSchema[] {
    return Object.entries(this.#corpus.tables).map(([name, spec]) => {
      const pkIndex = spec.columns.findIndex(([column]) => column === spec.primary_key);
      ok(pkIndex >= 0, `${name}: primary_key ${spec.primary_key} is not a column`);
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

  /** Decode a cached row into its canonical comparison form. */
  #canonicalRow(table: string, bytes: Uint8Array): Record<string, unknown> {
    const spec = this.#corpus.tables[table];
    ok(spec, `corpus.json defines no table ${table}`);
    const columns = spec.columns.map(([name, type]) => ({ name, type }));
    const decoded = decodeRow(bytes, columns);
    const row: Record<string, unknown> = {};
    for (const [name] of spec.columns) row[name] = canonical(decoded[name] as FluxValue);
    return row;
  }

  #client(name: unknown): FluxumClient {
    const client = this.#clients.get(String(name));
    ok(client, `step names client "${String(name)}" before its connect step`);
    return client;
  }

  /** Resolve "$identity:NAME" / "*" escapes; everything else is literal. */
  #resolve(expected: unknown): unknown {
    if (typeof expected === 'string' && expected.startsWith('$identity:')) {
      const identity = this.#client(expected.slice('$identity:'.length)).identity;
      ok(identity, `${expected}: that session has no identity`);
      return toHex(identity);
    }
    return expected;
  }

  #matches(expected: unknown, actual: unknown): boolean {
    if (expected === '*') return true;
    return this.#resolve(expected) === actual;
  }

  #rowMatches(expected: Record<string, unknown>, actual: Record<string, unknown>): boolean {
    return Object.entries(expected).every(([column, value]) => this.#matches(value, actual[column]));
  }

  async runStep(step: Record<string, Record<string, unknown>>): Promise<void> {
    const [kind, body] = Object.entries(step)[0] as [string, Record<string, unknown>];

    switch (kind) {
      case 'connect': {
        const name = String(body['client']);
        ok(!this.#clients.has(name), `client "${name}" connected twice`);
        const token = body['token'];
        this.#clients.set(
          name,
          await this.#hooks.connect({
            tables: this.tableSchemas(),
            ...(token === undefined ? {} : { token: new TextEncoder().encode(String(token)) }),
          }),
        );
        return;
      }
      case 'close': {
        await this.#client(body['client']).close();
        return;
      }
      case 'restart_server': {
        await this.#hooks.restartServer();
        return;
      }
      case 'subscribe': {
        const ids = await this.#client(body['client']).subscribe(body['queries'] as string[]);
        if (typeof body['as'] === 'string') this.#handles.set(body['as'], ids);
        return;
      }
      case 'unsubscribe': {
        const label = String(body['handles']);
        const ids = this.#handles.get(label);
        ok(ids, `unsubscribe names handle "${label}" before its subscribe bound it`);
        await this.#client(body['client']).unsubscribe(ids);
        return;
      }
      case 'call': {
        const client = this.#client(body['client']);
        const call = client.callReducer(String(body['reducer']), body['args'] as unknown[]);
        const expectError = body['expect_error'] as ExpectError | undefined;
        if (expectError === undefined) {
          await call;
          return;
        }
        await rejects(call, (err) => matchError(err, expectError));
        return;
      }
      case 'subscribe_error': {
        // A subscription the server refuses (unknown table, non-public table):
        // the error arrives as an `Error` frame, surfaced as a typed rejection.
        const expectError = body['expect_error'] as ExpectError;
        await rejects(this.#client(body['client']).subscribe(body['queries'] as string[]), (err) =>
          matchError(err, expectError),
        );
        return;
      }
      case 'call_until_error': {
        const client = this.#client(body['client']);
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
        fail(`all ${attempts} calls succeeded; expected "${expectError.contains}"`);
        return;
      }
      case 'await_row':
      case 'await_gone':
      case 'await_count': {
        const client = this.#client(body['client']);
        const table = String(body['table']);
        const where = (body['where'] ?? {}) as Record<string, unknown>;
        const want = kind === 'await_row' ? 1 : kind === 'await_gone' ? 0 : Number(body['count']);
        const atLeast = kind === 'await_row'; // one matching row is enough; more is not a failure
        const deadline = Date.now() + AWAIT_MS;
        for (;;) {
          const matching = client.cache
            .rows(table)
            .filter((bytes) => this.#rowMatches(where, this.#canonicalRow(table, bytes))).length;
          if (atLeast ? matching >= want : matching === want) return;
          ok(
            Date.now() < deadline,
            `${kind} ${table} ${JSON.stringify(where)}: ${matching} matching row(s), wanted ${want} after ${AWAIT_MS}ms`,
          );
          await new Promise((resolve) => setTimeout(resolve, 25));
        }
      }
      case 'expect_cache': {
        const client = this.#client(body['client']);
        const table = String(body['table']);
        const expected = body['rows'] as Record<string, unknown>[];
        const actual = client.cache.rows(table).map((bytes) => this.#canonicalRow(table, bytes));

        // Set equality: every expected row consumes exactly one actual row.
        const remaining = [...actual];
        for (const want of expected) {
          const index = remaining.findIndex((row) => this.#rowMatches(want, row));
          ok(
            index >= 0,
            `${table}: no cached row matches ${JSON.stringify(want)}; cache: ${JSON.stringify(remaining)}`,
          );
          remaining.splice(index, 1);
        }
        ok(
          remaining.length === 0,
          `${table}: unexpected extra rows in the cache: ${JSON.stringify(remaining)}`,
        );
        return;
      }
      case 'expect_distinct_identities': {
        const names = body['clients'] as string[];
        const identities = names.map((name) => {
          const identity = this.#client(name).identity;
          ok(identity, `client "${name}" has no identity`);
          return toHex(identity);
        });
        ok(
          new Set(identities).size === names.length,
          `identities collide: ${identities.join(', ')}`,
        );
        return;
      }
      default:
        fail(`unknown step "${kind}" — runner and corpus_version disagree`);
    }
  }

  async close(): Promise<void> {
    await Promise.all([...this.#clients.values()].map((client) => client.close()));
  }
}
