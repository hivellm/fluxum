// `FluxumClient` — the object an application actually holds (SDK-040/042/043).
//
// Everything under it was built as an independent unit: the transport carries
// bytes, the cache applies diffs, the queue bounds them, the reconnect loop
// rebuilds a session. This is where they become one thing: request ids get
// correlated (RPC-002), server frames get routed by tag, and cache events
// reach the callbacks an application registered.
//
// The message envelopes here are POSITIONAL — compact MessagePack writes a
// struct as an array in declaration order, so the field order below IS the
// wire format (RPC-011). Two of them are easy to get wrong and surface only as
// an opaque 400: `Authenticate.token` is `bin`, never nil, and `ReducerCall`
// puts `version` BEFORE `args`.

import { RowCache } from './cache.ts';
import type { RowEvent, TableDiff, TableSchema, TableSnapshot } from './cache.ts';
import { decodeMessage, encodeMessage, sliceRowList } from './protocol.ts';
import { BoundedQueue } from './queue.ts';
import { reconnect } from './reconnect.ts';
import type { BackoffOptions } from './reconnect.ts';
import { connect as openTransport } from './transport/connect.ts';
import { HttpTransport } from './transport/http.ts';
import type { Transport } from './transport/types.ts';

/** A reducer rejected the call (RPC-031). */
export class ReducerError extends Error {
  /** Stable catalog code (SPEC-028, 5xxx range). */
  readonly code: number;
  /** Application-defined code, when the reducer attached one. */
  readonly appCode: string | null;
  constructor(code: number, appCode: string | null, message: string) {
    super(message);
    this.name = 'ReducerError';
    this.code = code;
    this.appCode = appCode;
  }
}

/** The server answered a request with an `Error` frame (RPC-034). */
export class ServerError extends Error {
  /** Stable catalog code. */
  readonly code: number;
  /** Canonical SCREAMING_SNAKE catalog name. */
  readonly catalog: string;
  constructor(code: number, catalog: string, message: string) {
    super(message);
    this.name = 'ServerError';
    this.code = code;
    this.catalog = catalog;
  }
}

/**
 * The client's generated bindings were built against a different schema
 * (SDK-043).
 *
 * Thrown rather than tolerated: generated types cannot change at runtime, so
 * continuing would decode rows with the wrong column layout and hand the
 * application confidently mistyped data.
 */
export class SchemaMismatchError extends Error {
  /** What the bindings were generated from. */
  readonly expected: number;
  /** What the server is running. */
  readonly actual: number;
  constructor(expected: number, actual: number) {
    super(
      `schema mismatch: this client was generated for schema_version ${expected}, ` +
        `the server is running ${actual}. Regenerate with \`fluxum generate\`.`,
    );
    this.name = 'SchemaMismatchError';
    this.expected = expected;
    this.actual = actual;
  }
}

/** A row-event listener. `old` is present only for `update`. */
export type RowListener = (row: Uint8Array, old?: Uint8Array) => void;

export interface FluxumClientOptions {
  /** `fluxum://host:15801` (Node) or `http(s)://host:15800` (anywhere). */
  url: string;
  /** Auth token. Empty under the `none` provider. */
  token?: Uint8Array;
  /** Per-table primary-key projections the cache needs (SDK-040). */
  tables?: TableSchema[];
  /** Embedded `schema_version` to check against the server (SDK-043). */
  schemaVersion?: number;
  /** Inbound queue capacity (SDK-046). Default 1024. */
  queueCapacity?: number;
  /** Reconnect tuning, or `false` to fail instead of retrying. */
  reconnect?: BackoffOptions | false;
  /** Injected in tests; HTTP only. */
  fetch?: typeof globalThis.fetch;
}

/**
 * A request awaiting its reply — or replies.
 *
 * Plural on purpose: a `Subscribe` carrying N queries is answered with N
 * `InitialData` frames, every one echoing the same request id (SPEC-006
 * RPC-032, "one entry per query" — the server emits one message per query,
 * not one message listing every query). A correlation map that resolves on
 * the first reply drops the rest **silently**: the first table populates and
 * the others simply look empty, with no error anywhere.
 */
interface Pending {
  resolve: (messages: DecodedMessage[]) => void;
  reject: (err: Error) => void;
  /** How many replies this request expects. */
  expected: number;
  /** What has arrived so far. */
  collected: DecodedMessage[];
}

interface DecodedMessage {
  tag: string;
  payload: unknown[];
}

/**
 * A connected Fluxum client.
 *
 * Construct with {@link FluxumClient.connect}, which resolves once the session
 * is authenticated — not merely once a socket is open.
 */
export class FluxumClient {
  readonly #options: FluxumClientOptions;
  readonly #cache: RowCache;
  readonly #inbound: BoundedQueue<DecodedMessage>;
  readonly #pending = new Map<number, Pending>();
  readonly #listeners = new Map<string, Set<RowListener>>();
  readonly #errorListeners = new Set<(err: Error) => void>();
  /** Live subscription queries, replayed verbatim on reconnect (SDK-047). */
  readonly #queries = new Set<string>();
  /**
   * The reconnect snapshot, held between `resubscribe` and `reconcile`.
   *
   * Reconciling inside `resubscribe` would run once per query batch and delete
   * rows the next batch is about to restore.
   */
  #pendingSnapshot: unknown[][] | null = null;

  #transport: Transport | null = null;
  #nextId = 1;
  #identity: Uint8Array | null = null;
  #closed = false;
  /** Guards against two reconnect loops racing after a double failure. */
  #reconnecting = false;
  /**
   * Whether this mismatch episode already spent its one automatic
   * refresh-and-reconnect pass (SDK-043). Reset when a reconnect delivers a
   * matching `InitialData` — a later migration starts a fresh episode.
   */
  #mismatchRetried = false;
  /**
   * Set once a mismatch is CONFIRMED — the refresh or the retry saw the same
   * wrong version again. From here reconnecting is pointless: the bindings
   * cannot change at runtime, so every attempt would end the same way.
   */
  #schemaFailure: SchemaMismatchError | null = null;
  /** Settled by the reconnect loop while a schema drill is in flight. */
  #drill: { resolve: () => void; reject: (err: Error) => void } | null = null;
  /** Resolved by `close()`, so a sleeping reconnect loop wakes and stops. */
  #closeRequested: (() => void) | null = null;
  readonly #closeSignal = new Promise<void>((resolve) => {
    this.#closeRequested = resolve;
  });

  private constructor(options: FluxumClientOptions) {
    this.#options = options;
    this.#cache = new RowCache(options.tables ?? []);
    this.#inbound = new BoundedQueue<DecodedMessage>({
      capacity: options.queueCapacity ?? 1024,
      name: 'inbound',
    });
  }

  /** Connect, authenticate, and return a live client. */
  static async connect(options: FluxumClientOptions): Promise<FluxumClient> {
    const client = new FluxumClient(options);
    // The pump starts BEFORE the handshake. Authenticating first deadlocks:
    // the reply lands in the inbound queue with no consumer, and the consumer
    // cannot start because it is behind the very reply it would deliver.
    void client.#pump();
    await client.#openAndAuthenticate();
    return client;
  }

  /** The 32-byte identity the server derived for this session (SPEC-009). */
  get identity(): Uint8Array | null {
    return this.#identity;
  }

  /** The local row cache. Stale while disconnected (SDK-047). */
  get cache(): RowCache {
    return this.#cache;
  }

  /**
   * Register a listener for `"<Table>:<insert|delete|update>"`.
   *
   * Returns an unsubscribe function rather than requiring an `off` that has to
   * be handed the identical closure — the usual way listeners leak.
   */
  on(event: string, listener: RowListener): () => void {
    let set = this.#listeners.get(event);
    if (set === undefined) {
      set = new Set();
      this.#listeners.set(event, set);
    }
    set.add(listener);
    return () => {
      set.delete(listener);
    };
  }

  /**
   * Listen for connection-level failures: a reconnect that gave up, an
   * inbound queue that overflowed, a server-initiated `Error` frame.
   *
   * These belong to nobody's request, so there is no promise to reject.
   */
  onError(listener: (err: Error) => void): () => void {
    this.#errorListeners.add(listener);
    return () => {
      this.#errorListeners.delete(listener);
    };
  }

  /** Register subscription queries and await the `InitialData` snapshot. */
  async subscribe(queries: string[]): Promise<void> {
    if (queries.length === 0) return;
    for (const query of queries) this.#queries.add(query);
    // One `InitialData` per query, all echoing this request's id.
    const messages = await this.#requestMany(
      'Subscribe',
      (id) => [id, queries],
      queries.length,
    );
    // The version gate runs BEFORE anything is applied: a mismatched
    // `InitialData` never reaches the cache, so the application never sees a
    // row its generated types would misread (SDK-043).
    const mismatch = this.#schemaMismatch(messages.map((m) => m.payload));
    if (mismatch !== null) return this.#runSchemaDrill(mismatch);
    for (const message of messages) {
      this.#applyInitialData(message.payload, { reconcile: false });
    }
  }

  /** Drop subscriptions by their server-assigned query ids. */
  async unsubscribe(queryIds: number[]): Promise<void> {
    await this.#request('Unsubscribe', (id) => [id, queryIds]);
  }

  /**
   * Call a reducer and await its outcome.
   *
   * Resolves when the reducer committed — not when the resulting `TxUpdate`
   * has been applied. Those are different moments: the update arrives on the
   * push stream and may land before or after this resolves, so an application
   * that needs the row should watch the callback rather than assume.
   */
  async callReducer(name: string, args: unknown[] = []): Promise<void> {
    const message = await this.#request('ReducerCall', (id) => [id, name, null, args, null]);
    const outcome = message.payload[1];
    if (!Array.isArray(outcome) || outcome[0] === 'Ok') return;

    const detail = outcome[1];
    if (Array.isArray(detail)) {
      const [code, appCode, text] = detail as [unknown, unknown, unknown];
      throw new ReducerError(
        Number(code),
        typeof appCode === 'string' ? appCode : null,
        String(text),
      );
    }
    throw new ReducerError(0, null, `reducer ${name} failed`);
  }

  /** Close the session. Idempotent. */
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const reason = new Error('client closed');
    this.#inbound.close(reason);
    for (const pending of this.#pending.values()) pending.reject(reason);
    this.#pending.clear();
    // A drill still in flight would otherwise hang its `subscribe` forever.
    this.#drill?.reject(reason);
    this.#drill = null;
    // Wake a reconnect loop out of its backoff sleep so it can observe the
    // close and stop, instead of retrying into a client that is gone.
    this.#closeRequested?.();
    await this.#transport?.close();
  }

  // --- Session lifecycle ----------------------------------------------------

  async #openAndAuthenticate(): Promise<void> {
    const transport = await openTransport(this.#options.url, {
      ...(this.#options.fetch ? { fetch: this.#options.fetch } : {}),
    });
    this.#transport = transport;

    // Frames go through the bounded queue rather than straight to the router:
    // that is where backpressure lives, and a synchronous handler would let a
    // slow application grow an unbounded backlog inside the transport.
    transport.onFrame((body) => {
      void this.#inbound.push(decodeMessage(body)).catch((err: unknown) => {
        this.#fail(err instanceof Error ? err : new Error(String(err)));
      });
    });
    transport.onClose((reason) => {
      if (!this.#closed) this.#onDisconnected(reason);
    });

    const auth = await this.#request('Authenticate', (id) => [
      id,
      this.#options.token ?? new Uint8Array(0),
      null,
      null,
      null,
    ]);
    const identity = auth.payload[1];
    this.#identity = identity instanceof Uint8Array ? identity : null;

    // The push stream only exists on HTTP; on TCP the connection carries
    // server-initiated frames itself.
    if (transport instanceof HttpTransport) transport.openPushStream();
  }

  #onDisconnected(reason: Error | null): void {
    // The cache is retained but marked behind the server, and no callbacks
    // fire until it has been reconciled (SDK-047).
    this.#cache.markStale();
    const err = reason ?? new Error('connection closed');
    for (const pending of this.#pending.values()) pending.reject(err);
    this.#pending.clear();

    // A confirmed schema failure is not retriable: the bindings are stale and
    // reconnecting cannot regenerate them (SDK-043).
    if (this.#options.reconnect === false || this.#reconnecting || this.#schemaFailure !== null) {
      return;
    }
    this.#reconnecting = true;
    void reconnect(
      {
        connect: async () => {
          // Thrown rather than returned: `fatal` below recognizes the closed
          // client and aborts the loop instead of scheduling another attempt.
          if (this.#closed) throw new Error('client closed');
          await this.#openAndAuthenticate();
        },
        // Authentication happens inside connect: on this transport the token
        // travels in the same handshake that establishes the session.
        authenticate: async () => {},
        resubscribe: async () => {
          if (this.#queries.size === 0) return;
          const queries = [...this.#queries];
          const messages = await this.#requestMany(
            'Subscribe',
            (id) => [id, queries],
            queries.length,
          );
          const payloads = messages.map((m) => m.payload);
          const mismatch = this.#schemaMismatch(payloads);
          if (mismatch !== null) {
            // Second sighting = confirmation: the refresh-and-reconnect pass
            // already ran (or this reconnect IS that pass) and the server
            // still answers with a version these bindings were not generated
            // from. The error is fatal to the loop (see `fatal` below).
            if (this.#mismatchRetried) {
              throw this.#confirmMismatch(mismatch.expected, mismatch.actual);
            }
            // First sighting on a reconnect — the server migrated while this
            // client was away. Run the refresh half of the drill here, then
            // fail the attempt so the loop retries once against fresh state.
            this.#mismatchRetried = true;
            const refreshed = await this.#refreshSchemaVersion();
            if (refreshed !== null && refreshed !== mismatch.expected) {
              throw this.#confirmMismatch(mismatch.expected, refreshed);
            }
            throw new Error(
              `InitialData.schema_version ${mismatch.actual} != ${mismatch.expected}; ` +
                'refreshed the schema, retrying once (SDK-043)',
            );
          }
          // Stashed for the reconcile step, which must run after every query
          // is registered — reconciling per-query would delete rows the next
          // query is about to restore.
          this.#pendingSnapshot = payloads;
        },
        reconcile: async () => {
          if (this.#pendingSnapshot === null) return;
          // Merged into one reconcile pass: `RowCache.reconcile` treats a
          // table absent from its input as unsubscribed and drops its rows,
          // so feeding it one query at a time would delete everything the
          // other queries cover.
          const tables = this.#pendingSnapshot.flatMap((payload) => {
            const [, , t] = payload as [unknown, unknown, unknown];
            return Array.isArray(t) ? t : [];
          });
          this.#pendingSnapshot = null;
          this.#dispatch(this.#cache.reconcile(this.#toSnapshots(tables)));
        },
      },
      {
        ...(this.#options.reconnect ?? {}),
        // A backoff sleep races the close signal: `close()` during a 30 s
        // backoff must stop the loop now, not half a minute later.
        sleep: (ms) =>
          new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, ms);
            void this.#closeSignal.then(() => {
              clearTimeout(timer);
              resolve();
            });
          }),
        fatal: (err) => this.#closed || err instanceof SchemaMismatchError,
      },
    ).then(
      () => {
        this.#reconnecting = false;
        // A matching InitialData ends the mismatch episode, if one was open.
        this.#mismatchRetried = false;
        const drill = this.#drill;
        this.#drill = null;
        drill?.resolve();
      },
      (err: unknown) => {
        this.#reconnecting = false;
        const error = err instanceof Error ? err : new Error(String(err));
        const drill = this.#drill;
        this.#drill = null;
        // With a drill in flight the error surfaces through the `subscribe`
        // that started it; otherwise nobody is awaiting, so it goes to the
        // connection-level listeners — unless the loop stopped because the
        // application closed the client, which is not a failure at all.
        if (drill !== null) drill.reject(error);
        else if (!this.#closed) this.#fail(error);
      },
    );
  }

  // --- Request correlation (RPC-002) ---------------------------------------

  /** Send a request expecting exactly one reply. */
  async #request(tag: string, build: (id: number) => unknown[]): Promise<DecodedMessage> {
    const [first] = await this.#requestMany(tag, build, 1);
    // Unreachable: `#requestMany` resolves only once `expected` replies are
    // collected, and `expected` is 1 here.
    if (first === undefined) throw new Error(`${tag} produced no reply`);
    return first;
  }

  /** Send a request expecting `expected` replies, all echoing its id. */
  async #requestMany(
    tag: string,
    build: (id: number) => unknown[],
    expected: number,
  ): Promise<DecodedMessage[]> {
    const transport = this.#transport;
    if (transport === null || this.#closed) throw new Error('client is not connected');

    const id = this.#nextId++;
    const settled = new Promise<DecodedMessage[]>((resolve, reject) => {
      this.#pending.set(id, { resolve, reject, expected, collected: [] });
    });
    try {
      await transport.send(encodeMessage(tag, build(id)));
    } catch (err) {
      // The caller gets THIS error, so `settled` will never be awaited —
      // removed unrejected, or a later disconnect would reject a promise
      // with no listener and surface as an unhandled rejection.
      this.#pending.delete(id);
      throw err;
    }
    return settled;
  }

  /** Drain decoded frames forever, routing each to its waiter or the cache. */
  async #pump(): Promise<void> {
    for (;;) {
      let message: DecodedMessage;
      try {
        message = await this.#inbound.shift();
      } catch {
        return; // the queue closed: the client is shutting down
      }
      try {
        this.#route(message);
      } catch (err) {
        this.#fail(err instanceof Error ? err : new Error(String(err)));
      }
    }
  }

  #route(message: DecodedMessage): void {
    switch (message.tag) {
      case 'TxUpdate': {
        // Field 5 is `tables`; the four before it are tx_id, timestamp,
        // reducer_name and caller.
        const diffs = this.#toDiffs(message.payload[5]);
        this.#dispatch(this.#cache.applyTxUpdate(diffs));
        return;
      }
      case 'Error': {
        const [id, code, catalog, text] = message.payload as [unknown, unknown, unknown, unknown];
        const err = new ServerError(Number(code), String(catalog), String(text));
        // A null id is server-initiated and belongs to nobody in particular.
        if (typeof id === 'number') this.#settle(id, () => err);
        else this.#fail(err);
        return;
      }
      default: {
        const id = message.payload[0];
        if (typeof id === 'number') this.#settle(id, () => message);
      }
    }
  }

  #settle(id: number, produce: () => DecodedMessage | Error): void {
    const pending = this.#pending.get(id);
    if (pending === undefined) return;

    const result = produce();
    if (result instanceof Error) {
      // An error ends the request whatever it was expecting: the server stops
      // at the first failing query in a batch rather than answering the rest.
      this.#pending.delete(id);
      pending.reject(result);
      return;
    }

    pending.collected.push(result);
    if (pending.collected.length < pending.expected) return;
    this.#pending.delete(id);
    pending.resolve(pending.collected);
  }

  // --- Schema-mismatch drill (SDK-043, SPEC-011 acceptance 9) ---------------

  /**
   * The first payload whose `schema_version` differs from the embedded one,
   * or null when everything matches (or no version was embedded).
   */
  #schemaMismatch(payloads: unknown[][]): { expected: number; actual: number } | null {
    const expected = this.#options.schemaVersion;
    if (expected === undefined) return null;
    for (const payload of payloads) {
      const actual = Number(payload[1]);
      if (actual !== expected) return { expected, actual };
    }
    return null;
  }

  /**
   * SDK-043's mandated sequence: refresh the schema, reconnect once, and only
   * when the mismatch is CONFIRMED surface the typed error. Resolves silently
   * when the reconnect finds the server back on the bindings' version — a
   * read racing a migration window looks exactly like this.
   */
  async #runSchemaDrill(mismatch: { expected: number; actual: number }): Promise<void> {
    if (this.#mismatchRetried || this.#options.reconnect === false) {
      throw this.#confirmMismatch(mismatch.expected, mismatch.actual);
    }
    this.#mismatchRetried = true;

    // The refresh half. When /schema is reachable and still reports a version
    // the bindings were not generated from, no reconnect can fix it — the
    // confirmation is immediate.
    const refreshed = await this.#refreshSchemaVersion();
    if (refreshed !== null && refreshed !== mismatch.expected) {
      throw this.#confirmMismatch(mismatch.expected, refreshed);
    }

    // Either the refreshed document matches the bindings (transient mismatch)
    // or /schema was unreachable and InitialData remains the only witness.
    // Reconnect once; the loop's resubscribe re-checks the version and either
    // reconciles fresh data or confirms the failure.
    const drill = new Promise<void>((resolve, reject) => {
      this.#drill = { resolve, reject };
    });
    await this.#transport?.close();
    return drill;
  }

  /** Record the terminal failure that blocks further reconnects. */
  #confirmMismatch(expected: number, actual: number): SchemaMismatchError {
    const err = new SchemaMismatchError(expected, actual);
    this.#schemaFailure = err;
    return err;
  }

  /**
   * Best-effort `GET /schema` (SDK-043's refresh).
   *
   * Returns the server's current `schema_version`, or null when the document
   * cannot be read: a TCP client has no HTTP surface to ask, and over HTTP
   * the admin guard (SEC-054) only admits loopback and trusted operators.
   * Null is not an error — the reconnect half of the drill decides from the
   * next `InitialData` instead.
   */
  async #refreshSchemaVersion(): Promise<number | null> {
    const base = this.#options.url.replace(/\/+$/, '');
    if (!/^https?:\/\//i.test(base)) return null;
    const fetchImpl = this.#options.fetch ?? globalThis.fetch.bind(globalThis);
    try {
      const response = await fetchImpl(`${base}/schema`, {
        headers: { Accept: 'application/json' },
      });
      if (!response.ok) return null;
      const doc: unknown = await response.json();
      // The admin surface wraps payloads in the RPC-052 envelope; an exported
      // schema.json is the bare document. Accept both.
      const payload =
        typeof doc === 'object' && doc !== null && 'payload' in doc
          ? (doc as { payload: unknown }).payload
          : doc;
      if (typeof payload !== 'object' || payload === null) return null;
      const version = (payload as { schema_version?: unknown }).schema_version;
      return typeof version === 'number' ? version : null;
    } catch {
      return null;
    }
  }

  // --- Cache application ----------------------------------------------------

  #applyInitialData(payload: unknown[], options: { reconcile: boolean }): void {
    // The schema_version gate ran in the caller (#schemaMismatch) — by the
    // time a payload reaches here it is known to match the bindings.
    const [, , tables] = payload as [unknown, unknown, unknown];

    if (options.reconcile) {
      this.#dispatch(this.#cache.reconcile(this.#toSnapshots(tables)));
      return;
    }
    // A fresh subscription is a diff from nothing: inserts only, so callbacks
    // fire for the initial rows exactly as they would for live ones.
    this.#dispatch(this.#cache.applyTxUpdate(this.#toDiffs(tables)));
  }

  #toDiffs(tables: unknown): TableDiff[] {
    if (!Array.isArray(tables)) return [];
    return tables.map((entry) => {
      // TableUpdate: table_id, table_name, query_id, inserts, deletes.
      const [, name, , inserts, deletes] = entry as [unknown, unknown, unknown, unknown, unknown];
      return {
        table: String(name),
        inserts: sliceRowList(inserts),
        deletes: sliceRowList(deletes),
      };
    });
  }

  #toSnapshots(tables: unknown): TableSnapshot[] {
    return this.#toDiffs(tables).map((diff) => ({ table: diff.table, rows: diff.inserts }));
  }

  #dispatch(events: RowEvent[]): void {
    // The cache finished every mutation before returning these (SDK-045), so
    // a listener that reads the cache here sees the whole post-commit state.
    for (const event of events) {
      const listeners = this.#listeners.get(`${event.table}:${event.kind}`);
      if (listeners === undefined) continue;
      for (const listener of listeners) {
        if (event.kind === 'update') listener(event.row, event.old);
        else listener(event.row);
      }
    }
  }

  #fail(err: Error): void {
    if (this.#errorListeners.size === 0) {
      // Nobody is listening. A connection-level failure that vanishes is far
      // worse than a noisy log, so it goes to the console.
      console.error('[fluxum]', err);
      return;
    }
    for (const listener of this.#errorListeners) listener(err);
  }
}
