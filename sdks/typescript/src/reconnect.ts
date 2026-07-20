// Automatic session re-establishment (SPEC-011 SDK-047, SDK-082).
//
// This lives in the core runtime rather than an optional framework layer,
// which SDK-047 requires and calls a Fluxum differentiator: no SpacetimeDB SDK
// reconnects at the core layer, and none reconciles the cache on reconnect.
//
// The sequence is fixed — connect, authenticate, resubscribe, reconcile — and
// the order matters. Reconciling before resubscribing would compare the cache
// against an InitialData that does not yet cover the queries the application
// registered, and dutifully delete every row it could not see.

/** What the loop needs to rebuild a session. Each step is the caller's. */
export interface ReconnectHandlers {
  /** Open a transport. Throwing schedules another attempt. */
  connect(): Promise<void>;
  /** Authenticate the fresh connection (SPEC-009). */
  authenticate(): Promise<void>;
  /** Re-register every active subscription. */
  resubscribe(): Promise<void>;
  /** Reconcile the cache from the fresh `InitialData` and dispatch net-difference events. */
  reconcile(): Promise<void>;
}

export interface BackoffOptions {
  /** First delay. Default 100 ms. */
  initialMs?: number;
  /** Ceiling for the delay. Default 30 s. */
  maxMs?: number;
  /** Growth factor per attempt. Default 2. */
  factor?: number;
  /**
   * Random fraction of the delay added or removed. Default 0.2.
   *
   * Without jitter, every client knocked off by the same server restart comes
   * back on the same schedule and re-creates the load that took it down.
   */
  jitter?: number;
  /** Give up after this many consecutive failures. Default Infinity. */
  maxAttempts?: number;
}

/** Every attempt failed up to `maxAttempts`. */
export class ReconnectFailedError extends Error {
  readonly attempts: number;
  readonly last: Error;
  constructor(attempts: number, last: Error) {
    super(`reconnect gave up after ${attempts} attempts: ${last.message}`);
    this.name = 'ReconnectFailedError';
    this.attempts = attempts;
    this.last = last;
  }
}

/**
 * Delay before attempt `n` (0-based), exponential with jitter and a ceiling.
 *
 * Exported because a caller that shows "retrying in Ns" needs the same number
 * the loop will actually wait, and recomputing it elsewhere would drift.
 */
export function backoffDelay(attempt: number, options: BackoffOptions = {}): number {
  const initial = options.initialMs ?? 100;
  const max = options.maxMs ?? 30_000;
  const factor = options.factor ?? 2;
  const jitter = options.jitter ?? 0.2;

  const raw = Math.min(initial * factor ** attempt, max);
  if (jitter <= 0) return raw;
  const spread = raw * jitter;
  return Math.max(0, raw + (Math.random() * 2 - 1) * spread);
}

export interface ReconnectOptions extends BackoffOptions {
  /** Injected in tests so they do not spend real seconds sleeping. */
  sleep?: (ms: number) => Promise<void>;
  /** Called before each attempt, for logging or UI. */
  onAttempt?: (attempt: number, delayMs: number) => void;
  /**
   * A failure no retry can fix — a confirmed schema mismatch (SDK-043), for
   * one. The loop rethrows it immediately instead of backing off toward a
   * server that will give the same answer forever.
   */
  fatal?: (err: Error) => boolean;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Re-establish a session, retrying with exponential backoff.
 *
 * Resolves once the cache has been reconciled — that is, once the application
 * is looking at fresh data again, not merely once a socket is open.
 */
export async function reconnect(
  handlers: ReconnectHandlers,
  options: ReconnectOptions = {},
): Promise<number> {
  const sleep = options.sleep ?? defaultSleep;
  const maxAttempts = options.maxAttempts ?? Number.POSITIVE_INFINITY;
  let last: Error = new Error('no attempt was made');

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const delay = attempt === 0 ? 0 : backoffDelay(attempt - 1, options);
    options.onAttempt?.(attempt, delay);
    if (delay > 0) await sleep(delay);

    try {
      await handlers.connect();
      await handlers.authenticate();
      // Before reconcile, always: InitialData must cover every active query,
      // or reconciliation reads the gap as rows having been deleted.
      await handlers.resubscribe();
      await handlers.reconcile();
      return attempt + 1;
    } catch (err) {
      last = err instanceof Error ? err : new Error(String(err));
      if (options.fatal?.(last)) throw last;
    }
  }

  throw new ReconnectFailedError(maxAttempts, last);
}
