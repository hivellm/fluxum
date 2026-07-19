// Streamable HTTP transport (SPEC-006 RPC-004..RPC-007).
//
// Two halves of one session:
//   POST /rpc  — client→server frames; the response body streams back the
//                answers, correlated by id, as each completes (RPC-005).
//   GET  /rpc  — a long-lived stream of server-initiated frames: InitialData,
//                TxUpdate, Error (RPC-006).
//
// Binary end to end. `Content-Type: application/x-fluxum`, raw FluxRPC frames
// concatenated back to back — no SSE, no base64, no JSON anywhere on this path
// (SDK-080). Response bodies are consumed incrementally through the fetch
// `ReadableStream`, which is what makes this work unchanged in a browser.

import { FluxumFrameReader, DEFAULT_MAX_FRAME_BYTES } from '../protocol.ts';
import { SessionExpiredError, TransportError } from './types.ts';
import type { CloseHandler, FrameHandler, Transport } from './types.ts';

/** The binary content type both directions use (RPC-004). */
export const FLUXUM_CONTENT_TYPE = 'application/x-fluxum';

/** The session header the server issues and the client echoes (RPC-007). */
export const SESSION_HEADER = 'Fluxum-Session';

export interface HttpTransportOptions {
  /** Frame cap (RPC-061). Defaults to Fluxum's 16 MB. */
  maxFrameBytes?: number;
  /** Injected in tests; defaults to the global `fetch`. */
  fetch?: typeof globalThis.fetch;
}

/**
 * One Fluxum session over Streamable HTTP.
 *
 * The session id is learned, not configured: the server issues it on the
 * response carrying the first successful `AuthResult` (RPC-007), and every
 * later POST and the GET stream echo it back.
 */
export class HttpTransport implements Transport {
  readonly #url: string;
  readonly #fetch: typeof globalThis.fetch;
  readonly #maxFrameBytes: number;

  #session: string | null = null;
  #frameHandler: FrameHandler | null = null;
  #closeHandler: CloseHandler | null = null;
  #closed = false;
  #pushController: AbortController | null = null;

  constructor(url: string, options: HttpTransportOptions = {}) {
    this.#url = url.replace(/\/+$/, '') + '/rpc';
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
  }

  /** The session id, once the server has issued one. */
  get sessionId(): string | null {
    return this.#session;
  }

  onFrame(handler: FrameHandler): void {
    this.#frameHandler = handler;
  }

  onClose(handler: CloseHandler): void {
    this.#closeHandler = handler;
  }

  async send(frame: Uint8Array): Promise<void> {
    if (this.#closed) throw new TransportError('transport is closed');

    const response = await this.#fetch(this.#url, {
      method: 'POST',
      headers: this.#headers(),
      body: frame as BodyInit,
    });

    // RPC-007: an unknown or expired session is a 404, and the recovery is
    // re-auth + resubscribe rather than a retry of this request.
    if (response.status === 404) {
      throw new SessionExpiredError();
    }
    if (response.status === 415) {
      throw new TransportError(
        `server rejected ${FLUXUM_CONTENT_TYPE}; this endpoint is not FluxRPC`,
        415,
      );
    }
    if (!response.ok) {
      throw new TransportError(`POST /rpc failed with HTTP ${response.status}`, response.status);
    }

    // The session arrives on the response carrying the first AuthResult.
    // Captured before draining, so it is set even if the body is empty.
    const issued = response.headers.get(SESSION_HEADER);
    if (issued !== null && issued !== '') this.#session = issued;

    if (response.body === null) return;
    await this.#drain(response.body);
  }

  /**
   * Open the server-initiated push stream (RPC-006).
   *
   * Resolves once the stream is established, not when it ends — the stream is
   * long-lived by design, and awaiting its end would never return. Frames flow
   * to `onFrame` until the connection closes.
   */
  async openPushStream(): Promise<void> {
    if (this.#closed) throw new TransportError('transport is closed');
    if (this.#session === null) {
      throw new TransportError('cannot open the push stream before authenticating (RPC-007)');
    }
    // RPC-006: at most one stream per session, and a new one closes the old.
    // Doing that here as well keeps a reconnect from leaking the previous
    // reader if the server has not yet reaped it.
    this.#pushController?.abort();

    const controller = new AbortController();
    this.#pushController = controller;

    const response = await this.#fetch(this.#url, {
      method: 'GET',
      headers: this.#headers(),
      signal: controller.signal,
    });

    if (response.status === 404) throw new SessionExpiredError();
    if (!response.ok) {
      throw new TransportError(`GET /rpc failed with HTTP ${response.status}`, response.status);
    }
    if (response.body === null) {
      throw new TransportError('GET /rpc returned no body; cannot receive pushes');
    }

    // Not awaited: the stream outlives this call. Failures surface through
    // `onClose`, which is the only place a long-lived stream can report.
    void this.#drain(response.body).then(
      () => this.#finish(null),
      (err: unknown) => this.#finish(asError(err)),
    );
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#pushController?.abort();
    this.#finish(null);
  }

  #headers(): Record<string, string> {
    const headers: Record<string, string> = { 'Content-Type': FLUXUM_CONTENT_TYPE };
    if (this.#session !== null) headers[SESSION_HEADER] = this.#session;
    return headers;
  }

  /**
   * Feed a response body through the frame reader.
   *
   * Chunk boundaries are meaningless here — a frame can span several, and
   * several can share one — which is exactly what the reader absorbs. Nothing
   * downstream ever sees a partial frame.
   */
  async #drain(body: ReadableStream<Uint8Array>): Promise<void> {
    const reader = new FluxumFrameReader({ maxFrameBytes: this.#maxFrameBytes });
    const source = body.getReader();
    try {
      for (;;) {
        const { done, value } = await source.read();
        if (done) return;
        if (value === undefined || value.length === 0) continue;
        reader.push(value);
        for (;;) {
          const frameBody = reader.nextBody();
          if (frameBody === null) break;
          this.#frameHandler?.(frameBody);
        }
      }
    } finally {
      source.releaseLock();
    }
  }

  #finish(reason: Error | null): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#closeHandler?.(reason);
  }
}

function asError(err: unknown): Error {
  if (err instanceof Error) return err;
  return new TransportError(String(err));
}
