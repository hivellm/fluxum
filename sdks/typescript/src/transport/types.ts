// What a carrier has to provide, and nothing more.
//
// TCP and Streamable HTTP carry identical messages with identical framing
// (SPEC-006 RPC-003) — only the carrier differs. So everything above this
// interface (the envelope catalog, multiplexing, the cache) is written once
// and neither transport gets to have an opinion about it.

/** A decoded frame body, ready for `decodeMessage`. Keep-alives never reach here. */
export type FrameHandler = (body: Uint8Array) => void;

/** Why a transport stopped. `null` means the caller asked it to. */
export type CloseHandler = (reason: Error | null) => void;

/**
 * A live connection to a Fluxum server.
 *
 * Deliberately frame-shaped rather than request-shaped: correlating an `id`
 * back to a caller is multiplexing (RPC-002), which is identical on both
 * carriers and therefore belongs above this layer, not duplicated inside each.
 */
export interface Transport {
  /** Send one already-framed message. */
  send(frame: Uint8Array): Promise<void>;

  /** Called for every inbound frame body, in arrival order. */
  onFrame(handler: FrameHandler): void;

  /** Called once when the connection ends, for any reason. */
  onClose(handler: CloseHandler): void;

  /** Close the connection. Idempotent. */
  close(): Promise<void>;
}

/** A transport-level failure, carrying the wire code when one applies. */
export class TransportError extends Error {
  /** SPEC-028 wire code, or an HTTP status for HTTP-level failures. */
  readonly code: number | undefined;

  constructor(message: string, code?: number) {
    super(message);
    this.name = 'TransportError';
    this.code = code;
  }
}

/**
 * The session expired or was never known (RPC-007: HTTP 404).
 *
 * Distinct from a generic failure because the recovery is specific and
 * automatic: re-authenticate, then resubscribe (RPC-062). A caller that
 * cannot tell this apart from a network error would either retry forever
 * against a dead session or tear down a healthy client.
 */
export class SessionExpiredError extends TransportError {
  constructor(message = 'session expired; re-authenticate and resubscribe') {
    super(message, 404);
    this.name = 'SessionExpiredError';
  }
}
