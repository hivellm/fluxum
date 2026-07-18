// Fluxum message envelopes over the HiveLLM binary wire.
//
// Framing is NOT implemented here: it is the family standard and belongs to
// Thunder (`@hivehub/thunder`), which owns the `u32 LE length + MessagePack`
// codec, the frame cap, and the partial-buffer state machine in every
// language. This module is only what Thunder deliberately leaves to the
// product — Fluxum's own message catalog.
//
// Thunder's own split: "products keep what is theirs — command catalogs,
// URL schemes, capability semantics. Thunder owns everything below that
// line." Fluxum's line sits here: the RPC-011 tagged envelope
// (`[tag, payload]`), the RowList batches, and FluxBIN rows (see
// `fluxbin.ts`) are schema-driven and have no Thunder equivalent.
//
// Note the one place the two protocols differ in the framing layer itself:
// Fluxum uses a zero-length frame as a keep-alive (RPC-001/RPC-006) which
// receivers ignore, whereas Thunder treats a zero-length body as a decode
// error. `FluxumFrameReader` below handles that case before delegating, so
// keep-alives never reach Thunder's decoder.

import { FrameReader, DEFAULT_MAX_FRAME_BYTES as THUNDER_MAX_FRAME } from '@hivehub/thunder';
import { decode as decodeMsg, encode as encodeMsg } from '@msgpack/msgpack';

/** Bytes of the length prefix (the family standard). */
export const FRAME_HEADER_LEN = 4;

/**
 * Fluxum's `max_frame_bytes` (RPC-061): 16 MB — deliberately tighter than
 * Thunder's 64 MiB default, because a Fluxum frame carries one message, not
 * a bulk embedding payload.
 */
export const DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;

/** Thunder's cap, re-exported so a caller can see what it is relaxing to. */
export { THUNDER_MAX_FRAME };

/** Thrown on a malformed envelope. */
export class ProtocolError extends Error {
  /** The SPEC-028 wire code this maps to, when one applies. */
  readonly code: number | undefined;

  constructor(message: string, code?: number) {
    super(message);
    this.name = 'ProtocolError';
    this.code = code;
  }
}

/** The zero-length keep-alive frame (RPC-001). */
export const KEEPALIVE_FRAME = new Uint8Array([0, 0, 0, 0]);

/** Frame a message body for the wire. */
export function encodeFrame(body: Uint8Array): Uint8Array {
  const out = new Uint8Array(FRAME_HEADER_LEN + body.length);
  new DataView(out.buffer).setUint32(0, body.length, true);
  out.set(body, FRAME_HEADER_LEN);
  return out;
}

/**
 * A frame reader over Thunder's, adding Fluxum's keep-alive semantics.
 *
 * Thunder's `FrameReader` rejects a zero-length body; Fluxum's transports
 * send exactly that as a liveness tick. Rather than fork the reader, this
 * consumes a keep-alive itself and hands everything else to Thunder — so the
 * cap enforcement, partial-buffer handling and byte layout all stay in one
 * family-owned implementation.
 */
export class FluxumFrameReader {
  readonly #inner: FrameReader;
  // Explicitly `ArrayBufferLike`: chunks arrive from the transport and may be
  // backed by any buffer, so the field must not narrow to `ArrayBuffer` from
  // the empty-array initializer.
  #pending: Uint8Array<ArrayBufferLike> = new Uint8Array(0);

  constructor(options: { maxFrameBytes?: number } = {}) {
    this.#inner = new FrameReader({
      maxFrameBytes: options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES,
    });
  }

  /** Append bytes received from the transport. */
  push(chunk: Uint8Array): void {
    if (this.#pending.length === 0) {
      this.#pending = chunk;
    } else {
      const merged = new Uint8Array(this.#pending.length + chunk.length);
      merged.set(this.#pending, 0);
      merged.set(chunk, this.#pending.length);
      this.#pending = merged;
    }
    this.#drainKeepAlives();
    if (this.#pending.length > 0) {
      this.#inner.push(this.#pending);
      this.#pending = new Uint8Array(0);
    }
  }

  /**
   * The next complete message body, or `null` when more bytes are needed.
   * Keep-alives are consumed silently and never surface.
   */
  nextBody(): Uint8Array | null {
    return this.#inner.nextBody();
  }

  /** Strip any leading zero-length frames before Thunder sees them. */
  #drainKeepAlives(): void {
    while (this.#pending.length >= FRAME_HEADER_LEN) {
      const view = new DataView(
        this.#pending.buffer,
        this.#pending.byteOffset,
        this.#pending.byteLength,
      );
      if (view.getUint32(0, true) !== 0) return;
      this.#pending = this.#pending.subarray(FRAME_HEADER_LEN);
    }
  }
}

// --- Fluxum's tagged envelope (RPC-011) -------------------------------------

/**
 * Encode `[tag, payload]` and frame it.
 *
 * The payload is a positional array because compact MessagePack writes a
 * struct as an array in declaration order with no field names — so the field
 * order here IS the wire format. A new field is only compatible appended at
 * the TAIL; inserting one shifts everything after it and silently mis-decodes
 * older frames.
 */
export function encodeMessage(tag: string, payload: unknown[]): Uint8Array {
  return encodeFrame(encodeMsg([tag, payload]));
}

/** A decoded server message: its tag and positional payload. */
export interface ServerMessage {
  tag: string;
  payload: unknown[];
}

/** Decode one envelope body. */
export function decodeMessage(body: Uint8Array): ServerMessage {
  const value = decodeMsg(body);
  if (!Array.isArray(value) || value.length !== 2 || typeof value[0] !== 'string') {
    throw new ProtocolError('envelope is not a [tag, payload] pair', 400);
  }
  const payload: unknown = value[1];
  return { tag: value[0], payload: Array.isArray(payload) ? payload : [payload] };
}

// --- RowList (RPC-032) ------------------------------------------------------

/**
 * Slice a flat `RowList` into its rows.
 *
 * Wire shape: `[row_count, size_hint, rows_data]`, where `size_hint` is
 * `['Fixed', n]` (every row is n bytes) or `['Offsets', [start, ...]]`. Rows
 * come back as **subarrays of the frame buffer** — no copying, which is the
 * entire point of the flat encoding.
 */
export function sliceRowList(value: unknown): Uint8Array[] {
  if (!Array.isArray(value) || value.length < 3) {
    throw new ProtocolError('RowList is not a 3-field structure', 400);
  }
  const [countRaw, hint, data] = value as [unknown, unknown, unknown];
  const count = Number(countRaw);
  if (!(data instanceof Uint8Array)) {
    throw new ProtocolError('RowList.rows_data is not binary', 400);
  }
  if (!Array.isArray(hint) || typeof hint[0] !== 'string') {
    throw new ProtocolError('RowList.size_hint is not tagged', 400);
  }

  const rows: Uint8Array[] = [];
  if (hint[0] === 'Fixed') {
    const size = Number(hint[1]);
    if (size <= 0) {
      if (count !== 0) throw new ProtocolError('Fixed size_hint of 0 with rows present', 400);
      return rows;
    }
    if (data.byteLength !== count * size) {
      throw new ProtocolError(
        `inconsistent RowList: ${count} rows x ${size} bytes != ${data.byteLength}`,
        400,
      );
    }
    for (let i = 0; i < count; i++) rows.push(data.subarray(i * size, (i + 1) * size));
    return rows;
  }
  if (hint[0] === 'Offsets') {
    const offsets = hint[1] as unknown;
    if (!Array.isArray(offsets) || offsets.length !== count) {
      throw new ProtocolError('inconsistent RowList: offsets length != row_count', 400);
    }
    for (let i = 0; i < count; i++) {
      const start = Number(offsets[i]);
      const end = i + 1 < count ? Number(offsets[i + 1]) : data.byteLength;
      if (start > end || end > data.byteLength) {
        throw new ProtocolError('inconsistent RowList: offset out of range', 400);
      }
      rows.push(data.subarray(start, end));
    }
    return rows;
  }
  throw new ProtocolError(`unknown RowList size_hint '${hint[0]}'`, 400);
}
