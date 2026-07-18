// FluxBIN — the schema-driven binary row encoding (SPEC-006 RPC-040..042).
//
// No field names and no per-value type tags: the schema supplies the type
// context, so a row is its column values back-to-back in declaration order.
// All integers are little-endian. This is the hot path — rows are decoded
// straight out of the frame's ArrayBuffer via DataView, and JSON never enters
// it (SDK-084).

/** A column's declared type, as `/schema` spells it. */
export type FluxType =
  | 'Bool'
  | 'I8' | 'I16' | 'I32' | 'I64'
  | 'U8' | 'U16' | 'U32' | 'U64'
  | 'F32' | 'F64'
  | 'Str'
  | 'Bytes'
  | 'Identity'
  | 'ConnectionId'
  | 'EntityId'
  | 'Timestamp';

/** A decoded column value. */
export type FluxValue = boolean | number | bigint | string | Uint8Array;

/** Thrown when bytes do not match the schema being decoded against. */
export class FluxBinError extends Error {
  constructor(message: string) {
    super(`fluxbin: ${message}`);
    this.name = 'FluxBinError';
  }
}

const textDecoder = new TextDecoder('utf-8', { fatal: true });

/** Render raw bytes as lowercase hex — how Identity/ConnectionId surface. */
export function toHex(bytes: Uint8Array): string {
  let out = '';
  for (const byte of bytes) out += byte.toString(16).padStart(2, '0');
  return out;
}

/** Sequential FluxBIN reader over a row buffer. */
export class RowReader {
  private readonly view: DataView;
  private readonly bytes: Uint8Array;
  offset = 0;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  get remaining(): number {
    return this.bytes.byteLength - this.offset;
  }

  private need(n: number): void {
    if (this.remaining < n) {
      throw new FluxBinError(`unexpected end of row: needed ${n}, have ${this.remaining}`);
    }
  }

  private take(n: number): Uint8Array {
    this.need(n);
    const out = this.bytes.subarray(this.offset, this.offset + n);
    this.offset += n;
    return out;
  }

  /** Read one value of `type`. */
  read(type: FluxType): FluxValue {
    switch (type) {
      case 'Bool': {
        this.need(1);
        const b = this.view.getUint8(this.offset++);
        if (b > 1) throw new FluxBinError(`invalid bool byte 0x${b.toString(16)}`);
        return b === 1;
      }
      case 'I8': {
        this.need(1);
        return this.view.getInt8(this.offset++);
      }
      case 'U8': {
        this.need(1);
        return this.view.getUint8(this.offset++);
      }
      case 'I16': {
        this.need(2);
        const v = this.view.getInt16(this.offset, true);
        this.offset += 2;
        return v;
      }
      case 'U16': {
        this.need(2);
        const v = this.view.getUint16(this.offset, true);
        this.offset += 2;
        return v;
      }
      case 'I32': {
        this.need(4);
        const v = this.view.getInt32(this.offset, true);
        this.offset += 4;
        return v;
      }
      case 'U32': {
        this.need(4);
        const v = this.view.getUint32(this.offset, true);
        this.offset += 4;
        return v;
      }
      // 64-bit values stay bigint: rounding a u64 pk through a double is how
      // two distinct rows silently become one.
      case 'I64':
      case 'Timestamp': {
        this.need(8);
        const v = this.view.getBigInt64(this.offset, true);
        this.offset += 8;
        return v;
      }
      case 'U64':
      case 'EntityId': {
        this.need(8);
        const v = this.view.getBigUint64(this.offset, true);
        this.offset += 8;
        return v;
      }
      case 'F32': {
        this.need(4);
        const v = this.view.getFloat32(this.offset, true);
        this.offset += 4;
        return v;
      }
      case 'F64': {
        this.need(8);
        const v = this.view.getFloat64(this.offset, true);
        this.offset += 8;
        return v;
      }
      case 'Str': {
        const len = this.readLen();
        try {
          return textDecoder.decode(this.take(len));
        } catch {
          throw new FluxBinError('string is not valid UTF-8');
        }
      }
      case 'Bytes': {
        const len = this.readLen();
        return this.take(len);
      }
      // Fixed-width raw blobs, no length prefix; surfaced as hex, which is
      // how identities are written everywhere else (admin JSON, logs).
      case 'Identity':
        return toHex(this.take(32));
      case 'ConnectionId':
        return toHex(this.take(16));
      default: {
        const exhaustive: never = type;
        throw new FluxBinError(`unsupported type ${String(exhaustive)}`);
      }
    }
  }

  private readLen(): number {
    this.need(4);
    const len = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return len;
  }
}

/** Decode one row into an object keyed by column name. */
export function decodeRow(
  bytes: Uint8Array,
  columns: readonly { name: string; type: FluxType }[],
): Record<string, FluxValue> {
  const reader = new RowReader(bytes);
  const row: Record<string, FluxValue> = {};
  for (const column of columns) {
    row[column.name] = reader.read(column.type);
  }
  if (reader.remaining !== 0) {
    // A row longer than its schema means the two sides disagree about the
    // table — surfacing it beats handing back a half-decoded row.
    throw new FluxBinError(
      `row has ${reader.remaining} trailing byte(s): schema mismatch for this table`,
    );
  }
  return row;
}
