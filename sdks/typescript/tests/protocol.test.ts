// Fluxum envelope + RowList tests, and the one place Fluxum's framing
// semantics differ from Thunder's (keep-alive frames).
import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  FluxumFrameReader,
  KEEPALIVE_FRAME,
  ProtocolError,
  decodeMessage,
  encodeFrame,
  encodeMessage,
  sliceRowList,
} from '../src/protocol.ts';

test('a message round-trips through the tagged envelope', () => {
  const frame = encodeMessage('Authenticate', [1, new Uint8Array([1, 2]), null, null, null]);
  const reader = new FluxumFrameReader();
  reader.push(frame);
  const body = reader.nextBody();
  assert.ok(body, 'a complete frame yields a body');
  const message = decodeMessage(body);
  assert.equal(message.tag, 'Authenticate');
  assert.equal(message.payload[0], 1);
});

test('keep-alive frames are consumed, not surfaced or thrown on', () => {
  // The divergence that matters: Thunder treats a zero-length body as a
  // decode error; Fluxum sends it as a liveness tick on the GET stream. If
  // this regresses, an idle HTTP subscription tears itself down.
  const reader = new FluxumFrameReader();
  reader.push(KEEPALIVE_FRAME);
  assert.equal(reader.nextBody(), null, 'a lone keep-alive yields nothing');

  // ...and a keep-alive ahead of a real frame does not eat it.
  reader.push(KEEPALIVE_FRAME);
  reader.push(encodeMessage('Error', [null, 408, 'idle']));
  const body = reader.nextBody();
  assert.ok(body, 'the real frame still arrives after keep-alives');
  assert.equal(decodeMessage(body).tag, 'Error');
});

test('a partial frame asks for more bytes instead of throwing', () => {
  const frame = encodeMessage('Ping', []);
  const reader = new FluxumFrameReader();
  reader.push(frame.subarray(0, 2));
  assert.equal(reader.nextBody(), null);
  reader.push(frame.subarray(2));
  assert.ok(reader.nextBody());
});

test('an oversized frame is refused from its prefix alone', () => {
  const reader = new FluxumFrameReader({ maxFrameBytes: 8 });
  const header = new Uint8Array(4);
  new DataView(header.buffer).setUint32(0, 4096, true);
  reader.push(header); // no body at all — the cap must fire anyway
  assert.throws(() => reader.nextBody());
});

test('a non-envelope body is a typed protocol error', () => {
  const bogus = encodeFrame(new Uint8Array([0x92, 0x01, 0x02])); // [1, 2]
  const reader = new FluxumFrameReader();
  reader.push(bogus);
  const body = reader.nextBody();
  assert.ok(body);
  assert.throws(() => decodeMessage(body), ProtocolError);
});

test('a Fixed RowList slices without copying', () => {
  const data = new Uint8Array([1, 2, 3, 4, 5, 6]);
  const rows = sliceRowList([3, ['Fixed', 2], data]);
  assert.equal(rows.length, 3);
  assert.deepEqual([...rows[1]!], [3, 4]);
  // Subarrays share the frame buffer — that is the point of the flat encoding.
  assert.equal(rows[0]!.buffer, data.buffer);
});

test('an Offsets RowList slices variable-size rows', () => {
  const data = new Uint8Array([1, 2, 3, 4, 5, 6]);
  const rows = sliceRowList([2, ['Offsets', [0, 4]], data]);
  assert.deepEqual([...rows[0]!], [1, 2, 3, 4]);
  assert.deepEqual([...rows[1]!], [5, 6]);
});

test('an inconsistent RowList is rejected, not half-decoded', () => {
  // row_count x size != rows_data length (RPC-032 validation).
  assert.throws(() => sliceRowList([4, ['Fixed', 2], new Uint8Array(6)]), ProtocolError);
  assert.throws(() => sliceRowList([2, ['Offsets', [0]], new Uint8Array(6)]), ProtocolError);
  assert.throws(() => sliceRowList([1, ['Offsets', [99]], new Uint8Array(6)]), ProtocolError);
});
