"""The SDK's own unit surface: the codec pieces the conformance corpus drives
indirectly, pinned here directly (SPEC-013 — an SDK's own suite proves how it
surfaces the protocol; the corpus proves protocol-observable behavior)."""

from __future__ import annotations

import struct

import pytest

from fluxum import _msgpack
from fluxum.fluxbin import FluxBinError, RowReader, decode_row, to_hex
from fluxum.protocol import (
    FrameReader,
    ProtocolError,
    decode_message,
    encode_frame,
    encode_message,
    slice_row_list,
)


def test_msgpack_roundtrips_every_used_shape():
    for value in [
        None,
        True,
        False,
        0,
        127,
        -1,
        -32,
        255,
        65535,
        4_294_967_295,
        2**63,
        2**64 - 1,
        -128,
        -32768,
        -(2**31),
        -(2**63),
        3.5,
        "hello",
        "u" * 40,  # str8
        b"\x00\x01\x02",
        [1, "two", [3, 4]],
        {"a": 1, "b": [2, 3]},
    ]:
        assert _msgpack.decode(_msgpack.encode(value)) == value


def test_msgpack_rejects_trailing_bytes():
    with pytest.raises(_msgpack.MsgpackError):
        _msgpack.decode(_msgpack.encode(1) + b"\x02")


def test_fluxbin_reads_each_type_little_endian():
    # id:U64=1, done:Bool=true, x:I32=-5, name:Str="hi"
    row = struct.pack("<Q", 1) + b"\x01" + struct.pack("<i", -5) + struct.pack("<I", 2) + b"hi"
    decoded = decode_row(
        row, [("id", "U64"), ("done", "Bool"), ("x", "I32"), ("name", "Str")]
    )
    assert decoded == {"id": 1, "done": True, "x": -5, "name": "hi"}


def test_fluxbin_identity_is_hex_and_trailing_bytes_are_an_error():
    ident = bytes(range(32))
    assert to_hex(ident) == ident.hex()
    assert RowReader(ident).read("Identity") == ident.hex()
    with pytest.raises(FluxBinError):
        decode_row(b"\x01\x02", [("b", "Bool")])  # one trailing byte


def test_frame_reader_skips_keepalives_and_splits_frames():
    reader = FrameReader()
    body_a = _msgpack.encode(["A", [1]])
    body_b = _msgpack.encode(["B", [2]])
    keepalive = b"\x00\x00\x00\x00"
    # Two frames with a keep-alive between them, delivered in torn chunks.
    stream = encode_frame(body_a) + keepalive + encode_frame(body_b)
    reader.push(stream[:3])
    assert reader.next_body() is None  # header not yet complete
    reader.push(stream[3:])
    assert reader.next_body() == body_a
    assert reader.next_body() == body_b  # keep-alive was skipped
    assert reader.next_body() is None


def test_envelope_encode_decode_is_positional():
    frame = encode_message("ReducerCall", [7, "add_task", None, ["ship it"], None])
    # Strip the 4-byte length prefix to get the body back.
    body = frame[4:]
    message = decode_message(body)
    assert message.tag == "ReducerCall"
    assert message.payload == [7, "add_task", None, ["ship it"], None]


def test_decode_message_rejects_a_non_pair_envelope():
    with pytest.raises(ProtocolError):
        decode_message(_msgpack.encode([1, 2, 3]))


def test_row_list_slices_fixed_and_offset_layouts():
    # Fixed: two 8-byte rows.
    data = struct.pack("<Q", 1) + struct.pack("<Q", 2)
    assert slice_row_list([2, ["Fixed", 8], data]) == [data[:8], data[8:]]
    # Offsets: variable-length rows.
    rows = [b"ab", b"cde"]
    packed = b"".join(rows)
    assert slice_row_list([2, ["Offsets", [0, 2]], packed]) == rows
    # Empty.
    assert slice_row_list([0, ["Fixed", 0], b""]) == []


def test_row_list_rejects_inconsistent_layout():
    with pytest.raises(ProtocolError):
        slice_row_list([2, ["Fixed", 8], b"\x00"])  # 2 rows x 8 != 1 byte
