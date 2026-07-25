"""Fluxum message envelopes over the HiveLLM binary wire.

Framing is the family standard: `u32 LE length + MessagePack body`, with a
zero-length body as the WIRE-024 keep-alive (RPC-001/RPC-006). On top of it
sit Fluxum's own pieces — the RPC-011 tagged envelope (`[tag, payload]`), the
RowList batches (RPC-032), and FluxBIN rows (`fluxbin`) — which are
schema-driven and have no family equivalent.
"""

from __future__ import annotations

import struct
from typing import Any, List, Optional, Tuple

from . import _msgpack

#: Bytes of the length prefix (the family standard).
FRAME_HEADER_LEN = 4

#: Fluxum's `max_frame_bytes` (RPC-061): 16 MB — one message per frame.
DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024


class ProtocolError(ValueError):
    """Raised on a malformed envelope or an oversized frame."""

    def __init__(self, message: str, code: Optional[int] = None) -> None:
        super().__init__(message)
        self.code = code


def encode_frame(body: bytes) -> bytes:
    """Frame a message body for the wire."""
    return struct.pack("<I", len(body)) + body


def encode_message(tag: str, payload: List[Any]) -> bytes:
    """Encode `[tag, payload]` and frame it (RPC-011).

    The payload is a positional array: compact MessagePack writes a struct as
    an array in declaration order with no field names, so the field ORDER here
    IS the wire format. New fields are only compatible appended at the tail.
    """
    return encode_frame(_msgpack.encode([tag, payload]))


class ServerMessage:
    """A decoded server envelope: its tag and positional payload."""

    __slots__ = ("tag", "payload")

    def __init__(self, tag: str, payload: List[Any]) -> None:
        self.tag = tag
        self.payload = payload

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"ServerMessage(tag={self.tag!r}, payload={self.payload!r})"


def decode_message(body: bytes) -> ServerMessage:
    """Decode one envelope body into a [`ServerMessage`]."""
    value = _msgpack.decode(body)
    if not isinstance(value, list) or len(value) != 2 or not isinstance(value[0], str):
        raise ProtocolError("envelope is not a [tag, payload] pair", 400)
    payload = value[1]
    return ServerMessage(value[0], payload if isinstance(payload, list) else [payload])


class FrameReader:
    """Accumulates transport bytes and yields complete message bodies.

    Keep-alive frames (a zero-length body, WIRE-024) are consumed silently and
    never surface — callers see only real messages.
    """

    def __init__(self, max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES) -> None:
        self._buf = bytearray()
        self._max = max_frame_bytes

    def push(self, chunk: bytes) -> None:
        """Append bytes received from the transport."""
        self._buf.extend(chunk)

    def next_body(self) -> Optional[bytes]:
        """The next complete message body, or `None` when more bytes are
        needed. Keep-alives are skipped."""
        while True:
            if len(self._buf) < FRAME_HEADER_LEN:
                return None
            (length,) = struct.unpack_from("<I", self._buf, 0)
            if length > self._max:
                raise ProtocolError(
                    f"frame of {length} bytes exceeds the {self._max}-byte cap", 413
                )
            end = FRAME_HEADER_LEN + length
            if len(self._buf) < end:
                return None
            body = bytes(self._buf[FRAME_HEADER_LEN:end])
            del self._buf[:end]
            if length == 0:
                continue  # keep-alive; loop for a real message
            return body


def slice_row_list(value: Any) -> List[bytes]:
    """Slice a flat RowList into its rows (RPC-032).

    Wire shape: `[row_count, size_hint, rows_data]`, where `size_hint` is
    `['Fixed', n]` (every row n bytes) or `['Offsets', [start, ...]]`.
    """
    if not isinstance(value, list) or len(value) < 3:
        raise ProtocolError("RowList is not a 3-field structure", 400)
    count_raw, hint, data = value[0], value[1], value[2]
    count = int(count_raw)
    if not isinstance(data, (bytes, bytearray)):
        raise ProtocolError("RowList.rows_data is not binary", 400)
    data = bytes(data)
    if not isinstance(hint, list) or not hint or not isinstance(hint[0], str):
        raise ProtocolError("RowList.size_hint is not tagged", 400)

    rows: List[bytes] = []
    if hint[0] == "Fixed":
        size = int(hint[1])
        if size <= 0:
            if count != 0:
                raise ProtocolError("Fixed size_hint of 0 with rows present", 400)
            return rows
        if len(data) != count * size:
            raise ProtocolError(
                f"inconsistent RowList: {count} rows x {size} bytes != {len(data)}", 400
            )
        for i in range(count):
            rows.append(data[i * size : (i + 1) * size])
        return rows
    if hint[0] == "Offsets":
        offsets = hint[1]
        if not isinstance(offsets, list) or len(offsets) != count:
            raise ProtocolError("inconsistent RowList: offsets length != row_count", 400)
        for i in range(count):
            start = int(offsets[i])
            end = int(offsets[i + 1]) if i + 1 < count else len(data)
            if start > end or end > len(data):
                raise ProtocolError("inconsistent RowList: offset out of range", 400)
            rows.append(data[start:end])
        return rows
    raise ProtocolError(f"unknown RowList size_hint '{hint[0]}'", 400)


def table_update(entry: List[Any]) -> Tuple[int, List[bytes], List[bytes]]:
    """Decode one `TableUpdate` payload entry into
    `(query_id, insert_rows, delete_rows)` (RPC-032)."""
    # [table_id, table_name, query_id, inserts(RowList), deletes(RowList)]
    query_id = int(entry[2])
    inserts = slice_row_list(entry[3])
    deletes = slice_row_list(entry[4])
    return query_id, inserts, deletes
