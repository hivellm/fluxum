"""FluxBIN — the schema-driven binary row encoding (SPEC-006 RPC-040..042).

No field names and no per-value type tags: the schema supplies the type
context, so a row is its column values back-to-back in declaration order.
All integers are little-endian. This is the row decode path — rows arrive as
raw bytes inside a frame and are decoded against the table's column types.

64-bit values (`U64`/`I64`/`Timestamp`/`EntityId`) surface as Python `int`
(unbounded, so no precision is lost); `Identity`/`ConnectionId` surface as
lowercase hex strings, matching how they are written everywhere else.
"""

from __future__ import annotations

import struct
from typing import List, Sequence, Tuple, Union

FluxValue = Union[bool, int, float, str, bytes]

#: Every FluxType the wire carries, as `/schema` spells it.
FLUX_TYPES = frozenset(
    {
        "Bool",
        "I8",
        "I16",
        "I32",
        "I64",
        "U8",
        "U16",
        "U32",
        "U64",
        "F32",
        "F64",
        "Str",
        "Bytes",
        "Identity",
        "ConnectionId",
        "EntityId",
        "Timestamp",
    }
)


class FluxBinError(ValueError):
    """Raised when bytes do not match the schema being decoded against."""


def to_hex(data: bytes) -> str:
    """Render raw bytes as lowercase hex — how Identity/ConnectionId surface."""
    return data.hex()


class RowReader:
    """Sequential FluxBIN reader over a row buffer."""

    __slots__ = ("_data", "offset")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.offset = 0

    @property
    def remaining(self) -> int:
        return len(self._data) - self.offset

    def _need(self, n: int) -> None:
        if self.remaining < n:
            raise FluxBinError(f"unexpected end of row: needed {n}, have {self.remaining}")

    def _take(self, n: int) -> bytes:
        self._need(n)
        out = self._data[self.offset : self.offset + n]
        self.offset += n
        return out

    def _read_len(self) -> int:
        (n,) = struct.unpack_from("<I", self._data, self.offset)
        self.offset += 4
        return n

    def read(self, flux_type: str) -> FluxValue:
        """Read one value of `flux_type`."""
        if flux_type == "Bool":
            b = self._take(1)[0]
            if b > 1:
                raise FluxBinError(f"invalid bool byte 0x{b:02x}")
            return b == 1
        if flux_type == "I8":
            return struct.unpack("<b", self._take(1))[0]
        if flux_type == "U8":
            return self._take(1)[0]
        if flux_type == "I16":
            return struct.unpack("<h", self._take(2))[0]
        if flux_type == "U16":
            return struct.unpack("<H", self._take(2))[0]
        if flux_type == "I32":
            return struct.unpack("<i", self._take(4))[0]
        if flux_type == "U32":
            return struct.unpack("<I", self._take(4))[0]
        if flux_type in ("I64", "Timestamp"):
            return struct.unpack("<q", self._take(8))[0]
        if flux_type in ("U64", "EntityId"):
            return struct.unpack("<Q", self._take(8))[0]
        if flux_type == "F32":
            return struct.unpack("<f", self._take(4))[0]
        if flux_type == "F64":
            return struct.unpack("<d", self._take(8))[0]
        if flux_type == "Str":
            self._need(4)
            length = self._read_len()
            raw = self._take(length)
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise FluxBinError("string is not valid UTF-8") from exc
        if flux_type == "Bytes":
            self._need(4)
            length = self._read_len()
            return self._take(length)
        if flux_type == "Identity":
            return to_hex(self._take(32))
        if flux_type == "ConnectionId":
            return to_hex(self._take(16))
        raise FluxBinError(f"unsupported type {flux_type}")


def decode_row(data: bytes, columns: Sequence[Tuple[str, str]]) -> dict:
    """Decode one row into a dict keyed by column name.

    `columns` is a sequence of `(name, flux_type)` in declaration order.
    """
    reader = RowReader(data)
    row = {}
    for name, flux_type in columns:
        row[name] = reader.read(flux_type)
    if reader.remaining != 0:
        raise FluxBinError(
            f"row has {reader.remaining} trailing byte(s): schema mismatch for this table"
        )
    return row


def read_prefix(data: bytes, types: Sequence[str], upto: int) -> FluxValue:
    """Read `types[0..=upto]` and return the last value — the primary-key
    field of a row whose pk sits at column index `upto`."""
    reader = RowReader(data)
    value: FluxValue = None  # type: ignore[assignment]
    for i in range(upto + 1):
        value = reader.read(types[i])
    return value
