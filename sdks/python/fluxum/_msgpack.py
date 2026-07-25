"""A minimal MessagePack codec — just the subset the Fluxum wire uses.

The SDK ships zero runtime dependencies (SDK-080), so rather than pull in an
external MessagePack package it carries the small codec it needs: the
`[tag, payload]` envelopes (RPC-011) and the values inside them (nil, bool,
ints of every width, float, str, bin, array, map). Framing and FluxBIN rows
are handled elsewhere (`protocol`, `fluxbin`); this is only the envelope
serialization Thunder leaves to the product.

Encoding is deliberately canonical-enough for the wire, not minimal-width
byte-golf: the server decodes positionally and does not care whether an id
came as `uint8` or `uint32`, so we pick a correct width, not the smallest.
"""

from __future__ import annotations

import struct
from typing import Any, Tuple


class MsgpackError(ValueError):
    """Raised when bytes are not valid MessagePack (or exceed our subset)."""


# --- encode ------------------------------------------------------------------


def encode(value: Any) -> bytes:
    """Encode `value` to MessagePack bytes."""
    out = bytearray()
    _encode_into(value, out)
    return bytes(out)


def _encode_into(value: Any, out: bytearray) -> None:
    # bool BEFORE int: bool is a subclass of int in Python and would otherwise
    # serialize as 0/1 integers.
    if value is None:
        out.append(0xC0)
    elif value is True:
        out.append(0xC3)
    elif value is False:
        out.append(0xC2)
    elif isinstance(value, int):
        _encode_int(value, out)
    elif isinstance(value, float):
        out.append(0xCB)
        out.extend(struct.pack(">d", value))
    elif isinstance(value, str):
        _encode_str(value, out)
    elif isinstance(value, (bytes, bytearray)):
        _encode_bin(bytes(value), out)
    elif isinstance(value, (list, tuple)):
        _encode_array(value, out)
    elif isinstance(value, dict):
        _encode_map(value, out)
    else:
        raise MsgpackError(f"cannot encode {type(value).__name__}")


def _encode_int(n: int, out: bytearray) -> None:
    if 0 <= n <= 0x7F:
        out.append(n)  # positive fixint
    elif -32 <= n < 0:
        out.append(n & 0xFF)  # negative fixint
    elif n >= 0:
        if n <= 0xFF:
            out.append(0xCC)
            out.append(n)
        elif n <= 0xFFFF:
            out.append(0xCD)
            out.extend(struct.pack(">H", n))
        elif n <= 0xFFFF_FFFF:
            out.append(0xCE)
            out.extend(struct.pack(">I", n))
        elif n <= 0xFFFF_FFFF_FFFF_FFFF:
            out.append(0xCF)
            out.extend(struct.pack(">Q", n))
        else:
            raise MsgpackError("integer above u64::MAX")
    else:
        if n >= -(1 << 7):
            out.append(0xD0)
            out.extend(struct.pack(">b", n))
        elif n >= -(1 << 15):
            out.append(0xD1)
            out.extend(struct.pack(">h", n))
        elif n >= -(1 << 31):
            out.append(0xD2)
            out.extend(struct.pack(">i", n))
        elif n >= -(1 << 63):
            out.append(0xD3)
            out.extend(struct.pack(">q", n))
        else:
            raise MsgpackError("integer below i64::MIN")


def _encode_str(s: str, out: bytearray) -> None:
    data = s.encode("utf-8")
    n = len(data)
    if n <= 31:
        out.append(0xA0 | n)  # fixstr
    elif n <= 0xFF:
        out.append(0xD9)
        out.append(n)
    elif n <= 0xFFFF:
        out.append(0xDA)
        out.extend(struct.pack(">H", n))
    else:
        out.append(0xDB)
        out.extend(struct.pack(">I", n))
    out.extend(data)


def _encode_bin(data: bytes, out: bytearray) -> None:
    n = len(data)
    if n <= 0xFF:
        out.append(0xC4)
        out.append(n)
    elif n <= 0xFFFF:
        out.append(0xC5)
        out.extend(struct.pack(">H", n))
    else:
        out.append(0xC6)
        out.extend(struct.pack(">I", n))
    out.extend(data)


def _encode_array(items: Any, out: bytearray) -> None:
    n = len(items)
    if n <= 15:
        out.append(0x90 | n)
    elif n <= 0xFFFF:
        out.append(0xDC)
        out.extend(struct.pack(">H", n))
    else:
        out.append(0xDD)
        out.extend(struct.pack(">I", n))
    for item in items:
        _encode_into(item, out)


def _encode_map(mapping: dict, out: bytearray) -> None:
    n = len(mapping)
    if n <= 15:
        out.append(0x80 | n)
    elif n <= 0xFFFF:
        out.append(0xDE)
        out.extend(struct.pack(">H", n))
    else:
        out.append(0xDF)
        out.extend(struct.pack(">I", n))
    for key, val in mapping.items():
        _encode_into(key, out)
        _encode_into(val, out)


# --- decode ------------------------------------------------------------------


def decode(data: bytes) -> Any:
    """Decode a single MessagePack value; the whole buffer must be consumed."""
    value, offset = _decode_at(data, 0)
    if offset != len(data):
        raise MsgpackError(f"{len(data) - offset} trailing byte(s) after value")
    return value


def _decode_at(data: bytes, offset: int) -> Tuple[Any, int]:
    if offset >= len(data):
        raise MsgpackError("unexpected end of buffer")
    b = data[offset]
    offset += 1

    if b <= 0x7F:
        return b, offset  # positive fixint
    if b >= 0xE0:
        return b - 0x100, offset  # negative fixint
    if 0x80 <= b <= 0x8F:
        return _decode_map(data, offset, b & 0x0F)
    if 0x90 <= b <= 0x9F:
        return _decode_array(data, offset, b & 0x0F)
    if 0xA0 <= b <= 0xBF:
        return _decode_str(data, offset, b & 0x1F)

    if b == 0xC0:
        return None, offset
    if b == 0xC2:
        return False, offset
    if b == 0xC3:
        return True, offset
    if b == 0xC4:
        n = data[offset]
        return _take(data, offset + 1, n)
    if b == 0xC5:
        (n,) = struct.unpack_from(">H", data, offset)
        return _take(data, offset + 2, n)
    if b == 0xC6:
        (n,) = struct.unpack_from(">I", data, offset)
        return _take(data, offset + 4, n)
    if b == 0xCA:
        (v,) = struct.unpack_from(">f", data, offset)
        return v, offset + 4
    if b == 0xCB:
        (v,) = struct.unpack_from(">d", data, offset)
        return v, offset + 8
    if b == 0xCC:
        return data[offset], offset + 1
    if b == 0xCD:
        (v,) = struct.unpack_from(">H", data, offset)
        return v, offset + 2
    if b == 0xCE:
        (v,) = struct.unpack_from(">I", data, offset)
        return v, offset + 4
    if b == 0xCF:
        (v,) = struct.unpack_from(">Q", data, offset)
        return v, offset + 8
    if b == 0xD0:
        (v,) = struct.unpack_from(">b", data, offset)
        return v, offset + 1
    if b == 0xD1:
        (v,) = struct.unpack_from(">h", data, offset)
        return v, offset + 2
    if b == 0xD2:
        (v,) = struct.unpack_from(">i", data, offset)
        return v, offset + 4
    if b == 0xD3:
        (v,) = struct.unpack_from(">q", data, offset)
        return v, offset + 8
    if b == 0xD9:
        n = data[offset]
        return _decode_str(data, offset + 1, n)
    if b == 0xDA:
        (n,) = struct.unpack_from(">H", data, offset)
        return _decode_str(data, offset + 2, n)
    if b == 0xDB:
        (n,) = struct.unpack_from(">I", data, offset)
        return _decode_str(data, offset + 4, n)
    if b == 0xDC:
        (n,) = struct.unpack_from(">H", data, offset)
        return _decode_array(data, offset + 2, n)
    if b == 0xDD:
        (n,) = struct.unpack_from(">I", data, offset)
        return _decode_array(data, offset + 4, n)
    if b == 0xDE:
        (n,) = struct.unpack_from(">H", data, offset)
        return _decode_map(data, offset + 2, n)
    if b == 0xDF:
        (n,) = struct.unpack_from(">I", data, offset)
        return _decode_map(data, offset + 4, n)

    raise MsgpackError(f"unsupported MessagePack byte 0x{b:02x}")


def _take(data: bytes, offset: int, n: int) -> Tuple[bytes, int]:
    end = offset + n
    if end > len(data):
        raise MsgpackError("bin/str length exceeds buffer")
    return data[offset:end], end


def _decode_str(data: bytes, offset: int, n: int) -> Tuple[str, int]:
    raw, end = _take(data, offset, n)
    try:
        return raw.decode("utf-8"), end
    except UnicodeDecodeError as exc:
        raise MsgpackError("string is not valid UTF-8") from exc


def _decode_array(data: bytes, offset: int, n: int) -> Tuple[list, int]:
    items = []
    for _ in range(n):
        item, offset = _decode_at(data, offset)
        items.append(item)
    return items, offset


def _decode_map(data: bytes, offset: int, n: int) -> Tuple[dict, int]:
    out = {}
    for _ in range(n):
        key, offset = _decode_at(data, offset)
        val, offset = _decode_at(data, offset)
        out[key] = val
    return out, offset
