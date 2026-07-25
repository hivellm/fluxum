"""Fluxum — the asyncio-first Python client for the Fluxum realtime database.

    import asyncio
    from fluxum import Connection, TableSchema

    async def main():
        db = await Connection.connect("fluxum://127.0.0.1:15800", b"token", tables=[...])
        await db.subscribe(["SELECT * FROM ChatMessage"])
        await db.call_reducer("send_chat", [1, "hello"])
        await db.close()

    asyncio.run(main())

The public API is fully type-hinted and ships `py.typed` (SDK-062). Typed
per-table row classes and reducer signatures come from
`fluxum generate --lang python` against a server's `/schema`.
"""

from __future__ import annotations

from .client import Cache, Connection, FluxumError, TableSchema
from .fluxbin import FluxBinError, RowReader, decode_row, to_hex

__all__ = [
    "Connection",
    "TableSchema",
    "Cache",
    "FluxumError",
    "RowReader",
    "decode_row",
    "to_hex",
    "FluxBinError",
]

__version__ = "0.2.0"
