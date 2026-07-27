# Fluxum Python SDK

The asyncio-first Python client for the
[Fluxum](https://github.com/hivellm/fluxum) realtime database (SPEC-011,
T7.4). Zero runtime dependencies — the SDK carries its own minimal
MessagePack codec and FluxBIN row reader.

```sh
pip install fluxum
```

```python
import asyncio
from fluxum import Connection, TableSchema

async def main():
    db = await Connection.connect(
        "fluxum://127.0.0.1:15801",
        token=b"my-token",
        tables=[...],  # from `fluxum generate --lang python`
    )
    await db.subscribe(["SELECT * FROM ChatMessage"])
    await db.call_reducer("send_chat", [1, "hello"])
    # TxUpdates land in db.cache; poll or read db.cache.rows("ChatMessage").
    await db.close()

asyncio.run(main())
```

## What you get

- **`Connection`** — one asyncio session over FluxRPC/TCP: authenticate,
  subscribe (each query's `InitialData` populates a local row cache), call
  reducers, and receive `TxUpdate` diffs on the same socket.
- **Transparent reconnect** (SDK-047): on connection loss the client
  reconnects, re-authenticates, resubscribes every active query and
  reconciles its cache — the application keeps its handle across the outage.
- **A per-table row cache** keyed by primary key, with per-query ownership so
  an `unsubscribe` drops only the rows that query held (SDK-044).
- **Full type hints** (`py.typed`, SDK-062).

## Typed bindings

Generate typed row classes and reducer wrappers from a running server's
schema (or a saved `schema.json`):

```sh
fluxum generate --lang python --schema http://127.0.0.1:15800 --out ./gen
```

This emits `tables.py` (a `@dataclass` row + decoder + cache hooks per
table), `reducers.py` (a typed `async def` per client-callable reducer),
`__init__.py`, and `py.typed`. Offline generation from a saved schema
produces byte-identical output (SPEC-011 acceptance 11).

## Testing

The SDK is validated by the shared **conformance corpus**
([`tests/conformance/`](https://github.com/hivellm/fluxum/tree/main/tests/conformance))
— the same declarative
scenarios every Fluxum SDK runs against the same server build (TST-052). The
runner boots a fresh `fluxum-server` per scenario, so build it first:

```sh
cargo build -p fluxum-server
cd sdks/python && python -m pytest -q
```
