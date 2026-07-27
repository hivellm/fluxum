# `fluxum-sdk` — Fluxum Rust SDK

The Rust client for the [Fluxum](https://github.com/hivellm/fluxum) realtime
database (SPEC-011 SDK-050): typed table access, reducer calls, and live
subscriptions speaking FluxRPC over raw TCP (`fluxum://host:15801`) or
Streamable HTTP (`http://host:15800`).

```sh
cargo add fluxum-sdk
```

```rust
use fluxum_sdk::{Connection, protocol::FluxValue};

let db = Connection::connect(
    "fluxum://127.0.0.1:15801",
    b"my-token",
    tables, // from `fluxum generate --lang rust`
)?;
db.subscribe(&["SELECT * FROM ChatMessage"])?;
db.call_reducer(
    "send_chat",
    vec![FluxValue::I64(1), FluxValue::Str("hello".into())],
)?;
// TxUpdates land in the local row cache, and row listeners fire per event.
```

## What you get

- **`Connection`** — one session over FluxRPC: authenticate, subscribe (each
  query's `InitialData` populates a local row cache), call reducers —
  blocking or pipelined (`call_reducer_async`, SDK-032) — and receive
  `TxUpdate` diffs on the same socket.
- **Transparent reconnect** (SDK-047): on connection loss the client
  reconnects, re-authenticates, resubscribes every active query and
  reconciles its cache to the **net difference** — the application keeps its
  handle across the outage.
- **Optimistic mutations** (SPEC-021 CS-010..012): `call_optimistic` applies
  a mutation to the local cache immediately as an overlay, swapped for the
  authoritative rows when the commit's `TxUpdate` arrives — an update, never
  a delete/insert flicker; a rejection rolls the overlay back exactly.
- **Offline queue** (CS-032): while disconnected, calls stay queued under
  stable idempotency keys and replay exactly-once on reconnect.
- **Durable local persistence** (CS-040/041): opt-in via
  `Connection::connect_persistent` — subscribed rows and the offline queue
  written through to a `PersistenceBackend` (file or memory built in),
  hydrated on restart before any network I/O.
- **Vendored wire layer**: the `protocol` module is byte-identical to the
  server's crate (enforced by test), and the crate depends on nothing
  internal — only the shared HiveLLM `thunder-rpc` framing.

## Typed bindings

Generate typed row structs and reducer wrappers from a running server's
schema (or a saved `schema.json`):

```sh
fluxum generate --lang rust --schema http://127.0.0.1:15800 --out ./src/gen
```

Offline generation from a saved schema produces byte-identical output
(SPEC-011 acceptance 11).

## Testing

The SDK is validated by the shared **conformance corpus**
([`tests/conformance/`](https://github.com/hivellm/fluxum/tree/main/tests/conformance))
— the same declarative scenarios every Fluxum SDK runs against the same
server build (TST-052). The runner boots a fresh `fluxum-server` per
scenario, so build it first:

```sh
cargo build -p fluxum-server
cargo test -p fluxum-sdk
```

## License

Apache-2.0
