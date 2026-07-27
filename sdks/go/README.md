# Fluxum Go SDK

The context-aware Go client for the
[Fluxum](https://github.com/hivellm/fluxum) realtime database (SPEC-011,
T7.5). No third-party dependencies — the SDK carries its own minimal
MessagePack codec and FluxBIN row reader.

```sh
go get github.com/hivellm/fluxum-go/fluxum
```

```go
import (
    "context"

    fluxum "github.com/hivellm/fluxum-go/fluxum"
)

func main() {
    ctx := context.Background()
    db, err := fluxum.Connect(ctx, "fluxum://127.0.0.1:15801", []byte("my-token"), tables)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    if _, err := db.Subscribe(ctx, []string{"SELECT * FROM ChatMessage"}); err != nil {
        panic(err)
    }
    if err := db.CallReducer(ctx, "send_chat", []any{1, "hello"}); err != nil {
        panic(err)
    }
    // TxUpdates land in db.Cache(); read db.Cache().Rows("ChatMessage").
}
```

## What you get

- **`Connection`** — one session over FluxRPC/TCP: authenticate, subscribe
  (each query's `InitialData` populates a local row cache), call reducers, and
  receive `TxUpdate` diffs on the same socket. Every blocking call takes a
  `context.Context`.
- **Transparent reconnect** (SDK-047): on connection loss the client
  reconnects, re-authenticates, resubscribes every active query and
  reconciles its cache — the application keeps its handle across the outage.
- **A per-table row cache** keyed by primary key, with per-query ownership so
  an `Unsubscribe` drops only the rows that query held (SDK-044).
- **Idiomatic errors**: a server failure is a `*fluxum.Error` carrying the
  stable SPEC-028 `Code` (and `Catalog` name for an `Error` frame).

## Typed bindings

Generate typed row structs and reducer wrappers from a running server's
schema (or a saved `schema.json`):

```sh
fluxum generate --lang go --schema http://127.0.0.1:15800 --out ./fluxumgen
```

This emits `tables.go` (a struct row + `Decode<Table>` + the cache-hook
`TableSchema<Table>()` per table) and `reducers.go` (a `func <Reducer>(ctx,
db, ...) error` per client-callable reducer), package `fluxumgen`. Offline
generation from a saved schema produces byte-identical output (SPEC-011
acceptance 11).

## Testing

The SDK is validated by the shared **conformance corpus**
([`tests/conformance/`](https://github.com/hivellm/fluxum/tree/main/tests/conformance))
— the same declarative
scenarios every Fluxum SDK runs against the same server build (TST-052). The
runner boots a fresh `fluxum-server` per scenario, so build it first:

```sh
cargo build -p fluxum-server
cd sdks/go && go test -count=1 ./...
```
