# Fluxum C# SDK

The async/await .NET client for the [Fluxum](../../README.md) realtime
database (SPEC-011, T7.6), distributed as the `Fluxum.Sdk` NuGet package. No
third-party dependencies — the SDK carries its own minimal MessagePack codec
and FluxBIN row reader.

```csharp
using Fluxum.Sdk;

await using var db = await Connection.ConnectAsync(
    "fluxum://127.0.0.1:15800",
    System.Text.Encoding.UTF8.GetBytes("my-token"),
    tables); // from `fluxum generate --lang csharp`

await db.SubscribeAsync(new[] { "SELECT * FROM ChatMessage" });
await db.CallReducerAsync("send_chat", new object?[] { 1, "hello" });
// TxUpdates land in db.Cache; read db.Cache.Rows("ChatMessage").
```

## What you get

- **`Connection`** — one session over FluxRPC/TCP: authenticate, subscribe
  (each query's `InitialData` populates a local row cache), call reducers, and
  receive `TxUpdate` diffs on the same socket. Every operation is `async` and
  takes a `CancellationToken`.
- **Transparent reconnect** (SDK-047): on connection loss the client
  reconnects, re-authenticates, resubscribes every active query and
  reconciles its cache — the application keeps its handle across the outage.
- **A per-table row cache** keyed by primary key, with per-query ownership so
  an `UnsubscribeAsync` drops only the rows that query held (SDK-044).
- **Idiomatic errors**: a server failure is a `FluxumException` carrying the
  stable SPEC-028 `Code` (and `Catalog` name for an `Error` frame).

## Typed bindings

Generate typed row records and reducer wrappers from a running server's
schema (or a saved `schema.json`):

```sh
fluxum generate --lang csharp --schema http://127.0.0.1:15800 --out ./FluxumGen
```

This emits `Tables.cs` (a `record` row + `<Table>Codec.Decode` + the cache
hook `<Table>Codec.TableSchema()` per table) and `Reducers.cs` (a static
`async Task <Reducer>(...)` per client-callable reducer), namespace
`FluxumGen`. Offline generation from a saved schema produces byte-identical
output (SPEC-011 acceptance 11).

## Testing

The SDK is validated by the shared **conformance corpus**
([`tests/conformance/`](../../tests/conformance/)) — the same declarative
scenarios every Fluxum SDK runs against the same server build (TST-052). The
runner boots a fresh `fluxum-server` per scenario, so build it first:

```sh
cargo build -p fluxum-server
cd sdks/csharp && dotnet test Fluxum.Sdk.Tests/Fluxum.Sdk.Tests.csproj
```
