# `@hivehub/fluxum`

TypeScript / JavaScript client for [Fluxum](../../README.md). Runs in Node.js (FluxRPC over TCP,
`fluxum://host:15801`) and in browsers (Streamable HTTP, `http(s)://host:15800`) from the same
package — SPEC-011 SDK-082.

> **Status:** complete — generator, transports, cache, reconnect, `FluxumClient`, packaging,
> and the shared conformance corpus green in **Node and headless Chromium**
> (`tests/conformance.test.ts` / `tests/conformance.chromium.test.ts`, one shared interpreter).
> `npm test` runs the suite with no build step — Node strips types directly. `npm run build`
> emits ESM + CJS + `.d.ts` and the self-contained browser bundle (`dist/fluxum.min.js`),
> asserting the SDK-083 50 KB min+gzip budget.

The Chromium runner drives the real browser SDK the way a real deployment does: the page is
served by the fluxum server itself (`server.static_dir`, same-origin with `/rpc` — which sends
no CORS headers), and Chromium is discovered on the machine (`FLUXUM_CHROMIUM` override, a
Playwright cache, or installed Chrome/Edge) and driven over raw CDP — no browser download, no
new dependency. Without a server binary or a browser, those tests skip loudly instead of
passing silently.

## Optimistic mutations & the offline queue (SPEC-021 CS-010..012, CS-032)

`callOptimistic(reducer, args, updater)` applies a mutation to the local cache **immediately**
and returns the call's stable idempotency key:

```ts
const key = await db.callOptimistic('add_task', ['buy milk'], (store) => {
  store.insert('Task', myPlausibleRowBytes); // upsert by pk; store.delete(table, pk) too
});
```

The updater's rows render instantly (in `db.cache` and the row listeners) as an **overlay** on
top of the authoritative cache. When the commit's own `TxUpdate` arrives, the overlay is swapped
for the server's rows in one atomic event batch — an `update` (or nothing, when the bytes match),
never a delete/insert flicker. If the reducer rejects, the overlay rolls back to the exact
pre-mutation state and an `OptimisticRejectedError` (reducer, key, cause) reaches the `onError`
listeners. Concurrent optimistic calls layer in submission order, and a rolled-back row can never
be resurrected by a later update (CS-012).

While **disconnected**, optimistic calls are not failed — they stay queued (and rendered) and
replay in submission order when the session comes back. Every queued call carries an
`idempotency_key` minted at enqueue time and reused verbatim on every retry (CS-032), so a replay
whose first send actually reached the server is deduplicated server-side: exactly-once, even
across a lost ack. `db.pendingMutations` is the number of calls still awaiting acknowledgement,
and `OfflineQueue.snapshot()/restore()` round-trip the queue for the durable-persistence layer
(CS-040, a separate task).

One caveat inherits from the wire: commits are attributed to their overlay by
`(caller identity, reducer)` in FIFO order, so concurrently mixing `callOptimistic` and plain
`callReducer` on the same reducer — or two connections under one identity calling it — can drop
an overlay one update early. The cost is a transient re-render, never divergence.

## Offline local persistence (SPEC-021 CS-040/CS-041)

Opt-in: pass `persistence: { backend, clientId }` to `FluxumClient.connect` — in the browser,
`backend: new IndexedDbBackend()`; tests use `MemoryBackend`; anything implementing the
four-method `PersistenceBackend` interface works. Subscribed rows and the offline mutation queue
are written through as they change, keyed by `(url, clientId)` with the session identity stored
inside.

On the next load, `connect` hydrates the cache and queue BEFORE any network I/O, then
re-establishes exactly like a reconnect: the persisted queries are resubscribed, the fresh
`InitialData` is reconciled so listeners hear only the **net difference** (never a cold
re-download's worth of inserts), and queued calls replay in submission order under their
original idempotency keys — exactly-once across the reload. If the fresh session authenticates
as a **different identity**, the hydrated queue is discarded rather than replayed as the new
user, and the store is cleared. Optimistic overlays are not persisted (an updater is a closure,
not data); a restored call's effect arrives with its authoritative `TxUpdate`.

## Schema mismatch (SDK-043)

Pass `schemaVersion` (the version your generated bindings embed) to
`FluxumClient.connect`. Every `InitialData` is checked against it **before** anything reaches
the cache — generated types cannot change at runtime, so a mismatched snapshot is never
applied, and no callback ever fires with a row the types would misread.

On the first mismatch the client runs the drill: it re-fetches `GET /schema` (best-effort — a
TCP client has no HTTP surface, and the admin guard may refuse a remote one) and reconnects
once. If the fresh `InitialData` matches, the mismatch was a migration-window read and heals
silently. If it does not, a typed `SchemaMismatchError` surfaces through the awaiting
`subscribe` (or `onError` for background reconnects), reconnecting stops — retrying cannot
regenerate bindings — and the fix is `fluxum generate`.

## Why the wire layer is not ours

Fluxum's frame is `u32 LE length prefix + MessagePack body`. That is not a Fluxum format — it is
the HiveLLM family binary wire (SPEC-001), shared with every other product in the family, and it
is frozen. This SDK used to carry its own ~400-line MessagePack codec and its own framing loop.
It no longer does: `protocol.ts` wraps `FrameReader` from `@hivehub/thunder`, and message bodies
go through `@msgpack/msgpack`.

The reasoning is narrow and worth stating, because "zero dependencies" is otherwise a good
default for a client SDK (and was, until recently, what SDK-077 required):

- A private copy of a shared frozen format is a *liability*, not independence. It can only ever
  match the standard or silently diverge from it, and the second failure mode is the expensive
  one — it desynchronizes a connection rather than failing a message.
- The dependency is not third-party in any meaningful sense. `@hivehub/thunder` is the family's
  own wire layer; depending on it is depending on the specification.
- The footprint stays inside the SDK-083 budget (≤ 50 KB min+gzip for the hand-written runtime),
  which is what actually protects browser users. The size is asserted in CI.

SDK-077 was amended accordingly: no third-party dependencies, with the family wire layer and its
MessagePack codec as the stated exception.

**What is still Fluxum's**, and stays dependency-free — everything above the frame boundary:

| Layer | Owner |
| --- | --- |
| Length prefix, frame cap, body slicing | `@hivehub/thunder` |
| MessagePack encode/decode | `@msgpack/msgpack` |
| `[tag, payload]` envelope catalog | Fluxum — `protocol.ts` |
| RowList slicing | Fluxum — `sliceRowList` |
| FluxBIN row decoding | Fluxum — `fluxbin.ts` |

### What `FluxumFrameReader` still adds

Two things, both genuinely Fluxum's: it passes the 16 MB cap (RPC-061) instead of Thunder's
64 MiB default, and it skips keep-alive frames so callers only ever see real messages.

A keep-alive is a zero-length frame (SPEC-006 RPC-001/006) — the HTTP push stream emits them on
idle. That used to be a Fluxum extension the wrapper had to parse out of the byte stream itself;
it is now WIRE-024 in the family spec, and Thunder's reader hands one back as an empty body. The
wrapper is a `length > 0` check over Thunder's reader, nothing more. That change came from
[hivellm/thunder#6](https://github.com/hivellm/thunder/issues/6), filed while adopting Thunder
here and shipped in 0.2.0.
