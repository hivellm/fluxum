# `@hivehub/fluxum`

TypeScript / JavaScript client for [Fluxum](../../README.md). Runs in Node.js (FluxRPC over TCP,
`fluxum://host:15801`) and in browsers (Streamable HTTP, `http(s)://host:15800`) from the same
package — SPEC-011 SDK-082.

> **Status:** the wire layer and FluxBIN row decoding are in place; the connection runtime and
> generated bindings land with T6.2. `npm test` runs the current suite with no build step —
> Node 24 strips types directly.

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
