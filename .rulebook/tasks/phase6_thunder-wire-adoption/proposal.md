# Proposal: phase6_thunder-wire-adoption

## Why

The HiveLLM family has one binary RPC standard (`u32 LE length + MessagePack`) implemented
**18 times** across products. Thunder (https://github.com/hivellm/thunder, local checkout at
`E:\HiveLLM\Thunder`) exists to collapse those into one implementation per language, conformance
tested against one golden corpus. Its README names the exact anti-pattern Fluxum currently
embodies: "per-product `-protocol` crates force-published on every release".

Fluxum duplicates that wire layer twice over: `crates/fluxum-protocol/src/frame.rs` in Rust, and —
until this task started — a hand-rolled MessagePack codec plus framing in the new TypeScript SDK
(`sdks/typescript`). The TS duplication was ~400 lines of codec that Thunder already ships,
vetted, in `@hivehub/thunder`.

**User decision (2026-07-18):** adopt Thunder for the **wire layer only**. Fluxum keeps what
Thunder deliberately leaves to products — its message catalog, FluxBIN and RowList. Full
re-modelling onto Thunder's `Request{id,command,args}` was explicitly rejected: it would be a
wire-breaking redesign after the G5 format freeze.

## What Changes

- **TypeScript SDK** — framing/caps/partial-buffer handling delegate to Thunder's `FrameReader`;
  bodies use `@msgpack/msgpack`. The hand-rolled `msgpack.ts` is deleted. Fluxum keeps
  `protocol.ts` (its `[tag, payload]` catalog + RowList slicing) and `fluxbin.ts`.
- **Rust `fluxum-protocol`** — `frame.rs` delegates the length prefix, cap enforcement and body
  slicing to `thunder::wire`, preserving Fluxum's keep-alive semantics. `messages.rs`,
  `fluxbin.rs`, `rowlist.rs` are untouched (product-owned).
- **SPEC-011 SDK-083** — amend the "zero runtime dependencies" requirement, which adopting Thunder
  necessarily violates.

## Impact

- Affected specs: SPEC-006 (framing — delegation only, bytes unchanged), SPEC-011 (SDK-083
  dependency requirement)
- Affected code: `crates/fluxum-protocol/src/frame.rs`, `sdks/typescript/*`
- Breaking change: NO (framing bytes are identical — both are the same family standard)
- User benefit: one owner for the wire layer; Fluxum stops maintaining a private copy of a family
  standard and inherits Thunder's conformance corpus and cap discipline

## Open decisions (blocking the Rust half)

1. **How does Fluxum depend on Thunder in Rust?** `thunder-rpc` is **not published on crates.io**
   (only `@hivehub/thunder@0.1.1` is on npm). Options: path dep to `../Thunder/rust/thunder`
   (breaks standalone builds), git dep, or publish `thunder-rpc` first. **Awaiting the user.**
2. **SDK-083 amendment** — the spec demands zero runtime dependencies for the TS SDK; Thunder
   brings `@hivehub/thunder` + `@msgpack/msgpack`. Thunder's own thesis is that one vetted shared
   dependency beats 18 hand-rolled copies. Needs an explicit spec change, not a silent violation.

## Finding to report upstream to Thunder

**Zero-length frame divergence.** Fluxum uses a zero-length frame as a keep-alive (RPC-001/006) —
its HTTP GET stream sends them on idle. Thunder treats a zero-length body as a *decode error*
(`zero_length_body_is_a_decode_error` in `rust/thunder/src/wire/frame.rs`; the TS `FrameReader`
inherits the same shape). Fluxum works around it in `FluxumFrameReader` by consuming keep-alives
before delegating, with a test pinning the behaviour — but the family standard arguably should
*define* the zero-length frame rather than reject it.
