## 0. State on entry (read this first)

Work already landed in the working tree (see "Done" below) — this task resumes mid-flight, and
the Rust half is **blocked on a user decision**. Do not start 1.3 before it is answered.

## 1. Implementation

- [x] 1.1 TypeScript SDK: delete the hand-rolled MessagePack codec and delegate framing to Thunder
      — `sdks/typescript/src/msgpack.ts` (~400 lines) removed; `protocol.ts` now wraps
      `FrameReader` from `@hivehub/thunder` and uses `@msgpack/msgpack` for bodies. Fluxum keeps
      only its own layer: the `[tag, payload]` catalog, `sliceRowList` and `fluxbin.ts`
- [x] 1.2 Preserve Fluxum's keep-alive semantics over Thunder — `FluxumFrameReader` consumes
      zero-length frames before delegating, because Thunder treats a zero-length body as a decode
      error while Fluxum uses it as a liveness tick (RPC-001/006). Pinned by a test
- [ ] 1.3 **[BLOCKED — needs the user's answer]** Rust: make `crates/fluxum-protocol/src/frame.rs`
      delegate the prefix/cap/body-slicing to `thunder::wire`, keeping the keep-alive branch and
      the sans-IO borrowed-body API. Blocked because `thunder-rpc` is NOT on crates.io — decide
      path dep (`../Thunder/rust/thunder`, breaks standalone builds) vs git dep vs publishing
      `thunder-rpc` first
- [ ] 1.4 **[BLOCKED — needs the user's answer]** Amend SPEC-011 SDK-083: it requires ZERO runtime
      dependencies for the TS SDK, which adopting Thunder violates
      (`@hivehub/thunder` + `@msgpack/msgpack`). Also re-check the 50 KB min+gzip budget against
      the new dependency footprint
- [ ] 1.5 Report the zero-length-frame divergence upstream to Thunder (see proposal) so the family
      standard defines the keep-alive rather than each product working around it
- [ ] 1.6 Verification: TS tests green against the real `@hivehub/thunder`; Rust suite green after
      the framing delegation; framing bytes proven unchanged (the freeze at G5 must hold)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation — SPEC-006 note that framing
      is delegated to Thunder (bytes unchanged); SPEC-011 SDK-083 amendment; a short note in the
      SDK README on why the wire layer is not ours
- [x] 2.2 Write tests covering the new behavior — `sdks/typescript/tests/protocol.test.ts`
      (8 tests: envelope round-trip, keep-alive consumption, partial frame, oversized-prefix cap,
      non-envelope rejection, Fixed/Offsets RowList slicing, inconsistent RowList). Rust tests
      pending with 1.3
- [ ] 2.3 Run tests and confirm they pass — TS: 8/8 green via `node --test tests/`. Rust: pending

## Done so far (uncommitted at the time of writing — verify with `git status`)

- `sdks/typescript/package.json` — new package `@hivehub/fluxum`, deps `@hivehub/thunder@^0.1.1`
  and `@msgpack/msgpack@^3.1.0`; scripts `test` (node --test) and `typecheck`
- `sdks/typescript/src/protocol.ts` — Thunder-backed framing + Fluxum's envelope/RowList
- `sdks/typescript/src/fluxbin.ts` — FluxBIN row decoder (Fluxum-owned; Thunder has no equivalent)
- `sdks/typescript/tests/protocol.test.ts` — 8 passing tests
- `sdks/typescript/node_modules/` is installed locally; do NOT commit it (needs a .gitignore)

## Context you will not have after a session clear

- Node 24 runs TypeScript directly (type stripping), so the SDK tests need **no build step** —
  but strip-only mode rejects **parameter properties** (`constructor(private readonly x: T)`).
  Both `protocol.ts` and `fluxbin.ts` were rewritten to explicit fields because of this.
- Thunder's TS package exports `FrameReader` (raw frame bodies — reusable regardless of body
  model), `PUSH_ID`, `DEFAULT_MAX_FRAME_BYTES`, and Request/Response codecs. It does **not**
  export raw MessagePack primitives, which is why Fluxum depends on `@msgpack/msgpack` directly
  for its own tagged bodies.
- Fluxum's frame cap is 16 MB (RPC-061); Thunder's default is 64 MiB. `FluxumFrameReader`
  passes Fluxum's cap explicitly.
- This task interrupted `phase6_typescript-sdk-browser` (T6.2) mid-unit-2. That task's own
  progress log records units 1–4; unit 2 (the runtime) resumes **after** this one, and should now
  build on Thunder rather than hand-rolled framing.
