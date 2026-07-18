## 1. Implementation

- [x] 1.1 TypeScript SDK: delete the hand-rolled MessagePack codec and delegate framing to Thunder
      — `sdks/typescript/src/msgpack.ts` (~400 lines) removed; `protocol.ts` now wraps
      `FrameReader` from `@hivehub/thunder` and uses `@msgpack/msgpack` for bodies. Fluxum keeps
      only its own layer: the `[tag, payload]` catalog, `sliceRowList` and `fluxbin.ts`
- [x] 1.2 Preserve Fluxum's keep-alive semantics over Thunder — `FluxumFrameReader` consumes
      zero-length frames before delegating, because Thunder treats a zero-length body as a decode
      error while Fluxum uses it as a liveness tick (RPC-001/006). Pinned by a test
- [x] 1.3 Rust framing — **resolved as: keep Fluxum's codec, pin it, ask upstream.** The dep
      question was moot (`thunder-rpc` 0.1.1 *is* on crates.io — Synap already consumes it from
      the registry). The real blocker was an API gap: `thunder::wire` decodes a frame only by
      deserializing the body into its own `Request`/`Response` and has no borrowed-body variant,
      which the sans-IO zero-copy API in `frame.rs` requires. So `fluxum-protocol` keeps its
      ~40-line codec, `thunder-rpc` is a **dev-dependency**, and
      `crates/fluxum-protocol/tests/thunder_parity.rs` (4 tests) asserts byte-for-byte equality
      with `thunder::wire::encode_frame` so the duplication cannot become a divergence
- [x] 1.4 SDK-077 amended (not SDK-083 — the task pointed at the wrong requirement; SDK-083 is
      the 50 KB budget, SDK-077 was the zero-dependency rule). It now reads: no third-party
      dependencies, with the family wire layer (`@hivehub/thunder`) and its MessagePack codec as
      the stated exception, and everything above the frame boundary staying Fluxum-owned and
      dependency-free. Footprint re-checked against SDK-083: ~12.4 KB gz for Thunder's whole
      unminified bundle + ~6.3 KB gz for minified `@msgpack/msgpack` ≈ **< 20 KB gz worst case**
      before tree-shaking — comfortably inside the 50 KB budget. The CI assertion lands with the
      bundling step in T6.2
- [x] 1.5 Reported upstream: **hivellm/thunder#6** — covers both gaps in one issue, the
      borrowed-body `decode_frame_raw` (1.3) and defining the zero-length keep-alive in SPEC-001
      instead of leaving each product to work around it
- [x] 1.6 Verification — Rust: full workspace suite green (110 test binaries, 0 failures) and
      `cargo clippy --workspace --all-targets` clean. TS: 8/8 green plus `tsc --noEmit` clean.
      Framing bytes proven unchanged by the parity test against the family golden vector (the G5
      freeze holds)

## 2. Tail (docs + tests)
- [x] 2.1 Documentation — SPEC-006 RPC-001 now states that framing is the family standard and
      SHALL be delegated where the family layer exposes it, records why the Rust side cannot yet,
      and points at the parity test and hivellm/thunder#6; SPEC-011 SDK-077 amended (see 1.4);
      new `sdks/typescript/README.md` with a "why the wire layer is not ours" section and an
      ownership table; `frame.rs` module docs explain the asymmetry with the TS SDK
- [x] 2.2 Tests — `sdks/typescript/tests/protocol.test.ts` (8 tests: envelope round-trip,
      keep-alive consumption, partial frame, oversized-prefix cap, non-envelope rejection,
      Fixed/Offsets RowList slicing, inconsistent RowList) and
      `crates/fluxum-protocol/tests/thunder_parity.rs` (4 tests: byte-for-byte framing parity,
      family golden ping vector, decoding a stream of Thunder frames, and the intended
      keep-alive divergence pinned as intentional)
- [x] 2.3 Run tests and confirm they pass — see 1.6

## Fixed along the way (not in the original plan)

- `package.json` `test` script was `node --test tests/`, which Node 24 resolves as a *module*
  path and fails on. Now `node --test "tests/*.test.ts"`. The "8/8 green" claim in the previous
  session was true only when the pattern was typed by hand
- `tsconfig.json` did not exist, so `npm run typecheck` had never actually run. Created, with
  `erasableSyntaxOnly` so the compiler rejects exactly what Node's type-stripping rejects
- `protocol.ts` `#pending` was inferred as `Uint8Array<ArrayBuffer>` from its empty initializer
  and would not accept transport chunks (`ArrayBufferLike`). Annotated explicitly

## Context worth keeping

- Node 24 runs TypeScript directly (type stripping), so the SDK tests need **no build step** —
  but strip-only mode rejects **parameter properties** (`constructor(private readonly x: T)`).
  Both `protocol.ts` and `fluxbin.ts` use explicit fields because of this; `tsconfig.json` now
  enforces it via `erasableSyntaxOnly`
- Thunder's TS package exports `FrameReader` (raw frame bodies — reusable regardless of body
  model), `PUSH_ID`, `DEFAULT_MAX_FRAME_BYTES`, and Request/Response codecs. It does **not**
  export raw MessagePack primitives, which is why Fluxum depends on `@msgpack/msgpack` directly
  for its own tagged bodies. The **Rust** crate has no `FrameReader` equivalent — that asymmetry
  is the whole of 1.3
- Fluxum's frame cap is 16 MB (RPC-061); Thunder's default is 64 MiB. Both `FluxumFrameReader`
  and `FrameCodec` pass Fluxum's cap explicitly
- When hivellm/thunder#6 lands: delete `FluxumFrameReader`'s keep-alive drain, and reduce
  `frame.rs` to a thin wrapper keeping only the 16 MB cap and the `Frame` enum
- This task interrupted `phase6_typescript-sdk-browser` (T6.2) mid-unit-2. That task's own
  progress log records units 1–4; unit 2 (the runtime) resumes **next**, and should build on
  Thunder rather than hand-rolled framing
