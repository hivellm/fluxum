## 1. Implementation

Layered opt-in delta compression for the realtime push path; every
layer is negotiated, default behavior unchanged. Sequenced after
phase9_fanout-event-batching so its burst bench + MMO-shape workload
attribute each layer's win separately (batch × light × compression
compose).

- [x] 1.1 SPEC-006 prep for `tx_updates: "light"` (RPC-035) — decided: the light form gains
      `shard_id` + `tx_offset` at its additive tail, same order as `TxUpdate`'s (RPC-011), so
      light sessions retain and echo `Resume.from_offset` from the same cursor space as full
      ones — "light strips provenance, never the cursor" is now normative text. Audit result:
      only the Rust SDK had a `TxUpdateLight` decode arm, and it *dropped* the message;
      TS/Python/Go/C# dispatch by tag string and silently ignored it. All five now apply the
      light form through their existing diff path (tables lane at index 2 instead of 5).
- [x] 1.2 P3a server — `Authenticate.tx_updates` (and `?tx_updates=` on `/rpc`, Authenticate
      wins) honored end to end: pinned per connection at first auth (a differing re-auth value
      is a 400, like the namespace binding), echoed in the new `AuthResult` additive tail, and
      the fan-out partitions each delivery group into at most two shared encodes — SUB-024
      partitions, never multiplies; the light body only clones tables when a group actually
      mixes forms. Resume replays serve the session's negotiated form (the window stores
      pre-encode diffs). `GET /sessions` reports `wire.tx_updates`/`wire.compression` per
      connection. Corpus scenario `light-updates` (full + light subscriber to one query
      converge to identical caches whichever side commits) green on all five runners.
- [x] 1.3 P3b codec — RPC-008 **amended first**: the spec's per-frame tag + 1024-byte
      threshold would exempt every position-sync frame, defeating the feature's own target
      workload; `gzip` is now defined as one connection-lifetime raw-DEFLATE stream with
      sync-flushed chunks (context carryover IS the generic delta layer), tag byte kept, tag
      0x00 bypasses the context, threshold default 64, keep-alives exempt (zero-length, no
      body to tag), the accepting `AuthResult` is the untagged boundary (raw-flagged in the
      writer against the arming race), per-GET-stream contexts on Streamable HTTP with POST
      bodies untagged, brotli reserved-and-refused (dependency budget: flate2/miniz_oxide
      only). Kill-switch `server.compression_enabled` degrades with an honest `none` echo;
      `server.compression_threshold_bytes` configurable; metrics
      `fluxum_wire_compression_{raw,sent,cpu_us}_*` + Grafana panel (the dashboard-coverage
      test enforces it). Client side: the Rust SDK decodes the tagged stream behind the
      `compression` cargo feature, arms off the echo alone, and refuses gzip on its HTTP
      transport rather than desync; other SDKs simply do not negotiate it yet (SDK-048
      support matrix).
- [x] 1.4 P3b guardrails — measured on the MMO-shape rig (below): compression adds ~75 µs
      e2e p50 / ~100 µs p99 on loopback for the negotiated session at ~25 µs server CPU per
      compressed frame; the un-negotiated default path is untouched by construction (the
      writer checks an unarmed `OnceLock` per frame — nothing else changed on that path), so
      the parity harness, which never negotiates compression, is unaffected; the in-test
      latency guard (gzip p99 within 10× of baseline) rides every CI run. Posture documented
      in `docs/DEPLOYMENT.md` §6: on for WAN/browser clients, off for loopback, server
      kill-switch is policy.
- [x] 1.5 P4 spec ONLY — landed as SPEC-006 **RPC-036** [P2], explicitly
      specified-not-implemented (`tx_updates: delta` is refused with a 400 until its own
      task lands): changed-column bitmask + values lane for updates computed once per commit
      and shared (SUB-024), full-row rules for inserts and visibility-boundary rows, retained
      windows keep full row + mask so one stored delta serves any negotiated form, SDK
      desync-means-resubscribe contract (SDK-045/CS-022).
- [x] 1.6 Before/after per layer, MMO shape (delete+insert of one Player row per move, real
      provenance: 20 rotating caller identities + `move_player` name), measured by
      `crates/fluxum-server/tests/wire_layers_measure.rs` (also the regression guard —
      layering must keep paying in order):
      full/none **159.3 B/update** (the ~150 B analysis baseline) → light/none **117.3 B**
      (−26%) → full/gzip **53.1 B** → light/gzip **52.0 B** (−67%, 3.1× compounded);
      stream ratio sent/raw 0.35. Honest caveat recorded: with only 20 identities the
      32 KiB window amortizes the caller bytes, so light's marginal win under gzip is small
      here and grows with real-world caller cardinality (hundreds+ per window). The
      phase9_fanout-event-batching burst rig can re-attribute these numbers when that task
      lands; batching multiplies with both layers.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation — SPEC-006 rewritten
      where it governs (RPC-008 stream semantics + kill-switch/echo, RPC-030 AuthResult tail,
      RPC-035 tail + resume + lifetime rule, new RPC-036, acceptance 16/17); SPEC-011 gains
      SDK-048 (apply/negotiate contract + per-SDK support matrix); SPEC-021 CS-021 notes the
      negotiated-form replay; docs/DEPLOYMENT.md §6 carries the wire-compression posture with
      the measured numbers; docs/CONSOLE.md documents the sessions-view wire posture;
      config/config.example.yml names the two new keys (its completeness test enforces this).
- [x] 2.2 Write tests covering the new behavior — protocol unit suites (negotiate parse
      matrix; compress round-trip, carryover witness, zip-bomb bound, corrupt-stream error);
      `crates/fluxum-server/tests/wire_negotiation.rs` e2e against the real TCP transport
      (light/full split of one delivery group, light resume replay, 400 matrix incl. brotli
      and delta, re-auth pinning, gzip tagged stream with carryover, kill-switch echo);
      `wire_layers_measure.rs` (the 1.6 numbers as a layering regression guard + latency
      guard); `sdks/rust/tests/wire_compression.rs` (SDK-side inflate path, light × gzip
      composed, feature-gated); corpus scenario `light-updates` across all five runners;
      Grafana coverage enforced by the dashboard-metrics test.
- [x] 2.3 Run tests and confirm they pass — full workspace green (`cargo test --workspace
      --all-features`), clippy --all-features --all-targets clean, fmt clean, codespell
      clean; 5-SDK corpus green (Rust corpus TCP+HTTP, TS 123, Python 12, Go, C# 14);
      coverage gate run with the parity rig live (see the archive note for the figure).
