# SpacetimeDB competitive baseline — setup and pinning (TST-097)

Decision 001 (2026-07-21): SpacetimeDB is the competitive baseline Fluxum must reach, measured
head-to-head by `fluxum-bench` — both products run the **same demo app** (tasks + chat +
presence) over **real sockets** through their **published SDKs**, on the same machine, with the
same side-agnostic workload driver (`workload::Side`). SpacetimeDB itself publishes only
in-process vs SQLite comparisons; this harness is the stricter, more honest one.

## Pinned versions (must move together)

| What | Pin | Where |
|---|---|---|
| Server | `clockworklabs/spacetime:v2.6.1` (Docker) | this doc, report `stacks.spacetimedb.version` |
| CLI (`publish` / `generate`) | same image via `docker exec`/`docker run` — CLI ≡ server by construction | this doc |
| Module crate `spacetimedb` | `= 2.6.1` | `crates/fluxum-bench/spacetimedb-module/Cargo.toml` |
| Client SDK `spacetimedb-sdk` | `= 2.6.1` | workspace `Cargo.toml` |
| Generated bindings | `spacetime generate` @ 2.6.1 | `crates/fluxum-bench/src/spacetimedb_bindings/` |

Deployment is **Docker, not a native binary**: SpacetimeDB ships no Windows server build (the
bench box is Win10), and the pinned image also carries the exact-matching CLI. The container
runs Linux under Docker Desktop while `fluxum-server` runs native — a recorded asymmetry (see
below), unavoidable without changing the bench box OS.

Module toolchain of record: `rustc 1.95.0-nightly (3a70d0349 2026-02-27)`, target
`wasm32-unknown-unknown`, default release profile (what `spacetime init`'s official template
ships).

## One-command setup (TST-096 rigor)

```text
# 1. Server (persistent volume so restarts recover, like Fluxum's data dir):
docker volume create fluxum-parity-stdb-data
docker run -d --name fluxum-parity-stdb -p 15300:3000 \
  -v fluxum-parity-stdb-data:/stdb-data \
  clockworklabs/spacetime:v2.6.1 start --data-dir /stdb-data

# 2. Build the demo module (from crates/fluxum-bench/spacetimedb-module/):
cargo build --release --target wasm32-unknown-unknown

# 3. Publish it (CLI from the same pinned image; auto server-issued login):
docker cp target/wasm32-unknown-unknown/release/fluxum_parity_demo.wasm \
  fluxum-parity-stdb:/tmp/module.wasm
docker exec fluxum-parity-stdb spacetime publish -s http://127.0.0.1:3000 \
  --bin-path /tmp/module.wasm fluxum-parity-demo

# 4. (After a module change) regenerate the client bindings:
docker run --rm -v "<repo>/crates/fluxum-bench:/work" clockworklabs/spacetime:v2.6.1 \
  generate --lang rust \
  --bin-path /work/spacetimedb-module/target/wasm32-unknown-unknown/release/fluxum_parity_demo.wasm \
  --out-dir /work/src/spacetimedb_bindings
```

Harness commands (`--stdb-url http://127.0.0.1:15300`):

```text
# any single workload:
fluxum-bench write --side spacetimedb --stdb-url http://127.0.0.1:15300
# cold needs the server bounce:
fluxum-bench cold --side spacetimedb --stdb-url http://127.0.0.1:15300 \
  --stdb-restart-cmd "docker restart fluxum-parity-stdb"
# full three-side report (reset = republish with data wipe, for equal data footing):
fluxum-bench report --database-url postgres://fluxum:fluxum@127.0.0.1:15432/parity \
  --cold-restart-cmd "docker restart fluxum-parity-pg" \
  --stdb-url http://127.0.0.1:15300 \
  --stdb-restart-cmd "docker restart fluxum-parity-stdb" \
  --stdb-reset-cmd "docker exec fluxum-parity-stdb spacetime publish -s http://127.0.0.1:3000 --bin-path /tmp/module.wasm --delete-data=always --yes fluxum-parity-demo"
```

## Durability honesty (TST-091 applied to the competitor)

Observed from the pinned source (`spacetimedb-durability` v2.6.1, `imp::local`): the reducer
result is acked at **in-memory commit**; commit-log appends and fsyncs happen in a background
actor that drains a queue and calls `flush_and_sync` per batch (group commit). A process or OS
crash can lose acked transactions committed since the last sync. Fluxum's TXN-004 ack is
strictly stronger: the append has reached the OS **before** the ack (process-crash safe), with
fsync as async group commit (~50 ms OS-crash window, NFR-08). The report records both.

## Behavior mirror and recorded asymmetries

The module (`spacetimedb-module/src/lib.rs`) mirrors `crates/fluxum-demo` 1:1 by behavior;
each platform pays its own idiomatic cost:

- **Row visibility**: Fluxum `owner_only(owner)` (DM-060) ↔ SpacetimeDB RLS
  `client_visibility_filter` with `:sender`. Both filter server-side.
- **Presence**: Fluxum `ephemeral` + `#[owner]` (engine-dropped, DMX-011) ↔
  `client_connected`/`client_disconnected` lifecycle reducers.
- **Chat rate limit 20/s** (RED-050): Fluxum enforces pre-transaction in memory; SpacetimeDB
  has no reducer rate limiting, so the module uses a private budget table updated inside the
  transaction — one extra indexed upsert per `send_chat`. Offered chat load stays below the
  limit on both sides (10 msg/s), so the limiter never rejects during measurement.
- **Indexes**: `task.owner` and `chat_message.channel` are btree-indexed — the competitor gets
  its best-practice setup, symmetric with the covering indexes the PostgreSQL side receives.
- **Transport**: both SDKs speak WebSocket/TCP to a local server; SpacetimeDB's server is in a
  Linux container (Docker Desktop NAT on Win10), Fluxum's is a native process. The container
  hop is a recorded asymmetry in SpacetimeDB's disfavor on latency classes; treat sub-ms
  deltas accordingly.
- **hot_read**: both sides read their SDK's materialized local cache (Fluxum live view ↔
  SpacetimeDB client cache unique-index `find`) — in-process on both sides, no socket.
- **Identity reuse**: the driver caches server-issued tokens per seed, so the same seed is the
  same user across sessions and server restarts on both sides.

## Ratio block and guard

The report's `competitive` block (fluxum/spacetimedb per class: `write`, `e2e_p99`, `hot_p99`,
`cold_p99`, `mixed_*`) targets **≥ 1.0×** per class — parity to reach, oriented
bigger-is-better-for-Fluxum. It is informational (never a release gate, never mixed with the
NFR-11 verdicts); `fluxum-bench regression` floors a class at 1.0 once a published report first
reaches it. Every class below 1.0× gets a recorded finding with the measured delta.

## First measured head-to-head (2026-07-21, harness 0.1.0)

Machine quiet, defaults (3 runs/class), same machine and driver for all sides — full numbers in
`report-v0.1.0.{json,md}`:

| class | fluxum/spacetimedb | reached ≥ 1.0× |
|---|---|---|
| write_throughput | **59.35** | ✅ |
| e2e_p99 | **14.27** | ✅ |
| hot_p99 | **2.67** | ✅ |
| cold_p99 | **11.47** | ✅ |
| mixed_write_throughput | **49.21** | ✅ |
| mixed_read_p99 | **1.00** | ✅ (structural tie, see below) |
| mixed_e2e_p99 | **4.48** | ✅ |

**No class is below 1.0×** — no gap findings to record from this run; from the moment this
report is published, the TST-095 guard floors **all seven** classes at 1.0.

Recorded observations (honesty notes, not gaps):

1. **`mixed_read_p99`/`hot_p99` are structurally ~1×**: both sides serve hot reads from their
   SDK's in-process cache (Fluxum live view ↔ SpacetimeDB client cache), so these classes
   compare two local map lookups quantized at the timer's ~100 ns steps — expect flutter
   (2.67 vs 1.00 across classes/runs is quantization, not product delta). A sub-1.0 reading
   here would trip the floor spuriously; if that starts flapping, refine the guard with
   measured variance rather than weakening the floor for the socket classes.
2. **SpacetimeDB acked-write latency grows with concurrency** (p50 3.7 ms at 2 clients →
   12.3 ms at 8; aggregate ~620 ops/s): reducer transactions serialize in the datastore. This
   is its published-SDK, over-socket behavior — the same protocol Fluxum is measured under
   (36k ops/s aggregate on this machine). SpacetimeDB's own "150k tx/s" figure is an
   in-process number; the two are different measurements and the report never mixes them.
3. **Container hop**: the SpacetimeDB server runs in a Linux container (Docker Desktop NAT)
   while `fluxum-server` is native — sub-ms of the socket-class deltas is environment, not
   product; at the measured multiples (≥ 2.7×) it changes no verdict.
