# SPEC-016 — Hardware Adaptivity & SIMD

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 0 · T0.2 (probe) + Phase 2 · T2.10 (SIMD) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-05, FR-110, FR-111, FR-112, FR-113; NFR-12, NFR-14 |
| **Requirement prefix** | `HWA-` |
| **Source** | new — family precedent: Nexus `simd/` runtime dispatch, Vectorizer SIMD matrix CI |

Fluxum adapts to the machine instead of assuming one ([PRD](../PRD.md) §3.1.7). The same binary
**MUST** be correct on a 1 vCPU / 512 MB droplet and fast on a large multi-core server, with no
hand-tuning and no per-ISA build artifacts. This spec defines the two mechanisms that deliver
that:

1. A **boot-time hardware probe** (container-aware) whose output drives every derived default —
   worker counts, memory budget, buffer sizes, cadences (FR-05, FR-113).
2. **SIMD kernels with runtime dispatch** on the hot paths, bound by a hard scalar-parity
   correctness rule (FR-111, FR-112, NFR-14).

Module placement ([ARCHITECTURE](../ARCHITECTURE.md)): the probe and derivation logic live in
`crates/fluxum-core/src/hw/`; SIMD kernels and the dispatch layer live in
`crates/fluxum-core/src/simd/`. Test machinery referenced here is defined in
[SPEC-013](SPEC-013-testing-conformance.md); the buffer pool and memory budget it feeds are
defined in [SPEC-015](SPEC-015-tiered-storage.md).

## 1. Design rules

- **Probe once, derive centrally, override anywhere.** All adaptive values are pure functions of
  one immutable `HardwareProfile`, and every one of them can be pinned in `config.yml` /
  `FLUXUM_*` env (FR-04).
- **Scalar first, SIMD behind the curtain.** Scalar reference implementations land first and
  remain the oracle; SIMD variants replace them behind the dispatch layer without changing
  behavior ([DAG](../DAG.md) §2 note on T2.10).
- **Bit-identical or it does not ship.** A SIMD kernel that differs from its scalar reference on
  any input is a correctness bug, not a performance trade-off (FR-112).
- **Justified by measurement.** Kernels earn their complexity through criterion benchmarks and,
  ultimately, the PostgreSQL parity harness (NFR-11) — never by assumption.

## 2. Boot-time hardware probe

- **HWA-001** [P0] At process start, before the tokio runtime, shards, or buffer pool are
  constructed, Fluxum **MUST** probe the following and record them in an immutable
  `HardwareProfile` struct:
  - logical CPU cores and physical CPU cores;
  - cgroup CPU quota (cgroup v2 `cpu.max`; cgroup v1 `cpu.cfs_quota_us` / `cpu.cfs_period_us`)
    when present;
  - total system RAM and currently available RAM;
  - cgroup memory limit (cgroup v2 `memory.max`; cgroup v1 `memory.limit_in_bytes`) when present.

  The probe **MUST** be container-aware: when the process runs under a cgroup with limits set,
  the limits — not the host totals — define the effective resources.
- **HWA-002** [P0] The **effective CPU count** SHALL be
  `max(1, min(logical_cores, ceil(cpu_quota / cpu_period)))` when a CPU quota is present, and
  `logical_cores` otherwise. The **effective memory** SHALL be
  `min(total_ram, cgroup_memory_limit)` when a memory limit is present, and `total_ram`
  otherwise. Sentinel "unlimited" values (`max`, `-1`) **MUST** be treated as absent limits.
- **HWA-003** [P0] Absent limits are normal, not an error: on bare metal, on non-Linux hosts, or
  when the cgroup filesystem is unreadable, the probe **MUST** fall back to OS-reported totals
  and **MUST NOT** fail boot. The choice of detection crate and the precise absent-limit behavior
  are tracked by [PRD OQ-10](../PRD.md) (due before T0.2); to keep that decision swappable, all
  consumers **MUST** read the `HardwareProfile` API only — no direct cgroup/sysinfo calls outside
  `hw/`.
- **HWA-004** [P0] If any individual probe value cannot be determined, the probe **MUST**
  substitute a documented conservative fallback (e.g., 1 core, 512 MiB) for that value, log a
  `WARN` naming the value and the fallback used, and continue booting. The probe **MUST NOT**
  panic.
- **HWA-005** [P1] Disk characteristics **MAY** be probed (free space on `storage.data_dir`,
  SSD-vs-rotational heuristic). Consumers **MUST** treat disk hints as advisory only: their
  absence or inaccuracy **MUST NOT** affect correctness, only default tuning.
- **HWA-006** [P0] The probe runs exactly once per process lifetime. Runtime re-probing (e.g.,
  reacting to a live cgroup limit change) is out of scope; picking up new limits requires a
  restart, and this **MUST** be documented.

```
Given a container with cpu.max = "150000 100000" and memory.max = 536870912 on a 32-core host
When the Fluxum process boots
Then HardwareProfile reports effective_cores = 2 and effective_memory = 512 MiB
And every derived default is computed from those effective values, not the host's 32 cores
```

## 3. Derived defaults & adaptive tuning

The following values are derived from the `HardwareProfile` when their config key is `auto`
(the default). The **initial derivation** column is normative until retuned under HWA-014.

| Derived value | Config key | Initial `auto` derivation | Consumer |
|---|---|---|---|
| tokio worker threads | `runtime.worker_threads` | `effective_cores` (min 1) | async runtime (`fluxum-server`) |
| shard count ⇒ shard writer threads (one writer per shard, [SPEC-002](SPEC-002-storage-engine.md)) | `sharding.shards` | `clamp(floor(effective_cores / 2), 1, 16)` | [SPEC-007](SPEC-007-sharding.md) |
| buffer-pool size | `memory.budget` | **delegated to [SPEC-015](SPEC-015-tiered-storage.md)**: `auto = f(effective_memory)`; this spec only supplies the input | buffer pool / pager |
| fan-out concurrency | `subscriptions.fanout_concurrency` | `clamp(2 × effective_cores, 2, 64)` | [SPEC-005](SPEC-005-subscriptions.md) fan-out |
| commit-log write-buffer size | `storage.commit_log_write_buffer_bytes` | `clamp(effective_memory / 1024, 64 KiB, 4 MiB)` | [SPEC-002](SPEC-002-storage-engine.md) async log writer |
| checkpoint cadence | `storage.checkpoint_interval_tx` | fixed `10000` committed tx (disk-class-adaptive cadence is P2) | [SPEC-002](SPEC-002-storage-engine.md) snapshots (FR-14) |

- **HWA-010** [P0] Every derived value in the table **MUST** have a config key that accepts
  `auto` (the default) or an explicit value; an explicit value **MUST** always win over the
  derivation, even when it exceeds detected hardware (in which case a `WARN` **MUST** be logged,
  but the operator's choice stands). Env-var overrides use the standard `FLUXUM_*` mapping
  (FR-04).
- **HWA-011** [P0] All derivation formulas **MUST** live in one module (`hw/`) as a pure function
  `fn derive(profile: &HardwareProfile, config: &Config) -> EffectiveConfig`, unit-testable with
  synthetic profiles — no live cgroup or real hardware required to test any derivation.
- **HWA-012** [P0] **Effective configuration logging**: at boot, after derivation, Fluxum
  **MUST** emit a single structured log event (`effective configuration`) listing the probe
  inputs and, for every value in the table, its effective value and its source
  (`auto` | `config` | `env`). This is the first stop for "why is it using N threads?" questions.
- **HWA-013** [P1] `GET /health` **MUST** expose the `HardwareProfile` and the effective
  configuration (same values and sources as HWA-012). The data is captured once at boot, so this
  adds no locks and **MUST NOT** jeopardize the 50 ms health budget
  ([SPEC-012](SPEC-012-observability.md), FR-91).
- **HWA-014** [P1] The initial derivations above **MAY** be retuned, but only with benchmark
  evidence from the suites in [SPEC-013](SPEC-013-testing-conformance.md), and every retune
  **MUST** update the table in this spec in the same PR (change control, [README](README.md)).
- **HWA-015** [P0] Derivation **MUST** be self-consistent under small memory: the sum of fixed
  allocations it produces (write buffers, queue capacities, buffer-pool floor per
  [SPEC-015](SPEC-015-tiered-storage.md)) **MUST** fit within the effective memory on every
  supported profile, including 512 MiB. A derivation that cannot fit **MUST** fail boot with a
  clear error naming the shortfall — never silently oversubscribe.

## 4. Deployment profiles

- **HWA-020** [P0] One binary, full range: the same release binary **MUST** be functionally
  correct from **1 vCPU / 512 MB** (NFR-12) to large multi-core servers. No compile-time feature
  flag, alternate build, or manual tuning may be required at either extreme (FR-05).
- **HWA-021** [P0] **Small-droplet profile**: a named CI test profile `droplet` **MUST** exist —
  a cgroup-constrained container pinned to 1 CPU and 512 MiB memory — and **MUST** run (a) the
  functional integration suite, (b) the tiered-storage dataset-10×-budget suite
  ([SPEC-015](SPEC-015-tiered-storage.md)), and (c) an idle-baseline check asserting RSS
  < 100 MB (NFR-12). Like every named suite, it **MUST** be runnable locally with one documented
  command (TST-007).
- **HWA-022** [P1] **Scale-up sanity**: on a runner with ≥ 8 effective cores, CI **MUST** assert
  that derived values scale with the hardware (worker threads = cores, shard count > 1 under
  `sharding.shards: auto`, fan-out concurrency > droplet values) rather than remaining pinned at
  small-profile numbers.
- **HWA-023** [P1] Profile documentation: the operations guide **MUST** document the derived
  defaults at three reference points (1 vCPU / 512 MB, 4 vCPU / 8 GB, 32 vCPU / 128 GB) so
  operators can predict behavior before deploying.

## 5. SIMD runtime dispatch

- **HWA-030** [P0] Fluxum ships **one portable binary per OS/architecture target**: correctness
  **MUST NOT** depend on compile-time ISA features above the platform baseline (x86-64-v1,
  aarch64 NEON baseline). All ISA specialization above the baseline is selected at **runtime**.
- **HWA-031** [P1] **Per-kernel function pointers, selected once**: at startup, feature detection
  (CPUID on x86-64, auxval/HWCAP on aarch64 Linux — via `std::arch` `is_*_feature_detected!` or
  equivalent) **MUST** populate a dispatch table mapping each kernel to its best available
  implementation. Selection order is `AVX-512 → AVX2 → SSE4.2 → scalar` on x86-64 and
  `NEON → scalar` on aarch64; every other architecture uses scalar. A kernel **MAY** implement
  only a subset of tiers; dispatch falls through to the next available tier. Detection **MUST
  NOT** occur per call.
- **HWA-032** [P1] **Forced selection for debugging**: the config key
  `simd: auto | avx512 | avx2 | neon | scalar` (default `auto`; env override `FLUXUM_SIMD`)
  **MUST** be honored as follows: a forced tier selects that tier for every kernel that
  implements it, with kernels lacking that tier falling back to scalar; forcing a tier the CPU
  does not support **MUST** abort boot with a clear error (fail fast — this is a debugging
  knob, silent fallback would mask the very thing being bisected). `scalar` is always valid on
  every machine.
- **HWA-033** [P1] **Selection logged and visible**: the chosen implementation per kernel
  (e.g., `crc32=pclmul hash=avx2 fluxbin=avx2 predicate=avx2 compression=lib`) **MUST** be part
  of the boot-time effective-configuration event (HWA-012) and exposed in `/health`
  (HWA-013).
- **HWA-034** [P0] **Forced-scalar mode is fully functional**: with `simd: scalar`, every feature
  of the database behaves identically (bit-identical outputs, same test results). The entire
  workspace test suite **MUST** pass under forced scalar; scalar-only platforms are first-class,
  not degraded.
- **HWA-035** [P1] **Batch-oriented kernel APIs**: dispatched kernels **MUST** operate on
  batches (a buffer, a row batch, a page) so the indirect-call cost amortizes; per-row or
  per-element dispatch through the function-pointer table **MUST NOT** appear on hot paths.

```
Given a server started with FLUXUM_SIMD=avx2 on an AVX2-capable host
When boot completes
Then the effective-configuration log names avx2 as the selected tier for every AVX2-capable kernel
And /health reports the same per-kernel selection

Given FLUXUM_SIMD=avx512 on a host without AVX-512
When the process boots
Then it exits with a clear error naming the unsupported forced tier
```

## 6. Kernel catalogue (initial)

The initial dispatched kernels. Adding a kernel later requires updating this table and satisfying
§7 in the same PR.

| Kernel | ISA variants (initial) | Used by |
|---|---|---|
| CRC32 | hardware CRC instructions / PCLMUL folding (x86-64), CRC extension (aarch64), scalar | commit-log entries, page checksums ([SPEC-002](SPEC-002-storage-engine.md), [SPEC-015](SPEC-015-tiered-storage.md)) |
| xxHash-class hashing | AVX2 / SSE2, NEON, scalar | partition routing ([SPEC-007](SPEC-007-sharding.md)), index hashing |
| FluxBIN batch encode/decode | AVX2, NEON, scalar | fan-out serialization ([SPEC-005](SPEC-005-subscriptions.md)), page materialization ([SPEC-015](SPEC-015-tiered-storage.md)) |
| Batched predicate evaluation | AVX-512 / AVX2, NEON, scalar | subscription filters over row batches, scans ([SPEC-005](SPEC-005-subscriptions.md)) |
| LZ4 / zstd block paths | via library SIMD (not hand-rolled) | page compression, checkpoints, backups ([SPEC-015](SPEC-015-tiered-storage.md)) |

- **HWA-040** [P1] Every kernel in the table **MUST** be registered in the dispatch table
  (HWA-031), carry a scalar reference implementation (HWA-051), and pass the parity suite
  (HWA-052) before any accelerated variant is merged.
- **HWA-041** [P0] **CRC32 polynomial discipline**: the polynomial is mandated by
  [SPEC-002](SPEC-002-storage-engine.md), not by hardware convenience. Note the trap: the x86-64
  SSE4.2 `crc32` instruction computes CRC-32C (Castagnoli), while PCLMUL folding can compute any
  polynomial; the aarch64 CRC extension provides both. Every variant **MUST** implement the
  SPEC-002 polynomial exactly — a mismatch corrupts recovery, replication, and PITR, all of
  which replay CRC-validated entries.
- **HWA-042** [P0] **Hash stability**: the xxHash-class function used for partition routing and
  index hashing **MUST** produce identical values across all ISA variants, platforms, and
  endianness — a divergent hash silently misroutes rows to the wrong shard (data-placement
  corruption). Any future change of hash algorithm **MUST** be versioned and migrated, never
  swapped in place.
- **HWA-043** [P1] **FluxBIN batch codec**: batch encode/decode of N same-schema rows **MUST**
  produce output byte-identical to N invocations of the row-at-a-time codec defined in
  [SPEC-006](SPEC-006-protocol-fluxrpc.md) — the wire format is frozen at G5 and admits no
  variant-dependent bytes.
- **HWA-044** [P1] **Batched predicate evaluation**: vectorized evaluation over a row batch
  produces a selection bitmap that **MUST** equal row-at-a-time evaluation for every supported
  predicate, including `Option`/null semantics and floating-point comparisons (NaN and signed
  zero behavior **MUST** match scalar Rust operator semantics exactly and be covered by parity
  tests).
- **HWA-045** [P1] **Compression is delegated, not hand-rolled**: LZ4/zstd acceleration comes
  from the library's own SIMD paths. Required guarantee: `decompress(compress(x)) == x` on every
  platform (round-trip property tests per [SPEC-015](SPEC-015-tiered-storage.md)). Compressed
  byte streams **MUST NOT** be assumed byte-identical across machines or library versions;
  integrity checks (page CRC, backup verification) **MUST** therefore be computed over the bytes
  as written, never recomputed from a re-compression.

## 7. Correctness: scalar parity (FR-112, NFR-14)

- **HWA-050** [P0] **Bit-identical rule**: every hand-rolled SIMD kernel **MUST** produce output
  bit-identical to its scalar reference implementation for every input. There is no tolerance,
  no "close enough" for floats, no ISA-specific behavior. A parity violation is a release
  blocker.
- **HWA-051** [P0] **Scalar is the oracle**: the scalar implementation of each kernel lands
  first, compiles on every supported target, remains in the tree permanently, and stays
  selectable via `simd: scalar` (HWA-032). It **MUST NOT** be deleted or de-maintained when an
  accelerated variant lands; the SIMD variant is an optimization of the oracle, never its
  replacement.
- **HWA-052** [P0] **Parity property tests**: for every kernel, proptest suites (TST-003)
  **MUST** compare each SIMD variant against the scalar reference over randomized inputs,
  explicitly covering: empty inputs; lengths at and around vector-lane boundaries
  (0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, …); misaligned buffers; and, for predicate kernels,
  NaN, ±0.0, and null-heavy batches. Failing seeds are committed as permanent regressions
  (TST-003).
- **HWA-053** [P0] **ISA matrix in CI**: the parity suite **MUST** run on an ISA matrix with
  both **x86-64 and aarch64 runners** (family precedent: Vectorizer `simd-matrix.yml`). Each
  runner executes the parity suite for every variant its hardware supports (forced via HWA-032).
  A variant no CI runner can execute (e.g., AVX-512 without capable runners) **MUST** either be
  tested under an instruction emulator (e.g., Intel SDE) in CI or be excluded from release
  selection until a runner exists — unexercised variants do not ship.
- **HWA-054** [P0] **`unsafe` discipline**: every `unsafe` block in `simd/` **MUST** carry a
  `// SAFETY:` comment; this is enforced by the workspace lint
  `clippy::undocumented_unsafe_blocks` (TST-011). Intrinsics **MUST** be confined to the `simd/`
  module behind safe batch APIs, and any `#[target_feature]` function **MUST** only be reachable
  after runtime detection has proven the feature present (HWA-031).
- **HWA-055** [P1] **Boot-time known-answer self-check**: at startup, each selected non-scalar
  kernel **SHOULD** be run against a fixed known-answer vector and compared with the scalar
  result; on mismatch, the process **MUST** log an error and fall back to scalar for that kernel
  rather than serve with a divergent kernel. The check is one-shot and adds no steady-state cost.

## 8. Performance justification & regression guards

- **HWA-060** [P1] **Benchmark-gated merges**: every SIMD variant **MUST** be accompanied by a
  criterion benchmark (`harness = false`, TST-004) comparing it against the scalar reference on
  representative batch sizes. A variant that does not demonstrate a measured speedup on its
  target ISA **MUST NOT** be merged — parity without performance is dead weight.
- **HWA-061** [P1] **Committed baselines**: kernel benchmarks join the committed-baseline
  regression scheme of [SPEC-013](SPEC-013-testing-conformance.md) (TST-063/TST-064): per-PR
  smoke benches with the ±20% fence, full runs nightly, baseline updates as explicit reviewed
  commits.
- **HWA-062** [P1] **Macro-level justification**: SIMD work is ultimately justified by the
  parity harness against the app-server + PostgreSQL baseline (NFR-11) and the throughput/latency
  targets (NFR-01–NFR-05) — microbenchmarks select implementations; the parity report justifies
  the effort ([PRD](../PRD.md) §3.1.6).

## Acceptance criteria

1. **Probe correctness under cgroup limits.** In a container pinned to a fractional CPU quota
   and a 512 MiB memory limit on a many-core host, `HardwareProfile` reports the effective
   values per HWA-002 (quota and limit win over host totals), and every derived default is
   computed from them. The same suite passes with limits absent (bare-metal fallback, HWA-003)
   and with individual probe failures injected (HWA-004) — no panic, documented fallbacks,
   `WARN` logs present.
2. **Droplet profile suite green.** The named `droplet` CI profile (1 CPU / 512 MiB cgroup)
   boots the release binary, passes the functional integration suite and the SPEC-015
   dataset-10×-budget suite, and holds idle RSS < 100 MB (HWA-021, NFR-12). The suite runs
   locally with one documented command.
3. **ISA-matrix scalar-parity suite green.** The HWA-052 parity property suites pass on x86-64
   and aarch64 CI runners for every kernel variant selectable on those runners; no release
   selects a variant that the matrix did not exercise (HWA-053, NFR-14).
4. **Forced-scalar mode functional.** With `FLUXUM_SIMD=scalar`, the entire workspace test suite
   passes and the server serves the demo application correctly (HWA-034); forcing an unsupported
   tier aborts boot with a clear error (HWA-032).
5. **Effective-config visibility.** Boot emits the single `effective configuration` event
   containing probe inputs, every derived value with its source (`auto`/`config`/`env`), and the
   per-kernel SIMD selection; `GET /health` exposes the same data without exceeding its 50 ms
   budget (HWA-012, HWA-013, HWA-033).
