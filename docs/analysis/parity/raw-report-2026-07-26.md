# Fluxum parity report (harness 0.1.0)

Date: 2026-07-26

## Scope and method

The **NFR-11 verdicts** below come from a **PostgreSQL parity harness**: the baseline is tuned PostgreSQL behind an axum+sqlx app server in its own process (pooled prepared statements, covering indexes, LISTEN/NOTIFY fan-out) — the stack a team would replace with Fluxum. They are **not** SpacetimeDB numbers; the competitive SpacetimeDB baseline (TST-097) is measured against a real SpacetimeDB server and reported in its own section, and the two never mix.

Method (TST-091): every class runs on the same idle machine, remote socket transport on every side except where a row is footnoted as an architectural asymmetry. Raw rows report mean ± stddev across runs plus a 95% Student-t confidence half-width on p99, so a verdict is distinguishable from noise. Core pinning (`--pin server=0xMASK,driver=0xMASK`) is a documented methodology knob; the canonical report runs UNPINNED — on the 32-core bench box confining the server to half the cores measurably degrades every heavy phase (recorded 2026-07-22, phase0_parity-fanout-latency 1.4) — and the active setting is recorded in each stack's config line.

## Hardware (both sides, same machine — TST-091)

- CPU: AMD Ryzen 9 7950X3D 16-Core Processor (32 logical cores)
- RAM: 127.2 GiB
- OS: Windows 10 (19045)
- Disk: NVMe SSD

## Stacks

- **fluxum**: fluxum-server 0.1.0 (release)
  - durability: TXN-004: ReducerResult acked after the commit-log append reaches the OS (process-crash safe); fsync is async group commit — ~50 ms OS-crash window (NFR-08)
  - config: development profile, memory budget default (auto), cores pinned server=0xFFFF,driver=0xFFFF0000 (server processes vs driver — P0-A 1.4)
- **postgres**: PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
  - durability: synchronous_commit=on (WAL fsync before commit ack when on)
  - config: axum+sqlx app server (own process), pooled prepared statements (max_connections=16), covering indexes task(owner) and chat_message(channel,id), LISTEN/NOTIFY fan-out, cores pinned server=0xFFFF,driver=0xFFFF0000 (server processes vs driver — P0-A 1.4)
- **spacetimedb**: clockworklabs/spacetime:v2.6.1 (standalone, pinned)
  - durability: reducer acked at in-memory commit, BEFORE the commit-log append: durability is a background actor batching appends and fsyncing per batch (group commit) — a process or OS crash can lose acked transactions since the last sync (spacetimedb-durability v2.6.1, imp::local). Weaker ack than Fluxum's TXN-004 (append reaches the OS pre-ack)
  - config: demo module 1:1 (spacetimedb-module/, spacetimedb =2.6.1 wasm), client spacetimedb-sdk =2.6.1 over WebSocket; task visibility via RLS owner filter (:sender); btree indexes task.owner and chat_message.channel; send_chat budget table in-module (Fluxum enforces the same 20/s pre-transaction, RED-050)

## NFR-11 ratios (vs the PostgreSQL parity baseline)

| ratio | value | target | met |
| --- | --- | --- | --- |
| write_throughput | 5.43 | ≥ 10 | ❌ |
| e2e_p99 | 8.37 | ≥ 10 | ❌ |
| hot_p99† | 8644.08 | ≥ 50 | ✅ |
| cold_p99 | 0.40 | ≥ 0.5 (within 2×) | ❌ |

† *hot_p99 compares an **in-process cache read** (the Fluxum client reads its subscribed rows from local memory — no socket round-trip) against PostgreSQL's **remote prepared read** over a pooled connection. The asymmetry is the architecture being sold — subscribe once, read locally — but it is not a same-transport ratio, so it must never lead the summary. The same applies to the `hot` and `mixed/read` raw rows below (and to SpacetimeDB's, whose SDK reads its local cache too).*

## Competitive baseline vs SpacetimeDB (TST-097)

Ratios oriented bigger-is-better-for-Fluxum; ≥ 1.00 = parity with SpacetimeDB reached for that class. Informational until reached, floored by the regression guard afterwards.

| ratio | value | target | reached |
| --- | --- | --- | --- |
| write_throughput | 14.33 | ≥ 1.0 | ✅ |
| e2e_p99 | 12.50 | ≥ 1.0 | ✅ |
| hot_p99 | 1.69 | ≥ 1.0 | ✅ |
| cold_p99 | 1.32 | ≥ 1.0 | ✅ |
| mixed_write_throughput | 10.01 | ≥ 1.0 | ✅ |
| mixed_read_p99 | 0.72 | ≥ 1.0 | ⏳ |
| mixed_e2e_p99 | 11.90 | ≥ 1.0 | ✅ |

## Raw measurements (mean ± stddev across runs — TST-091)

| side | class | ops/s | p50 ms | p99 ms | p99 σ ms | p99 CI95 ± ms | max ms | ops | runs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fluxum | cold | 153 ±44 | 4.7311 | 25.5461 | 44.6940 | 55.4860 | 105.496 | 80 | 5 |
| fluxum | e2e | ‡ (rate-capped) | 0.4796 | 0.9395 | 0.1132 | 0.1405 | 1.430 | 25000 | 5 |
| fluxum | hot | 63267312 ±5448213 | 0.0001 | 0.0003 | 0.0001 | 0.0001 | 3.345 | 1581834779 | 5 |
| fluxum | mixed/e2e | ‡ (rate-capped) | 0.8129 | 2.5271 | 2.4074 | 2.9888 | 7.183 | 23900 | 5 |
| fluxum | mixed/read | 55867504 ±1135069 | 0.0001 | 0.0004 | 0.0001 | 0.0001 | 4.683 | 2793504600 | 5 |
| fluxum | mixed/write | 6724 ±1099 | 0.8261 | 5.1422 | 2.3067 | 2.8637 | 150.029 | 336223 | 5 |
| fluxum | write | 8972 ±588 | 0.6270 | 3.9376 | 0.4498 | 0.5584 | 55.355 | 448636 | 5 |
| fluxum | write/pipelined(32) | 6585 ±344 | 36.0724 | 115.5726 | 39.5484 | 49.0979 | 346.660 | 329281 | 5 |
| postgres | cold | 332 ±36 | 2.2901 | 10.3206 | 2.3539 | 2.9223 | 14.465 | 80 | 5 |
| postgres | e2e | ‡ (rate-capped) | 5.3684 | 7.8641 | 2.4566 | 3.0497 | 17.911 | 25000 | 5 |
| postgres | hot | 5135 ±112 | 1.5320 | 2.2475 | 0.0548 | 0.0680 | 5.019 | 128398 | 5 |
| postgres | mixed/e2e | ‡ (rate-capped) | 8.6902 | 39.1351 | 54.8290 | 68.0683 | 137.771 | 22500 | 5 |
| postgres | mixed/read | 1640 ±74 | 4.7306 | 8.6000 | 0.6393 | 0.7937 | 37.016 | 82008 | 5 |
| postgres | mixed/write | 1034 ±179 | 7.0583 | 26.8923 | 29.2069 | 36.2593 | 152.325 | 51694 | 5 |
| postgres | write | 1651 ±697 | 4.7374 | 20.6135 | 16.4727 | 20.4503 | 168.917 | 82558 | 5 |
| spacetimedb | cold | 71 ±12 | 10.1521 | 33.6969 | 43.4532 | 53.9457 | 110.654 | 80 | 5 |
| spacetimedb | e2e | ‡ (rate-capped) | 8.1748 | 11.7394 | 2.3883 | 2.9650 | 21.034 | 25000 | 5 |
| spacetimedb | hot | 49923475 ±12832794 | 0.0001 | 0.0004 | 0.0004 | 0.0005 | 3.882 | 1248244989 | 5 |
| spacetimedb | mixed/e2e | ‡ (rate-capped) | 10.2242 | 30.0626 | 7.7557 | 9.6284 | 41.090 | 22500 | 5 |
| spacetimedb | mixed/read | 53552513 ±3677587 | 0.0001 | 0.0003 | 0.0001 | 0.0001 | 3.302 | 2677766496 | 5 |
| spacetimedb | mixed/write | 671 ±3 | 11.5885 | 23.5042 | 0.1492 | 0.1853 | 44.057 | 33575 | 5 |
| spacetimedb | write | 626 ±52 | 12.0633 | 35.4998 | 27.1707 | 33.7314 | 224.801 | 31318 | 5 |

‡ *e2e and mixed/e2e rows are **latency-only**: the workload caps the chat event rate (a fixed messages-per-second sender), so their delivered-updates/s is that cap times the subscriber count on every side — a harness constant, not a throughput result. Only their latency columns are measurements.*

*write/pipelined(N) is a **fluxum-only NFR-01 evidence row**: the same acked reducer write with N calls held in flight per connection (Rust SDK `call_reducer_async`). Its latency columns include the deliberate client-held window queueing — **throughput is the meaningful column** — and it feeds no ratio: the incumbent's app-server protocol is strictly request/response, so its concurrency lever (connection count) is already the `write` row. The acked-serial `write` row above remains the honest latency number.*

*Cold-read honesty note: restarts clear database-level caches (Fluxum buffer pool / PostgreSQL `shared_buffers`) symmetrically; the OS page cache is not dropped on either side, so cold numbers measure database page-in, not platter latency.*
