# Fluxum parity report (harness 0.1.0)

Date: 2026-07-23

## Scope and method

The **NFR-11 verdicts** below come from a **PostgreSQL parity harness**: the baseline is tuned PostgreSQL behind an axum+sqlx app server in its own process (pooled prepared statements, covering indexes, LISTEN/NOTIFY fan-out) — the stack a team would replace with Fluxum. They are **not** SpacetimeDB numbers; the competitive SpacetimeDB baseline (TST-097) is measured against a real SpacetimeDB server and reported in its own section, and the two never mix.

Method (TST-091): every class runs on the same idle machine, remote socket transport on every side except where a row is footnoted as an architectural asymmetry. Raw rows report mean ± stddev across runs plus a 95% Student-t confidence half-width on p99, so a verdict is distinguishable from noise. Core pinning (`--pin server=0xMASK,driver=0xMASK`) is a documented methodology knob; the canonical report runs UNPINNED — on the 32-core bench box confining the server to half the cores measurably degrades every heavy phase (recorded 2026-07-22, phase0_parity-fanout-latency 1.4) — and the active setting is recorded in each stack's config line.

## Hardware (both sides, same machine — TST-091)

- CPU: AMD Ryzen 9 7950X3D 16-Core Processor (32 logical cores)
- RAM: 127.2 GiB
- OS: Windows 10 (19045)
- Disk: NVMe SSD (operator-stated)

## Stacks

- **fluxum**: fluxum-server 0.1.0 (release)
  - durability: TXN-004: ReducerResult acked after the commit-log append reaches the OS (process-crash safe); fsync is async group commit — ~50 ms OS-crash window (NFR-08)
  - config: development profile, memory budget default (auto)
- **postgres**: PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
  - durability: synchronous_commit=on (WAL fsync before commit ack when on)
  - config: axum+sqlx app server (own process), pooled prepared statements (max_connections=16), covering indexes task(owner) and chat_message(channel,id), LISTEN/NOTIFY fan-out
- **spacetimedb**: clockworklabs/spacetime:v2.6.1 (standalone, pinned)
  - durability: reducer acked at in-memory commit, BEFORE the commit-log append: durability is a background actor batching appends and fsyncing per batch (group commit) — a process or OS crash can lose acked transactions since the last sync (spacetimedb-durability v2.6.1, imp::local). Weaker ack than Fluxum's TXN-004 (append reaches the OS pre-ack)
  - config: demo module 1:1 (spacetimedb-module/, spacetimedb =2.6.1 wasm), client spacetimedb-sdk =2.6.1 over WebSocket; task visibility via RLS owner filter (:sender); btree indexes task.owner and chat_message.channel; send_chat budget table in-module (Fluxum enforces the same 20/s pre-transaction, RED-050)

## NFR-11 ratios (vs the PostgreSQL parity baseline)

| ratio | value | target | met |
| --- | --- | --- | --- |
| write_throughput | 25.42 | ≥ 10 | ✅ |
| e2e_p99 | 13.06 | ≥ 10 | ✅ |
| hot_p99† | 10075.11 | ≥ 50 | ✅ |
| cold_p99 | 51.47 | ≥ 0.5 (within 2×) | ✅ |

† *hot_p99 compares an **in-process cache read** (the Fluxum client reads its subscribed rows from local memory — no socket round-trip) against PostgreSQL's **remote prepared read** over a pooled connection. The asymmetry is the architecture being sold — subscribe once, read locally — but it is not a same-transport ratio, so it must never lead the summary. The same applies to the `hot` and `mixed/read` raw rows below (and to SpacetimeDB's, whose SDK reads its local cache too).*

## Competitive baseline vs SpacetimeDB (TST-097)

Ratios oriented bigger-is-better-for-Fluxum; ≥ 1.00 = parity with SpacetimeDB reached for that class. Informational until reached, floored by the regression guard afterwards.

| ratio | value | target | reached |
| --- | --- | --- | --- |
| write_throughput | 54.22 | ≥ 1.0 | ✅ |
| e2e_p99 | 40.06 | ≥ 1.0 | ✅ |
| hot_p99 | 2.89 | ≥ 1.0 | ✅ |
| cold_p99 | 13.39 | ≥ 1.0 | ✅ |
| mixed_write_throughput | 47.29 | ≥ 1.0 | ✅ |
| mixed_read_p99 | 1.00 | ≥ 1.0 | ✅ |
| mixed_e2e_p99 | 58.95 | ≥ 1.0 | ✅ |

## Raw measurements (mean ± stddev across runs — TST-091)

| side | class | ops/s | p50 ms | p99 ms | p99 σ ms | p99 CI95 ± ms | max ms | ops | runs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fluxum | cold | 986 ±29 | 0.2863 | 0.9043 | 0.0507 | 0.0629 | 0.973 | 80 | 5 |
| fluxum | e2e | ‡ (rate-capped) | 0.4573 | 0.6944 | 0.0625 | 0.0776 | 1.023 | 25000 | 5 |
| fluxum | hot | 58935240 ±3559669 | 0.0001 | 0.0002 | 0.0000 | 0.0001 | 1.639 | 1473498748 | 5 |
| fluxum | mixed/e2e | ‡ (rate-capped) | 0.3898 | 0.6547 | 0.0381 | 0.0473 | 1.350 | 24250 | 5 |
| fluxum | mixed/read | 57120677 ±3827715 | 0.0001 | 0.0003 | 0.0000 | 0.0000 | 43.340 | 2856451934 | 5 |
| fluxum | mixed/write | 31618 ±2867 | 0.1791 | 1.8752 | 0.1130 | 0.1403 | 890.977 | 1581138 | 5 |
| fluxum | write | 36390 ±2553 | 0.1591 | 1.8773 | 0.1657 | 0.2057 | 115.064 | 1819589 | 5 |
| fluxum | write/pipelined(32) | 50273 ±4271 | 5.1618 | 9.5470 | 0.7042 | 0.8742 | 96.586 | 2513781 | 5 |
| postgres | cold | 222 ±18 | 1.4935 | 46.5417 | 7.5997 | 9.4348 | 59.904 | 80 | 5 |
| postgres | e2e | ‡ (rate-capped) | 5.1311 | 9.0703 | 7.3040 | 9.0676 | 22.321 | 25000 | 5 |
| postgres | hot | 5967 ±90 | 1.3247 | 1.8135 | 0.0278 | 0.0345 | 3.913 | 149193 | 5 |
| postgres | mixed/e2e | ‡ (rate-capped) | 7.1794 | 85.5119 | 154.3441 | 191.6128 | 362.190 | 23100 | 5 |
| postgres | mixed/read | 2145 ±46 | 3.6114 | 6.2890 | 0.2616 | 0.3247 | 11.002 | 107247 | 5 |
| postgres | mixed/write | 1356 ±27 | 5.6197 | 9.2785 | 0.5243 | 0.6509 | 456.477 | 67821 | 5 |
| postgres | write | 1431 ±11 | 5.4913 | 7.7405 | 0.1653 | 0.2052 | 19.752 | 71568 | 5 |
| spacetimedb | cold | 75 ±1 | 10.8610 | 12.1105 | 1.0855 | 1.3476 | 13.859 | 80 | 5 |
| spacetimedb | e2e | ‡ (rate-capped) | 8.8433 | 27.8170 | 17.1474 | 21.2879 | 69.477 | 25000 | 5 |
| spacetimedb | hot | 45627889 ±13672476 | 0.0001 | 0.0005 | 0.0003 | 0.0004 | 11.282 | 1140827770 | 5 |
| spacetimedb | mixed/e2e | ‡ (rate-capped) | 10.2505 | 38.5941 | 38.2254 | 47.4555 | 109.248 | 22500 | 5 |
| spacetimedb | mixed/read | 53322566 ±4883212 | 0.0001 | 0.0003 | 0.0001 | 0.0001 | 14.510 | 2666227309 | 5 |
| spacetimedb | mixed/write | 669 ±10 | 11.6025 | 23.6897 | 0.1806 | 0.2242 | 163.711 | 33434 | 5 |
| spacetimedb | write | 671 ±8 | 11.6921 | 22.1844 | 3.0835 | 3.8280 | 79.531 | 33560 | 5 |

‡ *e2e and mixed/e2e rows are **latency-only**: the workload caps the chat event rate (a fixed messages-per-second sender), so their delivered-updates/s is that cap times the subscriber count on every side — a harness constant, not a throughput result. Only their latency columns are measurements.*

*write/pipelined(N) is a **fluxum-only NFR-01 evidence row**: the same acked reducer write with N calls held in flight per connection (Rust SDK `call_reducer_async`). Its latency columns include the deliberate client-held window queueing — **throughput is the meaningful column** — and it feeds no ratio: the incumbent's app-server protocol is strictly request/response, so its concurrency lever (connection count) is already the `write` row. The acked-serial `write` row above remains the honest latency number.*

*Cold-read honesty note: restarts clear database-level caches (Fluxum buffer pool / PostgreSQL `shared_buffers`) symmetrically; the OS page cache is not dropped on either side, so cold numbers measure database page-in, not platter latency.*
