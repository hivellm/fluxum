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
| write_throughput | 26.70 | ≥ 10 | ✅ |
| e2e_p99 | 9.39 | ≥ 10 | ❌ |
| hot_p99† | 15442.50 | ≥ 50 | ✅ |
| cold_p99 | 52.83 | ≥ 0.5 (within 2×) | ✅ |

† *hot_p99 compares an **in-process cache read** (the Fluxum client reads its subscribed rows from local memory — no socket round-trip) against PostgreSQL's **remote prepared read** over a pooled connection. The asymmetry is the architecture being sold — subscribe once, read locally — but it is not a same-transport ratio, so it must never lead the summary. The same applies to the `hot` and `mixed/read` raw rows below (and to SpacetimeDB's, whose SDK reads its local cache too).*

## Competitive baseline vs SpacetimeDB (TST-097)

Ratios oriented bigger-is-better-for-Fluxum; ≥ 1.00 = parity with SpacetimeDB reached for that class. Informational until reached, floored by the regression guard afterwards.

| ratio | value | target | reached |
| --- | --- | --- | --- |
| write_throughput | 56.05 | ≥ 1.0 | ✅ |
| e2e_p99 | 14.31 | ≥ 1.0 | ✅ |
| hot_p99 | 4.33 | ≥ 1.0 | ✅ |
| cold_p99 | 13.36 | ≥ 1.0 | ✅ |
| mixed_write_throughput | 49.74 | ≥ 1.0 | ✅ |
| mixed_read_p99 | 0.93 | ≥ 1.0 | ⏳ |
| mixed_e2e_p99 | 30.78 | ≥ 1.0 | ✅ |

## Raw measurements (mean ± stddev across runs — TST-091)

| side | class | ops/s | p50 ms | p99 ms | p99 σ ms | p99 CI95 ± ms | max ms | ops | runs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fluxum | cold | 1005 ±60 | 0.2963 | 0.8733 | 0.0411 | 0.0510 | 0.936 | 80 | 5 |
| fluxum | e2e | ‡ (rate-capped) | 0.4649 | 0.7151 | 0.0451 | 0.0560 | 1.021 | 25000 | 5 |
| fluxum | hot | 81182011 ±2439447 | 0.0001 | 0.0001 | 0.0000 | 0.0001 | 0.362 | 2029728631 | 5 |
| fluxum | mixed/e2e | ‡ (rate-capped) | 0.3796 | 0.6723 | 0.0450 | 0.0558 | 1.317 | 24700 | 5 |
| fluxum | mixed/read | 63799729 ±3184674 | 0.0001 | 0.0003 | 0.0000 | 0.0001 | 21.105 | 3190120755 | 5 |
| fluxum | mixed/write | 33223 ±585 | 0.1746 | 1.9155 | 0.1349 | 0.1675 | 105.013 | 1661201 | 5 |
| fluxum | write | 37798 ±563 | 0.1556 | 1.7670 | 0.1458 | 0.1810 | 60.476 | 1889991 | 5 |
| postgres | cold | 220 ±22 | 1.4152 | 46.1363 | 7.3913 | 9.1761 | 58.409 | 80 | 5 |
| postgres | e2e | ‡ (rate-capped) | 5.0932 | 6.7124 | 1.0424 | 1.2941 | 17.313 | 25000 | 5 |
| postgres | hot | 5884 ±115 | 1.3429 | 1.8531 | 0.0381 | 0.0473 | 3.739 | 147110 | 5 |
| postgres | mixed/e2e | ‡ (rate-capped) | 7.2525 | 11.5438 | 2.5904 | 3.2159 | 16.328 | 23250 | 5 |
| postgres | mixed/read | 2122 ±43 | 3.6582 | 6.3011 | 0.1733 | 0.2151 | 12.053 | 106112 | 5 |
| postgres | mixed/write | 1363 ±10 | 5.6463 | 8.9403 | 0.2328 | 0.2890 | 21.090 | 68164 | 5 |
| postgres | write | 1415 ±23 | 5.5353 | 8.2254 | 2.7267 | 3.3851 | 27.243 | 70778 | 5 |
| spacetimedb | cold | 77 ±2 | 10.4108 | 11.6713 | 1.1990 | 1.4886 | 13.085 | 80 | 5 |
| spacetimedb | e2e | ‡ (rate-capped) | 7.9992 | 10.2304 | 0.8746 | 1.0858 | 20.198 | 25000 | 5 |
| spacetimedb | hot | 39935146 ±7843644 | 0.0001 | 0.0005 | 0.0003 | 0.0004 | 1.076 | 998487221 | 5 |
| spacetimedb | mixed/e2e | ‡ (rate-capped) | 11.0853 | 20.6971 | 3.0987 | 3.8469 | 24.053 | 22500 | 5 |
| spacetimedb | mixed/read | 48557235 ±6321229 | 0.0001 | 0.0003 | 0.0001 | 0.0001 | 1.548 | 2427945951 | 5 |
| spacetimedb | mixed/write | 668 ±5 | 11.7347 | 22.1952 | 2.6770 | 3.3234 | 38.363 | 33399 | 5 |
| spacetimedb | write | 674 ±6 | 11.6434 | 21.1980 | 3.1666 | 3.9312 | 83.074 | 33720 | 5 |

‡ *e2e and mixed/e2e rows are **latency-only**: the workload caps the chat event rate (a fixed messages-per-second sender), so their delivered-updates/s is that cap times the subscriber count on every side — a harness constant, not a throughput result. Only their latency columns are measurements.*

*Cold-read honesty note: restarts clear database-level caches (Fluxum buffer pool / PostgreSQL `shared_buffers`) symmetrically; the OS page cache is not dropped on either side, so cold numbers measure database page-in, not platter latency.*
