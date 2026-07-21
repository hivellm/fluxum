# Fluxum parity report (harness 0.1.0)

Date: 2026-07-21

## Hardware (both sides, same machine — TST-091)

- CPU: AMD Ryzen 9 7950X3D 16-Core Processor (32 logical cores)
- RAM: 127.2 GiB
- OS: Windows 10 (19045)
- Disk: NVMe SSD (developer workstation)

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

## NFR-11 ratios

| ratio | value | target | met |
| --- | --- | --- | --- |
| write_throughput | 25.66 | ≥ 10 | ✅ |
| e2e_p99 | 8.54 | ≥ 10 | ❌ |
| hot_p99 | 9423.33 | ≥ 50 | ✅ |
| cold_p99 | 9.04 | ≥ 0.5 (within 2×) | ✅ |

## Competitive baseline vs SpacetimeDB (TST-097)

Ratios oriented bigger-is-better-for-Fluxum; ≥ 1.00 = parity with SpacetimeDB reached for that class. Informational until reached, floored by the regression guard afterwards.

| ratio | value | target | reached |
| --- | --- | --- | --- |
| write_throughput | 59.35 | ≥ 1.0 | ✅ |
| e2e_p99 | 14.27 | ≥ 1.0 | ✅ |
| hot_p99 | 2.67 | ≥ 1.0 | ✅ |
| cold_p99 | 11.47 | ≥ 1.0 | ✅ |
| mixed_write_throughput | 49.21 | ≥ 1.0 | ✅ |
| mixed_read_p99 | 1.00 | ≥ 1.0 | ✅ |
| mixed_e2e_p99 | 4.48 | ≥ 1.0 | ✅ |

## Raw measurements (mean ± stddev across runs — TST-091)

| side | class | ops/s | p50 ms | p99 ms | p99 σ ms | max ms | ops | runs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fluxum | cold | 905 ±90 | 0.3482 | 0.9648 | 0.1242 | 1.107 | 48 | 3 |
| fluxum | e2e | 496 ±0 | 0.4583 | 0.7585 | 0.0025 | 1.000 | 15000 | 3 |
| fluxum | hot | 63135731 ±3013849 | 0.0001 | 0.0002 | 0.0000 | 1.412 | 947125541 | 3 |
| fluxum | mixed/e2e | 495 ±0 | 0.4957 | 4.4558 | 1.0494 | 5.789 | 14850 | 3 |
| fluxum | mixed/read | 59435779 ±4252421 | 0.0001 | 0.0003 | 0.0001 | 2.169 | 1783131268 | 3 |
| fluxum | mixed/write | 32425 ±1090 | 0.1816 | 1.8393 | 0.0497 | 16.658 | 972787 | 3 |
| fluxum | write | 36541 ±278 | 0.1618 | 1.8001 | 0.0466 | 8.766 | 1096277 | 3 |
| postgres | cold | 378 ±41 | 2.0824 | 8.7218 | 0.7053 | 9.359 | 48 | 3 |
| postgres | e2e | 475 ±0 | 4.9624 | 6.4754 | 0.6405 | 17.068 | 15000 | 3 |
| postgres | hot | 5805 ±99 | 1.3594 | 1.8847 | 0.0463 | 3.773 | 87091 | 3 |
| postgres | mixed/e2e | 465 ±0 | 7.1636 | 16.7781 | 5.0921 | 21.178 | 13950 | 3 |
| postgres | mixed/read | 2068 ±13 | 3.7479 | 6.5010 | 0.0748 | 12.794 | 62046 | 3 |
| postgres | mixed/write | 1347 ±8 | 5.6787 | 9.4806 | 0.2651 | 23.180 | 40418 | 3 |
| postgres | write | 1424 ±5 | 5.5254 | 7.2429 | 0.4687 | 21.192 | 42728 | 3 |
| spacetimedb | cold | 69 ±11 | 9.0793 | 11.0664 | 2.0231 | 13.178 | 48 | 3 |
| spacetimedb | e2e | 461 ±0 | 7.9976 | 10.8268 | 1.2836 | 13.904 | 15000 | 3 |
| spacetimedb | hot | 47355629 ±16068821 | 0.0001 | 0.0005 | 0.0006 | 1.331 | 710438241 | 3 |
| spacetimedb | mixed/e2e | 450 ±0 | 10.6255 | 19.9609 | 5.1106 | 24.157 | 13500 | 3 |
| spacetimedb | mixed/read | 49059172 ±8679716 | 0.0001 | 0.0003 | 0.0002 | 9.001 | 1471832019 | 3 |
| spacetimedb | mixed/write | 659 ±10 | 11.8398 | 23.5875 | 0.2090 | 46.285 | 19770 | 3 |
| spacetimedb | write | 616 ±73 | 12.2947 | 29.6256 | 16.3566 | 85.984 | 18471 | 3 |

*Cold-read honesty note: restarts clear database-level caches (Fluxum buffer pool / PostgreSQL `shared_buffers`) symmetrically; the OS page cache is not dropped on either side, so cold numbers measure database page-in, not platter latency.*
