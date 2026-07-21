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

## NFR-11 ratios

| ratio | value | target | met |
| --- | --- | --- | --- |
| write_throughput | 0.30 | ≥ 10 | ❌ |
| e2e_p99 | 4.87 | ≥ 10 | ❌ |
| hot_p99 | 9467.83 | ≥ 50 | ✅ |
| cold_p99 | 3.56 | ≥ 0.5 (within 2×) | ✅ |

## Raw measurements (mean ± stddev across runs — TST-091)

| side | class | ops/s | p50 ms | p99 ms | p99 σ ms | max ms | ops | runs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fluxum | cold | 170 ±37 | 5.5368 | 6.7611 | 3.1358 | 10.344 | 48 | 3 |
| fluxum | e2e | 496 ±0 | 0.4797 | 0.8021 | 0.0720 | 1.122 | 15000 | 3 |
| fluxum | hot | 60321449 ±5061860 | 0.0001 | 0.0002 | 0.0000 | 1.944 | 904899653 | 3 |
| fluxum | mixed/e2e | 375 ±18 | 30.3463 | 123.2051 | 24.1962 | 138.439 | 11250 | 3 |
| fluxum | mixed/read | 60784949 ±2870124 | 0.0001 | 0.0002 | 0.0000 | 13.320 | 1823631576 | 3 |
| fluxum | mixed/write | 257 ±49 | 28.8221 | 77.3440 | 16.9448 | 152.873 | 7703 | 3 |
| fluxum | write | 941 ±545 | 10.0184 | 17.8618 | 6.0108 | 37.938 | 28232 | 3 |
| postgres | cold | 275 ±207 | 17.7509 | 24.0822 | 33.4270 | 62.680 | 48 | 3 |
| postgres | e2e | 485 ±0 | 2.9349 | 3.9049 | 0.3093 | 7.653 | 15000 | 3 |
| postgres | hot | 5786 ±58 | 1.3638 | 1.8936 | 0.0616 | 7.237 | 86792 | 3 |
| postgres | mixed/e2e | 477 ±3 | 4.0068 | 7.3947 | 0.9994 | 8.714 | 14300 | 3 |
| postgres | mixed/read | 1557 ±1 | 4.9212 | 9.2675 | 0.0827 | 19.355 | 46718 | 3 |
| postgres | mixed/write | 2305 ±13 | 3.2885 | 7.1188 | 0.2120 | 16.339 | 69163 | 3 |
| postgres | write | 3116 ±12 | 2.4000 | 5.0949 | 0.7079 | 17.589 | 93497 | 3 |

*Cold-read honesty note: restarts clear database-level caches (Fluxum buffer pool / PostgreSQL `shared_buffers`) symmetrically; the OS page cache is not dropped on either side, so cold numbers measure database page-in, not platter latency.*
