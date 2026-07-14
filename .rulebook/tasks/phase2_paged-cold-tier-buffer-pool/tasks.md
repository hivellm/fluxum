## 1. Implementation
- [ ] 1.1 Design and implement the on-disk page format: FluxBIN rows + per-page CRC32C checksum; decide page size and read path (OQ-7: 4/8/16 KB, mmap vs pread) - format freezes at G5 (TIER-020/TIER-021)
- [ ] 1.2 Implement the buffer pool: clock-LRU eviction, pin/unpin, fault-in path; pin exhaustion returns BufferPoolExhausted + rollback, never OOM (TIER-002..004)
- [ ] 1.3 Enforce `memory.budget: auto | <bytes>` as the single memory ceiling; `auto` derived from the hardware probe (FR-110, FR-18); process RSS never exceeds budget + max(64 MiB, 10%)
- [ ] 1.4 Implement paged, evictable secondary/spatial index pages (the novel work vs SpacetimeDB RAM-bound indexes) (TIER-050)
- [ ] 1.5 Every fault-in verifies the per-page CRC32C before serving; tampered page is never served - always PageCorrupt (TIER-021/TIER-032/TIER-062); content hashes round-trip through evict/fault cycles unchanged (TIER-063)
- [ ] 1.6 Expose `fluxum_bufferpool_*` and `fluxum_page_reads_total` (with index flag) metrics counters (consumed by T5.6)
- [ ] 1.7 Hot-path zero-disk-I/O assertion: with working set resident, no I/O syscalls on the read path (strace/ETW harness) and point lookup < 1 microsecond (NFR-07, NFR-02, TIER-014)
- [ ] 1.8 Verification (DAG exit test): dataset 10x the memory budget served correctly on the droplet profile, incl. an index-dominated workload whose index pages alone exceed the budget (TIER-070, NFR-12); budget never exceeded
- [ ] 1.9 Gate G2 input: 10x-dataset suite green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
