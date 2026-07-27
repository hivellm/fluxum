# Proposal: phase5_multi-shard-server-assembly

## Why

T5.4 delivered `ShardCoord` and `ShardHost` as a library — partition routing,
the shard registry, entity handoff, cross-shard subscription aggregation — and
they are covered by `shard_coord.rs`, `entity_handoff.rs` and `shard_routing.rs`.
The **server binary never assembles them**. `boot.rs` says so outright:

```rust
// Shard 0 of this process. Multi-shard hosting is ShardCoord's job
// (SPEC-024); a single-process server owns one shard.
let shard = 0_u32;
```

Nothing outside `hw/mod.rs` reads `sharding.shards`, and there it only feeds
the `auto` hardware-derivation table. So a deployment configured with
`sharding.shards: 8` runs **one** shard, silently. The capability is tested;
it is just not reachable from the product.

Two consequences, both already biting:

- **TST-112 cannot be satisfied.** It requires a "sharded + tiered deployment"
  holding >= 1 billion rows with memory within budget "on every shard" — a G7
  exit criterion. `fluxum-bench soak --shards 8` was producing reports that
  named eight shards while one ran; the soak now records `shards_requested`
  beside `shards_observed` and says outright that such a run does not meet the
  clause, which makes the gap visible but does not close it.
- **NFR-01 has no horizontal lever.** The commit path is one serialized worker
  per shard (`TxPipelineWorker::run` processes one transaction at a time, by
  design, for serializability). Measured end-to-end throughput is
  ~11 000-17 000 tx/s against a target of >= 100 000 tx/s **per shard**. More
  client connections cannot help — with the writer serialized, added
  concurrency lengthens the queue rather than the throughput, which is exactly
  what measurement shows (32 clients x pipeline 128 was *slower* than 8 x 32).
  Scaling out across shards is the lever the architecture intends, and it is
  the one the binary does not offer.

## What Changes

Assemble the multi-shard host in the server: `sharding.shards` provisions that
many `ShardHost`s behind a `ShardCoord`, connections route by partition key
(SHD-001..004), and the admin surface reports per shard. A deployment
configured for N shards runs N shards, or refuses to start — the current
silent downgrade to one is the worst of the three outcomes.

Scope boundary: this is the *assembly*, not new sharding semantics. Routing,
handoff and cross-shard aggregation already exist and are tested; what is
missing is provisioning them from configuration and threading the transports,
admin endpoints, checkpointing and replication through the coordinator.

`/metrics` already labels its series by `shard`, so per-shard observability is
mostly a matter of the multi-shard assembly emitting one block per host —
worth confirming rather than assuming, since every shard-labelled series today
carries the constant 0.

## Impact

- Affected specs: SPEC-007 (SHD-001..004, SHD-010..013), SPEC-024, SPEC-013
  (TST-112), SPEC-015 (TIER-004 per shard)
- Affected code: `crates/fluxum-server/src/boot.rs` (single-shard assembly),
  `crates/fluxum-server/src/shard.rs` (`ShardCoord`/`ShardHost`, already
  written), the TCP/HTTP transports' session routing, `admin.rs` (per-shard
  metrics/health), checkpoint and replication wiring
- PRD requirements: NFR-01 (throughput per shard), NFR-13 (billion-row scale)
- Depends on: T5.4 (landed as a library)
- Breaking change: NO for single-shard deployments (`shards: 1` stays the
  default shape); a deployment that had `shards: N > 1` configured will start
  behaving as configured, which is the point
- Risk: MEDIUM-HIGH — routing a key to the wrong shard strands rows, and the
  handoff path is timing-sensitive. The existing `shard_coord` and
  `entity_handoff` suites are the guardrails, and they must run against the
  assembled server rather than only against hand-built hosts.
- User benefit: the horizontal scaling the architecture was designed for
  becomes reachable from configuration, and the billion-row soak can evidence
  the clause it is meant to evidence

## Notes

Sequencing against `phase2_superseded-page-reclamation`: that task lifts the
per-shard ceiling, this one multiplies it. They are independent, but measuring
this task's benefit is cleaner once the reclamation cliff is gone — otherwise
per-shard throughput depends on how much version garbage has accumulated.

SPEC-007 OQ-2 records the decision that the reference deployment hosts every
`ShardHost` as a tokio task inside one process, with process-per-shard as a
deployment alternative. That decision stands; this task implements the
normative shape.

When the assembly lands, tighten `fluxum-bench soak` from *warning* to
*failing* when `shards_observed < shards_requested` — the warning exists only
because the capability was unreachable.
