# Proposal: phase9_delta-compression

## Why

Game-netcode delta compression (MMO/FPS position sync — the technique
the user runs on their own MMO server) maps onto Fluxum in layers, and
the profitable layers are **already designed into the wire protocol and
dormant**:

- The subscription model is already row-level delta: `InitialData`
  once, then changed rows only — world state is never re-sent. What
  still travels fat is everything AROUND a changed row.
- **Envelope**: every `TxUpdate` carries ~70 B of per-update metadata —
  `caller` (32 B), `reducer_name` (e.g. `"move_player"`, 11 B),
  `duration_us`, shard, offset. `TxUpdateLight` (RPC-035: only
  `tx_id`, `timestamp`, `tables`) is implemented in `fluxum-protocol`
  (`messages.rs:353-360`) and **never emitted**; the
  `Authenticate.tx_updates: Option<String>` negotiation field
  (`messages.rs:142`) is ignored server-side.
- **Stream**: `Authenticate.compression` (`"none"|"gzip"|"brotli"`) is
  negotiated today and **no codec exists** anywhere in
  `fluxum-protocol`/`fluxum-server` (the only construction sites set
  `None`). Per-session compression with context carryover across
  frames IS the generic delta compressor: identities, names and other
  repeated bytes become ~2 B LZ window references.
- **Row**: an update travels as delete(PK) + **full row** — the MMO
  `Player` row re-sends identity (32 B) + name + hue on every move when
  only x/y changed. A changed-column bitmask can be computed **once per
  commit** (`TxDiff` holds old AND new rows) and shared by every
  subscriber, so the encode-once model (SUB-024) survives.

Rough wire math on the MMO-shape workload (`demo/mmo_bots.py`, ~150 B
per move today): light ≈ 80 B → + session compression ≈ 30–40 B → +
column delta ≈ 20 B. At 1k updates/s × 100 subscribers that is
~15 MB/s → ~2–4 MB/s.

**Rejected up front** (recorded so it is not re-litigated): per-client
acked-baseline deltas (Quake/Source style). Per-client encoding
collapses the fan-out cost model — the shared encode is what makes
Fluxum's fan-out ~19 µs/event. Quantization also stays app-level: the
module chooses its coordinate grid, the database does not degrade user
data.

## What Changes

Three layers, cheapest first; each composes with the previous and with
phase9_fanout-event-batching (batch × light × compression multiply).

1. **P3a — honor `tx_updates: "light"`**: sessions that negotiate it
   receive `TxUpdateLight` frames. Prerequisite decision (spec): the
   light form lacks `tx_offset` — either append it at the additive tail
   (RPC-011 rule) before enabling, or document that light sessions
   resume on `tx_id` (which the cursor currently mirrors). All five
   SDKs must decode the `TxUpdateLight` tag — audit first; a reader
   without that arm sees an unknown tag.
2. **P3b — per-session stream compression**: implement the negotiated
   `gzip`/`brotli` with per-connection context carryover, applied
   AFTER the shared encode (per-connection CPU is the price; the shared
   `Arc<Vec<u8>>` bytes stay shared). Config kill-switch, compression
   ratio + CPU metrics, and a latency guard: p50/p99 must hold on the
   parity harness.
3. **P4 — column-level update deltas** (spec-first): a changed-column
   bitmask + values wire form for updates, computed once per commit
   from `TxDiff` old+new and shared across subscribers; negotiated via
   the same `tx_updates` field. Inserts and rows APPEARING through a
   visibility change stay full-row; resume windows (CS-021) store the
   same form the session negotiated; the SDK caches apply deltas onto
   rows they are guaranteed to hold (SDK-045). Implementation only
   after the spec addition lands.

## Impact

- Affected specs: SPEC-006 (RPC-035 emission semantics + tx_offset
  tail decision; compression codec definition; P4 wire form addition),
  SPEC-011/021 (SDK decode + resume semantics per negotiated form),
  SPEC-012 (compression/delta metrics).
- Affected code: `crates/fluxum-server/src/lib.rs` (fan-out frame
  selection per session), `session.rs`/`http.rs`/`tcp.rs` (negotiation
  plumbing + codec), `crates/fluxum-protocol` (P4 form; possible
  `TxUpdateLight` tail field), `crates/fluxum-core/src/subscription/`
  (P4 bitmask computation, resume windows), all five SDKs (decode
  arms + conformance corpus scenarios).
- Breaking change: NO — every layer is negotiated opt-in; default
  behavior unchanged.
- Risk: MEDIUM for P3a/P3b (opt-in, kill-switch, guarded by the parity
  harness); HIGH for P4 without its spec (touches resume + visibility),
  which is why P4 is spec-gated.
- User benefit: wire bytes per update drop ~4–7× compounded for
  realtime-sync workloads (MMO/FPS position streams), on top of the
  packet-count wins from phase9_fanout-event-batching.

## Notes

Sequencing: run AFTER (or at least measured against) the batching task
— the burst bench and MMO-shape workload from
phase9_fanout-event-batching are the attribution rig for every layer
here; landing both at once makes the wins unattributable. Analysis
context: `docs/analysis/fanout-event-batching/` (F-001..F-020) maps the
push path this rides on.
