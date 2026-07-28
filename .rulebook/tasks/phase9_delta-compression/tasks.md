## 1. Implementation

Layered opt-in delta compression for the realtime push path; every
layer is negotiated, default behavior unchanged. Sequenced after
phase9_fanout-event-batching so its burst bench + MMO-shape workload
attribute each layer's win separately (batch × light × compression
compose).

- [ ] 1.1 SPEC-006 prep for `tx_updates: "light"` (RPC-035): decide the
      resume-cursor story — append `tx_offset` at the light form's
      additive tail (RPC-011 rule) or document light sessions resuming
      on `tx_id` (the cursor currently mirrors it, `messages.rs:341-347`
      keeps the divergence door open). Audit all five SDK readers for a
      `TxUpdateLight` decode arm; add where missing (protocol-only
      change, no behavior until negotiated).
- [ ] 1.2 P3a server: honor the `Authenticate.tx_updates` negotiation —
      sessions that asked for `light` get `TxUpdateLight` frames from
      the fan-out (`lib.rs` frame build), everyone else is untouched.
      Session flag surfaced in `GET /sessions`; conformance corpus
      scenario: a light session and a full session subscribed to the
      same query receive equivalent row diffs.
- [ ] 1.3 P3b codec: implement negotiated per-session compression
      (`gzip` baseline; `brotli` if the dependency budget allows) with
      context carryover across frames, applied per connection AFTER the
      shared encode — the `Arc`'d encoded bytes stay shared (SUB-024).
      Config kill-switch (`server.compression.enabled`), metrics
      (ratio, compressed bytes, CPU time), and keep-alive frames exempt.
- [ ] 1.4 P3b guardrails: parity-harness p50/p99 must hold with
      compression on; CPU-per-connection measured at the MMO-shape
      workload (99 bots × 10 Hz × N subscribers); document the
      recommended posture (on for WAN clients, off for loopback).
- [ ] 1.5 P4 spec ONLY: the column-delta wire form — changed-column
      bitmask + values for updates, computed once per commit from
      `TxDiff` old+new rows and shared across subscribers; full-row
      rules (inserts; rows appearing via visibility change); resume
      windows store the negotiated form (CS-021); SDK cache apply
      contract (SDK-045). Implementation goes to its own task once the
      spec addition is approved — HIGH risk unspecced, it touches
      resume and visibility.
- [ ] 1.6 Before/after wire-bytes measurement per layer on the MMO
      workload against the ~150 B/move baseline (target: light ≈ 80 B,
      + compression ≈ 30–40 B), recorded beside the
      phase9_fanout-event-batching numbers so the compounded win is
      attributable.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
      (SPEC-006 RPC-035 emission + codec; SPEC-011/021 SDK + resume
      notes; docs/CONSOLE.md sessions-view flag)
- [ ] 2.2 Write tests covering the new behavior (negotiation matrix,
      light/full equivalence, compression round-trip incl. context
      carryover across frames, kill-switch, SDK conformance scenarios)
- [ ] 2.3 Run tests and confirm they pass (workspace + 5-SDK corpus +
      coverage floor)
