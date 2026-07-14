# Proposal: phase1_fluxbin-codec-fluxrpc-types

## Why
FluxBIN is the row encoding persisted to disk and sent on the wire, and FluxRPC message types are shared by server and every SDK — both must exist before storage or transport work starts.

## What Changes
Implement the `FluxValue` enum, the FluxBIN row codec covering all primitive and product/sum types, the FluxRPC message type set, and the `u32 LE + MessagePack` frame codec.

## Impact
- DAG task: T1.2
- Affected specs: SPEC-006 (FluxRPC protocol)
- PRD requirements: FR-40, FR-41
- Affected code: crates/fluxum-protocol
- Depends on: G0
- Breaking change: NO (format freezes later, at G5)
- User benefit: one compact binary encoding shared by storage, wire, and all SDKs
