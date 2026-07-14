# Proposal: phase6_rust-sdk

## Why
Rust services and tools need a native client; sharing fluxum-protocol guarantees the SDK can never drift from the server codec.

## What Changes
Implement the Rust client SDK (fluxum-sdk) sharing the fluxum-protocol crate: connection, auth, reducer calls, subscriptions with typed local cache.

## Impact
- DAG task: T6.4
- Affected specs: SPEC-011 (SDK codegen)
- PRD requirements: FR-84
- Affected code: sdks/rust (fluxum-sdk), reuses crates/fluxum-protocol
- Depends on: G5
- Breaking change: NO
- User benefit: first-class typed Rust client with zero codec duplication
