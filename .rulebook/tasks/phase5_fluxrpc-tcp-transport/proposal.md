# Proposal: phase5_fluxrpc-tcp-transport

## Why
TCP is the primary low-latency transport for native SDKs; the session state machine and message routing built here are reused by every other transport.

## What Changes
Implement the FluxRPC TCP transport (:15801): frame parser, session state machine, routing for Authenticate/ReducerCall/Subscribe/SubscribeSingle/Unsubscribe/OneOffQuery, idle timeout, and max frame size.

## Impact
- DAG task: T5.1
- Affected specs: SPEC-006 (FluxRPC protocol)
- PRD requirements: FR-40, FR-42, FR-45
- Affected code: crates/fluxum-server (transport/tcp)
- Depends on: G4
- Breaking change: NO
- User benefit: native clients connect over a compact binary protocol with well-defined session semantics
