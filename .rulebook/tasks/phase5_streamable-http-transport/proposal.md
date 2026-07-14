# Proposal: phase5_streamable-http-transport

## Why
Browsers cannot open raw TCP; a binary streamable HTTP transport gives web clients the same FluxRPC message layer with no protocol translation.

## What Changes
Implement the Streamable HTTP transport (:15800 /rpc): binary POST frames + GET push stream (fetch ReadableStream), Fluxum-Session binding, keep-alive, same message layer as TCP.

## Impact
- DAG task: T5.2
- Affected specs: SPEC-006 (FluxRPC protocol)
- PRD requirements: FR-42
- Affected code: crates/fluxum-server (transport/http)
- Depends on: T5.1 (phase5_fluxrpc-tcp-transport)
- Breaking change: NO
- User benefit: browser clients get first-class binary realtime access via plain fetch
