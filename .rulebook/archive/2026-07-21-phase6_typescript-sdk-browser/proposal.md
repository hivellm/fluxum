# Proposal: phase6_typescript-sdk-browser

## Why
The browser is the largest client platform; a zero-dependency, binary-protocol TS runtime with generated types is the primary developer-facing deliverable of the MVP.

## What Changes
Implement fluxum generate --lang typescript plus the browser-native JS/TS runtime: binary FluxRPC over Streamable HTTP (fetch ReadableStream, ArrayBuffer/DataView, no JSON hot path), plain-JS consumable (ESM/CJS + .d.ts, zero deps, max 50 KB min+gzip), Node TCP support, typed cache/events.

## Impact
- DAG task: T6.2
- Affected specs: SPEC-011 (SDK codegen)
- PRD requirements: FR-82
- Affected code: crates/fluxum-cli (generator), sdks/typescript
- Depends on: T6.1 (phase6_schema-export-api-freeze)
- Breaking change: NO
- User benefit: fully typed realtime client for browser and Node with no manual stubs and no heavy deps
