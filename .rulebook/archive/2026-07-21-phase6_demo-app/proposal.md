# Proposal: phase6_demo-app

## Why
A real application on the generated SDK is the only end-to-end proof that schema, reducers, subscriptions, transport, and codegen compose; it doubles as the canonical example.

## What Changes
Build the demo app (chat + presence + per-user tasks) running end-to-end on the generated TypeScript SDK, with the demo scenario scripted in CI.

## Impact
- DAG task: T6.5
- Affected specs: SPEC-013 (testing and conformance)
- PRD requirements: FR-82
- Affected code: examples/demo-app (module + web client)
- Depends on: T6.2 (phase6_typescript-sdk-browser)
- Breaking change: NO
- User benefit: a working reference application showing the full developer workflow
