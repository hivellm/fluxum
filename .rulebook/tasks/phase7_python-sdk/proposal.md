# Proposal: phase7_python-sdk

## Why
Python is the default language for data/ML and scripting consumers; an asyncio-first SDK opens Fluxum to that ecosystem for the 0.2.0 launch.

## What Changes
Implement the Python SDK (asyncio-first) over FluxRPC with generated typed bindings, validated by the shared conformance corpus.

## Impact
- DAG task: T7.4
- Affected specs: SPEC-011 (SDK codegen)
- PRD requirements: FR-83
- Affected code: sdks/python, crates/fluxum-cli (generate --lang python)
- Depends on: G6
- Breaking change: NO
- User benefit: typed async realtime client for Python services and notebooks
