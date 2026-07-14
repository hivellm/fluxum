# Proposal: phase7_csharp-sdk

## Why
C#/.NET covers enterprise backends and the Unity ecosystem; an async/await NuGet SDK is the last of the five launch SDKs.

## What Changes
Implement the C# SDK (async/await, distributed via NuGet) over FluxRPC with generated typed bindings, validated by the shared conformance corpus.

## Impact
- DAG task: T7.6
- Affected specs: SPEC-011 (SDK codegen)
- PRD requirements: FR-86
- Affected code: sdks/csharp, crates/fluxum-cli (generate --lang csharp)
- Depends on: G6
- Breaking change: NO
- User benefit: typed async realtime client for .NET applications via NuGet
