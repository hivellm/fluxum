# Proposal: phase0_core-types-config-hardware-probe

## Why
Every crate downstream needs the shared error type, identity newtypes, and a config loader before any feature work can start; the boot-time hardware probe seeds adaptive defaults from day one.

## What Changes
Implement `FluxumError` (thiserror), `Identity`/`ConnectionId`/`EntityId`/`Timestamp` newtypes, the YAML config loader with `FLUXUM_` env overrides, and the boot-time hardware probe (cores/RAM/cgroup limits) that feeds adaptive config defaults.

## Impact
- DAG task: T0.2
- Affected specs: SPEC-001 (data model), SPEC-009 (authentication), SPEC-016 (hardware adaptivity)
- PRD requirements: FR-04
- Affected code: crates/fluxum-core
- Depends on: T0.1 (workspace + CI skeleton)
- Breaking change: NO
- User benefit: consistent errors, stable identity types, and one config surface (file + env) across the whole server
