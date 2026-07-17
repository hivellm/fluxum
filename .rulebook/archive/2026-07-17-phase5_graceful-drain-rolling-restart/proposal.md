# Proposal: phase5_graceful-drain-rolling-restart

## Why
Both transports already carry a shutdown handle: TcpTransport and the HTTP transport hold an `Arc<Notify>` and expose `shutdown()` (crates/fluxum-server/src/tcp.rs:64, crates/fluxum-server/src/http.rs:82), and the accept loop breaks on `shutdown.notified()` (http.rs:144). But `shutdown()` is an immediate stop — it does not stop accepting while letting in-flight transactions finish, it does not force a final checkpoint before exit, and there is no signal (SIGTERM) or `fluxum drain` command wired to it. So every deploy = a hard binary restart that can drop in-flight reducer calls and leaves the last transactions to crash-recovery replay instead of a clean checkpoint. SPEC-025 OPS-030/031 specify a bounded, clean drain so a rolling restart costs clients only a brief reconnect.

## What Changes
Add a graceful drain path: a `fluxum drain` command and SIGTERM handling that (1) flips the accept loops to refuse new connections/subscriptions with a retryable signal, (2) lets in-flight transactions commit, (3) triggers a final checkpoint (crates/fluxum-core/src/checkpoint) so restart replays little or nothing, and (4) exits within a bounded deadline (force-close stragglers past the deadline). New reducer calls arriving mid-drain are refused with a retryable code so SDK reconnect/resubscribe (SPEC-021 CS-02x) transparently retries them against the restarted process, making the restart invisible beyond a brief reconnect.

## Impact
- Governing spec: SPEC-025 §4 Graceful drain & rolling restart (OPS-030, OPS-031) — docs/specs/SPEC-025-operations-multitenancy.md
- Related specs: SPEC-006 (transports/session), SPEC-013 (checkpoints), SPEC-021 (SDK reconnect/resubscribe CS-02x)
- New PRD requirements: FR-141 (graceful drain)
- Requirements covered: OPS-030, OPS-031
- Affected code: crates/fluxum-server/src/lib.rs (drain orchestration), crates/fluxum-server/src/tcp.rs + http.rs (accept loop refuse-new + bounded deadline over the existing Arc<Notify>), crates/fluxum-server/src/main.rs (SIGTERM), crates/fluxum-core/src/checkpoint (final checkpoint), crates/fluxum-cli/src (drain subcommand)
- Depends on: phase5 transports (archived)
- Breaking change: NO (new signal/command; existing shutdown() behavior preserved as the force path)
- User benefit: zero-downtime rolling deploys — in-flight writes commit, no crash-recovery replay on restart, clients see only a brief reconnect
