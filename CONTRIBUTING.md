# Contributing to Fluxum

Thank you for your interest in contributing! Fluxum follows the HiveLLM family conventions.

## Getting started

```bash
git clone https://github.com/hivellm/fluxum.git
cd fluxum
rustup toolchain install nightly          # pinned by rust-toolchain.toml
cargo build --workspace
cargo nextest run --workspace             # or: cargo test --workspace
```

Requirements: Rust nightly (edition 2024, pinned by `rust-toolchain.toml`),
[cargo-nextest](https://nexte.st/) for running tests.

## Spec-driven development

Fluxum is built spec-first. The traceability chain is:

```
PRD (FR-xx / NFR-xx) → DAG task (T<phase>.<n>) → SPEC requirement (STG-xxx, RED-xxx, …) → tests
```

- Before implementing, read the governing spec in [`docs/specs/`](docs/specs/README.md) and the
  [DAG](docs/DAG.md) task you are executing.
- A change to product behavior lands as: PRD update (if requirements change) + SPEC update +
  DAG update (if work items change) — **in the same PR**.
- Commits, issues, and PRs should reference the task ID (`T2.3`) and/or requirement IDs
  (`STG-012`) they implement.
- Requirement IDs are stable; changing one's meaning requires the same review bar as the
  behavior change itself.

## Quality gates (every PR)

| Gate | Command |
|------|---------|
| Formatting | `cargo +nightly fmt --all -- --check` |
| Lints (zero warnings) | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| Tests | `cargo nextest run --workspace` (Linux/macOS/Windows in CI) |
| Docs build | `cargo doc --workspace --no-deps` |

Additional policies (enforced via workspace lints):

- No `unwrap()` / `expect()` in production code (`clippy::unwrap_used`/`expect_used` = deny);
  tests may opt out with `#[allow]`.
- Every `unsafe` block carries a `// SAFETY:` comment (`undocumented_unsafe_blocks` = deny).
- Library crates use `thiserror` (`FluxumError`); binaries and tests may use `anyhow`.
- New behavior ships with tests; crash/durability code ships with fault-injection tests
  (see [SPEC-013](docs/specs/SPEC-013-testing-conformance.md)).

## Commit convention

[Conventional Commits](https://www.conventionalcommits.org/), in English:

```
<type>(<scope>): <description>

feat(subscriptions): deliver TxUpdate diffs for spatial plans
fix(commitlog): truncate at first CRC mismatch during replay (STG-014)
docs(specs): clarify ORDER BY semantics in SPEC-005
```

Types: `feat` · `fix` · `docs` · `refactor` · `perf` · `test` · `build` · `ci` · `chore`.

## Pull request process

1. Branch from `main` (never commit to `main` directly).
2. Keep PRs scoped to one DAG task or one coherent change.
3. Update `CHANGELOG.md` under `[Unreleased]` (Keep a Changelog format).
4. Ensure all quality gates pass locally before opening the PR.
5. PR description: what changed, why, which task/requirement IDs, and how it was tested.

## Documentation rules

- All documentation in English.
- Repository root contains only: `README.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`,
  `LICENSE` (plus tooling dotfiles). Everything else lives under `docs/`.
- Wire-format changes (FluxRPC framing, FluxBIN, commit-log entries) after the G5 freeze require
  a protocol-version bump proposal — see [change control](docs/specs/README.md#change-control).

## Questions

Open a GitHub issue or reach the team at team@hivellm.org.
