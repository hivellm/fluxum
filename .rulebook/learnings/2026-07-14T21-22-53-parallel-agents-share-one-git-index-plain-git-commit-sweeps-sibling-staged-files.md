# Parallel agents share one git index — plain `git commit` sweeps sibling-staged files
**Source**: manual
**Date**: 2026-07-14
**Related Task**: phase2_commitlog
**Tags**: git, orchestration, parallel-agents, index
During T2.2/T2.10 parallel work in the same checkout, agent A staged files (including crafted index entries via `git hash-object` + `git update-index --cacheinfo`) while agent B ran a plain `git commit`. B's commit swallowed A's staged files AND A's crafted (older-base) versions of shared manifests, silently reverting B's own dependency lines — main went red and required two fix-forward commits.

Rules for concurrent same-checkout work:
1. Always commit with explicit pathspecs (`git commit -- <paths>`), never a bare `git commit`, so only your files land regardless of what else is in the index.
2. Do not leave crafted/partial index entries sitting in the shared index; stage and commit in one tight window.
3. After any race, verify HEAD with `git show --stat` before pushing — check that shared files (Cargo.toml, lib.rs, Cargo.lock) still contain BOTH parties' lines.
4. Fix forward: re-adding the current worktree version of a shared file restores everything, since the worktree accumulates all agents' edits.