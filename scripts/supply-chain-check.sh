#!/usr/bin/env bash
# Supply-chain gate (SPEC-026 SEC-057; OWASP A03 F-009/F-010).
#
# Runs `cargo deny check` — advisories, bans, licenses, sources — as part of
# the local quality gate (this project does not run GitHub Actions; see
# CONTRIBUTING.md). Fails on a known RUSTSEC advisory, a disallowed license, a
# banned/duplicate/yanked crate, or a dependency from an unexpected source.
#
# Windows note: cargo-deny fetches the RustSec advisory database as a git
# checkout. Under git's default `core.autocrlf=true` on Windows the advisory
# `.md` files get CRLF line endings, which breaks cargo-deny's TOML-frontmatter
# parser ("failed to find toml block"). This script pins the advisory-db to a
# LF checkout it manages, then runs cargo-deny `--offline` against it, so the
# gate is deterministic on every platform.
set -euo pipefail

ADVISORY_DB_URL="https://github.com/rustsec/advisory-db"
# cargo-deny derives this directory name from a hash of the URL; it is stable.
DB_DIR="${CARGO_HOME:-$HOME/.cargo}/advisory-dbs/advisory-db-3157b0e258782691"

ensure_advisory_db() {
    if [ ! -d "$DB_DIR/.git" ]; then
        echo "supply-chain: cloning advisory-db (LF checkout) ..."
        rm -rf "$DB_DIR"
        git clone --quiet --depth 1 \
            -c core.autocrlf=false -c core.eol=lf \
            "$ADVISORY_DB_URL" "$DB_DIR"
        # Persist LF in the clone so later `git pull` keeps it.
        git -C "$DB_DIR" config core.autocrlf false
        git -C "$DB_DIR" config core.eol lf
    else
        # Refresh; the repo config (set above) keeps the checkout LF.
        git -C "$DB_DIR" config core.autocrlf false
        git -C "$DB_DIR" config core.eol lf
        git -C "$DB_DIR" pull --quiet --ff-only || true
        # Re-normalize in case a prior CRLF checkout is cached.
        git -C "$DB_DIR" checkout --quiet -- . 2>/dev/null || true
    fi
}

ensure_advisory_db
echo "supply-chain: cargo deny check ..."
exec cargo deny --offline check
