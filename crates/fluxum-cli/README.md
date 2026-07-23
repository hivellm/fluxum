# fluxum — the command-line tool

The developer inner loop (SPEC-024 DEV-010/011/012, PRD FR-135) plus the
schema/bindings surface. Dependency-light by design: hand-rolled argument
parsing and HTTP over `std::net`, `serde_json` as the only runtime
dependency — the tool ships as one small binary.

## The inner loop

```sh
# 1. Scaffold a runnable application (a Fluxum app IS a crate):
fluxum init my-notes --fluxum-path /path/to/fluxum-checkout
cd my-notes

# 2. Edit-save-see (DEV-010):
fluxum dev
```

`fluxum dev` watches `Cargo.toml`, `config.yml` and `src/**` (mtime poll,
debounced — one rebuild per save burst), rebuilds with cargo, restarts the
server **over the same data dir** — recovery replays the checkpoint +
commit log, so your data survives the edit — regenerates SDK bindings from
the fresh `/schema` (`--bindings <dir> --lang rust|typescript`), and streams
the merged module + system logs (the child server inherits the console).

**A failed build never kills your server**: cargo's errors print, the
previous server keeps running, and the next save retries. A server that
dies at boot is reported and the loop keeps watching.

## Logs (DEV-012)

```sh
fluxum logs --server 127.0.0.1:15800          # the recent ring, then exit
fluxum logs --server 127.0.0.1:15800 -f       # follow live
fluxum logs --server 127.0.0.1:15800 -f --level warn --format pretty
```

Backed by `GET /logs[?follow=1]` — NDJSON over chunked HTTP on the admin
port, fed by an in-process tap on the tracing subscriber (a 256-line
catch-up ring + a live broadcast; a lagging follower gets an honest
`{"fluxum_logs_dropped":n}` marker, never a silent gap). The endpoint rides
the SEC-054 admin guard: loopback is free, a remote needs `server.admin`
trust and (when configured) an operator credential. Lines are always JSON —
`--format pretty` is client-side rendering, `--level` narrows client-side
below the server's own configured level (OBS-082).

## Schema and bindings

```sh
# The module contract, canonical bytes (an API-freeze gate when committed):
fluxum schema export --server 127.0.0.1:15800 --out schema.json

# Typed client bindings from a server or a saved schema.json:
fluxum generate --lang typescript --schema 127.0.0.1:15800 --out bindings/
```

## Templates

`fluxum init --template notes` (the default and, today, the only one): a
`Note` table + `add_note`/`delete_note` reducers with an ownership check,
`config.yml` on the documented dev ports (15800/15801), and a README with
the one-command boot. Until the fluxum crates are published, dependencies
point at a checkout via `--fluxum-path`.
