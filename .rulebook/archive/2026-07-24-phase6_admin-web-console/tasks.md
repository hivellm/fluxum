## 1. Implementation

- [x] 1.1 Serve self-contained console static assets from the HTTP admin port (DEV-030; crates/fluxum-server/src/console.rs + console.html embedded via include_str, served at GET /console with a no-external-origin CSP)
- [x] 1.2 Build the table browser UI backed by the existing `/schema` and `/query` endpoints (DEV-030; console.html Tables tab)
- [x] 1.3 Wire a read-only query panel to `/query` that rejects mutating statements (DEV-030; console.html Query tab; server-side rejection pinned by tests/console.rs::the_query_surface_rejects_mutating_statements)
- [x] 1.4 Add a live subscription viewer streaming diffs over ShardContext::subscribe_commits (DEV-030; GET /console/watch NDJSON stream with ?table= filter, crates/fluxum-server/src/http.rs::handle_console_watch + console::render_commit)
- [x] 1.5 Surface `/metrics` and `/schema` in the console views (DEV-030; console.html Metrics + Schema tabs)
- [x] 1.6 Enforce auth — no anonymous access outside the `development` profile (DEV-031; AdminPolicy.console_open from the profile, admin::check_console_access requires a server-peer operator token even from loopback outside development; SEC-054 network guard on every console route)
- [x] 1.7 Guarantee the console takes no storage locks that violate the `/health` latency budget (DEV-031; the watch path reads only the commit broadcast + static table catalog; pinned by tests/console.rs::health_answers_while_a_watch_stream_is_open_and_commits_flow)
- [x] 1.8 Display reducer invocation logs and slow-reducer warnings (DEV-032; console.html Logs tab rides GET /logs?follow=1 with a reducer-only filter and slow_reducer highlighting)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation (SPEC-024 status row; admin.rs route table; console.rs module docs)
- [x] 2.2 Write tests covering the new behavior (tests/console.rs: 6 integration tests; console.rs unit tests: routing + self-containment pin)
- [x] 2.3 Run tests and confirm they pass (cargo test -p fluxum-server --all-features: console suite 6/6 + lib 2/2 green)
