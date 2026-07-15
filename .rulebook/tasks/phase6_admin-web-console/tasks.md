## 1. Implementation

- [ ] 1.1 Serve self-contained console static assets from the HTTP admin port (DEV-030; crates/fluxum-server/src/admin.rs)
- [ ] 1.2 Build the table browser UI backed by the existing `/schema` and `/query` endpoints (DEV-030; crates/fluxum-server/src/admin.rs)
- [ ] 1.3 Wire a read-only query panel to `/query` that rejects mutating statements (DEV-030; crates/fluxum-server/src/admin.rs)
- [ ] 1.4 Add a live subscription viewer streaming diffs over ShardContext::subscribe_commits (DEV-030; crates/fluxum-server/src/admin.rs, crates/fluxum-server/src/lib.rs)
- [ ] 1.5 Surface `/metrics` and `/schema` in the console views (DEV-030; crates/fluxum-server/src/admin.rs)
- [ ] 1.6 Enforce auth — no anonymous access outside the `development` profile (DEV-031; crates/fluxum-server/src/admin.rs)
- [ ] 1.7 Guarantee the console takes no storage locks that violate the `/health` latency budget (DEV-031; crates/fluxum-server/src/admin.rs)
- [ ] 1.8 Display reducer invocation logs and slow-reducer warnings (DEV-032; crates/fluxum-server/src/admin.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
