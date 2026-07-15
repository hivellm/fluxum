## 1. Implementation
- [ ] 1.1 Introduce a namespace registry: named databases each owning an independent MemStore + TxPipeline + schema, created/looked up by name; a default namespace preserves single-DB behavior (OPS-050; crates/fluxum-core/src/store, crates/fluxum-core/src/txn)
- [ ] 1.2 Namespace selection on connect: a session names its database at auth/connect and is bound to it for the connection lifetime (OPS-050; crates/fluxum-server/src/session.rs)
- [ ] 1.3 Route reducer calls, queries, and subscriptions through the connection's namespace so each sees only its own tables (OPS-050; server routing + crates/fluxum-server/src/session.rs)
- [ ] 1.4 Enforce strict isolation: reject any transaction or subscription that references another namespace with a typed error — no cross-namespace access (OPS-050; crates/fluxum-core/src/txn)
- [ ] 1.5 Per-namespace identity scope: identities/connection ids are scoped within a namespace, not global (OPS-050; crates/fluxum-server/src/session.rs)
- [ ] 1.6 Attribute metrics per namespace via a namespace label on fluxum_* series, and make checkpoints/backups per-namespace (OPS-051; crates/fluxum-server metrics, crates/fluxum-core/src/checkpoint)
- [ ] 1.7 Verification: with namespaces acme and globex, a client authenticated into acme sees only acme tables and is refused when subscribing to or mutating globex; metrics carry the namespace label

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
