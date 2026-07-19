# OWASP 2025 analysis: admin surface is unauthenticated (F-001, critical)

**Category**: security
**Tags**: analysis:owasp-security, security, owasp, access-control, admin-api

## Description

OWASP Top 10:2025 analysis lives at docs/analysis/owasp-security/ (README + 01..10, findings F-001..F-023). Headline F-001 (critical): the HTTP admin API is served on the same public http_port as /rpc with NO auth and NO loopback gate for almost every route — admin.rs:81-103 dispatch has no credential check; http.rs:411-428 handle_admin calls it directly; only POST /audit checks a server-peer token (admin.rs:894-902). POST /reducer/:name runs under admin_identity (arbitrary writes); POST /query uses Subscriber::server_peer(admin_identity) = RLS bypass (admin.rs:835) = arbitrary reads; drain/config-reload/plugins-disable = DoS/control-disable. tcp_host defaults 0.0.0.0 (boot.rs:150). Sharpened by the project's direct-port-exposure model + no TLS anywhere.

Other action-required gaps: no query LIMIT ceiling/timeout + no reducer time/mem bound = single-writer DoS (F-014/F-015); no cargo-deny/audit/SBOM in CI (F-009); no durable security-event trail, auth failures & RLS denials log at debug/invisible at default info (F-022); no transport TLS, tokens cleartext (F-011). Strong already: at-rest/field crypto (XChaCha20-Poly1305/ECIES, zeroized keys), closed typed SQL grammar = no injection (F-013), reducer panic isolation, pre-auth connguard. RLS partial: shard_local/custom/member_of impose no filter (sql/mod.rs:743-765, F-003). Plan phases A(admin authz)+B(TLS/secrets) are P0 and must precede any public-exposure milestone.
