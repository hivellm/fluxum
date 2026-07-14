## 1. Implementation
- [ ] 1.1 Implement `POST /rpc` (:15800): binary FluxRPC frames in/out, `Content-Type: application/x-fluxum` required (415 otherwise), Fluxum-Session header issued on Authenticate (FR-42)
- [ ] 1.2 Implement `GET /rpc` binary push stream consumed via fetch ReadableStream; zero-length keep-alive frames at http_keepalive_s; no SSE, no base64, no JSON anywhere on the path
- [ ] 1.3 Session lifecycle: Fluxum-Session binding, expiry after idle_timeout_s (408 on open stream, 404 on stale POST), SDK reauth drill (SPEC-006 acceptance 8)
- [ ] 1.4 Transport equivalence: byte-identical frames drive the identical auth -> subscribe -> reducer -> TxUpdate session over TCP and /rpc (SPEC-006 acceptance 6)
- [ ] 1.5 Reverse-proxy compatibility: full flow unmodified through a standard HTTP reverse proxy (SPEC-006 acceptance 15)
- [ ] 1.6 Verification (DAG exit test): browser fetch-stream integration test in headless Chromium
- [ ] 1.7 Gate G5 input - wire format freezes here

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
