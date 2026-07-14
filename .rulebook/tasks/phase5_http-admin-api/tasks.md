## 1. Implementation
- [ ] 1.1 Implement the HTTP/JSON admin surface on :15800 (axum), unversioned paths: `/health`, `/metrics`, `/schema`, `POST /reducer/:name`, `POST /query`, `GET /view/:name` (FR-44, FR-26 view half; RPC-050)
- [ ] 1.2 Response envelopes per RPC-051/RPC-052
- [ ] 1.3 `/health` responds in < 50 ms without taking storage locks, incl. under sustained write load; per-shard id/state/tx_id/queue_depth; degraded state returns 503 (FR-91, OBS-060/OBS-061)
- [ ] 1.4 Verification (DAG exit test): curl tests for all endpoints
- [ ] 1.5 Gate G5 input

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
