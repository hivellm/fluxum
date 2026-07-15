## 1. Implementation
- [ ] 1.1 ColumnGrant resolution: public | owner (vs #[visibility(owner_only)] field) | "role" (ctx.roles) | server_peer; server peers bypass all grants (CT-040)
- [ ] 1.2 MaskStrategy on the read path: null (project nullable) | redact (zero/empty) | ciphertext (envelope, encrypted cols only) | hash (SHA-256 pseudonym) (CT-041)
- [ ] 1.3 Feed the per-column authorized flag into the phase3 on_read hook so decryption happens only when granted; unauthorized never receives plaintext (CT-031, CT-012)
- [ ] 1.4 Apply masking uniformly across InitialData, TxUpdate diffs, one-off query, and HTTP /query reads (CT-041)
- [ ] 1.5 Compose with row-level #[visibility]: masked-column changes still fan out a TxUpdate to authorized subscribers and leak nothing (presence/ordering) to unauthorized ones (CT-042)
- [ ] 1.6 /schema JSON + fluxum schema export emit logical type, stored type, transform descriptors, grant, mask (key names only); schema hash incorporates transforms (CT-052)
- [ ] 1.7 Migration interaction: __schema_meta__ records transform descriptor set; binary started against data written under a different transform set aborts with a descriptive error (CT-060)
- [ ] 1.8 Verification: two clients each see raw only for granted columns and masked otherwise in InitialData + diffs; server-peer sees all raw; PostgreSQL parity scenario (pgcrypto + column GRANT) produces equivalent authorized/unauthorized results

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
