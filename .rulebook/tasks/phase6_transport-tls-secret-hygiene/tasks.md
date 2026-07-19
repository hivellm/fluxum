## 1. Implementation
- [ ] 1.1 Optional built-in TLS (`rustls`) on both listeners: `server.tls.{cert,key}`; when set, terminate TLS before the first handshake read on the TCP/RPC listener and the HTTP listener
- [ ] 1.2 Plaintext-on-public-bind guard: refuse to start an authenticating listener bound to a non-loopback address without TLS (analogous to the `none`-provider loopback guard), with an explicit `allow_plaintext` opt-out for trusted-network deploys; clear startup error naming the offending listener
- [ ] 1.3 `Secret<T>` newtype in `fluxum-core`: redacting `Debug` (`Secret(***)`) and `Serialize` (emits redaction or errors), zeroize-on-drop; expose (`expose_secret()`) only at the point of use
- [ ] 1.4 Wrap every secret config field in `Secret<T>`: `auth.secret`, `server_peers[].token`, `encryption.keys[].key_hex`, `transforms.keys[].secret`, sidecar `token`; audit all `serde`/`Debug` paths for the config tree (F-006)
- [ ] 1.5 Metrics/logs: TLS-enabled state surfaced in `/health` effective view (boolean only, no material); startup log records TLS on/off per listener
- [ ] 1.6 Spec: SPEC transport section — optional TLS + no-cleartext-credentials-on-public-bind requirement; config reference updated for `server.tls` and the `Secret` redaction contract
- [ ] 1.7 Verification: a non-loopback authenticating listener without TLS refuses to start unless `allow_plaintext`; a TLS handshake succeeds with a valid cert/key; a serialized/`Debug`-printed config never emits secret bytes; `Secret` zeroizes on drop

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
