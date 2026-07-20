## 1. Implementation
- [x] 1.1 Optional built-in TLS (`rustls` via `tokio-rustls`, ring provider) on both listeners: `server.tls.{cert,key}`. A single `MaybeTls` stream type (plain or TLS) lets the read/route/write loops stay TLS-agnostic; the handshake runs in the per-connection accept task, before the first frame/request. `tls.rs::load_acceptor` parses PEM via the maintained `rustls-pki-types` `PemObject` (no unmaintained `rustls-pemfile`)
- [x] 1.2 Plaintext-on-public-bind guard: `Config::validate` refuses a `token`/`jwt` listener on a non-loopback `tcp_host` without TLS unless `server.allow_plaintext` is set; error names the bind and the remedies. `cert` xor `key` is a load error. (The `none` provider is already loopback-guarded by AUTH-040.) TLS terminates only the *direct* accept path; a trusted-proxy hop stays plaintext (the proxy did TLS)
- [x] 1.3 `Secret<T>` newtype (`fluxum_core::secret`): `Debug`/`Serialize` redact to `[redacted]`, zeroize-on-drop, plaintext only via `expose_secret()`/`expose_str()`. `From<T>`/`From<&str>` for ergonomic construction
- [x] 1.4 Wrapped every secret config field in `Secret<T>`: `auth.secret`, `auth.server_peers[].token`, `encryption.keys[].key_hex`, `transforms.keys[].secret` + `.previous`, and the sidecar `PluginHost::Sidecar.token` / `SidecarConfig.token`; all read sites now go through `expose_*` (exposed only at the point of use — the HMAC key, the peer-token digest, the ECIES/sign key material, the sidecar handshake)
- [x] 1.5 `GET /health` gains `"tls": bool` (posture only, never material); boot logs TLS on/off per listener once. `ShardContext::set_tls_enabled`/`tls_enabled`
- [x] 1.6 Spec: SPEC-026 SEC-058 (Secret hygiene) + SEC-059 (transport TLS + no-cleartext-on-public-bind) with config blocks; the `Secret` redaction contract and `server.tls`/`allow_plaintext` documented
- [x] 1.7 Verification (`transport_tls.rs`): FluxRPC/TCP authenticates over a real TLS handshake (self-signed fixture cert, rustls client); a plaintext client gets nothing from the TLS listener; the public-authenticating-bind-without-TLS config is refused at load (and `allow_plaintext`/loopback are accepted); a serialized and a `Debug`-printed config never emit secret bytes. `Secret` zeroize/redaction unit-tested in `secret.rs`

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
