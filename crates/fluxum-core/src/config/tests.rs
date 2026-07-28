use super::*;
use std::io::Write as _;

fn no_env(_: &str) -> Option<String> {
    None
}

fn env_of<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |key| {
        pairs
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| (*v).to_owned())
    }
}

fn write_config(yaml: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    file
}

#[test]
fn defaults_require_auth_secret() {
    // Built-in default provider is `token`; without a secret the loader
    // must fail with a typed error naming the key.
    let err = Config::load_with(None, &no_env).unwrap_err();
    assert!(err.to_string().contains("auth.secret"), "{err}");
}

#[test]
fn development_profile_flips_dev_defaults() {
    let cfg = Config::load_with(None, &env_of(&[("FLUXUM_PROFILE", "development")])).unwrap();
    assert_eq!(cfg.profile, Profile::Development);
    assert_eq!(cfg.sharding.shards, AutoOr::Value(1));
    assert_eq!(cfg.auth.provider, AuthProvider::None);
    assert_eq!(cfg.logging.format, LogFormat::Pretty);
    // Untouched keys keep their built-in defaults.
    assert_eq!(cfg.server.http_port, 15800);
    assert_eq!(cfg.server.tcp_port, 15801);
    assert!(cfg.memory.budget.is_auto());
}

#[test]
fn file_beats_profile_defaults() {
    let file = write_config("profile: development\nlogging:\n  format: json\n");
    let cfg = Config::load_with(Some(file.path()), &no_env).unwrap();
    assert_eq!(cfg.profile, Profile::Development);
    assert_eq!(cfg.logging.format, LogFormat::Json);
    assert_eq!(cfg.source_of("logging.format"), ValueSource::File);
    assert_eq!(cfg.source_of("auth.provider"), ValueSource::Profile);
}

#[test]
fn env_beats_file_beats_default() {
    let file =
        write_config("server:\n  tcp_port: 16000\n  http_port: 16001\nauth:\n  provider: none\n");
    let env = env_of(&[("FLUXUM_SERVER_TCP_PORT", "17000")]);
    let cfg = Config::load_with(Some(file.path()), &env).unwrap();
    assert_eq!(cfg.server.tcp_port, 17000, "env wins over file");
    assert_eq!(cfg.server.http_port, 16001, "file wins over default");
    assert_eq!(cfg.server.tcp_host, "127.0.0.1", "default preserved");
    assert_eq!(cfg.source_of("server.tcp_port"), ValueSource::Env);
    assert_eq!(cfg.source_of("server.http_port"), ValueSource::File);
    assert_eq!(cfg.source_of("server.tcp_host"), ValueSource::Default);
}

#[test]
fn nested_env_override_maps_underscored_keys() {
    let env = env_of(&[
        ("FLUXUM_PROFILE", "development"),
        ("FLUXUM_OBSERVABILITY_SLOW_REDUCER_THRESHOLD_US", "250"),
        ("FLUXUM_STORAGE_CHECKPOINT_INTERVAL_TX", "500"),
    ]);
    let cfg = Config::load_with(None, &env).unwrap();
    assert_eq!(cfg.observability.slow_reducer_threshold_us, 250);
    assert_eq!(cfg.storage.checkpoint_interval_tx, 500);
}

#[test]
fn memory_budget_parses_human_sizes() {
    let file = write_config("memory:\n  budget: 512MiB\nauth:\n  provider: none\n");
    let cfg = Config::load_with(Some(file.path()), &no_env).unwrap();
    assert_eq!(cfg.memory.budget, AutoOr::Value(ByteSize(512 << 20)));

    // Env override with a "2GiB"-style string wins over the file.
    let env = env_of(&[("FLUXUM_MEMORY_BUDGET", "2GiB")]);
    let cfg = Config::load_with(Some(file.path()), &env).unwrap();
    assert_eq!(cfg.memory.budget, AutoOr::Value(ByteSize(2 << 30)));
    assert_eq!(cfg.source_of("memory.budget"), ValueSource::Env);

    // And "auto" restores derivation.
    let env = env_of(&[("FLUXUM_MEMORY_BUDGET", "auto")]);
    let cfg = Config::load_with(Some(file.path()), &env).unwrap();
    assert!(cfg.memory.budget.is_auto());
}

#[test]
fn explicit_budget_below_floor_is_rejected() {
    let file = write_config("memory:\n  budget: 64MiB\nauth:\n  provider: none\n");
    let err = Config::load_with(Some(file.path()), &no_env).unwrap_err();
    assert!(err.to_string().contains("memory.budget"), "{err}");
}

#[test]
fn invalid_values_yield_typed_config_errors() {
    let bad_fraction = write_config("memory:\n  auto_fraction: 1.5\nauth:\n  provider: none\n");
    let err = Config::load_with(Some(bad_fraction.path()), &no_env).unwrap_err();
    assert!(matches!(err, FluxumError::Config(_)));
    assert!(err.to_string().contains("memory.auto_fraction"), "{err}");

    let same_ports =
        write_config("server:\n  http_port: 15900\n  tcp_port: 15900\nauth:\n  provider: none\n");
    let err = Config::load_with(Some(same_ports.path()), &no_env).unwrap_err();
    assert!(err.to_string().contains("server.http_port"), "{err}");

    let zero_shards = write_config("sharding:\n  shards: 0\nauth:\n  provider: none\n");
    let err = Config::load_with(Some(zero_shards.path()), &no_env).unwrap_err();
    assert!(err.to_string().contains("sharding.shards"), "{err}");

    // REP-021: an explicit quorum count must be satisfiable by the
    // configured set — 3 of a 2-member set (1 peer + this node) is not.
    let bad_quorum = write_config(
        "replication:\n  peers: [\"h:1\"]\n  member_name: a\n  peer_token: t\n\
             \x20 semi_sync:\n    quorum: \"3\"\nauth:\n  provider: none\n",
    );
    let err = Config::load_with(Some(bad_quorum.path()), &no_env).unwrap_err();
    assert!(
        err.to_string().contains("replication.semi_sync.quorum"),
        "{err}"
    );
    let zero_quorum =
        write_config("replication:\n  semi_sync:\n    quorum: \"0\"\nauth:\n  provider: none\n");
    let err = Config::load_with(Some(zero_quorum.path()), &no_env).unwrap_err();
    assert!(err.to_string().contains("outside 1..="), "{err}");
}

#[test]
fn unknown_keys_are_rejected() {
    let file = write_config("server:\n  tcp_prot: 1\nauth:\n  provider: none\n");
    let err = Config::load_with(Some(file.path()), &no_env).unwrap_err();
    assert!(matches!(err, FluxumError::ConfigParse(_)), "{err}");
}

#[test]
fn unknown_profile_is_rejected() {
    let err = Config::load_with(None, &env_of(&[("FLUXUM_PROFILE", "staging")])).unwrap_err();
    assert!(err.to_string().contains("staging"), "{err}");
}

#[test]
fn dollar_brace_secret_expands_from_env() {
    let file = write_config("auth:\n  provider: token\n  secret: ${MY_APP_SECRET}\n");
    let env = env_of(&[("MY_APP_SECRET", "s3cret")]);
    let cfg = Config::load_with(Some(file.path()), &env).unwrap();
    assert_eq!(
        cfg.auth.secret.as_ref().map(|s| s.expose_str()),
        Some("s3cret")
    );

    // Unset variable → empty secret → typed validation error.
    let err = Config::load_with(Some(file.path()), &no_env).unwrap_err();
    assert!(err.to_string().contains("auth.secret"), "{err}");
}

#[test]
fn load_reads_the_real_environment_and_fails_on_a_missing_file() {
    // The env-backed entry point: a nonexistent file is a typed Config
    // error naming the path, regardless of the process environment.
    let err = Config::load(Some(std::path::Path::new(
        "definitely/not/a/fluxum-config.yml",
    )))
    .unwrap_err();
    assert!(matches!(err, FluxumError::Config(_)), "{err:?}");
    assert!(err.to_string().contains("cannot read config file"), "{err}");
}

#[test]
fn every_semantic_validation_names_its_key() {
    let cases: &[(&str, &str)] = &[
        ("server:\n  http_port: 0\n", "server.http_port"),
        ("server:\n  tcp_port: 0\n", "server.tcp_port"),
        ("runtime:\n  worker_threads: 0\n", "runtime.worker_threads"),
        (
            "memory:\n  bufferpool_fraction: 1.5\n",
            "memory.bufferpool_fraction",
        ),
        (
            "storage:\n  checkpoint_interval_tx: 0\n",
            "storage.checkpoint_interval_tx",
        ),
        ("storage:\n  page_size: 1234\n", "storage.page_size"),
        (
            "storage:\n  evictor_low_watermark: 0.99\n",
            "evictor_low_watermark",
        ),
        (
            "subscriptions:\n  fanout_concurrency: 0\n",
            "subscriptions.fanout_concurrency",
        ),
        // SEC-046: the RED-052 shard guard is mandatory-on.
        (
            "reducer:\n  shard_max_reducers_per_sec: 0\n",
            "reducer.shard_max_reducers_per_sec",
        ),
    ];
    for (yaml, key) in cases {
        let file = write_config(&format!("{yaml}auth:\n  provider: none\n"));
        let err = Config::load_with(Some(file.path()), &no_env).unwrap_err();
        assert!(matches!(err, FluxumError::Config(_)), "{yaml}: {err:?}");
        assert!(err.to_string().contains(key), "{yaml}: {err}");
    }
}

#[test]
fn unknown_nested_mappings_record_leaves_then_fail_deserialization() {
    // A whole unknown subtree merges (recording every leaf's provenance)
    // and is then rejected by the typed deserialization.
    let file = write_config("extra:\n  nested:\n    a: 1\n    b: 2\nauth:\n  provider: none\n");
    let err = Config::load_with(Some(file.path()), &no_env).unwrap_err();
    assert!(matches!(err, FluxumError::ConfigParse(_)), "{err:?}");
}

#[test]
fn empty_env_override_parses_as_an_empty_string() {
    let env = env_of(&[
        ("FLUXUM_PROFILE", "development"),
        ("FLUXUM_SERVER_TCP_HOST", ""),
    ]);
    let cfg = Config::load_with(None, &env).unwrap();
    assert_eq!(cfg.server.tcp_host, "");
    assert_eq!(cfg.source_of("server.tcp_host"), ValueSource::Env);
}

#[test]
fn auto_or_displays_auto_and_values() {
    assert_eq!(AutoOr::<usize>::Auto.to_string(), "auto");
    assert_eq!(AutoOr::Value(7usize).to_string(), "7");
    assert_eq!(AutoOr::Value(ByteSize(2 << 20)).to_string(), "2MiB");
}

#[test]
fn full_architecture_example_shape_parses() {
    let file = write_config(
        r#"
server:
  tcp_host: "0.0.0.0"
  http_port: 15800
  tcp_port: 15801
  # A public bind behind a TLS-terminating proxy (SEC-059): the operator
  # accepts plaintext on the trusted link between proxy and Fluxum.
  allow_plaintext: true
sharding:
  shards: auto
  strategy: hash
memory:
  budget: auto
storage:
  data_dir: ./data
  commit_log_dir: ./data/log
  checkpoint_dir: ./data/checkpoints
  checkpoint_interval_tx: 10000
  page_compression: lz4
  compression_min_bytes: 1024
  checkpoint_compression_level: 3
replication:
  role: primary
  mode: async
  peers: []
simd: auto
auth:
  provider: token
  secret: ${FLUXUM_AUTH_SECRET}
  server_peers:
    - name: "ingest_service"
      token: ${FLUXUM_INGEST_TOKEN}
subscriptions:
  send_buffer_bytes: 2097152
observability:
  slow_reducer_threshold_us: 5000
logging:
  level: info
  format: json
"#,
    );
    let env = env_of(&[
        ("FLUXUM_AUTH_SECRET", "topsecret"),
        ("FLUXUM_INGEST_TOKEN", "peertoken"),
    ]);
    let cfg = Config::load_with(Some(file.path()), &env).unwrap();
    assert_eq!(cfg.server.tcp_host, "0.0.0.0");
    assert!(cfg.sharding.shards.is_auto());
    assert_eq!(cfg.auth.server_peers.len(), 1);
    assert_eq!(cfg.auth.server_peers[0].token.expose_str(), "peertoken");
    assert_eq!(cfg.simd, SimdMode::Auto);
    assert_eq!(cfg.subscriptions.send_buffer_bytes, ByteSize(2 << 20));
}

// --- phase8 coverage floor: the validate() refusal arms, each named ---------------

/// Load a full YAML under the development profile (so the auth-secret
/// default refusal stays out of the way) and return the error text.
fn load_err(yaml: &str) -> String {
    let file = write_config(&format!("profile: development\n{yaml}"));
    Config::load_with(Some(file.path()), &no_env)
        .unwrap_err()
        .to_string()
}

#[test]
fn every_validation_refusal_names_its_key() {
    for (yaml, needle) in [
        (
            "server:\n  http_port: 15800\n  tcp_port: 15800\n",
            "must differ",
        ),
        ("pgwire:\n  enabled: true\n  port: 0\n", "pgwire.port"),
        (
            "pgwire:\n  enabled: true\n  port: 15800\n",
            "must differ from server.http_port",
        ),
        (
            "replication:\n  peers: [\"127.0.0.1:1\"]\n",
            "replication.member_name",
        ),
        (
            "replication:\n  peers: [\"127.0.0.1:1\"]\n  member_name: a\n",
            "replication.peer_token",
        ),
        (
            "replication:\n  semi_sync:\n    on_quorum_loss: explode\n",
            "block|degrade",
        ),
        (
            "replication:\n  semi_sync:\n    quorum: \"0\"\n",
            "outside 1..=",
        ),
        (
            "replication:\n  semi_sync:\n    quorum: sometimes\n",
            "`majority` or a count",
        ),
        (
            "replication:\n  archive:\n    remote:\n      enabled: true\n",
            "replication.archive.remote.endpoint",
        ),
        (
            "replication:\n  archive:\n    remote:\n      enabled: true\n      endpoint: e\n      bucket: b\n      access_key: k\n",
            "replication.archive.remote.secret_key",
        ),
        (
            "server:\n  trusted_proxies: [\"not-an-ip\"]\n",
            "server.trusted_proxies",
        ),
        (
            "server:\n  admin:\n    trusted: [\"not-an-ip\"]\n",
            "server.admin.trusted",
        ),
        (
            "server:\n  connection_limits:\n    blocklist: [\"nope\"]\n",
            "server.connection_limits.blocklist",
        ),
        (
            "server:\n  connection_limits:\n    allowlist: [\"nope\"]\n",
            "server.connection_limits.allowlist",
        ),
        (
            "server:\n  connection_limits:\n    overload_shed_fraction: 1.5\n",
            "[0.0, 1.0]",
        ),
        (
            "server:\n  connection_limits:\n    overload_shed_fraction: 0.9\n    overload_shed_all_fraction: 0.5\n",
            "must be >=",
        ),
        ("server:\n  tls:\n    cert: c.pem\n", "server.tls.key"),
        ("server:\n  tls:\n    key: k.pem\n", "server.tls.cert"),
    ] {
        let err = load_err(yaml);
        assert!(
            err.contains(needle),
            "expected `{needle}` in the refusal for:\n{yaml}\ngot: {err}"
        );
    }
}

#[test]
fn asymmetric_jwt_requires_the_public_key_not_a_secret() {
    let err = load_err("auth:\n  provider: jwt\n  jwt_algorithm: es256\n");
    assert!(err.contains("auth.jwt_public_key"), "{err}");
}

#[test]
fn file_and_shape_errors_are_named() {
    // An explicit config path that does not exist.
    let err = Config::load_with(Some(std::path::Path::new("Z:/no/such/file.yml")), &no_env)
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot read config file"), "{err}");
    // A file that is not YAML.
    let file = write_config(":\n  - definitely: [not\n");
    let err = Config::load_with(Some(file.path()), &no_env)
        .unwrap_err()
        .to_string();
    assert!(!err.is_empty(), "parse failure surfaces: {err}");
    // An unknown field is a typo, not silently ignored.
    let file = write_config("profile: development\nserver:\n  http_prot: 1\n");
    let err = Config::load_with(Some(file.path()), &no_env)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("http_prot") || err.contains("unknown field"),
        "{err}"
    );
}
