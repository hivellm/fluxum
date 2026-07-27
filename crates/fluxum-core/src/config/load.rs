//! Loading and validation (SPEC-012 OBS-080): the defaults → profile →
//! file → env layering, `${VAR}` expansion, and `validate()` — split from
//! `mod.rs` (the type definitions) to honour the file-size convention.

use super::*;

/// Environment lookup used by the loader; injected for testability.
pub type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

impl Config {
    /// Load configuration with full layering: built-in defaults → profile
    /// defaults → YAML file (`path`, optional) → `FLUXUM_*` env overrides →
    /// validation.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        Self::load_with(path, &|key| std::env::var(key).ok())
    }

    /// [`Config::load`] with an injected environment, for tests.
    pub fn load_with(path: Option<&Path>, env: EnvLookup<'_>) -> Result<Self> {
        let mut sources: BTreeMap<String, ValueSource> = BTreeMap::new();
        let mut merged = serde_yaml::to_value(Config::default())?;

        // Parse the file early: the profile key may live there.
        let file_value = match path {
            Some(p) => {
                let text = std::fs::read_to_string(p).map_err(|e| {
                    FluxumError::config(format!("cannot read config file '{}': {e}", p.display()))
                })?;
                let mut value: Value = serde_yaml::from_str(&text)?;
                expand_env_refs(&mut value, env);
                Some(value)
            }
            None => None,
        };

        // Profile selection: env > file > default (SPEC-012 OBS-081).
        let profile_str = env("FLUXUM_PROFILE").or_else(|| {
            file_value
                .as_ref()
                .and_then(|v| v.get("profile"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        let profile = match profile_str.as_deref() {
            None => Profile::Production,
            Some("production") => Profile::Production,
            Some("development") => Profile::Development,
            Some(other) => {
                return Err(FluxumError::config(format!(
                    "profile: unknown profile '{other}' (expected 'production' or 'development')"
                )));
            }
        };

        // Profile defaults layer (overridden by file and env below).
        if profile == Profile::Development {
            let overlay: Value = serde_yaml::from_str(
                "{sharding: {shards: 1}, auth: {provider: none}, logging: {format: pretty}}",
            )?;
            merge_value(
                &mut merged,
                overlay,
                &mut Vec::new(),
                ValueSource::Profile,
                &mut sources,
            );
        }

        // File layer.
        if let Some(value) = file_value {
            merge_value(
                &mut merged,
                value,
                &mut Vec::new(),
                ValueSource::File,
                &mut sources,
            );
        }

        // Env layer: every leaf key maps to FLUXUM_<PATH> (SPEC-012 OBS-080).
        let mut paths = Vec::new();
        collect_leaf_paths(&merged, &mut Vec::new(), &mut paths);
        for key_path in paths {
            let env_name = format!("FLUXUM_{}", key_path.join("_").to_ascii_uppercase());
            if let Some(raw) = env(&env_name) {
                set_path(&mut merged, &key_path, parse_env_scalar(&raw));
                sources.insert(key_path.join("."), ValueSource::Env);
            }
        }

        let mut config: Config = serde_yaml::from_value(merged)?;
        config.sources = sources;
        config.resolve_storage_dirs();
        config.validate()?;
        Ok(config)
    }

    /// Root the storage sub-directories under `storage.data_dir` unless they
    /// are configured in their own right.
    ///
    /// `storage.data_dir` is *the* placement knob: pointing it at
    /// `/var/lib/fluxum` must put the commit log, checkpoints, cold-tier
    /// pages, and the archive there too. Without this, each sub-directory
    /// keeps its own built-in default — all of them relative to the
    /// **process working directory** — so a deployment that sets only
    /// `data_dir` silently writes its durable artifacts next to wherever the
    /// binary happened to be started, outside the volume an operator mounted
    /// and outside their backups.
    ///
    /// An explicitly configured sub-directory always wins verbatim: for a
    /// config that came through [`Config::load`], "explicit" means the
    /// loader recorded a file/env/profile source; for a hand-built `Config`
    /// (embedders, tests) there is no provenance to consult, so a value that
    /// still equals its built-in default is treated as unset. Idempotent —
    /// re-running it changes nothing.
    pub fn resolve_storage_dirs(&mut self) {
        let defaults = StorageConfig::default();
        let archive_default = ArchiveConfig::default();
        let data_dir = self.storage.data_dir.clone();
        let mut rooted: Vec<(&'static str, PathBuf)> = Vec::new();

        {
            let mut resolve = |key: &'static str, current: &mut PathBuf, default: &PathBuf| {
                let explicit = matches!(
                    self.sources.get(key),
                    Some(ValueSource::File | ValueSource::Env | ValueSource::Profile)
                );
                if explicit || *current != *default {
                    return;
                }
                // The built-in defaults are `./data/<name>`; re-root the same
                // leaf under the configured data dir.
                let Some(leaf) = default.file_name() else {
                    return;
                };
                *current = data_dir.join(leaf);
                rooted.push((key, current.clone()));
            };
            resolve(
                "storage.commit_log_dir",
                &mut self.storage.commit_log_dir,
                &defaults.commit_log_dir,
            );
            resolve(
                "storage.checkpoint_dir",
                &mut self.storage.checkpoint_dir,
                &defaults.checkpoint_dir,
            );
            resolve(
                "storage.page_dir",
                &mut self.storage.page_dir,
                &defaults.page_dir,
            );
            resolve(
                "replication.archive.dir",
                &mut self.replication.archive.dir,
                &archive_default.dir,
            );
        }

        for (key, _) in rooted {
            self.sources.insert(key.to_owned(), ValueSource::Derived);
        }
    }

    /// Provenance of a key path, defaulting to [`ValueSource::Default`].
    pub fn source_of(&self, key_path: &str) -> ValueSource {
        self.sources
            .get(key_path)
            .copied()
            .unwrap_or(ValueSource::Default)
    }

    /// Semantic validation beyond YAML shape; every failure names its key.
    /// Public so a hand-built `Config` (tests, embedders) can be checked with
    /// the same rules the loader applies — including the SEC-059
    /// plaintext-on-public-bind guard.
    pub fn validate(&self) -> Result<()> {
        if self.server.http_port == 0 {
            return Err(FluxumError::config("server.http_port: must be non-zero"));
        }
        if self.server.tcp_port == 0 {
            return Err(FluxumError::config("server.tcp_port: must be non-zero"));
        }
        if self.server.http_port == self.server.tcp_port {
            return Err(FluxumError::config(format!(
                "server.http_port and server.tcp_port must differ (both {})",
                self.server.tcp_port
            )));
        }
        // SPEC-027 PGW-004: the read-only pgwire listener, only when enabled.
        if self.pgwire.enabled {
            if self.pgwire.port == 0 {
                return Err(FluxumError::config("pgwire.port: must be non-zero"));
            }
            if self.pgwire.port == self.server.http_port || self.pgwire.port == self.server.tcp_port
            {
                return Err(FluxumError::config(format!(
                    "pgwire.port ({}) must differ from server.http_port and server.tcp_port",
                    self.pgwire.port
                )));
            }
        }
        if let Some(threads) = self.runtime.worker_threads.explicit()
            && *threads == 0
        {
            return Err(FluxumError::config("runtime.worker_threads: must be >= 1"));
        }
        if let Some(shards) = self.sharding.shards.explicit()
            && *shards == 0
        {
            return Err(FluxumError::config("sharding.shards: must be >= 1"));
        }
        if !(self.memory.auto_fraction > 0.0 && self.memory.auto_fraction <= 1.0) {
            return Err(FluxumError::config(format!(
                "memory.auto_fraction: must be in (0.0, 1.0], got {}",
                self.memory.auto_fraction
            )));
        }
        if let Some(budget) = self.memory.budget.explicit()
            && budget.as_u64() < MIN_MEMORY_BUDGET
        {
            return Err(FluxumError::config(format!(
                "memory.budget: explicit value {budget} is below the {} floor (SPEC-015 TIER-001)",
                ByteSize(MIN_MEMORY_BUDGET)
            )));
        }
        if !(self.memory.bufferpool_fraction > 0.0 && self.memory.bufferpool_fraction <= 1.0) {
            return Err(FluxumError::config(format!(
                "memory.bufferpool_fraction: must be in (0.0, 1.0], got {}",
                self.memory.bufferpool_fraction
            )));
        }
        if self.storage.checkpoint_interval_tx == 0 {
            return Err(FluxumError::config(
                "storage.checkpoint_interval_tx: must be >= 1",
            ));
        }
        if !matches!(self.storage.page_size, 4096 | 8192 | 16384) {
            return Err(FluxumError::config(format!(
                "storage.page_size: must be 4096, 8192, or 16384 (SPEC-015 TIER-022), got {}",
                self.storage.page_size
            )));
        }
        let (low, high) = (
            self.storage.evictor_low_watermark,
            self.storage.evictor_high_watermark,
        );
        if !(low > 0.0 && low < high && high <= 1.0) {
            return Err(FluxumError::config(format!(
                "storage.evictor_low_watermark/evictor_high_watermark: need \
                 0 < low < high <= 1, got low={low} high={high}"
            )));
        }
        if let Some(fanout) = self.subscriptions.fanout_concurrency.explicit()
            && *fanout == 0
        {
            return Err(FluxumError::config(
                "subscriptions.fanout_concurrency: must be >= 1",
            ));
        }
        // SEC-046 (F-015): the RED-052 shard guard is mandatory-on — a
        // single-writer shard must always carry an aggregate admission
        // bound; raise the value instead of disabling it.
        if self.reducer.shard_max_reducers_per_sec == 0 {
            return Err(FluxumError::config(
                "reducer.shard_max_reducers_per_sec: 0 would disable the RED-052 shard \
                 guard, which is mandatory (SPEC-026 SEC-046); raise the value instead",
            ));
        }
        // REP-062: the archive retention window must parse (it is the PITR
        // window, so a typo here silently shrinking it would be costly).
        self.replication.archive.retention_duration()?;
        // REP-005: a member of a replica set must be able to identify and
        // authenticate itself to its peers.
        if !self.replication.peers.is_empty() {
            if self.replication.member_name.is_empty() {
                return Err(FluxumError::config(
                    "replication.member_name: required when replication.peers is set (REP-005)",
                ));
            }
            if self
                .replication
                .peer_token
                .as_ref()
                .is_none_or(|t| t.expose_str().is_empty())
            {
                return Err(FluxumError::config(
                    "replication.peer_token: required when replication.peers is set (REP-005)",
                ));
            }
        }
        if !matches!(
            self.replication.semi_sync.on_quorum_loss.as_str(),
            "block" | "degrade"
        ) {
            return Err(FluxumError::config(format!(
                "replication.semi_sync.on_quorum_loss: `{}` is not block|degrade (REP-022)",
                self.replication.semi_sync.on_quorum_loss
            )));
        }
        if let Ok(count) = self.replication.semi_sync.quorum.parse::<u32>() {
            // REP-021: an explicit count must be satisfiable by the
            // configured replica set (members = peers + this node).
            let members = u32::try_from(self.replication.peers.len()).unwrap_or(u32::MAX - 1) + 1;
            if count == 0 || count > members {
                return Err(FluxumError::config(format!(
                    "replication.semi_sync.quorum: {count} is outside 1..={members} \
                     (peers + this member, REP-021)"
                )));
            }
        }
        if self.replication.semi_sync.quorum != "majority"
            && self.replication.semi_sync.quorum.parse::<u32>().is_err()
        {
            return Err(FluxumError::config(format!(
                "replication.semi_sync.quorum: `{}` is not `majority` or a count (REP-021)",
                self.replication.semi_sync.quorum
            )));
        }
        // OPS-010: an enabled remote target needs a complete address and
        // credentials — a partial one would fail on the first nightly pass.
        let remote = &self.replication.archive.remote;
        if remote.enabled {
            for (key, value) in [
                ("endpoint", remote.endpoint.as_str()),
                ("bucket", remote.bucket.as_str()),
                ("access_key", remote.access_key.as_str()),
            ] {
                if value.is_empty() {
                    return Err(FluxumError::config(format!(
                        "replication.archive.remote.{key}: required when remote archival is \
                         enabled (SPEC-025 OPS-010)"
                    )));
                }
            }
            if remote
                .secret_key
                .as_ref()
                .is_none_or(|s| s.expose_str().is_empty())
            {
                return Err(FluxumError::config(
                    "replication.archive.remote.secret_key: required when remote archival is \
                     enabled (SPEC-025 OPS-010)",
                ));
            }
        }
        if let Err(e) = crate::net::IpSet::parse(&self.server.trusted_proxies) {
            return Err(FluxumError::config(format!("server.trusted_proxies: {e}")));
        }
        if let Err(e) = crate::net::IpSet::parse(&self.server.admin.trusted) {
            return Err(FluxumError::config(format!("server.admin.trusted: {e}")));
        }
        if let Err(e) = crate::net::IpSet::parse(&self.server.connection_limits.blocklist) {
            return Err(FluxumError::config(format!(
                "server.connection_limits.blocklist: {e}"
            )));
        }
        if let Err(e) = crate::net::IpSet::parse(&self.server.connection_limits.allowlist) {
            return Err(FluxumError::config(format!(
                "server.connection_limits.allowlist: {e}"
            )));
        }
        let (shed, shed_all) = (
            self.server.connection_limits.overload_shed_fraction,
            self.server.connection_limits.overload_shed_all_fraction,
        );
        if !(0.0..=1.0).contains(&shed) || !(0.0..=1.0).contains(&shed_all) {
            return Err(FluxumError::config(format!(
                "server.connection_limits.overload_shed_fraction/overload_shed_all_fraction: \
                 must be in [0.0, 1.0], got {shed}/{shed_all}"
            )));
        }
        if shed != 0.0 && shed_all != 0.0 && shed_all < shed {
            return Err(FluxumError::config(format!(
                "server.connection_limits.overload_shed_all_fraction ({shed_all}) must be >= \
                 overload_shed_fraction ({shed})"
            )));
        }
        // SEC-061: an asymmetric JWT provider verifies with a public key, not
        // a shared secret — require the key, not `auth.secret`.
        let asymmetric_jwt =
            self.auth.provider == AuthProvider::Jwt && self.auth.jwt_algorithm.is_asymmetric();
        if asymmetric_jwt {
            if self.auth.jwt_public_key.is_none() {
                return Err(FluxumError::config(format!(
                    "auth.jwt_public_key: required for asymmetric auth.jwt_algorithm '{:?}' (SEC-061)",
                    self.auth.jwt_algorithm
                )));
            }
        } else if matches!(self.auth.provider, AuthProvider::Token | AuthProvider::Jwt)
            && self
                .auth
                .secret
                .as_ref()
                .is_none_or(|s| s.expose_str().is_empty())
        {
            return Err(FluxumError::config(format!(
                "auth.secret: required for auth.provider '{:?}' (set it or use the development profile)",
                self.auth.provider
            )));
        }
        // SEC-059: TLS needs both halves.
        match (&self.server.tls.cert, &self.server.tls.key) {
            (Some(_), None) => {
                return Err(FluxumError::config(
                    "server.tls.key: required when server.tls.cert is set",
                ));
            }
            (None, Some(_)) => {
                return Err(FluxumError::config(
                    "server.tls.cert: required when server.tls.key is set",
                ));
            }
            _ => {}
        }
        // SEC-059: refuse a real-auth listener on a public (non-loopback) bind
        // without TLS, unless the operator explicitly opts out for a trusted
        // network. Otherwise bearer tokens and row data would cross the public
        // interface in cleartext. (`none` auth is already loopback-guarded by
        // AUTH-040, so this bites the `token`/`jwt` providers.)
        let authenticating = matches!(self.auth.provider, AuthProvider::Token | AuthProvider::Jwt);
        if authenticating
            && !crate::auth::is_loopback_host(&self.server.tcp_host)
            && !self.server.tls.is_enabled()
            && !self.server.allow_plaintext
        {
            return Err(FluxumError::config(format!(
                "server.tcp_host '{}' is a public bind with authentication but no TLS: bearer \
                 tokens and data would travel in cleartext (SPEC-026 SEC-059). Set server.tls.cert \
                 + server.tls.key, bind to loopback, or set server.allow_plaintext: true only if \
                 the link is encrypted below Fluxum (mesh/VPN).",
                self.server.tcp_host
            )));
        }
        Ok(())
    }
}

/// Merge `src` into `dst` recursively; scalars and sequences replace, and
/// every replaced leaf records `source` under its dotted path.
fn merge_value(
    dst: &mut Value,
    src: Value,
    path: &mut Vec<String>,
    source: ValueSource,
    sources: &mut BTreeMap<String, ValueSource>,
) {
    match (dst, src) {
        (Value::Mapping(dst_map), Value::Mapping(src_map)) => {
            for (key, value) in src_map {
                let key_str = key
                    .as_str()
                    .map_or_else(|| format!("{key:?}"), str::to_owned);
                path.push(key_str);
                if let Some(slot) = dst_map.get_mut(&key) {
                    merge_value(slot, value, path, source, sources);
                } else {
                    record_leaves(&value, path, source, sources);
                    dst_map.insert(key, value);
                }
                path.pop();
            }
        }
        (slot, value) => {
            record_leaves(&value, path, source, sources);
            *slot = value;
        }
    }
}

/// Record `source` for every leaf under `value`.
fn record_leaves(
    value: &Value,
    path: &mut Vec<String>,
    source: ValueSource,
    sources: &mut BTreeMap<String, ValueSource>,
) {
    if let Value::Mapping(map) = value {
        for (key, child) in map {
            let key_str = key
                .as_str()
                .map_or_else(|| format!("{key:?}"), str::to_owned);
            path.push(key_str);
            record_leaves(child, path, source, sources);
            path.pop();
        }
    } else {
        sources.insert(path.join("."), source);
    }
}

/// Collect the dotted paths of every leaf (non-mapping) value.
pub(super) fn collect_leaf_paths(
    value: &Value,
    prefix: &mut Vec<String>,
    out: &mut Vec<Vec<String>>,
) {
    if let Value::Mapping(map) = value {
        for (key, child) in map {
            if let Some(key_str) = key.as_str() {
                prefix.push(key_str.to_owned());
                collect_leaf_paths(child, prefix, out);
                prefix.pop();
            }
        }
    } else {
        out.push(prefix.clone());
    }
}

/// Set the value at a dotted path, creating intermediate mappings as needed.
fn set_path(root: &mut Value, path: &[String], value: Value) {
    let mut cursor = root;
    for (i, segment) in path.iter().enumerate() {
        let key = Value::String(segment.clone());
        let Value::Mapping(map) = cursor else { return };
        if i == path.len() - 1 {
            map.insert(key, value);
            return;
        }
        cursor = map
            .entry(key)
            .or_insert_with(|| Value::Mapping(serde_yaml::Mapping::new()));
    }
}

/// Expand string values of the exact form `${VAR}` from the environment;
/// an unset variable expands to the empty string (validation then reports
/// missing required values by key).
fn expand_env_refs(value: &mut Value, env: EnvLookup<'_>) {
    match value {
        Value::String(s) => {
            if let Some(name) = s.strip_prefix("${").and_then(|rest| rest.strip_suffix('}')) {
                *s = env(name).unwrap_or_default();
            }
        }
        Value::Mapping(map) => {
            for (_, child) in map.iter_mut() {
                expand_env_refs(child, env);
            }
        }
        Value::Sequence(seq) => {
            for child in seq {
                expand_env_refs(child, env);
            }
        }
        _ => {}
    }
}

/// Parse an env-var override: YAML scalar rules (numbers, booleans, `auto`,
/// inline sequences), falling back to a plain string.
fn parse_env_scalar(raw: &str) -> Value {
    if raw.is_empty() {
        return Value::String(String::new());
    }
    serde_yaml::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}
