//! Layered YAML configuration (ARCHITECTURE §Configuration, SPEC-012 OBS-080/081).
//!
//! Precedence: `FLUXUM_*` environment variable > config file > profile
//! defaults > built-in default. Every key is overridable by upper-casing its
//! path and joining with `_` (`server.tcp_port` → `FLUXUM_SERVER_TCP_PORT`).
//! The loader records where every key came from ([`ValueSource`]) so the boot
//! `effective configuration` event (SPEC-016 HWA-012) can report it.

mod bytes;

pub use bytes::{AutoOr, ByteSize, parse_byte_size};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use crate::error::{FluxumError, Result};

/// Explicit `memory.budget` values below this are rejected (SPEC-015 TIER-001).
pub const MIN_MEMORY_BUDGET: u64 = 128 << 20;

/// Where a resolved config value came from (highest precedence last: file
/// beats profile beats default; env beats everything).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValueSource {
    /// Built-in default.
    Default,
    /// Applied by the selected profile (e.g. `development`).
    Profile,
    /// Set in the YAML config file.
    File,
    /// Set by a `FLUXUM_*` environment variable.
    Env,
}

/// Deployment profile (SPEC-012 OBS-081).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Profile {
    /// Default: JSON logs, full auth.
    #[default]
    Production,
    /// Single shard, auth `none`, pretty logs.
    Development,
}

/// Network listeners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Bind address for both listeners.
    pub tcp_host: String,
    /// HTTP: admin API + `/rpc` (FluxRPC over Streamable HTTP).
    pub http_port: u16,
    /// FluxRPC binary TCP.
    pub tcp_port: u16,
    /// Idle-connection timeout, seconds (RPC-060): a connection with no
    /// inbound frame for this long is sent `408` and closed. `0` disables.
    pub idle_timeout_secs: u64,
    /// Max inbound frame body size (RPC-061); frames above it are rejected
    /// with `413` and the connection is closed.
    pub max_frame_bytes: ByteSize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            tcp_host: "127.0.0.1".to_owned(),
            http_port: 15800,
            tcp_port: 15801,
            idle_timeout_secs: 60,
            max_frame_bytes: ByteSize(u64::from(fluxum_protocol::DEFAULT_MAX_FRAME_BYTES)),
        }
    }
}

/// Async runtime tuning (SPEC-016 derived-defaults table).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RuntimeConfig {
    /// Tokio worker threads; `auto` = effective cores (min 1).
    pub worker_threads: AutoOr<usize>,
}

/// Partitioning strategy for sharded tables (SPEC-007).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShardStrategy {
    /// Hash partitioning (default).
    #[default]
    Hash,
    /// Range partitioning.
    Range,
    /// Region/label partitioning.
    Region,
}

/// Shard layout.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ShardingConfig {
    /// Shard count; `auto` = `clamp(effective_cores / 2, 1, 16)`.
    pub shards: AutoOr<u32>,
    /// Default partitioning strategy.
    pub strategy: ShardStrategy,
}

/// Process-wide memory budget (SPEC-015 §2).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MemoryConfig {
    /// `auto` = `max(auto_floor, auto_fraction × effective_memory)`;
    /// explicit values below 128 MiB are rejected (TIER-001/002).
    pub budget: AutoOr<ByteSize>,
    /// Fraction of effective memory used by the `auto` derivation.
    pub auto_fraction: f64,
    /// Floor for the `auto` derivation.
    pub auto_floor_bytes: ByteSize,
    /// Fraction of the budget handed to the buffer pool (TIER-003); the
    /// remainder is headroom for `TxState`, subscription buffers, and
    /// allocator slack.
    pub bufferpool_fraction: f64,
    /// RSS tolerance floor above the budget (TIER-004); the effective
    /// tolerance is `max(this, 0.10 × budget)`.
    pub budget_tolerance_bytes: ByteSize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            budget: AutoOr::Auto,
            auto_fraction: 0.5,
            auto_floor_bytes: ByteSize(MIN_MEMORY_BUDGET),
            bufferpool_fraction: 0.8,
            budget_tolerance_bytes: ByteSize(64 << 20),
        }
    }
}

/// Page compression codec (SPEC-015).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PageCompression {
    /// LZ4 (default).
    #[default]
    Lz4,
    /// zstd.
    Zstd,
    /// No compression.
    None,
}

/// On-disk layout and storage-engine tuning (SPEC-002, SPEC-015).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Root data directory.
    pub data_dir: PathBuf,
    /// Commit-log segment directory.
    pub commit_log_dir: PathBuf,
    /// Checkpoint directory.
    pub checkpoint_dir: PathBuf,
    /// Cold-tier page-file directory (TIER-023).
    pub page_dir: PathBuf,
    /// Logical page size in bytes: 4096 | 8192 | 16384 (TIER-022, OQ-7).
    pub page_size: u32,
    /// Checkpoint cadence in committed transactions.
    pub checkpoint_interval_tx: u64,
    /// Page compression codec.
    pub page_compression: PageCompression,
    /// Payloads smaller than this are stored raw (TIER-040).
    pub compression_min_bytes: u32,
    /// zstd level for checkpoint manifests/objects and backup artifacts
    /// (TIER-042).
    pub checkpoint_compression_level: i32,
    /// Pool-occupancy fraction that wakes eviction (TIER-031).
    pub evictor_high_watermark: f64,
    /// Pool-occupancy fraction eviction reclaims down to (TIER-031).
    pub evictor_low_watermark: f64,
    /// Commit-log write buffer; `auto` = `clamp(effective_memory / 1024, 64KiB, 4MiB)`.
    pub commit_log_write_buffer_bytes: AutoOr<ByteSize>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            commit_log_dir: PathBuf::from("./data/log"),
            checkpoint_dir: PathBuf::from("./data/checkpoints"),
            page_dir: PathBuf::from("./data/pages"),
            page_size: 8192,
            checkpoint_interval_tx: 10_000,
            page_compression: PageCompression::default(),
            compression_min_bytes: 1024,
            checkpoint_compression_level: 3,
            evictor_high_watermark: 0.95,
            evictor_low_watermark: 0.90,
            commit_log_write_buffer_bytes: AutoOr::Auto,
        }
    }
}

/// Replica-set role (SPEC-008).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationRole {
    /// Accepts writes (default).
    #[default]
    Primary,
    /// Read-only follower.
    Replica,
}

/// Replication acknowledgment mode (SPEC-008).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationMode {
    /// Fire-and-forget shipping (default).
    #[default]
    Async,
    /// Commit waits for one replica ack.
    SemiSync,
}

/// Replica-set membership.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ReplicationConfig {
    /// This node's role.
    pub role: ReplicationRole,
    /// Acknowledgment mode.
    pub mode: ReplicationMode,
    /// Replica-set member addresses.
    pub peers: Vec<String>,
}

/// SIMD tier forcing (SPEC-016 HWA-032).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SimdMode {
    /// Runtime feature detection picks the best tier (default).
    #[default]
    Auto,
    /// Force AVX-512 (abort boot if unsupported).
    Avx512,
    /// Force AVX2 (abort boot if unsupported).
    Avx2,
    /// Force NEON (abort boot if unsupported).
    Neon,
    /// Force the scalar reference implementations (valid everywhere).
    Scalar,
}

/// Authentication provider (SPEC-009).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthProvider {
    /// Static shared token(s); identity = SHA-256(token) (default).
    #[default]
    Token,
    /// JWT validation; identity = SHA-256("{iss}|{sub}").
    Jwt,
    /// Dev only: any token accepted.
    None,
}

/// A trusted server-to-server peer (SPEC-009 §server identity).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerPeer {
    /// Peer name; identity = SHA-256("SERVER:" + name).
    pub name: String,
    /// Shared token the peer authenticates with.
    pub token: String,
}

/// Authentication configuration (SPEC-009).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// Provider kind.
    pub provider: AuthProvider,
    /// Provider secret (`token`: the token; `jwt`: verification key).
    /// Supports `${VAR}` env expansion in the YAML file.
    pub secret: Option<String>,
    /// Trusted server peers.
    pub server_peers: Vec<ServerPeer>,
}

/// Subscription fan-out tuning (SPEC-005).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SubscriptionsConfig {
    /// Per-client send buffer.
    pub send_buffer_bytes: ByteSize,
    /// Fan-out concurrency; `auto` = `clamp(2 × effective_cores, 2, 64)`.
    pub fanout_concurrency: AutoOr<usize>,
}

impl Default for SubscriptionsConfig {
    fn default() -> Self {
        Self {
            send_buffer_bytes: ByteSize(2 << 20),
            fanout_concurrency: AutoOr::Auto,
        }
    }
}

/// Observability thresholds (SPEC-012).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ObservabilityConfig {
    /// WARN threshold for slow reducers, in microseconds.
    pub slow_reducer_threshold_us: u64,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            slow_reducer_threshold_us: 5_000,
        }
    }
}

/// Log output format.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Structured JSON lines (production default).
    #[default]
    Json,
    /// Human-readable output (development default).
    Pretty,
}

/// Logging configuration (SPEC-012).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoggingConfig {
    /// Level or tracing env-filter directive (e.g. `"info,fluxum_core=debug"`).
    pub level: String,
    /// Output format.
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            format: LogFormat::default(),
        }
    }
}

/// The fully resolved server configuration.
///
/// Load with [`Config::load`]; `sources` records the provenance of every key
/// that was set above the built-in defaults.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Deployment profile.
    pub profile: Profile,
    /// Network listeners.
    pub server: ServerConfig,
    /// Async runtime tuning.
    pub runtime: RuntimeConfig,
    /// Shard layout.
    pub sharding: ShardingConfig,
    /// Memory budget.
    pub memory: MemoryConfig,
    /// Storage engine.
    pub storage: StorageConfig,
    /// Replication.
    pub replication: ReplicationConfig,
    /// SIMD tier forcing.
    pub simd: SimdMode,
    /// Authentication.
    pub auth: AuthConfig,
    /// Subscription fan-out.
    pub subscriptions: SubscriptionsConfig,
    /// Observability thresholds.
    pub observability: ObservabilityConfig,
    /// Logging.
    pub logging: LoggingConfig,
    /// Provenance of every non-default key (`key.path` → source).
    #[serde(skip)]
    pub sources: BTreeMap<String, ValueSource>,
}

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
        config.validate()?;
        Ok(config)
    }

    /// Provenance of a key path, defaulting to [`ValueSource::Default`].
    pub fn source_of(&self, key_path: &str) -> ValueSource {
        self.sources
            .get(key_path)
            .copied()
            .unwrap_or(ValueSource::Default)
    }

    /// Semantic validation beyond YAML shape; every failure names its key.
    fn validate(&self) -> Result<()> {
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
        if matches!(self.auth.provider, AuthProvider::Token | AuthProvider::Jwt)
            && self.auth.secret.as_deref().is_none_or(str::is_empty)
        {
            return Err(FluxumError::config(format!(
                "auth.secret: required for auth.provider '{:?}' (set it or use the development profile)",
                self.auth.provider
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
fn collect_leaf_paths(value: &Value, prefix: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
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
        let file = write_config(
            "server:\n  tcp_port: 16000\n  http_port: 16001\nauth:\n  provider: none\n",
        );
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

        let same_ports = write_config(
            "server:\n  http_port: 15900\n  tcp_port: 15900\nauth:\n  provider: none\n",
        );
        let err = Config::load_with(Some(same_ports.path()), &no_env).unwrap_err();
        assert!(err.to_string().contains("server.http_port"), "{err}");

        let zero_shards = write_config("sharding:\n  shards: 0\nauth:\n  provider: none\n");
        let err = Config::load_with(Some(zero_shards.path()), &no_env).unwrap_err();
        assert!(err.to_string().contains("sharding.shards"), "{err}");
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
        assert_eq!(cfg.auth.secret.as_deref(), Some("s3cret"));

        // Unset variable → empty secret → typed validation error.
        let err = Config::load_with(Some(file.path()), &no_env).unwrap_err();
        assert!(err.to_string().contains("auth.secret"), "{err}");
    }

    #[test]
    fn full_architecture_example_shape_parses() {
        let file = write_config(
            r#"
server:
  tcp_host: "0.0.0.0"
  http_port: 15800
  tcp_port: 15801
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
        assert_eq!(cfg.auth.server_peers[0].token, "peertoken");
        assert_eq!(cfg.simd, SimdMode::Auto);
        assert_eq!(cfg.subscriptions.send_buffer_bytes, ByteSize(2 << 20));
    }
}
