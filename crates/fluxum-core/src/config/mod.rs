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
    /// Pre-auth connection-abuse limits (SPEC-026 SEC-030/031).
    pub connection_limits: ConnectionLimitsConfig,
    /// Streamable HTTP session-token security (SPEC-026 SEC-050..053).
    pub session: SessionConfig,
    /// HTTP admin-surface access control (SPEC-026 SEC-054).
    pub admin: AdminConfig,
    /// Transport TLS (SPEC-026 SEC-059).
    pub tls: TlsConfig,
    /// Permit an authenticating listener on a non-loopback address without
    /// TLS (SEC-059). Default `false`: a public bind with real auth and no
    /// TLS is refused at startup, since bearer tokens and row data would
    /// travel in cleartext. Set `true` only on a trusted network where the
    /// link is encrypted below Fluxum (a service mesh, a VPN, localhost pods).
    pub allow_plaintext: bool,
    /// Listen backlog for both listeners (SEC-042): pending un-accepted
    /// connections the kernel queues. `0` = the built-in default (1024).
    /// Raise alongside `somaxconn` on a directly exposed port.
    pub accept_backlog: u32,
    /// TCP keepalive probe time, seconds, applied to accepted sockets
    /// (SEC-042): dead peers holding connection slots are reaped by the
    /// kernel after this long. `0` (default) = keepalive off.
    pub tcp_keepalive_secs: u64,
    /// `TCP_DEFER_ACCEPT` window, seconds (SEC-042, Linux only): the kernel
    /// completes the handshake but only wakes the accept loop when data
    /// arrives, so bare-SYN/connect-and-idle floods never reach userspace.
    /// `0` (default) = off; ignored (logged) on other platforms.
    pub tcp_defer_accept_secs: u64,
    /// Reverse proxies / load balancers whose forwarding metadata is trusted
    /// (SPEC-026 SEC-035): IP or CIDR entries, IPv4/IPv6. Empty (the
    /// default) disables proxy awareness entirely — the socket peer address
    /// is the client IP, and forwarding metadata is never honored. When a
    /// peer *is* listed here, its `X-Forwarded-For` (HTTP) and PROXY
    /// protocol v2 preamble (TCP) resolve the real client IP that every
    /// per-IP defense then keys on.
    pub trusted_proxies: Vec<String>,
    /// Directory of static files served on unmatched `GET` paths, or empty
    /// (the default) to serve none.
    ///
    /// Exists for browser clients: `/rpc` sends no CORS headers, so a page
    /// that talks to Fluxum has to come from the same origin. Off by default —
    /// a server nobody configured this on has no file surface.
    pub static_dir: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            tcp_host: "127.0.0.1".to_owned(),
            http_port: 15800,
            tcp_port: 15801,
            idle_timeout_secs: 60,
            max_frame_bytes: ByteSize(u64::from(fluxum_protocol::DEFAULT_MAX_FRAME_BYTES)),
            connection_limits: ConnectionLimitsConfig::default(),
            session: SessionConfig::default(),
            admin: AdminConfig::default(),
            tls: TlsConfig::default(),
            allow_plaintext: false,
            accept_backlog: 0,
            tcp_keepalive_secs: 0,
            tcp_defer_accept_secs: 0,
            trusted_proxies: Vec::new(),
            static_dir: None,
        }
    }
}

/// Pre-auth connection-abuse protection (SPEC-026 §4, SEC-030/031): the
/// caps the transports enforce on the *unauthenticated* surface, keyed by
/// peer IP, independent of the post-auth per-`(Identity, reducer)` reducer
/// limiter (SPEC-004).
///
/// Every limit defaults **permissively** — high enough that a normal
/// deployment and its well-behaved clients never notice, low enough that a
/// flood, brute-force, or slowloris is contained. A `0` disables the
/// individual limit (opt-out), so an operator can turn any one off without
/// disabling the rest.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConnectionLimitsConfig {
    /// Max concurrent connections from one peer IP (SEC-030). `0` = no cap.
    pub max_conns_per_ip: u32,
    /// Sustained connection-accept rate per peer IP, connections/sec, with a
    /// matching burst (SEC-030). `0` = no rate limit.
    pub accept_rate_per_sec: f64,
    /// Time budget, seconds, for a connection to complete its first
    /// successful `Authenticate` (SEC-031, slowloris): a connection that has
    /// not authenticated within it is dropped. `0` = no handshake deadline
    /// (the ordinary idle timeout still applies).
    pub handshake_timeout_secs: u64,
    /// Max size, bytes, of a single *pre-auth* frame (SEC-031): a larger
    /// handshake frame is dropped before it is parsed. `0` = fall back to
    /// the ordinary `max_frame_bytes`.
    pub handshake_max_bytes: ByteSize,
    /// Consecutive failed `Authenticate` attempts from a peer IP before its
    /// further connection attempts are throttled with exponential backoff
    /// (SEC-031). `0` = no failed-auth throttle.
    pub failed_auth_threshold: u32,
    /// Base backoff after the threshold is crossed, milliseconds; doubles
    /// per subsequent failure up to `failed_auth_backoff_max_ms`.
    pub failed_auth_backoff_base_ms: u64,
    /// Ceiling for the exponential failed-auth backoff, milliseconds.
    pub failed_auth_backoff_max_ms: u64,
    /// Addresses refused outright, before any other check (SPEC-026
    /// SEC-033): IP or CIDR entries, IPv4/IPv6. Empty = nobody is banned by
    /// config. Runtime bans via `POST /admin/bans` merge with this list.
    pub blocklist: Vec<String>,
    /// When non-empty, **only** these addresses may connect (SEC-033,
    /// exclusive): IP or CIDR entries. Empty (the default) admits everyone
    /// the other checks admit. The blocklist still wins over an allowlist
    /// hit, so an operator can carve exceptions out of an allowed block.
    pub allowlist: Vec<String>,
    /// Global ceiling on concurrent connections across *all* peers
    /// (SEC-034): the backstop a distributed many-IP flood cannot walk past.
    /// `0` = uncapped.
    pub max_total_conns: u32,
    /// Cap on tracked per-IP guard entries (SEC-040): a many-distinct-IP
    /// flood grows the guard map at most to this size; beyond it,
    /// pressure eviction reclaims idle entries (never one holding live
    /// connections or an armed failed-auth streak). `0` = unbounded.
    pub max_tracked_ips: u32,
    /// Load fraction at which admission control starts shedding *pre-auth*
    /// connections (SEC-041): the highest of `total conns / max_total_conns`
    /// and `tracked IPs / max_tracked_ips` (only configured caps count).
    /// Established, authenticated sessions are never shed. `0` disables
    /// admission control.
    pub overload_shed_fraction: f64,
    /// Load fraction at which *all* new connections are shed (SEC-041),
    /// including reattaching sessions; must be >= `overload_shed_fraction`.
    /// `0` disables the shed-all stage.
    pub overload_shed_all_fraction: f64,
}

impl Default for ConnectionLimitsConfig {
    fn default() -> Self {
        Self {
            max_conns_per_ip: 1024,
            accept_rate_per_sec: 512.0,
            handshake_timeout_secs: 10,
            handshake_max_bytes: ByteSize(64 << 10),
            failed_auth_threshold: 10,
            failed_auth_backoff_base_ms: 100,
            failed_auth_backoff_max_ms: 30_000,
            blocklist: Vec::new(),
            allowlist: Vec::new(),
            max_total_conns: 0,
            // Generous: ~a few MB of guard state at worst, far below any
            // OOM territory, while a normal deployment never notices.
            max_tracked_ips: 100_000,
            overload_shed_fraction: 0.90,
            overload_shed_all_fraction: 0.98,
        }
    }
}

/// Streamable HTTP session-token security (SPEC-026 SEC-050..053). The
/// `Fluxum-Session` token is the bearer credential for every post-auth HTTP
/// request, so on a directly exposed port these knobs govern how hard a
/// stolen token is to obtain, replay, and outlive.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SessionConfig {
    /// Bind each session to the client IP that authenticated it (SEC-051):
    /// a request presenting the token from a different IP is refused and
    /// counted. Off by default — roaming clients (mobile, NAT rebinding)
    /// would otherwise be logged out on every network change. Composes with
    /// `server.trusted_proxies`: the *resolved* client IP is bound, not the
    /// proxy socket peer.
    pub bind_client_ip: bool,
    /// Rotate the token this often, seconds (SEC-052): after issue, the next
    /// request past the interval is answered with a fresh token and the old
    /// one enters a short grace window for in-flight requests. `0` = no
    /// interval rotation (a re-auth still rotates).
    pub rotate_interval_secs: u64,
    /// Grace window, seconds, during which a just-rotated token is still
    /// honored for in-flight requests (SEC-052). `0` = the old token dies
    /// the instant it rotates.
    pub rotate_grace_secs: u64,
    /// Absolute session lifetime, seconds (SEC-052): a session older than
    /// this is expired regardless of activity, on top of the RPC-060 idle
    /// expiry. `0` = no absolute cap (idle expiry only).
    pub absolute_lifetime_secs: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            bind_client_ip: false,
            rotate_interval_secs: 0,
            rotate_grace_secs: 30,
            absolute_lifetime_secs: 0,
        }
    }
}

/// HTTP admin-surface access control (SPEC-026 SEC-054). The admin API
/// (`/reducer`, `/query`, `/drain`, `/config/reload`, `/bans`, `/sessions`,
/// …) shares `http_port` with `/rpc`, so on a directly exposed port it must
/// be **safe by default**: reachable from loopback with no ceremony, but
/// refused from any other address unless the operator explicitly opts an IP
/// range in and presents a credential.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AdminConfig {
    /// Client IP ranges — beyond loopback, which is always allowed —
    /// permitted to reach the gated admin routes (IP/CIDR, v4/v6). Empty
    /// (the default) = loopback only. A request from a non-loopback IP not
    /// listed here is refused `403` before any handler runs.
    pub trusted: Vec<String>,
    /// Require an operator credential (a configured `auth.server_peers`
    /// token, in the `Fluxum-Operator` header or a JSON `token` field) on
    /// gated routes reached from a *non-loopback* trusted IP (SEC-054).
    /// Loopback never needs one — it is the operator's own host. Default
    /// `true`: exposing admin remotely without a credential is refused.
    pub require_operator: bool,
    /// Keep `/health` and `/metrics` open (ungated) so load balancers and
    /// Prometheus can always reach them (default `true`). Set `false` to put
    /// them behind the same gate as the rest of the admin surface.
    pub open_health_metrics: bool,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            trusted: Vec::new(),
            require_operator: true,
            open_health_metrics: true,
        }
    }
}

/// Transport TLS (SPEC-026 SEC-059): optional built-in `rustls` termination
/// on both listeners. Off by default — a deployment behind a TLS-terminating
/// proxy or on a trusted network needs none. When a `cert`/`key` pair is set,
/// both the FluxRPC/TCP and the HTTP listener terminate TLS before the first
/// handshake byte is read.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TlsConfig {
    /// PEM certificate chain file (leaf first). Empty = TLS off.
    pub cert: Option<PathBuf>,
    /// PEM private key file (PKCS#8 or RSA). Required when `cert` is set.
    pub key: Option<PathBuf>,
}

impl TlsConfig {
    /// Whether TLS is configured (both a cert and a key are set).
    pub fn is_enabled(&self) -> bool {
        self.cert.is_some() && self.key.is_some()
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

/// One named at-rest key (SPEC-026 SEC-010). `key_hex` is 64 hex characters
/// (256 bits). Config-embedded key material is the baseline; a KMS key
/// reference is a future `source` extension.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionKey {
    /// Stable key label, referenced by `active_key_id`.
    pub id: String,
    /// The 256-bit key as 64 hex characters (SEC-058: redacted, zeroized).
    pub key_hex: crate::secret::Secret<String>,
}

/// At-rest encryption keyring (SPEC-026 SEC-010/012): an enable flag, the
/// active key every write seals under, and the full key set (the active key
/// plus any retired keys still accepted for reads during lazy rotation).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EncryptionConfig {
    /// Whether cold-tier pages and checkpoint/backup artifacts are encrypted
    /// at rest. Enabling with no usable key material is a hard config error
    /// (SEC-010).
    pub enabled: bool,
    /// The label of the key fresh writes seal under (must be in `keys`).
    pub active_key_id: String,
    /// All known keys: the active one plus retired keys reads still accept
    /// (SEC-012). Order is irrelevant; the active key is chosen by id.
    pub keys: Vec<EncryptionKey>,
}

impl EncryptionConfig {
    /// Build the runtime [`Keyring`](crate::crypto::Keyring), or `None` when
    /// encryption is disabled. Enabling with no keys, an empty/unknown
    /// `active_key_id`, or malformed key material is rejected (SEC-010/011).
    pub fn keyring(&self) -> crate::error::Result<Option<crate::crypto::Keyring>> {
        use crate::crypto::{AtRestKey, Keyring};
        use crate::error::FluxumError;
        if !self.enabled {
            return Ok(None);
        }
        if self.keys.is_empty() {
            return Err(FluxumError::Config(
                "storage.encryption.enabled is true but no keys are configured (SEC-010)".into(),
            ));
        }
        if self.active_key_id.is_empty() {
            return Err(FluxumError::Config(
                "storage.encryption.active_key_id is required when encryption is enabled (SEC-010)"
                    .into(),
            ));
        }
        let mut active = None;
        let mut previous = Vec::new();
        for key in &self.keys {
            let parsed = AtRestKey::from_hex(&key.id, key.key_hex.expose_str())?;
            if key.id == self.active_key_id {
                active = Some(parsed);
            } else {
                previous.push(parsed);
            }
        }
        let active = active.ok_or_else(|| {
            FluxumError::Config(format!(
                "storage.encryption.active_key_id `{}` names no configured key (SEC-010)",
                self.active_key_id
            ))
        })?;
        Ok(Some(Keyring::new(active, previous)))
    }
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
    /// At-rest encryption keyring (SPEC-026 SEC-010; disabled by default).
    pub encryption: EncryptionConfig,
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
            encryption: EncryptionConfig::default(),
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
    /// Commit-log segment archival (SPEC-014 REP-062) — the PITR source.
    pub archive: ArchiveConfig,
}

/// Commit-log segment archival (SPEC-014 REP-062): with `enabled`, a segment
/// is copied durably to `dir` **before** checkpoint-driven truncation may
/// delete it, and archived copies are retained for `retention` — which is
/// therefore the PITR window (§9). Archival is asynchronous off the
/// checkpoint worker; a failed copy blocks segment deletion, never writes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ArchiveConfig {
    /// Whether segments are archived before truncation (default `true` —
    /// backups and PITR depend on it).
    pub enabled: bool,
    /// The archive directory.
    pub dir: PathBuf,
    /// Retention window: `<n>s | <n>m | <n>h | <n>d` (default `7d`). Equals
    /// the PITR window.
    pub retention: String,
    /// Remote (S3-compatible) archival target (SPEC-025 OPS-010/011).
    pub remote: RemoteArchiveConfig,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: PathBuf::from("./data/archive"),
            retention: "7d".to_owned(),
            remote: RemoteArchiveConfig::default(),
        }
    }
}

/// S3-compatible remote archival (SPEC-025 OPS-010/011): when enabled, the
/// checkpoint worker incrementally uploads checkpoint objects/manifests and
/// freshly archived segments (seekable-zstd) after each pass — never on the
/// write path — and `fluxum backup create --remote` / `restore --remote`
/// use the same target.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RemoteArchiveConfig {
    /// Whether remote archival is on (default `false`).
    pub enabled: bool,
    /// `http(s)://host[:port]` of the S3-compatible endpoint.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Key prefix inside the bucket (default `fluxum`).
    pub prefix: String,
    /// SigV4 region (any consistent value for non-AWS services).
    pub region: String,
    /// Access key id.
    pub access_key: String,
    /// Secret access key; supports `${VAR}` env expansion in the YAML file
    /// (SEC-058: redacted, zeroized).
    pub secret_key: Option<crate::secret::Secret<String>>,
}

impl RemoteArchiveConfig {
    /// Effective prefix (`fluxum` when unset).
    pub fn effective_prefix(&self) -> &str {
        if self.prefix.is_empty() {
            "fluxum"
        } else {
            &self.prefix
        }
    }

    /// Effective region (`us-east-1` when unset — S3-compatible services
    /// accept any consistent value).
    pub fn effective_region(&self) -> &str {
        if self.region.is_empty() {
            "us-east-1"
        } else {
            &self.region
        }
    }
}

impl ArchiveConfig {
    /// The parsed retention window.
    ///
    /// # Errors
    /// A retention string not of the form `<n>s|<n>m|<n>h|<n>d`.
    pub fn retention_duration(&self) -> Result<std::time::Duration> {
        parse_retention(&self.retention).ok_or_else(|| {
            FluxumError::config(format!(
                "replication.archive.retention: `{}` is not <n>s, <n>m, <n>h, or <n>d",
                self.retention
            ))
        })
    }
}

/// Parse a retention window: an integer followed by `s`, `m`, `h`, or `d`.
fn parse_retention(text: &str) -> Option<std::time::Duration> {
    let text = text.trim();
    let (number, unit) = text.split_at(text.len().checked_sub(1)?);
    let n: u64 = number.parse().ok()?;
    let seconds = match unit {
        "s" => n,
        "m" => n.checked_mul(60)?,
        "h" => n.checked_mul(3_600)?,
        "d" => n.checked_mul(86_400)?,
        _ => return None,
    };
    Some(std::time::Duration::from_secs(seconds))
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
    /// Shared token the peer authenticates with (SEC-058: redacted, zeroized).
    pub token: crate::secret::Secret<String>,
}

/// Default cap on permissive-provider identities (SEC-062): generous, so dev
/// never notices, bounded so it cannot multiply identities without limit.
fn default_max_permissive_identities() -> u32 {
    10_000
}

/// Authentication configuration (SPEC-009).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// Provider kind.
    pub provider: AuthProvider,
    /// Provider secret (`token`: the token; `jwt`: verification key).
    /// Supports `${VAR}` env expansion in the YAML file (SEC-058: redacted,
    /// zeroized).
    pub secret: Option<crate::secret::Secret<String>>,
    /// JWT signature algorithm (`provider: jwt`), default `hs256`
    /// (symmetric). An **asymmetric** choice (`rs256`/`es256`/`ed25519`) is
    /// verify-only: `jwt_public_key` holds the verification key and the DB
    /// never stores token-minting material — a DB compromise cannot forge
    /// tokens (SEC-061, F-019). Ignored for non-`jwt` providers.
    pub jwt_algorithm: JwtAlgorithm,
    /// PEM public key for an asymmetric `jwt_algorithm` (SEC-061): required
    /// then, ignored otherwise. Not a secret (a public key), so it is not
    /// redacted.
    pub jwt_public_key: Option<PathBuf>,
    /// Cap on distinct identities the permissive `none` provider will mint
    /// (SEC-062, F-020): beyond it a *new* identity is refused (already-seen
    /// ones keep working), so permissive dev auth cannot be used to multiply
    /// identities without limit. `0` = unbounded. Ignored for other
    /// providers. `none` is loopback-only regardless (AUTH-040).
    #[serde(default = "default_max_permissive_identities")]
    pub max_permissive_identities: u32,
    /// Trusted server peers.
    pub server_peers: Vec<ServerPeer>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            provider: AuthProvider::default(),
            secret: None,
            jwt_algorithm: JwtAlgorithm::default(),
            jwt_public_key: None,
            max_permissive_identities: default_max_permissive_identities(),
            server_peers: Vec::new(),
        }
    }
}

/// JWT signature algorithm (SPEC-009 SEC-061). `Hs256` is symmetric (the DB
/// holds the shared secret and can mint); the rest are asymmetric verify-only
/// (the DB holds only the public key).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JwtAlgorithm {
    /// HMAC-SHA256, symmetric (default; the current behavior).
    #[default]
    Hs256,
    /// RSA PKCS#1 v1.5 SHA-256, asymmetric verify-only.
    Rs256,
    /// ECDSA P-256 SHA-256, asymmetric verify-only.
    Es256,
    /// Ed25519 EdDSA, asymmetric verify-only.
    Ed25519,
}

impl JwtAlgorithm {
    /// Whether the algorithm is asymmetric (verify-only, needs a public key).
    pub fn is_asymmetric(self) -> bool {
        !matches!(self, Self::Hs256)
    }
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

/// Reducer admission and execution tuning (SPEC-004 §7, SPEC-026 SEC-046).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ReducerConfig {
    /// RED-052 global shard guard: total client reducer admissions per
    /// second before excess calls answer `503 "shard overloaded"`.
    /// **Mandatory-on** (SEC-046, F-015): `0` is rejected at load — a shard
    /// with a single writer must always carry an aggregate admission bound.
    pub shard_max_reducers_per_sec: u64,
    /// SEC-046: cooperative execution deadline for one client reducer call,
    /// milliseconds, polled at every host-call boundary; breach → rollback.
    /// `0` disables. Default 10 s — generous; a reducer holding the single
    /// writer that long is pathological.
    pub max_execution_ms: u64,
    /// SEC-046: ceiling on the estimated bytes one reducer transaction may
    /// buffer through inserts/upserts; breach → rollback. `0` disables.
    pub max_tx_bytes: ByteSize,
}

impl Default for ReducerConfig {
    fn default() -> Self {
        Self {
            shard_max_reducers_per_sec:
                crate::reducer::RateLimiterOptions::DEFAULT_SHARD_MAX_REDUCERS_PER_SEC,
            max_execution_ms: 10_000,
            max_tx_bytes: ByteSize(512 << 20),
        }
    }
}

/// How a `LIMIT` above `query.max_limit` is treated (SEC-045).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LimitAction {
    /// Clamp to `max_limit` (default: bounded results, no client breakage).
    #[default]
    Clamp,
    /// Refuse the query with a wire-ready 3030.
    Reject,
}

/// Query execution bounds and admission rates (SPEC-026 SEC-045/047).
///
/// Every `0` disables that bound; the defaults are **generous** — sized so a
/// legitimate workload never notices, while a single caller can no longer
/// pin the snapshot evaluator or register queries without limit.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct QueryConfig {
    /// Applied to queries that carry no `LIMIT` (`0` = none — today's
    /// semantics; set it to bound implicit full-table snapshots).
    pub default_limit: u32,
    /// Ceiling on any effective `LIMIT` (`0` = unbounded).
    pub max_limit: u32,
    /// Clamp or reject a `LIMIT` above `max_limit`.
    pub max_limit_action: LimitAction,
    /// Rows the snapshot evaluator may touch per query before aborting with
    /// 3031 (`0` = no budget).
    pub row_scan_budget: u64,
    /// Wall-clock evaluation deadline per query, milliseconds; breach
    /// aborts with 3032 (`0` = none).
    pub deadline_ms: u64,
    /// SEC-047: subscription registrations + one-off queries per second per
    /// caller identity (`0` = off). Server peers are exempt (AUTH-062).
    pub max_queries_per_sec_per_identity: u64,
    /// SEC-047: the source-keyed secondary bucket (resolved client IP, or
    /// connection id where none exists) — token rotation cannot refill it
    /// (`0` = off).
    pub max_queries_per_sec_per_source: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            default_limit: 0,
            max_limit: 1_000_000,
            max_limit_action: LimitAction::default(),
            row_scan_budget: 10_000_000,
            deadline_ms: 5_000,
            max_queries_per_sec_per_identity: 500,
            max_queries_per_sec_per_source: 2_000,
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
    /// Reducer admission and execution tuning.
    pub reducer: ReducerConfig,
    /// Query execution bounds and admission rates (SEC-045/047).
    pub query: QueryConfig,
    /// Observability thresholds.
    pub observability: ObservabilityConfig,
    /// Logging.
    pub logging: LoggingConfig,
    /// Field-level crypto keys for column transforms (SPEC-017 §5).
    pub transforms: TransformsConfig,
    /// Plugin manifest (SPEC-020 PLG-032): validated by
    /// `PluginRegistry::build` at assembly — capability exists, placement
    /// legal for the host, in-proc feature compiled, applies_to targets
    /// exist. Any violation aborts startup.
    pub plugins: Vec<PluginDecl>,
    /// Provenance of every non-default key (`key.path` → source).
    #[serde(skip)]
    pub sources: BTreeMap<String, ValueSource>,
}

/// One `plugins:` manifest entry (SPEC-020 PLG-032).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginDecl {
    /// The plugin's name — for an in-process plugin, the link-time
    /// registered name; unique across the manifest.
    pub name: String,
    /// The bound capability (`score_reranker`, `retriever`, `fusion`,
    /// `stream_sink`, …) — the set is closed (PLG-003).
    pub capability: String,
    /// Hosting mode.
    pub host: PluginHost,
    /// The tables/columns the plugin applies to (empty = unscoped).
    #[serde(default)]
    pub applies_to: PluginScope,
}

/// How a plugin is hosted (PLG-030/031).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginHost {
    /// Compiled into the binary behind a Cargo feature (PLG-030).
    InProcess {
        /// The gating feature name (documentation/introspection; the gate
        /// itself is whether the plugin's link-time def exists).
        #[serde(default)]
        feature: String,
    },
    /// A separate process called over Plugin RPC (PLG-031). Never legal
    /// for a WritePath capability (PLG-021).
    Sidecar {
        /// The sidecar endpoint (`host:port`).
        endpoint: String,
        /// Per-call timeout in milliseconds (ReadPath/OffPath calls).
        #[serde(default = "default_plugin_timeout_ms")]
        timeout_ms: u64,
        /// Shared secret the sidecar authenticates this host with (PLG-031),
        /// sent in the Plugin RPC handshake. `None` leaves the sidecar
        /// unauthenticated — only appropriate for a loopback/same-pod
        /// endpoint. Never logged or reported by `GET /plugins` (SEC-058:
        /// redacted, zeroized).
        #[serde(default)]
        token: Option<crate::secret::Secret<String>>,
    },
}

/// Default sidecar per-call timeout (PLG-031).
fn default_plugin_timeout_ms() -> u64 {
    50
}

/// The `applies_to` scope of a plugin binding (PLG-032).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PluginScope {
    /// Table struct names the plugin applies to.
    pub tables: Vec<String>,
    /// Column names within those tables (requires `tables`).
    pub columns: Vec<String>,
}

/// Named cryptographic keys for column transforms (SPEC-017 CT-035): the
/// `#[encrypted(ecies, key = "…")]` / `#[signed(…)]` executors resolve their
/// key by id against this set. Config-embedded key material is the baseline;
/// `FLUXUM_*` env injection overrides individual fields like any other key.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TransformsConfig {
    /// The declared keys, by id.
    pub keys: Vec<TransformKey>,
}

/// The key scheme (CT-030/033).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyScheme {
    /// X25519 recipient key for `#[encrypted(ecies)]`.
    X25519,
    /// Ed25519 signing key for `#[signed(ed25519)]`.
    Ed25519,
}

/// One named transform key (CT-035). `secret` is the 32-byte key as 64 hex
/// characters; `previous` holds retired secrets still accepted for reads
/// during rotation (CT-036).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformKey {
    /// Stable key label referenced by `key = "…"` in the attribute.
    pub id: String,
    /// The key scheme.
    pub scheme: KeyScheme,
    /// The active 32-byte secret as 64 hex characters (SEC-058: redacted,
    /// zeroized).
    pub secret: crate::secret::Secret<String>,
    /// Retired secrets (hex) still accepted for reads (rotation, CT-036).
    #[serde(default)]
    pub previous: Vec<crate::secret::Secret<String>>,
}

impl TransformsConfig {
    /// Build the X25519 ECIES key set (CT-030/035), keyed by id. Malformed
    /// key material or a duplicate id is a hard config error.
    pub fn ecies_keys(
        &self,
    ) -> crate::error::Result<std::collections::HashMap<String, crate::transform::crypto::EciesKey>>
    {
        use crate::error::FluxumError;
        use crate::transform::crypto::EciesKey;
        let mut out = std::collections::HashMap::new();
        for key in &self.keys {
            if key.scheme != KeyScheme::X25519 {
                continue; // ed25519 signing keys are resolved by the sign executor
            }
            if out.contains_key(&key.id) {
                return Err(FluxumError::Config(format!(
                    "duplicate transform key id `{}` (CT-035)",
                    key.id
                )));
            }
            let previous: Vec<String> = key
                .previous
                .iter()
                .map(|s| s.expose_str().to_owned())
                .collect();
            let ecies = EciesKey::from_hex(&key.id, key.secret.expose_str(), &previous)?;
            out.insert(key.id.clone(), ecies);
        }
        Ok(out)
    }

    /// Build the Ed25519 signing key set (CT-033/035), keyed by id. A
    /// `#[signed(ed25519, by = server)]` column signs with the key whose id is
    /// `server`. Malformed material or a duplicate id is a hard error.
    pub fn ed25519_keys(
        &self,
    ) -> crate::error::Result<std::collections::HashMap<String, crate::transform::crypto::SignKey>>
    {
        use crate::error::FluxumError;
        use crate::transform::crypto::SignKey;
        let mut out = std::collections::HashMap::new();
        for key in &self.keys {
            if key.scheme != KeyScheme::Ed25519 {
                continue;
            }
            if out.contains_key(&key.id) {
                return Err(FluxumError::Config(format!(
                    "duplicate transform key id `{}` (CT-035)",
                    key.id
                )));
            }
            out.insert(
                key.id.clone(),
                SignKey::from_hex(&key.id, key.secret.expose_str())?,
            );
        }
        Ok(out)
    }
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

// --- Hot reload (SPEC-025 §5, OPS-040/041) --------------------------------------

/// The config keys a running server can adopt without a restart (OPS-040).
///
/// This is an **allowlist, and deliberately so**: a key is reloadable only
/// by appearing here, so every key that exists now — and every key added
/// later — is non-reloadable until someone has thought about what changing
/// it under live traffic would do. The failure mode of forgetting to add a
/// key is a rejected reload (loud, harmless); the failure mode of an
/// opt-out list would be silently hot-swapping something like a storage
/// path (quiet, corrupting).
/// Every entry must be a real leaf path of [`Config`] — an entry naming a
/// key that does not exist would silently never match, quietly making the
/// key it was meant to free non-reloadable forever.
/// `reloadable_keys_all_exist` pins that.
pub const RELOADABLE_KEYS: &[&str] = &[
    "logging.level",
    "logging.format",
    "server.trusted_proxies",
    "server.connection_limits.blocklist",
    "server.connection_limits.allowlist",
    "server.connection_limits.max_total_conns",
    "server.admin.trusted",
    "server.admin.require_operator",
    "server.admin.open_health_metrics",
    "observability.slow_reducer_threshold_us",
    "reducer.shard_max_reducers_per_sec",
    "reducer.max_execution_ms",
    "reducer.max_tx_bytes",
    "query.default_limit",
    "query.max_limit",
    "query.max_limit_action",
    "query.row_scan_budget",
    "query.deadline_ms",
    "query.max_queries_per_sec_per_identity",
    "query.max_queries_per_sec_per_source",
    "subscriptions.send_buffer_bytes",
];

/// Whether `key` (a dotted path) may change on reload (OPS-040).
pub fn is_reloadable(key: &str) -> bool {
    RELOADABLE_KEYS.contains(&key)
}

/// The dotted key paths whose values differ between two configs.
fn changed_keys(old: &Config, new: &Config) -> Result<Vec<String>> {
    let old_value = serde_yaml::to_value(old)?;
    let new_value = serde_yaml::to_value(new)?;
    let mut paths = Vec::new();
    collect_leaf_paths(&new_value, &mut Vec::new(), &mut paths);
    // Union with the old side's paths, so a key that only exists on one side
    // still registers as a change rather than being skipped.
    let mut old_paths = Vec::new();
    collect_leaf_paths(&old_value, &mut Vec::new(), &mut old_paths);
    for path in old_paths {
        if !paths.contains(&path) {
            paths.push(path);
        }
    }

    let mut changed = Vec::new();
    for path in paths {
        if get_path(&old_value, &path) != get_path(&new_value, &path) {
            changed.push(path.join("."));
        }
    }
    changed.sort();
    Ok(changed)
}

/// Read the value at a dotted path, if present.
fn get_path<'v>(value: &'v Value, path: &[String]) -> Option<&'v Value> {
    let mut cursor = value;
    for segment in path {
        cursor = cursor.get(Value::String(segment.clone()))?;
    }
    Some(cursor)
}

/// A validated reload (OPS-040): the new config plus exactly which
/// reloadable keys changed, so the caller republishes only those and can log
/// what an operator actually changed.
#[derive(Debug, Clone)]
pub struct Reload {
    /// The freshly loaded configuration.
    pub config: Config,
    /// Reloadable keys whose values changed, dotted and sorted. Empty means
    /// the reload was a no-op — still a success, not an error.
    pub changed: Vec<String>,
}

impl Config {
    /// Re-read `path` + env through the same layered loader and validate the
    /// result against this (running) config for hot reload (OPS-040/041).
    ///
    /// Reload is **all-or-nothing**: if any non-reloadable key (a port, a
    /// storage path, the shard count) differs, this returns an error naming
    /// every offending key and applies nothing (OPS-041). The caller's
    /// running config is untouched — it is `&self`, and the new config only
    /// escapes inside `Ok`.
    ///
    /// # Errors
    /// The loader's own errors (unreadable file, malformed YAML, failed
    /// validation), or a `config` error listing changed non-reloadable keys.
    pub fn reload(&self, path: Option<&Path>) -> Result<Reload> {
        self.reload_with(path, &|key| std::env::var(key).ok())
    }

    /// [`Config::reload`] with an injected environment, for tests.
    pub fn reload_with(&self, path: Option<&Path>, env: EnvLookup<'_>) -> Result<Reload> {
        let candidate = Config::load_with(path, env)?;
        let changed = changed_keys(self, &candidate)?;
        let (reloadable, frozen): (Vec<String>, Vec<String>) =
            changed.into_iter().partition(|key| is_reloadable(key));

        if !frozen.is_empty() {
            // OPS-041: name every offending key at once — an operator
            // fixing them one error at a time is a worse deploy.
            return Err(FluxumError::config(format!(
                "reload rejected: these keys cannot change at runtime: {}. \
                 Restart to apply them. Reloadable keys: {}",
                frozen.join(", "),
                RELOADABLE_KEYS.join(", ")
            )));
        }
        Ok(Reload {
            config: candidate,
            changed: reloadable,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod reload_tests {
    use super::*;

    /// Write a config file and return its path (kept alive by the dir).
    /// Write a config file under the `development` profile (the default
    /// `production` profile requires an auth secret, which is orthogonal to
    /// what these tests are about).
    fn write(dir: &tempfile::TempDir, text: &str) -> std::path::PathBuf {
        let path = dir.path().join("config.yml");
        std::fs::write(
            &path,
            format!(
                "profile: development
{text}"
            ),
        )
        .unwrap();
        path
    }
    fn no_env(_key: &str) -> Option<String> {
        None
    }

    #[test]
    fn reloadable_keys_all_exist() {
        // An allowlist entry naming a key that does not exist would never
        // match, silently freezing the key it was meant to free.
        let value = serde_yaml::to_value(Config::default()).unwrap();
        for key in RELOADABLE_KEYS {
            let path: Vec<String> = key.split('.').map(str::to_owned).collect();
            assert!(
                get_path(&value, &path).is_some(),
                "RELOADABLE_KEYS names '{key}', which is not a real Config path"
            );
        }
    }

    #[test]
    fn raising_the_log_level_is_accepted_and_reported() {
        let dir = tempfile::tempdir().unwrap();
        let running =
            Config::load_with(Some(&write(&dir, "logging:\n  level: info\n")), &no_env).unwrap();
        assert_eq!(running.logging.level, "info");

        // The operator raises verbosity and reloads (OPS-040).
        let path = write(&dir, "logging:\n  level: debug\n");
        let reload = running.reload_with(Some(&path), &no_env).unwrap();
        assert_eq!(reload.config.logging.level, "debug");
        assert_eq!(
            reload.changed,
            vec!["logging.level"],
            "exactly what changed, so the caller republishes only that"
        );
        // The running config is untouched — the new one only escapes in Ok.
        assert_eq!(running.logging.level, "info");
    }

    #[test]
    fn a_changed_port_is_rejected_and_nothing_is_applied() {
        let dir = tempfile::tempdir().unwrap();
        let running =
            Config::load_with(Some(&write(&dir, "logging:\n  level: info\n")), &no_env).unwrap();
        let original_port = running.server.tcp_port;

        // A port change alongside a legitimately reloadable one: the whole
        // reload must fail, not partially apply the good half (OPS-041).
        let path = write(
            &dir,
            &format!(
                "logging:\n  level: debug\nserver:\n  tcp_port: {}\n",
                original_port + 1
            ),
        );
        let err = running.reload_with(Some(&path), &no_env).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("server.tcp_port"),
            "the error names the offending key: {message}"
        );
        assert!(
            message.contains("Restart to apply"),
            "and says what to do about it: {message}"
        );
        // Nothing applied: the running config kept BOTH values, including
        // the reloadable one that shared the rejected reload.
        assert_eq!(running.server.tcp_port, original_port);
        assert_eq!(running.logging.level, "info", "no partial apply");
    }

    #[test]
    fn every_changed_non_reloadable_key_is_named_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let running = Config::load_with(Some(&write(&dir, "")), &no_env).unwrap();
        let path = write(&dir, "server:\n  tcp_port: 19999\nsharding:\n  shards: 8\n");
        let message = running
            .reload_with(Some(&path), &no_env)
            .unwrap_err()
            .to_string();
        // An operator fixing these one error at a time is a worse deploy.
        assert!(message.contains("server.tcp_port"), "{message}");
        assert!(message.contains("sharding.shards"), "{message}");
    }

    #[test]
    fn an_unchanged_reload_is_a_no_op_success() {
        let dir = tempfile::tempdir().unwrap();
        let text = "logging:\n  level: warn\n";
        let running = Config::load_with(Some(&write(&dir, text)), &no_env).unwrap();
        // Re-reading identical config is a success with nothing to publish —
        // a SIGHUP with no edit must not be an error.
        let reload = running
            .reload_with(Some(&write(&dir, text)), &no_env)
            .unwrap();
        assert!(reload.changed.is_empty());
        assert_eq!(reload.config.logging.level, "warn");
    }

    #[test]
    fn env_overrides_ride_the_reload_like_any_other_layer() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, "logging:\n  level: info\n");
        let running = Config::load_with(Some(&path), &no_env).unwrap();

        // OBS-080 precedence still holds on reload: env beats file.
        let with_env = |key: &str| -> Option<String> {
            (key == "FLUXUM_LOGGING_LEVEL").then(|| "trace".to_owned())
        };
        let reload = running.reload_with(Some(&path), &with_env).unwrap();
        assert_eq!(reload.config.logging.level, "trace");
        assert_eq!(reload.changed, vec!["logging.level"]);
    }

    #[test]
    fn a_new_key_is_non_reloadable_until_someone_says_otherwise() {
        // The allowlist is the whole classification: anything absent from it
        // is frozen. This pins the fail-safe direction — the cost of
        // forgetting a key is a loud rejection, not a silent hot-swap.
        assert!(is_reloadable("logging.level"));
        assert!(!is_reloadable("storage.data_dir"));
        assert!(!is_reloadable("sharding.shards"));
        assert!(!is_reloadable("a.key.nobody.has.classified"));
    }
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
}
