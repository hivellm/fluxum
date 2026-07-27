//! Hot reload (SPEC-025 §5, OPS-040/041): the reloadable-keys allowlist
//! and the diff/apply flow — split from `mod.rs` to honour the file-size
//! convention.

use super::load::collect_leaf_paths;
use super::*;

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
pub(super) fn get_path<'v>(value: &'v Value, path: &[String]) -> Option<&'v Value> {
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
