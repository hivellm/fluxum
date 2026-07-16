//! Boot-time hardware probe and adaptive-default derivation (SPEC-016).
//!
//! Probe once into an immutable [`HardwareProfile`] (HWA-001), derive every
//! adaptive default centrally via [`derive`] (HWA-011), and report the result
//! as a loggable [`EffectiveConfig`] (HWA-012). Consumers read the profile
//! API only — no direct cgroup/sysinfo calls outside this module (HWA-003).

pub mod cgroup;

use serde::{Deserialize, Serialize};

use crate::config::{AutoOr, ByteSize, Config, SimdMode, ValueSource};
use crate::error::{FluxumError, Result};

/// Conservative fallback when a memory probe fails entirely (HWA-004).
const FALLBACK_MEMORY: u64 = 512 << 20;

/// Bounds for the commit-log write buffer `auto` derivation (SPEC-016 §3).
const WRITE_BUFFER_MIN: u64 = 64 << 10;
const WRITE_BUFFER_MAX: u64 = 4 << 20;

/// Immutable snapshot of the machine, captured once at boot (HWA-001).
///
/// Container-aware: when cgroup limits are present, the `effective_*`
/// accessors report the limits, not the host totals (HWA-002).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HardwareProfile {
    /// Logical CPU cores visible to the process.
    pub logical_cores: usize,
    /// Physical CPU cores.
    pub physical_cores: usize,
    /// Total system RAM in bytes.
    pub total_ram_bytes: u64,
    /// Currently available RAM in bytes.
    pub available_ram_bytes: u64,
    /// cgroup CPU quota in cores, when limited (v2 `cpu.max` / v1 cfs quota).
    pub cgroup_cpu_quota: Option<f64>,
    /// cgroup memory limit in bytes, when limited.
    pub cgroup_memory_limit_bytes: Option<u64>,
}

impl HardwareProfile {
    /// Probe the machine. Runs exactly once per process lifetime by contract
    /// (HWA-006); never panics — each undeterminable value gets a documented
    /// conservative fallback and a `WARN` (HWA-004).
    pub fn probe() -> Self {
        let logical_cores = match std::thread::available_parallelism() {
            Ok(n) => n.get(),
            Err(e) => {
                tracing::warn!(
                    target: "fluxum::hw",
                    "logical core probe failed ({e}); falling back to 1 core"
                );
                1
            }
        };
        let physical_cores = sysinfo::System::physical_core_count().unwrap_or_else(|| {
            tracing::warn!(
                target: "fluxum::hw",
                "physical core probe failed; falling back to logical count ({logical_cores})"
            );
            logical_cores
        });

        let sys = sysinfo::System::new_with_specifics(
            sysinfo::RefreshKind::nothing().with_memory(sysinfo::MemoryRefreshKind::everything()),
        );
        let mut total_ram_bytes = sys.total_memory();
        if total_ram_bytes == 0 {
            tracing::warn!(
                target: "fluxum::hw",
                "total RAM probe failed; falling back to {}",
                ByteSize(FALLBACK_MEMORY)
            );
            total_ram_bytes = FALLBACK_MEMORY;
        }
        let mut available_ram_bytes = sys.available_memory();
        if available_ram_bytes == 0 || available_ram_bytes > total_ram_bytes {
            available_ram_bytes = total_ram_bytes;
        }

        let limits = cgroup::read_limits();

        Self {
            logical_cores,
            physical_cores,
            total_ram_bytes,
            available_ram_bytes,
            cgroup_cpu_quota: limits.cpu_quota,
            cgroup_memory_limit_bytes: limits.memory_limit_bytes,
        }
    }

    /// Effective CPU count (HWA-002):
    /// `max(1, min(logical_cores, ceil(cpu_quota)))` under a quota,
    /// `logical_cores` otherwise.
    pub fn effective_cores(&self) -> usize {
        let logical = self.logical_cores.max(1);
        match self.cgroup_cpu_quota {
            Some(quota) if quota > 0.0 => logical.min((quota.ceil() as usize).max(1)),
            _ => logical,
        }
    }

    /// Effective memory (HWA-002): `min(total_ram, cgroup_memory_limit)`
    /// under a limit, `total_ram` otherwise.
    pub fn effective_memory_bytes(&self) -> u64 {
        match self.cgroup_memory_limit_bytes {
            Some(limit) => self.total_ram_bytes.min(limit),
            None => self.total_ram_bytes,
        }
    }
}

/// Where an effective value came from (HWA-012): derived (`auto`), pinned in
/// the config file / profile, or pinned by a `FLUXUM_*` env var.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provenance {
    /// Derived from the hardware profile.
    Auto,
    /// Pinned in the config file (or by the profile layer).
    Config,
    /// Pinned by a `FLUXUM_*` environment variable.
    Env,
}

/// An effective value together with its provenance.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Derived<T> {
    /// The effective value.
    pub value: T,
    /// Where it came from.
    pub source: Provenance,
}

impl<T> Derived<T> {
    const fn new(value: T, source: Provenance) -> Self {
        Self { value, source }
    }
}

/// Config plus derived defaults: every adaptive value with its provenance,
/// plus the probe inputs. Serializable for the boot event (HWA-012) and
/// `GET /health` (HWA-013).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectiveConfig {
    /// Probe inputs.
    pub hardware: HardwareProfile,
    /// Tokio worker threads (`runtime.worker_threads`).
    pub worker_threads: Derived<usize>,
    /// Shard count (`sharding.shards`).
    pub shards: Derived<u32>,
    /// Memory budget in bytes (`memory.budget`, SPEC-015 TIER-002).
    pub memory_budget_bytes: Derived<u64>,
    /// Buffer-pool capacity: `memory.bufferpool_fraction × budget`
    /// (SPEC-015 TIER-003; reported for TIER-005 budget transparency).
    pub bufferpool_capacity_bytes: Derived<u64>,
    /// Subscription fan-out concurrency (`subscriptions.fanout_concurrency`).
    pub fanout_concurrency: Derived<usize>,
    /// Commit-log write buffer (`storage.commit_log_write_buffer_bytes`).
    pub commit_log_write_buffer_bytes: Derived<u64>,
    /// Checkpoint cadence (`storage.checkpoint_interval_tx`).
    pub checkpoint_interval_tx: Derived<u64>,
    /// SIMD tier selection mode (`simd`, HWA-032).
    pub simd: Derived<SimdMode>,
    /// Per-kernel SIMD selection resolved for this CPU (HWA-033, T2.10).
    pub simd_kernels: Derived<crate::simd::Selection>,
}

impl EffectiveConfig {
    /// Emit the single structured `effective configuration` boot event
    /// (HWA-012): probe inputs plus every derived value with its source.
    pub fn emit_boot_event(&self) {
        match serde_json::to_string(self) {
            Ok(json) => tracing::info!(
                target: "fluxum::boot",
                event = "effective_configuration",
                effective = %json
            ),
            Err(e) => tracing::error!(
                target: "fluxum::boot",
                "failed to serialize effective configuration: {e}"
            ),
        }
    }
}

/// Map a loader [`ValueSource`] onto the HWA-012 vocabulary for a key whose
/// value was explicitly pinned.
fn pinned(config: &Config, key: &str) -> Provenance {
    match config.source_of(key) {
        ValueSource::Env => Provenance::Env,
        ValueSource::File | ValueSource::Profile => Provenance::Config,
        ValueSource::Default => Provenance::Auto,
    }
}

/// Resolve one `auto | value` key (HWA-010): an explicit value always wins;
/// when it exceeds the detected hardware, `over_hardware` triggers a `WARN`
/// but the operator's choice stands.
fn resolve<T, F>(
    config: &Config,
    key: &str,
    value: &AutoOr<T>,
    auto: F,
    over_hardware: impl FnOnce(&T) -> bool,
) -> Derived<T>
where
    T: Copy + std::fmt::Display,
    F: FnOnce() -> T,
{
    match value {
        AutoOr::Auto => Derived::new(auto(), Provenance::Auto),
        AutoOr::Value(v) => {
            if over_hardware(v) {
                tracing::warn!(
                    target: "fluxum::hw",
                    "{key} = {v} exceeds detected hardware; honoring the explicit value (HWA-010)"
                );
            }
            Derived::new(*v, pinned(config, key))
        }
    }
}

/// Pure derivation of every adaptive default (HWA-011): a function of one
/// immutable [`HardwareProfile`] and the loaded [`Config`], unit-testable
/// with synthetic profiles. Fails only on an HWA-015 memory shortfall or a
/// forced SIMD tier the running CPU does not support (HWA-032).
pub fn derive(hardware: &HardwareProfile, config: &Config) -> Result<EffectiveConfig> {
    let cores = hardware.effective_cores();
    let memory = hardware.effective_memory_bytes();

    let worker_threads = resolve(
        config,
        "runtime.worker_threads",
        &config.runtime.worker_threads,
        || cores.max(1),
        |&v| v > hardware.logical_cores,
    );

    let shards = resolve(
        config,
        "sharding.shards",
        &config.sharding.shards,
        || u32::try_from((cores / 2).clamp(1, 16)).unwrap_or(16),
        |&v| v as usize > cores,
    );

    let auto_floor = config.memory.auto_floor_bytes.as_u64();
    let auto_budget = auto_floor.max((config.memory.auto_fraction * memory as f64) as u64);
    let budget = resolve(
        config,
        "memory.budget",
        &config.memory.budget,
        || ByteSize(auto_budget),
        |v| v.as_u64() > memory,
    );

    let fanout_concurrency = resolve(
        config,
        "subscriptions.fanout_concurrency",
        &config.subscriptions.fanout_concurrency,
        || (2 * cores).clamp(2, 64),
        |&v| v > 2 * cores,
    );

    let write_buffer = resolve(
        config,
        "storage.commit_log_write_buffer_bytes",
        &config.storage.commit_log_write_buffer_bytes,
        || ByteSize((memory / 1024).clamp(WRITE_BUFFER_MIN, WRITE_BUFFER_MAX)),
        |v| v.as_u64() > memory,
    );

    // HWA-015: the auto derivation must fit the effective memory — fail boot
    // with the shortfall named rather than silently oversubscribe. (An
    // explicit over-budget value is the operator's call and only warns.)
    if config.memory.budget.is_auto() {
        let fixed = budget.value.as_u64() + write_buffer.value.as_u64();
        if fixed > memory {
            return Err(FluxumError::hardware(format!(
                "derived memory budget {} + write buffer {} exceeds effective memory {} \
                 (HWA-015); lower memory.auto_floor_bytes or raise the container limit",
                budget.value,
                write_buffer.value,
                ByteSize(memory)
            )));
        }
    }

    // SIMD dispatch resolution (SPEC-016 §5): builds the dispatch table for
    // the real CPU (detection + selection + HWA-055 self-check) so the boot
    // event reports the per-kernel tiers actually in use (HWA-033). A forced
    // tier the CPU does not support is a boot-abort error (HWA-032).
    let simd = Derived::new(config.simd, pinned(config, "simd"));
    let simd_kernels = Derived::new(
        crate::simd::Dispatch::new(config.simd)?.selection(),
        simd.source,
    );

    // TIER-003: the pool receives `bufferpool_fraction` of the budget; the
    // remainder is headroom for TxState, subscription buffers, and slack.
    let bufferpool_capacity =
        (config.memory.bufferpool_fraction * budget.value.as_u64() as f64) as u64;

    Ok(EffectiveConfig {
        hardware: hardware.clone(),
        worker_threads,
        shards,
        memory_budget_bytes: Derived::new(budget.value.as_u64(), budget.source),
        bufferpool_capacity_bytes: Derived::new(bufferpool_capacity, budget.source),
        fanout_concurrency,
        commit_log_write_buffer_bytes: Derived::new(
            write_buffer.value.as_u64(),
            write_buffer.source,
        ),
        checkpoint_interval_tx: Derived::new(
            config.storage.checkpoint_interval_tx,
            pinned(config, "storage.checkpoint_interval_tx"),
        ),
        simd,
        simd_kernels,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::Profile;

    /// SPEC-016 §2 example: 32-core host, cpu.max "150000 100000",
    /// memory.max 512 MiB.
    fn container_profile() -> HardwareProfile {
        HardwareProfile {
            logical_cores: 32,
            physical_cores: 16,
            total_ram_bytes: 128 << 30,
            available_ram_bytes: 96 << 30,
            cgroup_cpu_quota: Some(1.5),
            cgroup_memory_limit_bytes: Some(512 << 20),
        }
    }

    fn dev_config(env: &[(&str, &str)]) -> Config {
        let lookup = |key: &str| -> Option<String> {
            std::iter::once(("FLUXUM_PROFILE", "development"))
                .chain(env.iter().copied())
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_owned())
        };
        Config::load_with(None, &lookup).unwrap()
    }

    #[test]
    fn probe_returns_sane_values() {
        let hw = HardwareProfile::probe();
        assert!(hw.logical_cores >= 1);
        assert!(hw.physical_cores >= 1);
        assert!(hw.total_ram_bytes > 0);
        assert!(hw.available_ram_bytes > 0);
        assert!(hw.available_ram_bytes <= hw.total_ram_bytes);
        assert!(hw.effective_cores() >= 1);
        assert!(hw.effective_memory_bytes() > 0);
    }

    #[test]
    fn cgroup_limits_win_over_host_totals() {
        let hw = container_profile();
        // ceil(1.5) = 2 cores, not the host's 32 (SPEC-016 §2 example).
        assert_eq!(hw.effective_cores(), 2);
        // 512 MiB limit, not the host's 128 GiB.
        assert_eq!(hw.effective_memory_bytes(), 512 << 20);
    }

    #[test]
    fn absent_limits_fall_back_to_host_totals() {
        let hw = HardwareProfile {
            cgroup_cpu_quota: None,
            cgroup_memory_limit_bytes: None,
            ..container_profile()
        };
        assert_eq!(hw.effective_cores(), 32);
        assert_eq!(hw.effective_memory_bytes(), 128 << 30);
    }

    #[test]
    fn derives_spec_table_values_from_container_profile() {
        let effective = derive(&container_profile(), &dev_config(&[])).unwrap();
        assert_eq!(effective.worker_threads.value, 2);
        assert_eq!(effective.worker_threads.source, Provenance::Auto);
        // Profile pins shards=1 in development.
        assert_eq!(effective.shards.value, 1);
        assert_eq!(effective.shards.source, Provenance::Config);
        // TIER-002: max(128 MiB, 0.5 × 512 MiB) = 256 MiB (droplet reference).
        assert_eq!(effective.memory_budget_bytes.value, 256 << 20);
        assert_eq!(effective.memory_budget_bytes.source, Provenance::Auto);
        // TIER-003: pool capacity = 0.8 × 256 MiB.
        assert_eq!(
            effective.bufferpool_capacity_bytes.value,
            (0.8 * (256u64 << 20) as f64) as u64
        );
        // clamp(2 × 2, 2, 64) = 4.
        assert_eq!(effective.fanout_concurrency.value, 4);
        // clamp(512 MiB / 1024 = 512 KiB, 64 KiB, 4 MiB) = 512 KiB.
        assert_eq!(effective.commit_log_write_buffer_bytes.value, 512 << 10);
        assert_eq!(effective.checkpoint_interval_tx.value, 10_000);
        assert_eq!(effective.checkpoint_interval_tx.source, Provenance::Auto);
        assert_eq!(effective.simd.value, SimdMode::Auto);
    }

    #[test]
    fn derivation_scales_up_on_big_hardware() {
        let hw = HardwareProfile {
            cgroup_cpu_quota: None,
            cgroup_memory_limit_bytes: None,
            ..container_profile()
        };
        let mut config = dev_config(&[]);
        config.sharding.shards = AutoOr::Auto; // undo the dev-profile pin
        let effective = derive(&hw, &config).unwrap();
        assert_eq!(effective.worker_threads.value, 32);
        assert_eq!(effective.shards.value, 16, "clamp(32/2, 1, 16)");
        assert_eq!(effective.fanout_concurrency.value, 64, "clamp(64, 2, 64)");
        assert_eq!(
            effective.memory_budget_bytes.value,
            64 << 30,
            "0.5 × 128 GiB"
        );
        assert_eq!(
            effective.commit_log_write_buffer_bytes.value,
            4 << 20,
            "capped at 4 MiB"
        );
    }

    #[test]
    fn explicit_values_always_win_with_provenance() {
        let config = dev_config(&[
            ("FLUXUM_RUNTIME_WORKER_THREADS", "48"), // exceeds 32 logical → WARN, but stands
            ("FLUXUM_MEMORY_BUDGET", "2GiB"),        // exceeds 512 MiB limit → WARN, but stands
        ]);
        let effective = derive(&container_profile(), &config).unwrap();
        assert_eq!(effective.worker_threads.value, 48);
        assert_eq!(effective.worker_threads.source, Provenance::Env);
        assert_eq!(effective.memory_budget_bytes.value, 2 << 30);
        assert_eq!(effective.memory_budget_bytes.source, Provenance::Env);
    }

    #[test]
    fn over_hardware_fanout_and_write_buffer_values_still_stand() {
        // HWA-010: explicit values beyond the detected hardware only WARN.
        let config = dev_config(&[
            ("FLUXUM_SUBSCRIPTIONS_FANOUT_CONCURRENCY", "128"), // > 2 × 2 cores
            ("FLUXUM_STORAGE_COMMIT_LOG_WRITE_BUFFER_BYTES", "1GiB"), // > 512 MiB limit
            // Pin the budget so the HWA-015 auto-fit check does not apply:
            // explicit oversubscription is the operator's call.
            ("FLUXUM_MEMORY_BUDGET", "256MiB"),
        ]);
        let effective = derive(&container_profile(), &config).unwrap();
        assert_eq!(effective.fanout_concurrency.value, 128);
        assert_eq!(effective.fanout_concurrency.source, Provenance::Env);
        assert_eq!(effective.commit_log_write_buffer_bytes.value, 1 << 30);
        assert_eq!(
            effective.commit_log_write_buffer_bytes.source,
            Provenance::Env
        );
    }

    #[test]
    fn auto_derivation_that_cannot_fit_fails_boot() {
        let hw = HardwareProfile {
            total_ram_bytes: 64 << 20, // below the 128 MiB budget floor
            available_ram_bytes: 64 << 20,
            cgroup_cpu_quota: None,
            cgroup_memory_limit_bytes: None,
            logical_cores: 1,
            physical_cores: 1,
        };
        let err = derive(&hw, &dev_config(&[])).unwrap_err();
        assert!(matches!(err, FluxumError::Hardware(_)));
        assert!(err.to_string().contains("HWA-015"), "{err}");
    }

    #[test]
    fn effective_config_serializes_with_sources() {
        let config = dev_config(&[]);
        assert_eq!(config.profile, Profile::Development);
        let effective = derive(&container_profile(), &config).unwrap();
        let json = serde_json::to_string(&effective).unwrap();
        assert!(json.contains("\"logical_cores\":32"), "{json}");
        assert!(json.contains("\"source\":\"auto\""), "{json}");
        assert!(json.contains("\"source\":\"config\""), "{json}");
        // Round-trips (used by /health later).
        let back: EffectiveConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back, effective);
        // Emitting the boot event must not panic even with no subscriber.
        effective.emit_boot_event();
    }
}
