//! Per-tenant resource quotas (SPEC-025 §7, OPS-060/061): the ceilings that
//! keep one namespace from starving its neighbours.
//!
//! Namespaces (OPS-050) give tenants *separate* databases, but separate is
//! not the same as bounded: without ceilings one tenant can still saturate
//! the shared reducer path or eat the process's memory, and the isolation is
//! structural only. These quotas make it enforced.
//!
//! # What is bounded, and where it bites
//!
//! - **Reducer rate** — a per-namespace token bucket checked at admission,
//!   *above* the existing per-`(Identity, reducer)` limiter (SPEC-004
//!   RED-050): that one stops a single caller hammering one reducer, this one
//!   stops a tenant in aggregate. Exceeding it is a retryable 429, so a
//!   well-behaved client backs off instead of failing.
//! - **Subscriptions** — the live plan count of the tenant's own manager,
//!   checked before registering another. Read from the manager rather than
//!   counted alongside it, so the ceiling cannot drift out of step with
//!   reality as connections come and go.
//! - **Memory** — the tenant's estimated in-memory footprint, checked before
//!   admitting a write. Refusing the *write* is what protects the neighbours:
//!   no eviction is forced on anyone else's frames, because each namespace
//!   owns its own store.
//! - **Storage** — the tenant's durable commit-log bytes, likewise checked
//!   before a write. Sampled with a short cache, since it stats the log
//!   directory and a per-call stat would be a silly price for a ceiling that
//!   moves slowly.
//!
//! Every quota is optional; a namespace with none behaves exactly as an
//! unquotaed one always did. Exceeding one yields a typed error to the
//! offending tenant *only* — nothing here touches another namespace's
//! admission, latency, or memory.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fluxum_core::error::{FluxumError, Result};
use fluxum_protocol::codes;

/// How long a sampled storage figure is reused before re-stating the log.
const STORAGE_SAMPLE_TTL: Duration = Duration::from_secs(1);

/// The ceilings for one tenant (SPEC-025 OPS-060). Every field is optional;
/// `None` (or `0` from config) leaves that dimension unbounded.
#[derive(Debug, Clone, Copy, Default)]
pub struct TenantQuotas {
    /// Sustained reducer calls per second for the whole namespace, with an
    /// equal burst.
    pub max_reducer_calls_per_sec: Option<f64>,
    /// Maximum concurrent subscriptions across the namespace.
    pub max_subscriptions: Option<u64>,
    /// Ceiling on the tenant's estimated in-memory footprint, bytes.
    pub max_memory_bytes: Option<u64>,
    /// Ceiling on the tenant's durable commit-log footprint, bytes.
    pub max_storage_bytes: Option<u64>,
}

impl TenantQuotas {
    /// Whether any ceiling is set (an all-`None` quota skips every check).
    pub fn is_unbounded(&self) -> bool {
        self.max_reducer_calls_per_sec.is_none()
            && self.max_subscriptions.is_none()
            && self.max_memory_bytes.is_none()
            && self.max_storage_bytes.is_none()
    }
}

/// Which ceiling a tenant hit — the `quota` label of
/// `fluxum_tenant_quota_exceeded_total` (OPS-061).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quota {
    /// Reducer calls per second.
    ReducerRate,
    /// Concurrent subscriptions.
    Subscriptions,
    /// In-memory footprint.
    Memory,
    /// Durable storage footprint.
    Storage,
}

impl Quota {
    /// The metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReducerRate => "reducer_rate",
            Self::Subscriptions => "subscriptions",
            Self::Memory => "memory",
            Self::Storage => "storage",
        }
    }

    /// Every quota, so the exposition emits a zero series per label.
    pub const ALL: [Self; 4] = [
        Self::ReducerRate,
        Self::Subscriptions,
        Self::Memory,
        Self::Storage,
    ];
}

/// A token bucket sized to the namespace's reducer-rate ceiling.
#[derive(Debug)]
struct RateBucket {
    tokens: f64,
    burst: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl RateBucket {
    fn new(rate: f64, now: Instant) -> Self {
        Self {
            tokens: rate,
            burst: rate,
            refill_per_sec: rate,
            last: now,
        }
    }

    fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// One tenant's live quota state: the ceilings, the reducer bucket, a cached
/// storage sample, and the per-quota exceed counters `/metrics` reports.
#[derive(Debug)]
pub struct QuotaState {
    quotas: TenantQuotas,
    bucket: Mutex<Option<RateBucket>>,
    /// `(sampled_at, bytes)` — the durable footprint, refreshed at most once
    /// per [`STORAGE_SAMPLE_TTL`].
    storage_sample: Mutex<Option<(Instant, u64)>>,
    exceeded_reducer_rate: AtomicU64,
    exceeded_subscriptions: AtomicU64,
    exceeded_memory: AtomicU64,
    exceeded_storage: AtomicU64,
}

impl QuotaState {
    /// State for `quotas` (all-`None` = unbounded).
    pub fn new(quotas: TenantQuotas) -> Self {
        Self {
            bucket: Mutex::new(
                quotas
                    .max_reducer_calls_per_sec
                    .map(|rate| RateBucket::new(rate, Instant::now())),
            ),
            quotas,
            storage_sample: Mutex::new(None),
            exceeded_reducer_rate: AtomicU64::new(0),
            exceeded_subscriptions: AtomicU64::new(0),
            exceeded_memory: AtomicU64::new(0),
            exceeded_storage: AtomicU64::new(0),
        }
    }

    /// An unbounded state — the default for a namespace with no quotas.
    pub fn unbounded() -> Self {
        Self::new(TenantQuotas::default())
    }

    /// The configured ceilings.
    pub fn quotas(&self) -> &TenantQuotas {
        &self.quotas
    }

    fn counter(&self, quota: Quota) -> &AtomicU64 {
        match quota {
            Quota::ReducerRate => &self.exceeded_reducer_rate,
            Quota::Subscriptions => &self.exceeded_subscriptions,
            Quota::Memory => &self.exceeded_memory,
            Quota::Storage => &self.exceeded_storage,
        }
    }

    /// How many times this tenant hit `quota`.
    pub fn exceeded(&self, quota: Quota) -> u64 {
        self.counter(quota).load(Ordering::Relaxed)
    }

    fn refuse(&self, quota: Quota, message: String) -> FluxumError {
        self.counter(quota).fetch_add(1, Ordering::Relaxed);
        // The reducer-rate ceiling is a retryable 429 — the tenant is going
        // too fast, not doing something wrong. An exhaustion ceiling is not
        // retryable in the same breath: retrying a write against a full quota
        // just fails again until the operator raises it or the tenant frees
        // space, so it maps to the resource-exhausted code instead.
        let code = match quota {
            Quota::ReducerRate => codes::REDUCER_RATE_LIMITED,
            _ => codes::CLUSTER_SHARD_UNAVAILABLE,
        };
        FluxumError::query(code, message)
    }

    /// Admit one reducer call against the namespace's rate ceiling
    /// (OPS-060). Layered above the per-`(Identity, reducer)` limiter.
    pub fn admit_reducer_call(&self, namespace: &str) -> Result<()> {
        let mut guard = self.bucket.lock().unwrap_or_else(|e| e.into_inner());
        let Some(bucket) = guard.as_mut() else {
            return Ok(());
        };
        if bucket.try_take(Instant::now()) {
            return Ok(());
        }
        drop(guard);
        Err(self.refuse(
            Quota::ReducerRate,
            format!(
                "namespace `{namespace}` is over its reducer-rate quota; retry shortly (OPS-060)"
            ),
        ))
    }

    /// Admit one new subscription given the tenant's current live count
    /// (read from its own manager, so the ceiling cannot drift).
    pub fn admit_subscription(&self, namespace: &str, current: u64) -> Result<()> {
        let Some(max) = self.quotas.max_subscriptions else {
            return Ok(());
        };
        if current < max {
            return Ok(());
        }
        Err(self.refuse(
            Quota::Subscriptions,
            format!(
                "namespace `{namespace}` is at its subscription quota ({max}); \
                 unsubscribe before subscribing again (OPS-060)"
            ),
        ))
    }

    /// Admit a write against the memory and storage ceilings (OPS-060).
    /// `memory_bytes` is the tenant's estimated in-memory footprint;
    /// `storage` samples its durable bytes only when the cache is stale.
    pub fn admit_write(
        &self,
        namespace: &str,
        memory_bytes: impl FnOnce() -> u64,
        storage_bytes: impl FnOnce() -> Option<u64>,
    ) -> Result<()> {
        if let Some(max) = self.quotas.max_memory_bytes {
            let used = memory_bytes();
            if used >= max {
                return Err(self.refuse(
                    Quota::Memory,
                    format!(
                        "namespace `{namespace}` is over its memory quota \
                         ({used} B of {max} B); free rows before writing (OPS-060)"
                    ),
                ));
            }
        }
        if let Some(max) = self.quotas.max_storage_bytes {
            let used = self.sampled_storage(storage_bytes);
            if used >= max {
                return Err(self.refuse(
                    Quota::Storage,
                    format!(
                        "namespace `{namespace}` is over its storage quota \
                         ({used} B of {max} B) (OPS-060)"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// The tenant's durable footprint, re-sampled at most once per TTL.
    pub fn sampled_storage(&self, sample: impl FnOnce() -> Option<u64>) -> u64 {
        let mut guard = self
            .storage_sample
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if let Some((at, bytes)) = *guard
            && now.saturating_duration_since(at) < STORAGE_SAMPLE_TTL
        {
            return bytes;
        }
        let bytes = sample().unwrap_or_else(|| guard.map_or(0, |(_, b)| b));
        *guard = Some((now, bytes));
        bytes
    }
}

impl Default for QuotaState {
    fn default() -> Self {
        Self::unbounded()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn quotas() -> TenantQuotas {
        TenantQuotas {
            max_reducer_calls_per_sec: Some(3.0),
            max_subscriptions: Some(2),
            max_memory_bytes: Some(1_000),
            max_storage_bytes: Some(5_000),
        }
    }

    #[test]
    fn an_unbounded_tenant_is_never_refused() {
        let state = QuotaState::unbounded();
        assert!(state.quotas().is_unbounded());
        for _ in 0..1_000 {
            state.admit_reducer_call("t").unwrap();
        }
        state.admit_subscription("t", 10_000).unwrap();
        state
            .admit_write("t", || u64::MAX, || Some(u64::MAX))
            .unwrap();
    }

    #[test]
    fn the_reducer_rate_ceiling_is_a_retryable_429() {
        let state = QuotaState::new(quotas());
        for _ in 0..3 {
            state.admit_reducer_call("acme").unwrap();
        }
        let err = state.admit_reducer_call("acme").unwrap_err();
        let wire = err.to_wire();
        assert_eq!(wire.code, codes::REDUCER_RATE_LIMITED);
        assert!(wire.message.contains("acme"), "{wire:?}");
        assert_eq!(state.exceeded(Quota::ReducerRate), 1);
        // Nothing else was charged.
        assert_eq!(state.exceeded(Quota::Memory), 0);
    }

    #[test]
    fn the_subscription_ceiling_counts_live_plans() {
        let state = QuotaState::new(quotas());
        state.admit_subscription("acme", 0).unwrap();
        state.admit_subscription("acme", 1).unwrap();
        // At the ceiling: the next one is refused.
        assert!(state.admit_subscription("acme", 2).is_err());
        assert_eq!(state.exceeded(Quota::Subscriptions), 1);
        // Dropping back under it admits again — the count is read, not kept.
        state.admit_subscription("acme", 1).unwrap();
    }

    #[test]
    fn the_memory_ceiling_refuses_the_write() {
        let state = QuotaState::new(quotas());
        state.admit_write("acme", || 999, || Some(0)).unwrap();

        let err = state.admit_write("acme", || 1_000, || Some(0)).unwrap_err();
        assert!(err.to_string().contains("memory quota"), "{err}");
        assert_eq!(state.exceeded(Quota::Memory), 1);
        assert_eq!(
            state.exceeded(Quota::Storage),
            0,
            "only the breached quota is charged"
        );
    }

    #[test]
    fn the_storage_ceiling_refuses_the_write() {
        // A fresh state, so the storage sample is taken rather than served
        // from a cache primed by an earlier admit.
        let state = QuotaState::new(quotas());
        let err = state.admit_write("acme", || 0, || Some(5_000)).unwrap_err();
        assert!(err.to_string().contains("storage quota"), "{err}");
        assert_eq!(state.exceeded(Quota::Storage), 1);
    }

    #[test]
    fn the_storage_sample_is_cached_within_its_ttl() {
        let state = QuotaState::new(quotas());
        assert_eq!(state.sampled_storage(|| Some(100)), 100);
        // A second call inside the TTL must not re-sample — the closure below
        // would panic if it were called.
        assert_eq!(
            state.sampled_storage(|| panic!("re-sampled inside the TTL")),
            100
        );
    }

    #[test]
    fn a_failed_sample_reuses_the_last_known_figure() {
        let state = QuotaState::new(quotas());
        assert_eq!(state.sampled_storage(|| Some(42)), 42);
        std::thread::sleep(STORAGE_SAMPLE_TTL + Duration::from_millis(20));
        // The log could not be stat'd: keep the previous figure rather than
        // reporting a tenant as suddenly empty (which would lift its ceiling).
        assert_eq!(state.sampled_storage(|| None), 42);
    }
}
