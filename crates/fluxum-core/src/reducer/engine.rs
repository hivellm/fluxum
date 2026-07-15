//! The reducer engine (SPEC-004 §2–3, T3.3): the transport-independent
//! entry point that turns admitted `ReducerCall`s into transaction-pipeline
//! jobs and drives the shard lifecycle hooks.
//!
//! # What lives where
//!
//! - **Admission** (RED-001/RED-006) happens here, *before* the pipeline:
//!   an unknown reducer name is a wire-ready 404 and a declared-signature
//!   argument mismatch is a 400 — in both cases no transaction, no
//!   `TxState`, no commit-log entry ever exists.
//! - **Execution** rides the T3.1 pipeline ([`crate::txn::TxPipeline`]):
//!   the single writer runs the dispatch under its `catch_unwind` boundary,
//!   so a panicking reducer is a rollback plus a wire-ready 500 — never a
//!   dead shard (RED-061, TXN-022, FR-25).
//! - **Lifecycle** (RED-010..RED-013): `on_init` runs exactly once — the
//!   first startup with an empty `CommittedState` (no checkpoint, no commit
//!   log; the caller derives that from
//!   [`crate::checkpoint::RecoveryOutcome::last_tx_id`]) — and
//!   `on_shard_start` runs on every startup after recovery, both before the
//!   shard accepts calls (the server assembly orders that). `on_connect` /
//!   `on_disconnect` run per client session under the client's identity.
//!   Hooks of one kind run inside **one** transaction, in ascending function
//!   name order (deterministic across binaries; linker order is not).
//! - Lifecycle and scheduled executions run under the **server identity**
//!   with the reserved nil `ConnectionId(0)` (RED-025; never assigned to a
//!   real connection).

use std::sync::Arc;

use crate::error::Result;
use crate::txn::{CommitReceipt, TxPipeline};
use crate::types::{ConnectionId, Identity, Timestamp};

use super::ratelimit::{RateLimiter, RateLimiterOptions};
use super::{FluxValue, ReducerCaller, ReducerContext, ReducerRegistry, with_context};

/// Which lifecycle moment a hook is registered for (SPEC-004 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleKind {
    /// First startup with an empty `CommittedState` only (RED-010).
    OnInit,
    /// Every startup, after recovery, before the first call (RED-013).
    OnShardStart,
    /// An authenticated client connected (RED-011).
    OnConnect,
    /// A client connection dropped — clean close or timeout (RED-012).
    OnDisconnect,
}

/// The static handler shape the lifecycle macros emit.
pub type LifecycleFnPtr = fn(&ReducerContext<'_, '_, '_>) -> Result<()>;

/// One lifecycle hook in the link-time registry (RED-010..RED-013),
/// submitted by `#[fluxum::on_init]` / `#[fluxum::on_shard_start]` /
/// `#[fluxum::on_connect]` / `#[fluxum::on_disconnect]`.
pub struct LifecycleDef {
    /// Which moment the hook runs at.
    pub kind: LifecycleKind,
    /// Function name (deterministic execution order within one kind).
    pub name: &'static str,
    /// The hook body.
    pub handler: LifecycleFnPtr,
}

inventory::collect!(LifecycleDef);

/// Iterate every lifecycle hook registered in this binary.
pub fn registered_lifecycle() -> impl Iterator<Item = &'static LifecycleDef> {
    inventory::iter::<LifecycleDef>()
}

/// The shard's lifecycle hooks, grouped by kind and sorted by function name
/// (RED-010..RED-013).
#[derive(Default)]
pub struct LifecycleHooks {
    on_init: Vec<&'static LifecycleDef>,
    on_shard_start: Vec<&'static LifecycleDef>,
    on_connect: Vec<&'static LifecycleDef>,
    on_disconnect: Vec<&'static LifecycleDef>,
}

impl std::fmt::Debug for LifecycleHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names =
            |defs: &[&'static LifecycleDef]| -> Vec<&str> { defs.iter().map(|d| d.name).collect() };
        f.debug_struct("LifecycleHooks")
            .field("on_init", &names(&self.on_init))
            .field("on_shard_start", &names(&self.on_shard_start))
            .field("on_connect", &names(&self.on_connect))
            .field("on_disconnect", &names(&self.on_disconnect))
            .finish()
    }
}

impl LifecycleHooks {
    /// No hooks.
    pub fn none() -> Self {
        Self::default()
    }

    /// Collect every lifecycle hook of this binary from the link-time
    /// registry.
    pub fn from_registered() -> Self {
        Self::from_defs(registered_lifecycle())
    }

    /// [`LifecycleHooks::from_registered`] with explicit defs — the seam
    /// tests and embedders use instead of the link-time registry.
    pub fn from_defs(defs: impl IntoIterator<Item = &'static LifecycleDef>) -> Self {
        let mut hooks = Self::default();
        for def in defs {
            match def.kind {
                LifecycleKind::OnInit => hooks.on_init.push(def),
                LifecycleKind::OnShardStart => hooks.on_shard_start.push(def),
                LifecycleKind::OnConnect => hooks.on_connect.push(def),
                LifecycleKind::OnDisconnect => hooks.on_disconnect.push(def),
            }
        }
        for group in [
            &mut hooks.on_init,
            &mut hooks.on_shard_start,
            &mut hooks.on_connect,
            &mut hooks.on_disconnect,
        ] {
            group.sort_by_key(|def| def.name);
        }
        hooks
    }
}

/// What [`ReducerEngine::start`] did (RED-010/RED-013 observability).
#[derive(Debug, Default)]
pub struct StartupReport {
    /// `on_init` hooks that ran (fresh shard only), in execution order.
    pub ran_on_init: Vec<&'static str>,
    /// `on_shard_start` hooks that ran, in execution order.
    pub ran_on_shard_start: Vec<&'static str>,
}

/// The transport-independent reducer engine of one shard (T3.3).
///
/// Owns the admission path (registry pre-checks), the lifecycle hooks, and
/// the handle to the shard's transaction pipeline. Transports and the
/// scheduler (T3.4/T3.5) construct [`ReducerCaller`]s and call in; the
/// server assembly (phase 5) wires recovery → [`ReducerEngine::start`] →
/// transport accept, in that order.
pub struct ReducerEngine {
    registry: Arc<ReducerRegistry>,
    hooks: LifecycleHooks,
    pipeline: TxPipeline,
    shard_id: u32,
    server_identity: Identity,
    rate_limiter: RateLimiter,
}

impl ReducerEngine {
    /// Assemble an engine over a shard's pipeline.
    ///
    /// `server_identity` is the SPEC-009 §8 database identity lifecycle and
    /// scheduled executions run under (RED-025);
    /// [`crate::auth::server_identity`] derives it. Rate limiting starts
    /// with [`RateLimiterOptions::default`] and only the shard's own server
    /// identity exempt — [`ReducerEngine::with_rate_limiter`] installs the
    /// assembly's limiter (server-peer exemptions, configured shard cap).
    pub fn new(
        pipeline: TxPipeline,
        registry: Arc<ReducerRegistry>,
        hooks: LifecycleHooks,
        shard_id: u32,
        server_identity: Identity,
    ) -> Self {
        Self {
            registry,
            hooks,
            pipeline,
            shard_id,
            server_identity,
            rate_limiter: RateLimiter::new(RateLimiterOptions::default(), [server_identity]),
        }
    }

    /// Replace the admission rate limiter (RED-050..RED-052) — the server
    /// assembly wires the configured `shard_max_reducers_per_sec` and the
    /// AUTH-062 server-peer exemptions through here.
    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: RateLimiter) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    /// The engine's reducer registry (dispatch and admission share it).
    pub fn registry(&self) -> &Arc<ReducerRegistry> {
        &self.registry
    }

    /// The shard's transaction pipeline.
    pub fn pipeline(&self) -> &TxPipeline {
        &self.pipeline
    }

    /// Run the startup lifecycle (RED-010/RED-013): `on_init` hooks when
    /// `fresh` (first boot: no checkpoint and no commit log —
    /// `recovery.last_tx_id.is_none()`), then `on_shard_start` hooks on
    /// every boot. Each kind runs inside one transaction; an `Err` (or
    /// panic) rolls that transaction back and aborts startup.
    ///
    /// The server assembly must call this after recovery and **before**
    /// accepting any `ReducerCall` (RED-013).
    pub async fn start(&self, fresh: bool) -> Result<StartupReport> {
        let mut report = StartupReport::default();
        if fresh && !self.hooks.on_init.is_empty() {
            let defs = self.hooks.on_init.clone();
            report.ran_on_init = defs.iter().map(|def| def.name).collect();
            self.run_hooks(defs).await?;
        }
        if !self.hooks.on_shard_start.is_empty() {
            let defs = self.hooks.on_shard_start.clone();
            report.ran_on_shard_start = defs.iter().map(|def| def.name).collect();
            self.run_hooks(defs).await?;
        }
        Ok(report)
    }

    /// Run the `on_connect` hooks for an authenticated client session
    /// (RED-011), inside one transaction under the client's identity.
    pub async fn client_connected(
        &self,
        identity: Identity,
        connection_id: ConnectionId,
    ) -> Result<()> {
        if self.hooks.on_connect.is_empty() {
            return Ok(());
        }
        let caller = ReducerCaller {
            identity,
            connection_id,
            timestamp: Timestamp::now(),
            shard_id: self.shard_id,
        };
        self.run_hooks_as(self.hooks.on_connect.clone(), caller)
            .await
            .map(|_| ())
    }

    /// Run the `on_disconnect` hooks when a client connection drops —
    /// clean close or timeout (RED-012).
    pub async fn client_disconnected(
        &self,
        identity: Identity,
        connection_id: ConnectionId,
    ) -> Result<()> {
        if self.hooks.on_disconnect.is_empty() {
            return Ok(());
        }
        let caller = ReducerCaller {
            identity,
            connection_id,
            timestamp: Timestamp::now(),
            shard_id: self.shard_id,
        };
        self.run_hooks_as(self.hooks.on_disconnect.clone(), caller)
            .await
            .map(|_| ())
    }

    /// Execute reducer `name` for `caller` (FR-20).
    ///
    /// Admission runs first, with no transaction: an unregistered name is a
    /// 404 (RED-006), a schedule-only reducer is a 403 (RED-025), a
    /// rate-limited caller is a 429 — or 503 past the shard cap — with zero
    /// storage cost (RED-050/RED-052), and — for `#[fluxum::reducer]`-
    /// declared signatures — an argument count or type mismatch is a 400
    /// (RED-001). Admitted calls execute on the shard's single writer;
    /// `Err` or panic rolls back with no commit-log entry and no
    /// subscription events, and the shard keeps serving (RED-004, RED-061).
    pub async fn call(
        &self,
        caller: ReducerCaller,
        name: &str,
        args: Vec<FluxValue>,
    ) -> Result<CommitReceipt> {
        let max_rate = self.registry.admission(name)?;
        self.rate_limiter.check(&caller.identity, name, max_rate)?;
        self.registry.check_args(name, &args)?;
        let registry = Arc::clone(&self.registry);
        let name = name.to_owned();
        self.pipeline
            .call(Box::new(move |tx| {
                registry.dispatch(caller, &name, &args, tx)
            }))
            .await
    }

    /// Run `defs` in one transaction under the server identity (RED-025:
    /// nil `ConnectionId(0)`).
    async fn run_hooks(&self, defs: Vec<&'static LifecycleDef>) -> Result<CommitReceipt> {
        let caller = ReducerCaller {
            identity: self.server_identity,
            connection_id: ConnectionId::new(0),
            timestamp: Timestamp::now(),
            shard_id: self.shard_id,
        };
        self.run_hooks_as(defs, caller).await
    }

    /// Run `defs` in one transaction as `caller`, in order; the first `Err`
    /// rolls the whole transaction back.
    async fn run_hooks_as(
        &self,
        defs: Vec<&'static LifecycleDef>,
        caller: ReducerCaller,
    ) -> Result<CommitReceipt> {
        let registry = Arc::clone(&self.registry);
        self.pipeline
            .call(Box::new(move |tx| {
                with_context(&registry, caller, tx, |ctx| {
                    for def in &defs {
                        (def.handler)(ctx)?;
                    }
                    Ok(())
                })
            }))
            .await
    }
}
