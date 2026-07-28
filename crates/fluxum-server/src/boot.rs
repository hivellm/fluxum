//! Assembling a running server from a config (SPEC-025 OPS-040).
//!
//! Everything below already existed and was already exercised — the loopback
//! suites build this same stack by hand. What was missing was the path from a
//! `config.yml` to it, which is why `main.rs` was a T0.1 stub and the server
//! could not be started outside `cargo test`.
//!
//! The schema and the reducers come from the **link-time registry**: any crate
//! compiled into this binary that uses `#[fluxum::table]` / `#[fluxum::reducer]`
//! registers itself through `inventory`, and the assembly below picks it up
//! without knowing the module exists. That is what makes a Fluxum application a
//! crate rather than a config file.

use std::sync::Arc;
use std::time::Duration;

use fluxum_core::auth::{Authenticator, ServerPeerRegistry, provider_from_config};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::Config;
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry, registered_reducers};
use fluxum_core::schema::Schema;
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};

use crate::ShardContext;
use crate::http::{self, HttpOptions, HttpServer};
use crate::tcp::{self, TcpOptions, TcpServer};

/// A running server: the listeners plus the context they share.
pub struct Server {
    /// Streamable HTTP `/rpc` + the admin API.
    pub http: HttpServer,
    /// FluxRPC over raw TCP.
    pub tcp: TcpServer,
    /// The optional read-only Postgres wire endpoint (SPEC-027), when enabled.
    pub pg: Option<crate::pgwire::PgServer>,
    /// The default shard the listeners accept on (SHD-011: sessions rebind
    /// to their affinity shard after authentication).
    pub ctx: Arc<ShardContext>,
    /// The multi-shard coordinator (SHD-010), when `sharding.shards > 1`.
    /// This handle **owns** the cluster — every shard context holds only a
    /// weak back-reference — so it lives exactly as long as the server.
    pub coord: Option<Arc<crate::shard::ShardCoord>>,
}

impl Server {
    /// Stop every listener.
    pub fn shutdown(&self) {
        self.http.shutdown();
        self.tcp.shutdown();
        if let Some(pg) = &self.pg {
            pg.shutdown();
        }
    }
}

/// Why startup failed, in terms an operator can act on.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// The linked module registered no tables.
    #[error(
        "no tables are registered in this binary. A Fluxum application is a crate: add one \
         with #[fluxum::table] and make sure it is linked (an unused dependency is dropped \
         by the linker, taking its inventory registrations with it)."
    )]
    NoTables,
    /// Schema, reducer registry, or storage rejected the module.
    #[error("{0}")]
    Core(#[from] fluxum_core::FluxumError),
    /// A listener could not bind.
    #[error("cannot bind {addr}: {source}")]
    Bind {
        /// The address that failed.
        addr: String,
        /// The underlying OS error.
        #[source]
        source: std::io::Error,
    },
}

/// What [`assemble`] hands back: the default shard's context plus, on a
/// multi-shard boot, the coordinator that owns the whole cluster.
pub struct Assembled {
    /// The default shard (SHD-004) — what the listeners accept on.
    pub ctx: Arc<ShardContext>,
    /// The coordinator, `Some` iff `sharding.shards > 1`. **This is the
    /// owning handle**: shard contexts hold only weak back-references, so
    /// dropping it dissolves the cluster's routing. Keep it alive for the
    /// server's lifetime (the [`Server`] struct does).
    pub coord: Option<Arc<crate::shard::ShardCoord>>,
}

/// The storage directories one shard owns.
///
/// A single-shard boot keeps the configured directories verbatim, so every
/// existing deployment recovers its data unchanged. A multi-shard boot puts
/// **every** shard — including shard 0 — under `shard-<k>/`, because shards
/// share nothing (SHD-020) and two commit logs in one directory would
/// corrupt each other. Changing `sharding.shards` on an existing data
/// directory is therefore a topology change, not a reload: the old layout's
/// data is not re-partitioned (routing determinism, SHD-003, would be
/// violated by silently rehashing), and the operator migrates explicitly.
struct ShardDirs {
    page_dir: std::path::PathBuf,
    commit_log_dir: std::path::PathBuf,
    checkpoint_dir: std::path::PathBuf,
    archive_dir: std::path::PathBuf,
}

fn shard_dirs(config: &Config, shard_count: u32, shard: u32) -> ShardDirs {
    let place = |base: &std::path::Path| {
        if shard_count <= 1 {
            base.to_path_buf()
        } else {
            base.join(format!("shard-{shard}"))
        }
    };
    ShardDirs {
        page_dir: place(&config.storage.page_dir),
        commit_log_dir: place(&config.storage.commit_log_dir),
        checkpoint_dir: place(&config.storage.checkpoint_dir),
        archive_dir: place(&config.replication.archive.dir),
    }
}

/// Build the shard context(s) from `config` and the link-time registry.
///
/// Split from [`serve`] so a test can assemble a deployment without binding
/// ports. `sharding.shards = 1` (the default; the development and droplet
/// profiles pin it) assembles exactly what it always has — one context, no
/// coordinator. `> 1` assembles that many fully-independent [`ShardHost`]s
/// (SHD-020: each with its own store, pager, commit log, recovery and
/// checkpoint worker) behind a [`crate::shard::ShardCoord`], and the
/// configured count is **honoured or refused** — there is no silent
/// downgrade to one shard.
///
/// [`ShardHost`]: crate::shard::ShardHost
pub fn assemble(config: &Config) -> Result<Assembled, BootError> {
    // `assemble` collects and validates the link-time registry, which is the
    // same path `ServerBuilder::build` documents.
    let schema = Schema::assemble()?;
    if schema.is_empty() {
        return Err(BootError::NoTables);
    }
    let schema = Arc::new(schema);

    let hardware = fluxum_core::hw::HardwareProfile::probe();
    let effective = fluxum_core::hw::derive(&hardware, config)?;
    // Only an EXPLICIT `sharding.shards` provisions a multi-shard topology.
    // `auto` derives a per-hardware value for sizing, but honouring it here
    // would let a core count silently change the on-disk layout (flat →
    // `shard-<k>/`) on upgrade — an implicit re-partition, which SHD-003
    // forbids. Topology is an operator decision; `auto` hosts one shard.
    let shard_count = config
        .sharding
        .shards
        .explicit()
        .copied()
        .unwrap_or(1)
        .max(1);
    if shard_count == 1 && effective.shards.value > 1 {
        tracing::info!(
            target: "fluxum::server",
            derived = effective.shards.value,
            "sharding.shards is `auto`: hosting one shard (multi-shard \
             topology needs an explicit count — it changes the data layout)"
        );
    }

    // The one memory budget covers the whole process (TIER-004), so N pools
    // split it — a per-shard pool below a working floor means the operator
    // asked for more shards than the budget can host, which is a refusal,
    // not a degraded boot.
    let pool_per_shard = effective.bufferpool_capacity_bytes.value / u64::from(shard_count);
    const MIN_POOL_PER_SHARD: u64 = 8 << 20;
    if shard_count > 1 && pool_per_shard < MIN_POOL_PER_SHARD {
        return Err(BootError::Core(fluxum_core::FluxumError::config(format!(
            "sharding.shards: {} shards over a {} B buffer pool leaves {} B per shard \
             (< the {} MiB floor); raise memory.budget or lower the shard count",
            shard_count,
            effective.bufferpool_capacity_bytes.value,
            pool_per_shard,
            MIN_POOL_PER_SHARD >> 20,
        ))));
    }

    // SPEC-020 PLG-001/032: validate the plugin manifest once; the registry
    // is shared read-only by every shard.
    let plugins = Arc::new(fluxum_core::plugin::PluginRegistry::build(&schema, config)?);
    // SPEC-026 §4: ONE pre-auth guard for the whole process, so the per-IP
    // view stays unified across shards exactly as it is across transports.
    let conn_guard = Arc::new(crate::connguard::ConnGuard::new(
        crate::connguard::ConnLimits::from_config(&config.server.connection_limits),
    ));

    let mut hosts: Vec<crate::shard::ShardHost> = Vec::with_capacity(shard_count as usize);
    for shard in 0..shard_count {
        let dirs = shard_dirs(config, shard_count, shard);
        let ctx = assemble_shard(
            config,
            &schema,
            &effective,
            shard,
            pool_per_shard,
            &dirs,
            &plugins,
            &conn_guard,
        )?;
        hosts.push(crate::shard::ShardHost {
            shard_id: shard,
            ctx,
        });
    }

    // Single shard: no coordinator, no routing layer — the assembly every
    // deployment has run since T0. Multi-shard: the SHD-010 registry.
    if shard_count == 1 {
        let ctx = hosts.pop().map(|h| h.ctx).unwrap_or_else(|| unreachable!());
        finish_default_shard(config, &ctx)?;
        return Ok(Assembled { ctx, coord: None });
    }

    let router = fluxum_core::shard::ShardRouter::from_schema(&schema, shard_count);
    let default_shard = 0_u32;
    let coord = Arc::new(crate::shard::ShardCoord::new(
        Arc::clone(&schema),
        router,
        hosts,
    )?);
    for shard in coord.shard_ids().collect::<Vec<_>>() {
        if let Some(ctx) = coord.host(shard) {
            ctx.set_coord(&coord);
            // The transports spawn fan-out + sweepers for the shard they
            // accept on; every other shard gets them here, or its
            // subscribers would never receive a TxUpdate. The Notify is
            // shard-lifetime: these tasks end with the process.
            if shard != default_shard {
                crate::spawn_fanout(Arc::clone(ctx), Arc::new(tokio::sync::Notify::new()));
                ctx.start_ephemeral_sweeper();
                ctx.start_ttl_sweeper();
            }
        }
    }
    let ctx = coord.host(default_shard).cloned().ok_or_else(|| {
        BootError::Core(fluxum_core::FluxumError::config(
            "sharding: no default shard 0 in the assembled registry (SHD-004)",
        ))
    })?;
    finish_default_shard(config, &ctx)?;
    tracing::info!(
        target: "fluxum::server",
        shards = shard_count,
        "multi-shard deployment assembled (SHD-010): sessions route by \
         identity affinity after authentication (SHD-011)"
    );
    Ok(Assembled {
        ctx,
        coord: Some(coord),
    })
}

/// Assemble one fully-independent shard (SHD-020): its own pager + store,
/// recovery, commit log, pipeline, engine, subscriptions, checkpoint worker
/// and replication primary, over its own directories.
#[allow(clippy::too_many_arguments)] // the boot wiring, called exactly once
fn assemble_shard(
    config: &Config,
    schema: &Arc<Schema>,
    effective: &fluxum_core::hw::EffectiveConfig,
    shard: u32,
    pool_per_shard: u64,
    dirs: &ShardDirs,
    plugins: &Arc<fluxum_core::plugin::PluginRegistry>,
    conn_guard: &Arc<crate::connguard::ConnGuard>,
) -> Result<Arc<ShardContext>, BootError> {
    // SPEC-015: the live store serves through a paged cold tier whose buffer
    // pool is sized from the effective `memory.budget` (TIER-002/003), so
    // steady-state RSS is bounded by the budget rather than the resident row
    // count (TIER-004) — the pillar the billion-row soak (T7.7) exercises.
    // On a multi-shard boot each shard gets an equal split of the pool, so
    // the budget covers the process, not each shard (TST-112's "within
    // budget on every shard" reads each split's own gauges).
    std::fs::create_dir_all(&dirs.page_dir).map_err(fluxum_core::FluxumError::from)?;
    // `Pager::open` discards page files this build cannot read (an older
    // page format, a half-written run): the tier is a cache, and recovery
    // below rebuilds it from the checkpoint + commit log (TIER-021).
    let mut pager_options =
        fluxum_core::store::pager::PagerOptions::from_effective(config, effective, shard);
    pager_options.pool_capacity_bytes = pool_per_shard;
    let pager = fluxum_core::store::pager::Pager::open(&dirs.page_dir, pager_options)?;
    let store = Arc::new(MemStore::with_pager(
        schema,
        fluxum_core::store::StoreOptions::default(),
        pager,
    )?);

    // The commit log is what makes a restart non-destructive: the store is in
    // memory, but every committed transaction is on disk and folded back in
    // here (STG-030: newest verified checkpoint + log replay) BEFORE the
    // pipeline opens. Skipping this is not a fresh start — it is a server
    // that comes up empty over a non-empty log, whose STG-015 monotonicity
    // check then rejects every new commit: no durability AND no writes.
    // (Caught by the conformance corpus's reconnect-resync scenario.)
    // First boot: the log directory does not exist yet. Created here rather
    // than left to CommitLog::open (which also creates it) because recovery
    // replays the directory FIRST, and an absent directory is an I/O error,
    // not an empty log.
    std::fs::create_dir_all(&dirs.commit_log_dir).map_err(fluxum_core::FluxumError::from)?;
    let repo = Arc::new(fluxum_core::checkpoint::CheckpointRepo::open(
        &dirs.checkpoint_dir,
    )?);
    let recovery = fluxum_core::checkpoint::recover(&store, &repo, &dirs.commit_log_dir, shard)?;
    if recovery.last_tx_id.is_some() || !recovery.rejected.is_empty() {
        tracing::info!(
            target: "fluxum::server",
            last_tx_id = ?recovery.last_tx_id,
            checkpoint_tx_id = ?recovery.checkpoint_tx_id,
            replayed_records = recovery.applied_records,
            rejected_checkpoints = recovery.rejected.len(),
            shard,
            "recovered shard state (STG-030)"
        );
    }

    // SPEC-014 REP-004/REP-072: the fencing epoch this boot acts under is
    // the highest of the persisted replication epoch and any PITR lineage
    // marker (a forked history must start above everything it forked from).
    // It is persisted before the member acts under it.
    let epoch = fluxum_core::backup::pitr_lineage_min_epoch(&dirs.commit_log_dir)?
        .unwrap_or(0)
        .max(crate::replication::load_epoch(&dirs.commit_log_dir)?)
        .max(1);
    crate::replication::persist_epoch(&dirs.commit_log_dir, epoch)?;
    if epoch > 1 {
        tracing::info!(
            target: "fluxum::server",
            epoch,
            "booting under persisted fencing epoch (REP-004)"
        );
    }
    let log = Arc::new(CommitLog::open(
        &dirs.commit_log_dir,
        shard,
        epoch,
        CommitLogOptions {
            segment_max_bytes: config.storage.segment_max_bytes.as_u64(),
            ..CommitLogOptions::default()
        },
    )?);

    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default())?;
    tokio::spawn(worker.run());

    let reducers: Vec<_> = registered_reducers().collect();
    let registry = ReducerRegistry::from_defs(reducers)?;
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(registry),
        LifecycleHooks::from_registered(),
        shard,
        fluxum_core::auth::server_identity("fluxum-server"),
    );

    let subs = SubscriptionManager::new(Arc::clone(schema), SubscriptionLimits::default());
    // AUTH-062 / REP-005: the configured server peers — operators, ingest
    // services, and replica-set members all authenticate through this
    // registry. (It was silently empty before T7.1 wired replication in.)
    let auth = Authenticator::with_provider(
        provider_from_config(&config.auth)?,
        ServerPeerRegistry::from_config(&config.auth.server_peers)?,
    );

    let ctx = ShardContext::new(engine, subs, auth, shard, COMMIT_BROADCAST_CAPACITY);
    // SPEC-026 §4: the pre-auth guard enforces what the operator configured,
    // not the built-in defaults. (The SEC-033/034 lists and global ceiling
    // land through `install_config` → `publish_reloadable`, the same path a
    // hot reload takes.)
    ctx.set_conn_guard(Arc::clone(conn_guard));
    // SPEC-026 SEC-054: the admin access policy, from config; the profile
    // decides whether the console admits anonymous callers (DEV-031). (Also
    // republished on hot reload via `publish_reloadable`.)
    ctx.set_admin_policy(crate::AdminPolicy::from_config_with_profile(
        &config.server.admin,
        config.profile,
    )?);
    // FR-05 / HWA-012/013: install the derived config so `GET /health`
    // reports the probe inputs and every derived value with its provenance
    // (the caller probed and derived once for the whole assembly).
    ctx.set_effective_config(effective);
    // STG-020: the periodic checkpoint worker, wired for REP-062 archival —
    // covered segments are copied durably to the archive (the PITR source)
    // before truncation may delete them, and archived copies age out with
    // the retention window. Every commit feeds the cadence via the commit
    // hook; `POST /checkpoint` and the drain's final checkpoint reach the
    // worker through the context.
    let archive = &config.replication.archive;
    // SPEC-025 OPS-011: the incremental remote archiver, when configured —
    // uploads run on the checkpoint worker's own thread, never the writer's.
    let remote = if archive.remote.enabled {
        let remote = &archive.remote;
        let store =
            fluxum_core::backup::store::S3Store::new(fluxum_core::backup::store::S3Config {
                endpoint: remote.endpoint.clone(),
                bucket: remote.bucket.clone(),
                region: remote.effective_region().to_owned(),
                access_key: remote.access_key.clone(),
                secret_key: remote
                    .secret_key
                    .as_ref()
                    .map(|s| s.expose_str().to_owned())
                    .unwrap_or_default(),
            });
        // A multi-shard boot namespaces the remote prefix per shard, the
        // object-store analogue of the `shard-<k>/` directory split.
        let prefix = if dirs.commit_log_dir == config.storage.commit_log_dir {
            remote.effective_prefix().to_owned()
        } else {
            format!("{}/shard-{shard}", remote.effective_prefix())
        };
        Some(Arc::new(fluxum_core::backup::remote::RemoteArchiver::new(
            Arc::new(store),
            &prefix,
        )))
    } else {
        None
    };
    let worker = fluxum_core::checkpoint::SnapshotWorker::spawn(
        Arc::clone(ctx.store()),
        repo,
        shard,
        fluxum_core::checkpoint::WorkerOptions {
            interval_tx: config.storage.checkpoint_interval_tx,
            epoch,
            compaction: Some(fluxum_core::checkpoint::LogCompaction {
                log_dir: dirs.commit_log_dir.clone(),
                archive_dir: archive.enabled.then(|| dirs.archive_dir.clone()),
                archive_retention: archive
                    .enabled
                    .then(|| archive.retention_duration())
                    .transpose()?,
                remote,
            }),
            metrics: Some(Arc::clone(ctx.metrics())),
            ..fluxum_core::checkpoint::WorkerOptions::default()
        },
    )?;
    ctx.set_checkpoint_service(crate::CheckpointService::new(worker));
    // SPEC-014 T7.1: the primary-side replication service is always
    // installed — a single node simply never receives a ReplicaHello. The
    // replica CLIENT (dialing out) is spawned by `serve` per the role.
    // REP-021: the quorum barrier arms only on a semi-sync PRIMARY — a
    // replica's local fan-out is not quorum-gated in T7.1 (it needs the
    // T7.2 consensus watermark to know cluster-wide durability).
    let semi_sync = (config.replication.role == fluxum_core::config::ReplicationRole::Primary
        && config.replication.mode == fluxum_core::config::ReplicationMode::SemiSync)
        .then(|| crate::replication::SemiSyncRuntime {
            quorum_total: crate::replication::quorum_total(
                &config.replication.semi_sync.quorum,
                config.replication.peers.len() + 1,
            ),
            ack_timeout: Duration::from_millis(config.replication.semi_sync.ack_timeout_ms),
            degrade: config.replication.semi_sync.on_quorum_loss == "degrade",
        });
    ctx.set_replication_primary(crate::replication::ReplicationPrimary::new(
        shard,
        dirs.commit_log_dir.clone(),
        dirs.checkpoint_dir.clone(),
        epoch,
        crate::replication::PrimaryOptions {
            heartbeat_interval: Duration::from_millis(config.replication.heartbeat_interval_ms),
            window_bytes: config.replication.window_bytes.as_u64(),
            semi_sync,
        },
    ));
    ctx.set_plugins(Arc::clone(plugins));
    Ok(ctx)
}

/// The node-level services that live on the **default** shard only: the
/// T7.2 election (a member's role is per node, not per shard) and the CDC
/// pumps (SPEC-020 PLG-050; they follow the default shard's log — per-shard
/// CDC fan-in is future work, documented in `docs/DEPLOYMENT.md`).
fn finish_default_shard(config: &Config, ctx: &Arc<ShardContext>) -> Result<(), BootError> {
    let dirs = shard_dirs(config, 1, 0);
    // The default shard's own epoch, as persisted by its assembly above.
    let log_dir = if ctx.coord().is_some() {
        shard_dirs(config, 2, 0).commit_log_dir
    } else {
        dirs.commit_log_dir
    };
    // SPEC-014 §5 (T7.2): the election state serves votes and publishes
    // the role the moment the member has peers — REP-003: the config role
    // is a bootstrap hint; consensus owns it after the first election. A
    // standalone node (no peers) skips it and is always the primary.
    if !config.replication.peers.is_empty() {
        let epoch = crate::replication::load_epoch(&log_dir)?.max(1);
        let primary = config.replication.role == fluxum_core::config::ReplicationRole::Primary;
        let election = crate::election::ElectionState::new(
            ctx.shard_id,
            config.replication.member_name.clone(),
            log_dir.clone(),
            primary,
            epoch,
            Duration::from_millis(config.replication.election_timeout_ms),
            Duration::from_millis(config.replication.max_staleness_ms),
        )
        .map_err(BootError::Core)?;
        ctx.metrics().set_replication_role(primary);
        ctx.metrics().set_replication_epoch(epoch);
        ctx.set_election(election);
    }
    // SPEC-020 §6 (PLG-050): spawn a CDC pump per `stream_sink` binding,
    // fed off the durable commit log (never the write path). The pumps are
    // detached like the checkpoint worker; they end when the log closes.
    if let Some(plugins) = ctx.plugins() {
        let _cdc_pumps = crate::cdc::spawn_sinks(
            ctx,
            &Arc::clone(plugins),
            log_dir,
            config.storage.data_dir.join("cdc"),
        );
    }
    Ok(())
}

/// Depth of the shard-wide commit broadcast the fan-out task consumes.
///
/// Not a config key: it buffers commits between the pipeline and fan-out, and
/// per-subscriber backpressure is already governed by `send_queue_depth` on
/// each transport. Two knobs for one queue would only let them disagree.
const COMMIT_BROADCAST_CAPACITY: usize = 256;

/// Compute the read-only migration plan for this binary against the data
/// directories in `config` (SPEC-024 DEV-041): recover the stored state
/// into a fresh in-memory store (disk is only READ — the commit log is not
/// opened for writing, no checkpoint is taken, `__schema_meta__` is not
/// touched) and preview what the next real boot's migration run would do.
///
/// A data directory that does not exist yet is a first boot: the plan says
/// so without creating anything.
pub fn migration_plan(config: &Config) -> Result<fluxum_core::migration::MigrationPlan, BootError> {
    if fluxum_core::schema::registered_tables().next().is_none() {
        return Err(BootError::NoTables);
    }
    // The module's registry plus the runtime-owned `__schema_meta__` (the
    // migration runner's MIG-002 requirement — the plan reads it).
    let schema = Schema::from_tables(
        fluxum_core::schema::registered_tables()
            .chain(std::iter::once(&fluxum_core::migration::SCHEMA_META)),
    )?;
    let store = MemStore::new(&schema)?;
    if config.storage.commit_log_dir.is_dir() {
        let repo = fluxum_core::checkpoint::CheckpointRepo::open(&config.storage.checkpoint_dir)?;
        fluxum_core::checkpoint::recover(&store, &repo, &config.storage.commit_log_dir, 0)?;
    }
    Ok(fluxum_core::migration::plan(&store, &schema)?)
}

/// Assemble and bind both listeners.
///
/// Both are bound before either is reported as up: a server answering TCP
/// while its HTTP port is still unbound looks healthy to a supervisor and is
/// unreachable to a browser.
///
/// # `FLUXUM_MIGRATE_PLAN`
///
/// When the environment variable `FLUXUM_MIGRATE_PLAN=1` is set, `serve`
/// does not serve: it prints the read-only [`migration_plan`] to stdout and
/// **exits the process** — 0 when the next boot proceeds, 3 when it would
/// refuse (MIG-022/MIG-023). This is the seam `fluxum migrate --plan`
/// drives, and because it lives here every embedder — the reference binary,
/// `fluxum init` scaffolds, custom mains — gets the plan mode without code
/// changes.
pub async fn serve(config: Config) -> Result<Server, BootError> {
    if std::env::var("FLUXUM_MIGRATE_PLAN").is_ok_and(|v| v == "1") {
        match migration_plan(&config) {
            Ok(plan) => {
                print!("{}", plan.render());
                std::process::exit(i32::from(plan.refuses()) * 3);
            }
            Err(e) => {
                eprintln!("migrate --plan failed: {e}");
                std::process::exit(1);
            }
        }
    }
    let Assembled { ctx, coord } = assemble(&config)?;

    let idle = match config.server.idle_timeout_secs {
        0 => None,
        secs => Some(Duration::from_secs(secs)),
    };
    let max_frame_bytes = u32::try_from(config.server.max_frame_bytes.as_u64())
        .unwrap_or(fluxum_protocol::DEFAULT_MAX_FRAME_BYTES);
    // RPC-008: the compression kill-switch + threshold, consulted at every
    // negotiation (sessions pin what they negotiated for their lifetime).
    ctx.set_wire_policy(crate::WirePolicy {
        compression_enabled: config.server.compression_enabled,
        compression_threshold_bytes: usize::try_from(
            config.server.compression_threshold_bytes.as_u64(),
        )
        .unwrap_or(64),
    });

    let http_addr = format!("{}:{}", config.server.tcp_host, config.server.http_port);
    let tcp_addr = format!("{}:{}", config.server.tcp_host, config.server.tcp_port);
    // SEC-042: listener hardening knobs, shared by both listeners.
    let socket = crate::sock::SocketOptions::from_config(&config.server);
    // SEC-059: optional built-in TLS termination on both listeners.
    let tls = match (&config.server.tls.cert, &config.server.tls.key) {
        (Some(cert), Some(key)) => Some(crate::tls::load_acceptor(cert, key).map_err(
            |source| BootError::Bind {
                addr: format!("tls({}, {})", cert.display(), key.display()),
                source,
            },
        )?),
        _ => None,
    };
    let tls_on = tls.is_some();
    // SEC-059: record the transport-encryption posture at boot (boolean only,
    // never key material). A plaintext public bind is only reachable here
    // because `allow_plaintext` was set (validate() would else have refused).
    ctx.set_tls_enabled(tls_on);
    tracing::info!(
        target: "fluxum::server",
        tls = tls_on,
        tcp = %tcp_addr,
        http = %http_addr,
        "transport listeners: TLS {}", if tls_on { "on" } else { "off (plaintext)" }
    );

    let http = http::serve_tls(
        Arc::clone(&ctx),
        &http_addr,
        HttpOptions {
            idle_timeout: idle,
            max_frame_bytes,
            static_dir: config.server.static_dir.clone(),
            socket,
            session: crate::session_sec::SessionPolicy::from_config(&config.server.session),
            ..HttpOptions::default()
        },
        tls.clone(),
    )
    .await
    .map_err(|source| BootError::Bind {
        addr: http_addr.clone(),
        source,
    })?;

    let tcp = tcp::serve_tls(
        Arc::clone(&ctx),
        &tcp_addr,
        TcpOptions {
            idle_timeout: idle,
            max_frame_bytes,
            socket,
            ..TcpOptions::default()
        },
        tls,
    )
    .await
    .map_err(|source| {
        // The HTTP listener is already up; drop it rather than leave a
        // half-bound server behind.
        http.shutdown();
        BootError::Bind {
            addr: tcp_addr.clone(),
            source,
        }
    })?;

    // SPEC-014 §5 (T7.2): every member with peers runs the election task —
    // a follower runs the replica client (rotating through peers to find
    // the primary) under an election timer and stands for election on
    // contact loss (REP-030); a primary parks (the fenced→demote watch).
    if let Some(election) = ctx.election().cloned()
        && let Some(first_peer) = config.replication.peers.first()
    {
        let token = config
            .replication
            .peer_token
            .as_ref()
            .map(|t| t.expose_str().as_bytes().to_vec())
            .unwrap_or_default();
        crate::election::spawn_election(
            Arc::clone(&ctx),
            Arc::clone(&election),
            crate::election::ElectionOptions {
                peers: config.replication.peers.clone(),
                token: token.clone(),
                election_timeout: Duration::from_millis(config.replication.election_timeout_ms),
                replica: crate::replication::ReplicaOptions {
                    primary: first_peer.clone(),
                    member_name: config.replication.member_name.clone(),
                    token,
                    log_dir: config.storage.commit_log_dir.clone(),
                    checkpoint_dir: config.storage.checkpoint_dir.clone(),
                    ack_interval: Duration::from_millis(config.replication.ack_interval_ms),
                    contact: Some(election.contact_clock()),
                    // A dead peer must fail well within one election window
                    // so rotation reaches the live primary before the timer.
                    connect_timeout: Some(Duration::from_millis(
                        (config.replication.election_timeout_ms / 4).max(100),
                    )),
                    primary_hint: Some(election.primary_hint_cell()),
                },
            },
        );
    }

    // SPEC-027 PGW-001/004: the optional read-only Postgres wire endpoint.
    // Off by default; when enabled it binds a plaintext listener (loopback by
    // default — see PgWireConfig) that authenticates each connection's token
    // and serves SELECTs through the same compiled-query engine as /query.
    let pg = if config.pgwire.enabled {
        let pg_addr = format!("{}:{}", config.pgwire.host, config.pgwire.port);
        let server = crate::pgwire::serve(
            Arc::clone(&ctx),
            &pg_addr,
            crate::pgwire::PgOptions {
                idle_timeout: idle,
                socket,
            },
        )
        .await
        .map_err(|source| {
            http.shutdown();
            tcp.shutdown();
            BootError::Bind {
                addr: pg_addr.clone(),
                source,
            }
        })?;
        tracing::info!(
            target: "fluxum::server",
            addr = %pg_addr,
            "read-only Postgres wire endpoint listening (SPEC-027)"
        );
        Some(server)
    } else {
        None
    };

    Ok(Server {
        http,
        tcp,
        pg,
        ctx,
        coord,
    })
}
