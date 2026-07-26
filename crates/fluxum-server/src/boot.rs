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
    /// The shard the listeners drive.
    pub ctx: Arc<ShardContext>,
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

/// Build the shard context from `config` and the link-time registry.
///
/// Split from [`serve`] so a test can assemble a shard without binding ports.
pub fn assemble(config: &Config) -> Result<Arc<ShardContext>, BootError> {
    // `assemble` collects and validates the link-time registry, which is the
    // same path `ServerBuilder::build` documents.
    let schema = Schema::assemble()?;
    if schema.is_empty() {
        return Err(BootError::NoTables);
    }

    // Shard 0 of this process. Multi-shard hosting is ShardCoord's job
    // (SPEC-024); a single-process server owns one shard.
    let shard = 0_u32;

    // SPEC-015: the live store serves through a paged cold tier whose buffer
    // pool is sized from the effective `memory.budget` (TIER-002/003) over the
    // configured `storage.page_dir`, so steady-state RSS is bounded by the
    // budget rather than the resident row count (TIER-004) — the pillar the
    // billion-row soak (T7.7) exercises.
    let hardware = fluxum_core::hw::HardwareProfile::probe();
    let effective = fluxum_core::hw::derive(&hardware, config)?;
    std::fs::create_dir_all(&config.storage.page_dir).map_err(fluxum_core::FluxumError::from)?;
    let pager = fluxum_core::store::pager::Pager::open(
        &config.storage.page_dir,
        fluxum_core::store::pager::PagerOptions::from_effective(config, &effective, shard),
    )?;
    let store = Arc::new(MemStore::with_pager(
        &schema,
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
    std::fs::create_dir_all(&config.storage.commit_log_dir)
        .map_err(fluxum_core::FluxumError::from)?;
    let repo = Arc::new(fluxum_core::checkpoint::CheckpointRepo::open(
        &config.storage.checkpoint_dir,
    )?);
    let recovery =
        fluxum_core::checkpoint::recover(&store, &repo, &config.storage.commit_log_dir, shard)?;
    if recovery.last_tx_id.is_some() || !recovery.rejected.is_empty() {
        tracing::info!(
            target: "fluxum::server",
            last_tx_id = ?recovery.last_tx_id,
            checkpoint_tx_id = ?recovery.checkpoint_tx_id,
            replayed_records = recovery.applied_records,
            rejected_checkpoints = recovery.rejected.len(),
            "recovered shard state (STG-030)"
        );
    }

    // SPEC-014 REP-004/REP-072: the fencing epoch this boot acts under is
    // the highest of the persisted replication epoch and any PITR lineage
    // marker (a forked history must start above everything it forked from).
    // It is persisted before the member acts under it.
    let epoch = fluxum_core::backup::pitr_lineage_min_epoch(&config.storage.commit_log_dir)?
        .unwrap_or(0)
        .max(crate::replication::load_epoch(
            &config.storage.commit_log_dir,
        )?)
        .max(1);
    crate::replication::persist_epoch(&config.storage.commit_log_dir, epoch)?;
    if epoch > 1 {
        tracing::info!(
            target: "fluxum::server",
            epoch,
            "booting under persisted fencing epoch (REP-004)"
        );
    }
    let log = Arc::new(CommitLog::open(
        &config.storage.commit_log_dir,
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

    // SPEC-020 PLG-001/032: validate the plugin manifest against the schema
    // (capability exists, placement legal for the host, in-proc feature
    // compiled, applies_to targets exist) — any violation aborts startup. An
    // empty manifest yields a registry of just the adopted built-in seams.
    let schema = Arc::new(schema);
    let plugins = Arc::new(fluxum_core::plugin::PluginRegistry::build(&schema, config)?);
    let subs = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
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
    ctx.set_conn_guard(Arc::new(crate::connguard::ConnGuard::new(
        crate::connguard::ConnLimits::from_config(&config.server.connection_limits),
    )));
    // SPEC-026 SEC-054: the admin access policy, from config; the profile
    // decides whether the console admits anonymous callers (DEV-031). (Also
    // republished on hot reload via `publish_reloadable`.)
    ctx.set_admin_policy(crate::AdminPolicy::from_config_with_profile(
        &config.server.admin,
        config.profile,
    )?);
    // FR-05 / HWA-012/013: probe the hardware (container-aware — cgroup CPU
    // and memory limits win over host totals), derive the `auto` keys, and
    // install the result so `GET /health` reports the probe inputs and every
    // derived value with its provenance. `main.rs` runs the same derivation
    // earlier to size the Tokio runtime, which exists before this is reached.
    let hardware = fluxum_core::hw::HardwareProfile::probe();
    ctx.set_effective_config(&fluxum_core::hw::derive(&hardware, config)?);
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
        Some(Arc::new(fluxum_core::backup::remote::RemoteArchiver::new(
            Arc::new(store),
            remote.effective_prefix(),
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
                log_dir: config.storage.commit_log_dir.clone(),
                archive_dir: archive.enabled.then(|| archive.dir.clone()),
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
        config.storage.commit_log_dir.clone(),
        config.storage.checkpoint_dir.clone(),
        epoch,
        crate::replication::PrimaryOptions {
            heartbeat_interval: Duration::from_millis(config.replication.heartbeat_interval_ms),
            window_bytes: config.replication.window_bytes.as_u64(),
            semi_sync,
        },
    ));
    // SPEC-014 §5 (T7.2): the election state serves votes and publishes
    // the role the moment the member has peers — REP-003: the config role
    // is a bootstrap hint; consensus owns it after the first election. A
    // standalone node (no peers) skips it and is always the primary.
    if !config.replication.peers.is_empty() {
        let primary = config.replication.role == fluxum_core::config::ReplicationRole::Primary;
        let election = crate::election::ElectionState::new(
            shard,
            config.replication.member_name.clone(),
            config.storage.commit_log_dir.clone(),
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
    // SPEC-020 §6 (PLG-050): install the registry for `GET /plugins`
    // introspection and spawn a CDC pump per `stream_sink` binding, fed off
    // the durable commit log (never the write path). The pumps are detached
    // like the checkpoint worker and replica client; they end when the log
    // closes at shutdown.
    ctx.set_plugins(Arc::clone(&plugins));
    let _cdc_pumps = crate::cdc::spawn_sinks(
        &ctx,
        &plugins,
        config.storage.commit_log_dir.clone(),
        config.storage.data_dir.join("cdc"),
    );
    Ok(ctx)
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
    let ctx = assemble(&config)?;

    let idle = match config.server.idle_timeout_secs {
        0 => None,
        secs => Some(Duration::from_secs(secs)),
    };
    let max_frame_bytes = u32::try_from(config.server.max_frame_bytes.as_u64())
        .unwrap_or(fluxum_protocol::DEFAULT_MAX_FRAME_BYTES);

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

    Ok(Server { http, tcp, pg, ctx })
}
