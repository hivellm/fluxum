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
use fluxum_core::reducer::{
    LifecycleHooks, ReducerEngine, ReducerRegistry, registered_reducers,
};
use fluxum_core::schema::Schema;
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};

use crate::ShardContext;
use crate::http::{self, HttpOptions, HttpServer};
use crate::tcp::{self, TcpOptions, TcpServer};

/// A running server: both listeners plus the context they share.
pub struct Server {
    /// Streamable HTTP `/rpc` + the admin API.
    pub http: HttpServer,
    /// FluxRPC over raw TCP.
    pub tcp: TcpServer,
    /// The shard the two listeners drive.
    pub ctx: Arc<ShardContext>,
}

impl Server {
    /// Stop both listeners.
    pub fn shutdown(&self) {
        self.http.shutdown();
        self.tcp.shutdown();
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

    let store = Arc::new(MemStore::new(&schema)?);
    // Shard 0 of this process. Multi-shard hosting is ShardCoord's job
    // (SPEC-024); a single-process server owns one shard.
    let shard = 0_u32;

    // The commit log is what makes a restart non-destructive: the store is in
    // memory, but every committed transaction is on disk and replayed on the
    // way back up.
    let log = Arc::new(CommitLog::open(
        &config.storage.commit_log_dir,
        shard,
        1,
        CommitLogOptions::default(),
    )?);

    let (pipeline, worker) = TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default())?;
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

    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(
        provider_from_config(&config.auth)?,
        ServerPeerRegistry::empty(),
    );

    Ok(ShardContext::new(
        engine,
        subs,
        auth,
        shard,
        COMMIT_BROADCAST_CAPACITY,
    ))
}

/// Depth of the shard-wide commit broadcast the fan-out task consumes.
///
/// Not a config key: it buffers commits between the pipeline and fan-out, and
/// per-subscriber backpressure is already governed by `send_queue_depth` on
/// each transport. Two knobs for one queue would only let them disagree.
const COMMIT_BROADCAST_CAPACITY: usize = 256;

/// Assemble and bind both listeners.
///
/// Both are bound before either is reported as up: a server answering TCP
/// while its HTTP port is still unbound looks healthy to a supervisor and is
/// unreachable to a browser.
pub async fn serve(config: Config) -> Result<Server, BootError> {
    let ctx = assemble(&config)?;

    let idle = match config.server.idle_timeout_secs {
        0 => None,
        secs => Some(Duration::from_secs(secs)),
    };
    let max_frame_bytes = u32::try_from(config.server.max_frame_bytes.as_u64())
        .unwrap_or(fluxum_protocol::DEFAULT_MAX_FRAME_BYTES);

    let http_addr = format!("{}:{}", config.server.tcp_host, config.server.http_port);
    let tcp_addr = format!("{}:{}", config.server.tcp_host, config.server.tcp_port);

    let http = http::serve(
        Arc::clone(&ctx),
        &http_addr,
        HttpOptions {
            idle_timeout: idle,
            max_frame_bytes,
            ..HttpOptions::default()
        },
    )
    .await
    .map_err(|source| BootError::Bind {
        addr: http_addr.clone(),
        source,
    })?;

    let tcp = tcp::serve(
        Arc::clone(&ctx),
        &tcp_addr,
        TcpOptions {
            idle_timeout: idle,
            max_frame_bytes,
            ..TcpOptions::default()
        },
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

    Ok(Server { http, tcp, ctx })
}
