//! Paged cold tier + buffer pool under `memory.budget` (SPEC-015, T2.8).
//!
//! SpacetimeDB needs the whole dataset in RAM; Fluxum does not. Committed
//! state is tiered: the hot working set lives uncompressed in a fixed-size
//! [`pool::BufferPool`] (clock-LRU, pin/unpin, TIER-010..015), and cold data
//! lives in Fluxum's own on-disk page format ([`format`], TIER-020..026) —
//! one page file per table per shard ([`pagefile`], TIER-023) — faulted in
//! on demand with mandatory CRC32C verification (TIER-032) and evicted under
//! the memory budget. Both the primary row map and every secondary/spatial
//! index are paged B-trees ([`tree`], TIER-050/051): index pages fault and
//! evict exactly like data pages, so index memory counts against the same
//! budget. [`metrics`] exports the TIER-080 counters.
//!
//! # Budget enforcement (TIER-001..004)
//!
//! `memory.budget: auto | <bytes>` is the single ceiling; `auto` is derived
//! from the hardware probe ([`crate::hw::derive`], TIER-002). The pool gets
//! `memory.bufferpool_fraction × budget` (TIER-003), fixed at construction
//! in frames of `storage.page_size` — the pool never allocates past it, and
//! a fault that finds no evictable frame fails with
//! [`FluxumError::BufferPoolExhausted`] instead of growing (never OOM).
//! Steady-state RSS is therefore a function of the budget, never of rows on
//! disk; the TIER-004 tolerance is [`budget_tolerance_bytes`].
//!
//! # Boundaries with neighbouring tasks
//!
//! Durability is never this tier's job: writes land in the hot
//! [`crate::store::MemStore`] and the [`crate::commitlog::CommitLog`];
//! recovery is checkpoint root + log replay (TIER-061, T2.3). Accordingly
//! the live page directory (`page_id → extent`) is in-memory here and is
//! persisted by the T2.3 checkpoint manifest.
//!
//! # Compression (TIER-040..044, T2.9)
//!
//! Cold pages are compressed on the spill path — LZ4 by default, zstd or
//! none per `storage.page_compression` — subject to the
//! `storage.compression_min_bytes` threshold and the 12.5% saving gate; the
//! codec actually used is recorded per page in the header flag bits, so
//! every page is self-describing and mixed-codec files read correctly.
//! Decompression happens exactly once, on fault-in after CRC verification;
//! pool frames always hold uncompressed images. See [`codec`] for the
//! stored-payload layout (freeze surface) and the zstd artifact codec that
//! checkpoints/backups share (TIER-042).

pub mod codec;
pub mod format;
pub mod metrics;
pub mod pagefile;
pub mod pool;
pub mod tree;

mod cold;

pub use codec::PageCodec;
pub use cold::ColdTable;
pub use metrics::{MetricsSnapshot, PagerMetrics};
pub use pool::{BufferPool, PageGuard, PoolOptions};
pub use tree::PagedTree;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use sha2::{Digest as _, Sha256};

use crate::config::{Config, PageCompression};
use crate::error::{FluxumError, Result};
use crate::hw::EffectiveConfig;
use crate::store::TableId;

use pagefile::{Extent, PageFile};

/// Coordinates of one page in the tiered store (TIER-010).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageKey {
    /// Owning shard.
    pub shard_id: u32,
    /// Owning table (STG-050 stable id).
    pub table_id: u32,
    /// Page id, unique per (shard, table).
    pub page_id: u64,
}

/// Pager construction parameters, normally derived from the effective
/// configuration ([`PagerOptions::from_effective`]).
#[derive(Debug, Clone, Copy)]
pub struct PagerOptions {
    /// Owning shard (one pager per shard; the pool may be shared later).
    pub shard_id: u32,
    /// Logical page size in bytes (TIER-022).
    pub page_size: usize,
    /// Buffer-pool capacity in bytes (TIER-003).
    pub pool_capacity_bytes: u64,
    /// Watermark that starts reclaim (TIER-031).
    pub high_watermark: f64,
    /// Watermark reclaim drains to (TIER-031).
    pub low_watermark: f64,
    /// Codec for newly written cold pages (TIER-041).
    pub compression: PageCompression,
    /// Payloads below this many bytes are stored raw (TIER-040).
    pub compression_min_bytes: usize,
}

impl PagerOptions {
    /// Derive pager options for `shard_id` from the loaded config and the
    /// hardware-derived effective configuration (TIER-002/003).
    pub fn from_effective(config: &Config, effective: &EffectiveConfig, shard_id: u32) -> Self {
        Self {
            shard_id,
            page_size: config.storage.page_size as usize,
            pool_capacity_bytes: effective.bufferpool_capacity_bytes.value,
            high_watermark: config.storage.evictor_high_watermark,
            low_watermark: config.storage.evictor_low_watermark,
            compression: config.storage.page_compression,
            compression_min_bytes: config.storage.compression_min_bytes as usize,
        }
    }

    /// Pool capacity in frames: `capacity / page_size`, floored at 8 frames
    /// so a pathological configuration still admits a working tree path.
    pub fn pool_frames(&self) -> usize {
        usize::try_from(self.pool_capacity_bytes / self.page_size as u64)
            .unwrap_or(usize::MAX)
            .max(8)
    }
}

/// The TIER-004 RSS tolerance: `max(configured floor, 0.10 × budget)`.
/// Steady-state process RSS must stay within `budget + tolerance`; the
/// droplet-profile CI job asserts it (NFR-12).
pub fn budget_tolerance_bytes(budget_bytes: u64, floor_bytes: u64) -> u64 {
    floor_bytes.max(budget_bytes / 10)
}

/// Per-table cold-tier I/O state: the page file, the live page directory,
/// and the page-id allocator.
#[derive(Debug)]
struct TableIo {
    file: Mutex<PageFile>,
    /// Live page directory: `page_id → extent`. In-memory for T2.8; the
    /// T2.3 checkpoint manifest persists it as a CoW B-tree (TIER-060).
    directory: Mutex<HashMap<u64, Extent>>,
    /// Next page id (ids start at 1; 0 is the nil sentinel).
    next_page_id: AtomicU64,
}

/// The per-shard pager: buffer pool + page files + fault/spill paths.
#[derive(Debug)]
pub struct Pager {
    shard_id: u32,
    page_size: usize,
    compression: codec::PageCodec,
    compression_min_bytes: usize,
    /// At-rest encryption keyring (SPEC-026 SEC-010): `None` disables
    /// encryption; when present, spilled pages are sealed under the active
    /// key and fault-in decrypts after CRC verification.
    keyring: Option<Arc<crate::crypto::Keyring>>,
    dir: PathBuf,
    pool: Arc<BufferPool>,
    tables: Mutex<HashMap<TableId, Arc<TableIo>>>,
    metrics: Arc<PagerMetrics>,
}

impl Pager {
    /// Open a pager rooted at `storage.page_dir`-style directory `dir`
    /// (page files live under `dir/shard-<shard_id>/`). Unencrypted; use
    /// [`Pager::open_with_keyring`] to enable at-rest encryption.
    pub fn open(dir: impl Into<PathBuf>, options: PagerOptions) -> Result<Arc<Self>> {
        Self::open_with_keyring(dir, options, None)
    }

    /// Open a pager with an optional at-rest encryption keyring (SEC-010).
    /// A `Some` keyring seals every spilled page under its active key.
    pub fn open_with_keyring(
        dir: impl Into<PathBuf>,
        options: PagerOptions,
        keyring: Option<Arc<crate::crypto::Keyring>>,
    ) -> Result<Arc<Self>> {
        let metrics = Arc::new(PagerMetrics::default());
        let pool = BufferPool::new(
            PoolOptions {
                frames: options.pool_frames(),
                page_size: options.page_size,
                high_watermark: options.high_watermark,
                low_watermark: options.low_watermark,
            },
            Arc::clone(&metrics),
        );
        Ok(Arc::new(Self {
            shard_id: options.shard_id,
            page_size: options.page_size,
            compression: codec::PageCodec::from(options.compression),
            compression_min_bytes: options.compression_min_bytes,
            keyring,
            dir: dir.into(),
            pool,
            tables: Mutex::new(HashMap::new()),
            metrics,
        }))
    }

    /// The logical page size (TIER-022).
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// The owning shard.
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    /// The TIER-080 metric counters.
    pub fn metrics(&self) -> &PagerMetrics {
        &self.metrics
    }

    /// The buffer pool (capacity/occupancy diagnostics).
    pub fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    fn lock_tables(&self) -> MutexGuard<'_, HashMap<TableId, Arc<TableIo>>> {
        self.tables.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The per-table I/O state, creating the page file on first use
    /// (TIER-023 layout: `shard-<shard_id>/table-<table_id>.pages`).
    fn table_io(&self, table_id: TableId) -> Result<Arc<TableIo>> {
        if let Some(io) = self.lock_tables().get(&table_id) {
            return Ok(Arc::clone(io));
        }
        let path = self
            .dir
            .join(format!("shard-{}", self.shard_id))
            .join(format!("table-{}.pages", table_id.as_u32()));
        let page_size = u32::try_from(self.page_size).map_err(|_| {
            FluxumError::Storage(format!("page size {} overflows u32", self.page_size))
        })?;
        let file = if path.exists() {
            let (file, recorded) = PageFile::open(&path, self.shard_id, table_id.as_u32())?;
            if recorded as usize != self.page_size {
                return Err(FluxumError::Storage(format!(
                    "page file {} was created with page_size {recorded}, but this \
                     process runs {} — the page size is fixed at creation (TIER-022)",
                    path.display(),
                    self.page_size
                )));
            }
            file
        } else {
            PageFile::create(&path, page_size, self.shard_id, table_id.as_u32())?
        };
        let io = Arc::new(TableIo {
            file: Mutex::new(file),
            directory: Mutex::new(HashMap::new()),
            next_page_id: AtomicU64::new(1),
        });
        let mut tables = self.lock_tables();
        let entry = tables.entry(table_id).or_insert(io);
        Ok(Arc::clone(entry))
    }

    /// Allocate the next page id of `table_id` (never reused within a run;
    /// extents are what get recycled, TIER-024).
    pub(crate) fn allocate_page_id(&self, table_id: TableId) -> u64 {
        match self.table_io(table_id) {
            Ok(io) => io.next_page_id.fetch_add(1, Ordering::Relaxed),
            // table_io only fails on I/O; the subsequent install/fault on
            // the same table surfaces that error with context.
            Err(_) => 1,
        }
    }

    /// The spill path (TIER-013/TIER-025): compress the page per the
    /// configured codec (TIER-040 — raw when below the threshold or the
    /// saving gate), write it copy-on-write to a fresh extent, repoint the
    /// live directory, then free the superseded extent.
    fn spill(&self, key: PageKey, image: &[u8]) -> Result<()> {
        let compressed = codec::encode_for_storage(
            image,
            self.compression,
            self.compression_min_bytes,
            self.shard_id,
            self.keyring.as_deref(),
        )?;
        let stored: &[u8] = compressed.as_deref().unwrap_or(image);
        PagerMetrics::add(
            &self.metrics.compression_raw_bytes,
            codec::payload_len(image),
        );
        PagerMetrics::add(
            &self.metrics.compression_stored_bytes,
            codec::payload_len(stored),
        );
        let io = self.table_io(TableId::from_raw(key.table_id))?;
        let extent = {
            let mut file = io.file.lock().unwrap_or_else(PoisonError::into_inner);
            file.write_page(stored)?
        };
        let old = {
            let mut directory = io.directory.lock().unwrap_or_else(PoisonError::into_inner);
            directory.insert(key.page_id, extent)
        };
        if let Some(old) = old {
            let mut file = io.file.lock().unwrap_or_else(PoisonError::into_inner);
            file.free_extent(old);
        }
        Ok(())
    }

    /// Serve a page: pool hit (zero I/O, zero decompression, TIER-014) or
    /// fault-in (TIER-032: directory lookup → one `pread` → **mandatory
    /// CRC32C verification** → decompress if the codec bits say so
    /// (TIER-044, exactly once) → insert-evicting-if-needed → pin).
    /// Concurrent misses coalesce.
    pub fn fault(self: &Arc<Self>, table_id: TableId, page_id: u64) -> Result<PageGuard> {
        let key = PageKey {
            shard_id: self.shard_id,
            table_id: table_id.as_u32(),
            page_id,
        };
        let io = self.table_io(table_id)?;
        let read = || -> Result<Vec<u8>> {
            let extent = {
                let directory = io.directory.lock().unwrap_or_else(PoisonError::into_inner);
                directory.get(&page_id).copied()
            };
            let Some(extent) = extent else {
                return Err(FluxumError::Storage(format!(
                    "page {page_id} of table {table_id} is neither resident nor in the \
                     cold tier — tiering invariant broken"
                )));
            };
            let image = {
                let file = io.file.lock().unwrap_or_else(PoisonError::into_inner);
                file.read_page(extent)?
            };
            // TIER-032 step 3: verify before serving — a tampered page is
            // never served (TIER-062 handling is the caller's rollback).
            let (header, payload) =
                format::decode_page(&image, self.shard_id, table_id.as_u32(), page_id)?;
            if header.is_index() {
                PagerMetrics::add(&self.metrics.page_reads_index, 1);
            } else {
                PagerMetrics::add(&self.metrics.page_reads_data, 1);
            }
            // TIER-032 step 4 / TIER-044: rebuild the uncompressed pool
            // image — decryption (SEC-011, after the CRC above) and
            // decompression happen here and only here.
            if header.codec() != 0 || header.is_encrypted() {
                return codec::open_image(
                    &header,
                    payload,
                    self.page_size,
                    self.shard_id,
                    self.keyring.as_deref(),
                );
            }
            Ok(image)
        };
        let spill = |key: PageKey, image: &[u8]| self.spill(key, image);
        self.pool.get_or_fault(key, &read, &spill)
    }

    /// Install a freshly created page image (tree node / overflow page).
    /// The frame starts dirty and scan-resistant (TIER-015).
    pub(crate) fn install(
        self: &Arc<Self>,
        table_id: TableId,
        page_id: u64,
        image: Vec<u8>,
    ) -> Result<PageGuard> {
        let key = PageKey {
            shard_id: self.shard_id,
            table_id: table_id.as_u32(),
            page_id,
        };
        let spill = |key: PageKey, image: &[u8]| self.spill(key, image);
        self.pool.install(key, image, false, &spill)
    }

    /// Replace a pinned page's image (single-writer node rewrite).
    pub(crate) fn write_pinned(&self, guard: &mut PageGuard, image: Vec<u8>) -> Result<()> {
        self.pool.write_page(guard, image)
    }

    /// Drop a page entirely (superseded tree node / freed overflow chain):
    /// out of the pool without spilling, and its extent back to the free
    /// list.
    pub(crate) fn free_page(&self, table_id: TableId, page_id: u64) -> Result<()> {
        let key = PageKey {
            shard_id: self.shard_id,
            table_id: table_id.as_u32(),
            page_id,
        };
        self.pool.discard(key)?;
        let io = self.table_io(table_id)?;
        let old = {
            let mut directory = io.directory.lock().unwrap_or_else(PoisonError::into_inner);
            directory.remove(&page_id)
        };
        if let Some(extent) = old {
            let mut file = io.file.lock().unwrap_or_else(PoisonError::into_inner);
            file.free_extent(extent);
        }
        Ok(())
    }

    /// Spill every dirty frame and mark it clean (flush semantics the T2.3
    /// checkpoint builds on).
    pub fn flush(&self) -> Result<()> {
        let spill = |key: PageKey, image: &[u8]| self.spill(key, image);
        self.pool.flush(&spill)?;
        for io in self.lock_tables().values() {
            io.file
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .sync()?;
        }
        Ok(())
    }

    /// Flush, then evict every unpinned frame — forces the next reads cold.
    /// Test/maintenance surface for the evict/fault round-trip suites.
    pub fn evict_all(&self) -> Result<()> {
        let spill = |key: PageKey, image: &[u8]| self.spill(key, image);
        self.pool.evict_all(&spill)
    }

    /// The page's lazily computed content hash (TIER-063): SHA-256 over the
    /// uncompressed page image. Recomputed on demand, so it can never be
    /// stale; it is the object key of the STG-021 content-addressed
    /// checkpoint scheme (shared mechanism, wired by T2.3).
    pub fn content_hash(self: &Arc<Self>, table_id: TableId, page_id: u64) -> Result<[u8; 32]> {
        let guard = self.fault(table_id, page_id)?;
        let digest = Sha256::digest(guard.image());
        Ok(digest.into())
    }

    /// Where a page currently lives in its page file, if it has been
    /// spilled: `(offset, len)`. Diagnostics surface (crash/corruption
    /// drills target live extents with it).
    pub fn page_extent(&self, table_id: TableId, page_id: u64) -> Result<Option<(u64, u64)>> {
        let io = self.table_io(table_id)?;
        let directory = io.directory.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(directory.get(&page_id).map(|e| (e.offset, e.len)))
    }

    /// On-disk page-file footprint of `table_id` in bytes
    /// (`fluxum_coldtier_bytes` input).
    pub fn coldtier_bytes(&self, table_id: TableId) -> Result<u64> {
        let io = self.table_io(table_id)?;
        let file = io.file.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(file.allocated_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tolerance_is_max_of_floor_and_ten_percent() {
        let floor = 64 << 20;
        // Small budget: the 64 MiB floor dominates.
        assert_eq!(budget_tolerance_bytes(256 << 20, floor), 64 << 20);
        // Large budget: 10% dominates.
        assert_eq!(budget_tolerance_bytes(10 << 30, floor), 1 << 30);
    }

    #[test]
    fn options_derive_from_the_droplet_profile() {
        use crate::hw::{HardwareProfile, derive};

        // The NFR-12 reference droplet: 1 vCPU / 512 MB.
        let hw = HardwareProfile {
            logical_cores: 1,
            physical_cores: 1,
            total_ram_bytes: 512 << 20,
            available_ram_bytes: 512 << 20,
            cgroup_cpu_quota: None,
            cgroup_memory_limit_bytes: None,
        };
        let lookup = |key: &str| -> Option<String> {
            (key == "FLUXUM_PROFILE").then(|| "development".to_owned())
        };
        let config = match Config::load_with(None, &lookup) {
            Ok(c) => c,
            Err(e) => panic!("{e}"),
        };
        let effective = match derive(&hw, &config) {
            Ok(e) => e,
            Err(e) => panic!("{e}"),
        };
        // TIER-002: auto budget = max(128 MiB, 0.5 × 512 MiB) = 256 MiB.
        assert_eq!(effective.memory_budget_bytes.value, 256 << 20);

        let options = PagerOptions::from_effective(&config, &effective, 0);
        // TIER-003: pool capacity = 0.8 × 256 MiB; frames at 8 KiB pages.
        assert_eq!(
            options.pool_capacity_bytes,
            (0.8f64 * (256u64 << 20) as f64) as u64
        );
        assert_eq!(options.page_size, 8192);
        assert_eq!(
            options.pool_frames() as u64,
            options.pool_capacity_bytes / 8192
        );
        assert_eq!(options.high_watermark, 0.95);
        assert_eq!(options.low_watermark, 0.90);
        // TIER-040/041 defaults: LZ4 above 1 KiB.
        assert_eq!(options.compression, PageCompression::Lz4);
        assert_eq!(options.compression_min_bytes, 1024);
    }

    #[test]
    fn fault_of_an_unknown_page_names_the_invariant() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let pager = Pager::open(
            dir.path(),
            PagerOptions {
                shard_id: 0,
                page_size: 4096,
                pool_capacity_bytes: 16 * 4096,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: PageCompression::Lz4,
                compression_min_bytes: 1024,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let err = match pager.fault(TableId::from_raw(1), 42) {
            Ok(_) => panic!("faulted a page that never existed"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("neither resident"), "{err}");
    }

    fn open_pager(dir: &std::path::Path, page_size: usize) -> Arc<Pager> {
        Pager::open(
            dir,
            PagerOptions {
                shard_id: 0,
                page_size,
                pool_capacity_bytes: 16 * 4096,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: PageCompression::None,
                compression_min_bytes: 1024,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn shard_id_is_exposed() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let pager = open_pager(dir.path(), 4096);
        assert_eq!(pager.shard_id(), 0);
    }

    #[test]
    fn a_page_size_overflowing_u32_is_rejected_and_allocation_degrades() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let pager = open_pager(dir.path(), (u32::MAX as usize) + 1);
        let table = TableId::from_raw(7);
        let err = match pager.coldtier_bytes(table) {
            Ok(_) => panic!("4 GiB+ page size accepted"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("overflows u32"), "{err}");
        // allocate_page_id degrades to 1; the follow-up install/fault on the
        // same table surfaces the real error with context.
        assert_eq!(pager.allocate_page_id(table), 1);
    }

    #[test]
    fn reopening_a_page_file_with_a_different_page_size_is_rejected() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let table = TableId::from_raw(9);
        {
            let pager = open_pager(dir.path(), 4096);
            // First use creates shard-0/table-9.pages with page_size 4096.
            assert!(
                pager
                    .coldtier_bytes(table)
                    .unwrap_or_else(|e| panic!("{e}"))
                    > 0
            );
        }
        // Same page size: the existing file reopens fine (TIER-022).
        {
            let pager = open_pager(dir.path(), 4096);
            assert!(
                pager
                    .coldtier_bytes(table)
                    .unwrap_or_else(|e| panic!("{e}"))
                    > 0
            );
        }
        // Different page size: refused, naming both sizes.
        let pager = open_pager(dir.path(), 8192);
        let err = match pager.coldtier_bytes(table) {
            Ok(_) => panic!("page-size mismatch accepted"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("page_size 4096"), "{err}");
        assert!(err.to_string().contains("8192"), "{err}");
        assert!(err.to_string().contains("TIER-022"), "{err}");
    }

    #[test]
    fn free_page_returns_the_spilled_extent_to_the_free_list() {
        use super::format::{PageHeader, encode_page};

        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let pager = open_pager(dir.path(), 4096);
        let table = TableId::from_raw(3);
        let page_id = pager.allocate_page_id(table);
        let header = PageHeader::new(page_id, table.as_u32(), 0, 0);
        let image = encode_page(&header, &[0xEE; 100]).unwrap_or_else(|e| panic!("{e}"));
        drop(
            pager
                .install(table, page_id, image)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        pager.flush().unwrap_or_else(|e| panic!("{e}"));
        assert!(
            pager
                .page_extent(table, page_id)
                .unwrap_or_else(|e| panic!("{e}"))
                .is_some(),
            "flush must have spilled the dirty page to an extent"
        );
        pager
            .free_page(table, page_id)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            pager
                .page_extent(table, page_id)
                .unwrap_or_else(|e| panic!("{e}")),
            None,
            "freeing must drop the directory entry"
        );
        let err = match pager.fault(table, page_id) {
            Ok(_) => panic!("freed page still faults"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("neither resident"), "{err}");
    }

    #[test]
    fn encrypted_pager_seals_the_cold_tier_and_faults_in_clear() {
        use super::format::{PageHeader, encode_page};
        use crate::crypto::{AtRestKey, Keyring};

        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let keyring = Arc::new(Keyring::new(AtRestKey::new("active", [1u8; 32]), vec![]));
        let pager = Pager::open_with_keyring(
            dir.path(),
            PagerOptions {
                shard_id: 0,
                page_size: 4096,
                pool_capacity_bytes: 16 * 4096,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: PageCompression::Lz4,
                compression_min_bytes: 1024,
            },
            Some(keyring),
        )
        .unwrap_or_else(|e| panic!("{e}"));

        let table = TableId::from_raw(7);
        let page_id = pager.allocate_page_id(table);
        // A compressible payload carrying a recognizable plaintext marker.
        let marker: &[u8] = b"COLD-TIER-PLAINTEXT-MARKER";
        let mut payload: Vec<u8> = b"the quick brown fox jumps over the lazy dog -- "
            .iter()
            .copied()
            .cycle()
            .take(3000)
            .collect();
        payload[50..50 + marker.len()].copy_from_slice(marker);
        let image = encode_page(
            &PageHeader::new(page_id, table.as_u32(), 5, super::format::FLAG_INDEX),
            &payload,
        )
        .unwrap_or_else(|e| panic!("{e}"));

        // Install, then force the page out to the cold tier.
        drop(
            pager
                .install(table, page_id, image.clone())
                .unwrap_or_else(|e| panic!("{e}")),
        );
        pager.flush().unwrap_or_else(|e| panic!("{e}"));
        pager.evict_all().unwrap_or_else(|e| panic!("{e}"));

        // The stored extent must be encrypted: the plaintext marker is gone
        // and the page header carries FLAG_ENCRYPTED.
        let (offset, len) = match pager
            .page_extent(table, page_id)
            .unwrap_or_else(|e| panic!("{e}"))
        {
            Some(extent) => extent,
            None => panic!("page spilled to an extent"),
        };
        let io = pager.table_io(table).unwrap_or_else(|e| panic!("{e}"));
        let stored = {
            let file = io.file.lock().unwrap_or_else(PoisonError::into_inner);
            file.read_page(Extent { offset, len })
                .unwrap_or_else(|e| panic!("{e}"))
        };
        assert!(
            !stored.windows(marker.len()).any(|w| w == marker),
            "plaintext marker leaked to the cold-tier page file"
        );
        let (header, _) = super::format::decode_page(&stored, 0, table.as_u32(), page_id)
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(header.is_encrypted(), "cold page must carry FLAG_ENCRYPTED");

        // Fault-in decrypts and rebuilds the exact original pool image.
        let faulted = pager
            .fault(table, page_id)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            faulted.image(),
            image.as_slice(),
            "cold round-trip diverged"
        );
    }
}
