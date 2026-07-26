//! Version-scoped page reclamation for the copy-on-write paged store
//! (SPEC-015 TIER-061).
//!
//! The resident store bounded old-version memory for free: an `imbl` chunk
//! lived exactly as long as some `Arc` still pointed at it, and dropping the
//! last reference reclaimed it. The paged store reproduces that with explicit
//! bookkeeping. Each published [`crate::store::committed::CommittedState`] is a
//! *version*; a copy-on-write commit rewrites the touched root-to-leaf paths to
//! fresh pages and hands the pages it **superseded** to [`Reclaimer::retire`],
//! tagged with the new version. A superseded page recorded at version `V` was
//! reachable only from versions `< V`, so it is safe to free once **no live
//! version is `< V`** — i.e. once every snapshot, retained temporal-window
//! state, and in-flight transaction that could still reach it has dropped.
//!
//! Liveness is tracked by [`VersionGuard`]: a `CommittedState` holds one that
//! pins its version on construction and unpins it on drop, so a version stays
//! live for exactly as long as any `Arc<CommittedState>` at that version does —
//! piggy-backing on the same reference-count lifecycle `imbl`+`Arc` used, with
//! page frees deferred to the reclaimer instead of running inline.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use crate::error::Result;
use crate::store::TableId;

use super::Pager;

/// A page superseded by a copy-on-write write: `(table, page_id)` on the
/// per-shard pager.
pub type SupersededPage = (TableId, u64);

#[derive(Debug, Default)]
struct Inner {
    /// Pages that became unreachable when a version was published (keyed by
    /// that version): reachable only from strictly older versions, so freed
    /// once no live version is older than the key.
    garbage: BTreeMap<u64, Vec<SupersededPage>>,
    /// Live pinned versions → pin count. The smallest key is the reclamation
    /// floor: garbage keyed at or below it can never be reached again.
    live: BTreeMap<u64, u64>,
}

/// The per-shard version reclaimer (TIER-061). Shared (`Arc`) across every
/// table's paged trees and every [`VersionGuard`].
#[derive(Debug)]
pub struct Reclaimer {
    pager: Arc<Pager>,
    inner: Mutex<Inner>,
}

impl Reclaimer {
    /// A reclaimer over `pager`'s page files.
    pub fn new(pager: Arc<Pager>) -> Arc<Self> {
        Arc::new(Self {
            pager,
            inner: Mutex::new(Inner::default()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Pin `version` as live (a snapshot / committed state now references it).
    fn pin(&self, version: u64) {
        *self.lock().live.entry(version).or_insert(0) += 1;
    }

    /// Unpin `version`; when its last reference drops, reclaim every superseded
    /// batch that is now unreachable.
    fn unpin(&self, version: u64) -> Result<()> {
        {
            let mut inner = self.lock();
            if let Some(count) = inner.live.get_mut(&version) {
                *count -= 1;
                if *count == 0 {
                    inner.live.remove(&version);
                }
            }
        }
        self.collect()
    }

    /// Record the pages a commit at `version` superseded (reachable only from
    /// versions `< version`), then reclaim anything now unreachable.
    pub fn retire(&self, version: u64, pages: Vec<SupersededPage>) -> Result<()> {
        if !pages.is_empty() {
            let mut inner = self.lock();
            inner.garbage.entry(version).or_default().extend(pages);
        }
        self.collect()
    }

    /// Free every superseded batch keyed at or below the current reclamation
    /// floor (the smallest live version, or all of it when none is live). The
    /// pages are gathered under the lock, then freed outside it so page-file
    /// I/O never runs while the reclaimer mutex is held.
    fn collect(&self) -> Result<()> {
        let drained = {
            let mut inner = self.lock();
            let floor = inner.live.keys().next().copied().unwrap_or(u64::MAX);
            // Split off everything strictly above the floor; keep it pending,
            // and take what is at or below the floor to free.
            let keep = inner.garbage.split_off(&floor.saturating_add(1));
            std::mem::replace(&mut inner.garbage, keep)
        };
        for (_version, pages) in drained {
            for (table_id, page_id) in pages {
                self.pager.free_page(table_id, page_id)?;
            }
        }
        Ok(())
    }

    /// Test/diagnostic: how many superseded pages are still pending reclaim.
    #[cfg(test)]
    pub(crate) fn pending_pages(&self) -> usize {
        self.lock().garbage.values().map(Vec::len).sum()
    }
}

/// Pins one version live for as long as it exists (held inside the version's
/// [`CommittedState`]). Dropping it unpins the version and reclaims any pages
/// that its retirement made unreachable.
#[derive(Debug)]
pub struct VersionGuard {
    reclaimer: Arc<Reclaimer>,
    version: u64,
}

impl VersionGuard {
    /// Pin `version` in `reclaimer`.
    pub fn new(reclaimer: &Arc<Reclaimer>, version: u64) -> Self {
        reclaimer.pin(version);
        Self {
            reclaimer: Arc::clone(reclaimer),
            version,
        }
    }

    /// The version this guard pins.
    pub fn version(&self) -> u64 {
        self.version
    }
}

impl Drop for VersionGuard {
    fn drop(&mut self) {
        // A reclaim I/O error at drop time cannot be surfaced; the pages simply
        // stay pending (the page file is bounded by the checkpoint rewrite, not
        // by prompt reclaim), so swallowing it is safe.
        let _ = self.reclaimer.unpin(self.version);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::PageCompression;
    use crate::store::pager::{PagedTree, Pager, PagerOptions};

    fn pager(dir: &std::path::Path) -> Arc<Pager> {
        Pager::open(
            dir,
            PagerOptions {
                shard_id: 0,
                page_size: 256,
                pool_capacity_bytes: 64 * 256,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: PageCompression::None,
                compression_min_bytes: 1024,
            },
        )
        .unwrap()
    }

    #[test]
    fn garbage_is_held_until_the_older_version_unpins() {
        let dir = tempfile::tempdir().unwrap();
        let pager = pager(dir.path());
        let table = TableId::from_raw(1);
        let reclaimer = Reclaimer::new(Arc::clone(&pager));

        // Version 0 published and pinned; build a tree at it.
        let g0 = VersionGuard::new(&reclaimer, 0);
        let mut tree = PagedTree::create(&pager, table, false).unwrap();
        for i in 0..40u32 {
            tree.insert(format!("k-{i:04}").as_bytes(), &i.to_le_bytes())
                .unwrap();
        }
        let v0_root = tree.clone(); // a snapshot at version 0

        // Version 1: CoW updates, retire the superseded pages at version 1.
        let g1 = VersionGuard::new(&reclaimer, 1);
        let mut superseded = Vec::new();
        for i in 0..40u32 {
            tree.insert_cow(
                format!("k-{i:04}").as_bytes(),
                &(i + 1).to_le_bytes(),
                &mut superseded,
            )
            .unwrap();
        }
        let n = superseded.len();
        assert!(n > 0);
        reclaimer
            .retire(1, superseded.into_iter().map(|p| (table, p)).collect())
            .unwrap();

        // Version 0 is still pinned (the snapshot exists) → nothing freed, and
        // the snapshot still reads the original values.
        assert_eq!(
            reclaimer.pending_pages(),
            n,
            "garbage held while v0 is live"
        );
        assert_eq!(
            v0_root.get(b"k-0007").unwrap(),
            Some(7u32.to_le_bytes().to_vec())
        );

        // Drop the version-0 guard: with no live version below 1, the pages
        // reclaim.
        drop(g0);
        assert_eq!(reclaimer.pending_pages(), 0, "v0 retired → garbage freed");
        // The version-1 tree is intact (it references only fresh pages).
        assert_eq!(
            tree.get(b"k-0007").unwrap(),
            Some(8u32.to_le_bytes().to_vec())
        );
        drop(g1);
        drop(v0_root);
    }

    #[test]
    fn retire_with_no_live_version_frees_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let pager = pager(dir.path());
        let reclaimer = Reclaimer::new(Arc::clone(&pager));
        // No guards live at all: a retirement is unreachable by construction.
        reclaimer.retire(5, vec![]).unwrap();
        assert_eq!(reclaimer.pending_pages(), 0);
    }
}
