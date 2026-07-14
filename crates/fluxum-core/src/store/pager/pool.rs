//! [`BufferPool`] — fixed-capacity frame pool with clock-LRU (second-chance)
//! eviction, pin/unpin, and single-flight fault-in (TIER-010..TIER-015).
//!
//! The pool is the memory-budget enforcement point (TIER-003): capacity is
//! fixed at construction (`bufferpool_fraction × memory.budget`, in frames of
//! `storage.page_size` bytes) and is **never** exceeded — a fault that finds
//! no evictable frame fails with [`FluxumError::BufferPoolExhausted`] instead
//! of allocating past the ceiling. Watermark reclaim (TIER-031) runs inline
//! on the install path with hysteresis: crossing the high watermark starts
//! reclaiming victims until occupancy is back at the low watermark. (The
//! dedicated background evictor task that decouples spill I/O from the shard
//! writer thread arrives with the T2.3 checkpoint flusher, which owns the
//! same write path; the eviction policy and budget invariants are final
//! here.)
//!
//! A pool **hit** is: map lookup → set the `referenced` bit → clone the
//! frame's image `Arc` — no disk I/O, no decompression, no frame allocation
//! (TIER-014). Frames hold uncompressed page images; a hit never touches a
//! codec (TIER-044).
//!
//! Dirty frames are never discarded (TIER-013): eviction spills them to the
//! cold tier first, via the spill callback the pager supplies. While a spill
//! or fault is in flight its key is held in a single-flight set, so
//! concurrent misses on the same page coalesce into one physical read
//! (TIER-032) and a page being spilled cannot be faulted half-written.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use crate::error::{FluxumError, Result};

use super::PageKey;
use super::metrics::PagerMetrics;

/// Reads one page image from the cold tier (fault-in I/O + CRC verify).
pub type ReadFn<'a> = &'a dyn Fn() -> Result<Vec<u8>>;

/// Writes one dirty page image to the cold tier (evictor spill / flush).
pub type SpillFn<'a> = &'a dyn Fn(PageKey, &[u8]) -> Result<()>;

/// Pool sizing and watermarks, fixed at construction (TIER-003/TIER-031).
#[derive(Debug, Clone, Copy)]
pub struct PoolOptions {
    /// Capacity in frames; each frame holds one `page_size` image.
    pub frames: usize,
    /// Logical page size in bytes (frame accounting granularity).
    pub page_size: usize,
    /// Occupancy fraction that starts watermark reclaim.
    pub high_watermark: f64,
    /// Occupancy fraction reclaim drains down to.
    pub low_watermark: f64,
}

/// One resident page (TIER-010).
#[derive(Debug)]
struct Frame {
    key: PageKey,
    /// Uncompressed page image (header + payload). `Arc` so readers keep a
    /// consistent image without holding the pool lock; in-place mutation is
    /// an `Arc` swap, never a data race.
    data: Arc<Vec<u8>>,
    /// Pin count (TIER-012): pinned frames are never evicted.
    pins: u32,
    /// Second-chance bit (TIER-011).
    referenced: bool,
    /// Modified since last spill/flush (TIER-013).
    dirty: bool,
}

#[derive(Debug, Default)]
struct PoolInner {
    map: HashMap<PageKey, usize>,
    frames: Vec<Option<Frame>>,
    free_slots: Vec<usize>,
    /// Clock hand (TIER-011).
    hand: usize,
    /// Keys with a fault or spill in flight (single-flight, TIER-032).
    inflight: HashSet<PageKey>,
    /// Watermark-reclaim hysteresis state (TIER-031).
    reclaiming: bool,
}

impl PoolInner {
    fn occupied(&self) -> usize {
        self.map.len()
    }
}

/// The process-wide buffer pool (one per budget domain, TIER-010).
#[derive(Debug)]
pub struct BufferPool {
    inner: Mutex<PoolInner>,
    cond: Condvar,
    opts: PoolOptions,
    high_frames: usize,
    low_frames: usize,
    metrics: Arc<PagerMetrics>,
}

/// A pinned page (TIER-012): keeps its frame unevictable and its image
/// alive. Drop releases the pin — every pin a transaction takes is released
/// no later than its commit or rollback because guards live inside the
/// operation that took them.
#[derive(Debug)]
pub struct PageGuard {
    pool: Arc<BufferPool>,
    slot: usize,
    key: PageKey,
    data: Arc<Vec<u8>>,
}

impl PageGuard {
    /// The page's coordinates.
    pub fn key(&self) -> PageKey {
        self.key
    }

    /// The full uncompressed page image (header + payload).
    pub fn image(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        let mut inner = self.pool.lock();
        if let Some(frame) = inner.frames[self.slot].as_mut()
            && frame.key == self.key
        {
            debug_assert!(frame.pins > 0, "unpinning an unpinned frame");
            frame.pins = frame.pins.saturating_sub(1);
        } else {
            debug_assert!(false, "pinned frame vanished under its guard");
        }
    }
}

/// Clears an in-flight key on scope exit (read error, exhaustion, or
/// success) and wakes every waiter so coalesced misses can retry.
struct InflightGuard<'a> {
    pool: &'a BufferPool,
    key: PageKey,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        let mut inner = self.pool.lock();
        inner.inflight.remove(&self.key);
        self.pool.cond.notify_all();
    }
}

impl BufferPool {
    /// Build a pool with `opts.frames` empty frames.
    pub fn new(opts: PoolOptions, metrics: Arc<PagerMetrics>) -> Arc<Self> {
        let frames = opts.frames.max(1);
        let high_frames = ((opts.high_watermark * frames as f64).ceil() as usize).clamp(1, frames);
        let low_frames = ((opts.low_watermark * frames as f64).floor() as usize).min(high_frames);
        let mut inner = PoolInner {
            frames: Vec::with_capacity(frames),
            ..PoolInner::default()
        };
        for slot in 0..frames {
            inner.frames.push(None);
            inner.free_slots.push(slot);
        }
        PagerMetrics::add(
            &metrics.bufferpool_capacity_bytes,
            (frames * opts.page_size) as u64,
        );
        Arc::new(Self {
            inner: Mutex::new(inner),
            cond: Condvar::new(),
            opts: PoolOptions { frames, ..opts },
            high_frames,
            low_frames,
            metrics,
        })
    }

    /// Pool capacity in frames.
    pub fn capacity_frames(&self) -> usize {
        self.opts.frames
    }

    /// Resident frames right now.
    pub fn occupied_frames(&self) -> usize {
        self.lock().occupied()
    }

    fn lock(&self) -> MutexGuard<'_, PoolInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The hit path (TIER-014): map lookup → set `referenced` → pin. Zero
    /// I/O, zero allocation beyond the guard. `None` on miss.
    pub fn lookup(self: &Arc<Self>, key: PageKey) -> Option<PageGuard> {
        let mut inner = self.lock();
        let &slot = inner.map.get(&key)?;
        let frame = inner.frames[slot].as_mut()?;
        frame.referenced = true;
        frame.pins += 1;
        let data = Arc::clone(&frame.data);
        drop(inner);
        PagerMetrics::add(&self.metrics.hits, 1);
        Some(PageGuard {
            pool: Arc::clone(self),
            slot,
            key,
            data,
        })
    }

    /// Serve `key` from the pool, faulting it in via `read` on a miss
    /// (TIER-032). Concurrent misses on the same key coalesce: exactly one
    /// caller runs `read`, the rest wait and share the resulting frame.
    pub fn get_or_fault(
        self: &Arc<Self>,
        key: PageKey,
        read: ReadFn<'_>,
        spill: SpillFn<'_>,
    ) -> Result<PageGuard> {
        {
            let mut inner = self.lock();
            loop {
                if let Some(&slot) = inner.map.get(&key) {
                    // Hit under the same lock (no gap for eviction races).
                    let frame = inner.frames[slot].as_mut().ok_or_else(|| {
                        FluxumError::Storage("pool map points at an empty slot".into())
                    })?;
                    frame.referenced = true;
                    frame.pins += 1;
                    let data = Arc::clone(&frame.data);
                    drop(inner);
                    PagerMetrics::add(&self.metrics.hits, 1);
                    return Ok(PageGuard {
                        pool: Arc::clone(self),
                        slot,
                        key,
                        data,
                    });
                }
                if !inner.inflight.contains(&key) {
                    inner.inflight.insert(key);
                    break;
                }
                // Another thread is faulting (or spilling) this page —
                // coalesce: wait, then re-check (TIER-032 single flight).
                inner = self
                    .cond
                    .wait(inner)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
        let _inflight = InflightGuard { pool: self, key };
        PagerMetrics::add(&self.metrics.misses, 1);
        let image = read()?; // I/O + CRC verify outside the pool lock
        self.install_with(key, image, false, true, spill)
    }

    /// Insert a brand-new page image (bulk load / node split): the frame
    /// starts dirty (it exists nowhere in the cold tier yet) and pinned.
    /// `referenced` starts clear for bulk-load streams so one large load
    /// cannot flush the resident working set (TIER-015 scan resistance).
    pub fn install(
        self: &Arc<Self>,
        key: PageKey,
        image: Vec<u8>,
        referenced: bool,
        spill: SpillFn<'_>,
    ) -> Result<PageGuard> {
        self.install_with(key, image, true, referenced, spill)
    }

    /// Shared insert path: makes room (evicting under the watermark policy),
    /// installs the frame pinned once, and returns its guard.
    fn install_with(
        self: &Arc<Self>,
        key: PageKey,
        image: Vec<u8>,
        dirty: bool,
        referenced: bool,
        spill: SpillFn<'_>,
    ) -> Result<PageGuard> {
        let data = Arc::new(image);
        loop {
            let mut inner = self.lock();
            if let Some(&slot) = inner.map.get(&key) {
                // Lost a race (fault completed elsewhere): serve the
                // resident frame instead of double-installing.
                let frame = inner.frames[slot].as_mut().ok_or_else(|| {
                    FluxumError::Storage("pool map points at an empty slot".into())
                })?;
                frame.referenced = true;
                frame.pins += 1;
                let data = Arc::clone(&frame.data);
                return Ok(PageGuard {
                    pool: Arc::clone(self),
                    slot,
                    key,
                    data,
                });
            }

            let occupied = inner.occupied();
            // TIER-031 hysteresis: crossing high starts reclaim; reclaim
            // runs until occupancy is back at low.
            if occupied + 1 > self.high_frames {
                inner.reclaiming = true;
            }
            let must_evict = occupied >= self.opts.frames;
            let want_evict = inner.reclaiming && occupied + 1 > self.low_frames;
            if !must_evict && !want_evict {
                inner.reclaiming = false;
                let slot = inner.free_slots.pop().ok_or(FluxumError::Storage(
                    "pool accounting broke: occupancy under capacity with no free slot".into(),
                ))?;
                inner.frames[slot] = Some(Frame {
                    key,
                    data: Arc::clone(&data),
                    pins: 1,
                    referenced,
                    dirty,
                });
                inner.map.insert(key, slot);
                drop(inner);
                PagerMetrics::add(&self.metrics.bufferpool_bytes, self.opts.page_size as u64);
                return Ok(PageGuard {
                    pool: Arc::clone(self),
                    slot,
                    key,
                    data,
                });
            }

            // Need a victim (TIER-011): second-chance sweep preferring
            // clean frames over dirty ones.
            let Some(victim_slot) = pick_victim(&mut inner) else {
                if must_evict {
                    // Every frame is pinned: fail, never over-allocate
                    // (TIER-003/TIER-012).
                    return Err(FluxumError::BufferPoolExhausted {
                        capacity: self.opts.frames,
                    });
                }
                // Opportunistic reclaim found nothing evictable; give up on
                // the watermark for now and take the free slot.
                inner.reclaiming = false;
                continue;
            };
            let victim = inner.frames[victim_slot]
                .take()
                .ok_or_else(|| FluxumError::Storage("clock hand picked an empty slot".into()))?;
            inner.map.remove(&victim.key);
            inner.free_slots.push(victim_slot);
            let spill_pending = victim.dirty;
            if spill_pending {
                // Hold the key in the single-flight set while the spill
                // runs so a concurrent fault cannot read a stale extent.
                inner.inflight.insert(victim.key);
            }
            drop(inner);
            PagerMetrics::sub(&self.metrics.bufferpool_bytes, self.opts.page_size as u64);
            if spill_pending {
                let _inflight = InflightGuard {
                    pool: self,
                    key: victim.key,
                };
                let spilled = spill(victim.key, &victim.data);
                if let Err(e) = spilled {
                    // Never drop a dirty page on the floor: put it back
                    // (a slot was just freed) and surface the error.
                    let victim_key = victim.key;
                    let mut inner = self.lock();
                    if let Some(slot) = inner.free_slots.pop() {
                        inner.frames[slot] = Some(victim);
                        inner.map.insert(victim_key, slot);
                        drop(inner);
                        PagerMetrics::add(
                            &self.metrics.bufferpool_bytes,
                            self.opts.page_size as u64,
                        );
                    }
                    return Err(e);
                }
                PagerMetrics::add(&self.metrics.page_writes, 1);
                PagerMetrics::add(&self.metrics.evictions_spill, 1);
            } else {
                PagerMetrics::add(&self.metrics.evictions_clean, 1);
            }
            // Loop: re-take the lock and retry the insert.
        }
    }

    /// Replace a pinned page's image in place (single writer): swaps the
    /// frame's `Arc`, marks it dirty and referenced, and updates the guard.
    pub fn write_page(&self, guard: &mut PageGuard, image: Vec<u8>) -> Result<()> {
        let data = Arc::new(image);
        let mut inner = self.lock();
        let frame = inner.frames[guard.slot]
            .as_mut()
            .filter(|f| f.key == guard.key);
        let Some(frame) = frame else {
            return Err(FluxumError::Storage(
                "write_page: pinned frame vanished under its guard".into(),
            ));
        };
        frame.data = Arc::clone(&data);
        frame.dirty = true;
        frame.referenced = true;
        drop(inner);
        guard.data = data;
        Ok(())
    }

    /// Spill every dirty frame to the cold tier and mark it clean without
    /// leaving the pool (checkpoint-flush semantics, TIER-013 tail).
    pub fn flush(&self, spill: SpillFn<'_>) -> Result<()> {
        let dirty: Vec<(PageKey, Arc<Vec<u8>>)> = {
            let inner = self.lock();
            inner
                .frames
                .iter()
                .flatten()
                .filter(|f| f.dirty)
                .map(|f| (f.key, Arc::clone(&f.data)))
                .collect()
        };
        for (key, data) in dirty {
            spill(key, &data)?;
            PagerMetrics::add(&self.metrics.page_writes, 1);
            let mut inner = self.lock();
            if let Some(&slot) = inner.map.get(&key)
                && let Some(frame) = inner.frames[slot].as_mut()
                // A write that landed between the copy and now must stay
                // dirty: only mark clean if the image is the one we wrote.
                && Arc::ptr_eq(&frame.data, &data)
            {
                frame.dirty = false;
            }
        }
        Ok(())
    }

    /// Evict every unpinned frame (spilling dirty ones). Test/maintenance
    /// helper that forces the next reads cold; pinned frames stay.
    pub fn evict_all(&self, spill: SpillFn<'_>) -> Result<()> {
        self.flush(spill)?;
        let mut inner = self.lock();
        for slot in 0..inner.frames.len() {
            let evictable = matches!(&inner.frames[slot], Some(f) if f.pins == 0 && !f.dirty);
            if evictable && let Some(frame) = inner.frames[slot].take() {
                inner.map.remove(&frame.key);
                inner.free_slots.push(slot);
                PagerMetrics::sub(&self.metrics.bufferpool_bytes, self.opts.page_size as u64);
                PagerMetrics::add(&self.metrics.evictions_clean, 1);
            }
        }
        Ok(())
    }

    /// Drop `key` from the pool without spilling (page freed/superseded).
    /// Fails if the frame is pinned.
    pub fn discard(&self, key: PageKey) -> Result<()> {
        let mut inner = self.lock();
        if let Some(&slot) = inner.map.get(&key) {
            let pinned = inner.frames[slot].as_ref().is_some_and(|f| f.pins > 0);
            if pinned {
                return Err(FluxumError::Storage(format!(
                    "discard of pinned page {key:?}"
                )));
            }
            inner.frames[slot] = None;
            inner.map.remove(&key);
            inner.free_slots.push(slot);
            drop(inner);
            PagerMetrics::sub(&self.metrics.bufferpool_bytes, self.opts.page_size as u64);
        }
        Ok(())
    }
}

/// One clock sweep (TIER-011): skip pinned frames, give referenced frames a
/// second chance (clear the bit, move on), and return the first eligible
/// clean frame — falling back to the first eligible dirty frame only when a
/// full sweep found no clean victim. `None` when everything is pinned.
fn pick_victim(inner: &mut PoolInner) -> Option<usize> {
    let n = inner.frames.len();
    let mut dirty_candidate: Option<usize> = None;
    for _ in 0..2 * n {
        let slot = inner.hand;
        inner.hand = (inner.hand + 1) % n;
        let Some(frame) = inner.frames[slot].as_mut() else {
            continue;
        };
        if frame.pins > 0 {
            continue;
        }
        if frame.referenced {
            frame.referenced = false; // second chance
            continue;
        }
        if !frame.dirty {
            return Some(slot);
        }
        if dirty_candidate.is_none() {
            dirty_candidate = Some(slot);
        }
    }
    dirty_candidate
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn key(page_id: u64) -> PageKey {
        PageKey {
            shard_id: 0,
            table_id: 1,
            page_id,
        }
    }

    fn pool(frames: usize) -> (Arc<BufferPool>, Arc<PagerMetrics>) {
        let metrics = Arc::new(PagerMetrics::default());
        let pool = BufferPool::new(
            PoolOptions {
                frames,
                page_size: 64,
                high_watermark: 0.95,
                low_watermark: 0.90,
            },
            Arc::clone(&metrics),
        );
        (pool, metrics)
    }

    fn no_spill() -> impl Fn(PageKey, &[u8]) -> Result<()> {
        |_, _| Ok(())
    }

    #[test]
    fn hits_and_misses_count_and_share_frames() {
        let (pool, metrics) = pool(4);
        let reads = AtomicUsize::new(0);
        let read = || {
            reads.fetch_add(1, Ordering::SeqCst);
            Ok(vec![7u8; 16])
        };
        let spill = no_spill();
        let g1 = pool
            .get_or_fault(key(1), &read, &spill)
            .unwrap_or_else(|e| panic!("{e}"));
        drop(g1);
        let g2 = pool
            .get_or_fault(key(1), &read, &spill)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(g2.image(), &[7u8; 16]);
        assert_eq!(reads.load(Ordering::SeqCst), 1, "second get was a hit");
        let snap = metrics.snapshot();
        assert_eq!((snap.misses, snap.hits), (1, 1));
        assert_eq!(snap.bufferpool_bytes, 64);
        assert_eq!(snap.bufferpool_capacity_bytes, 4 * 64);
    }

    #[test]
    fn capacity_is_never_exceeded_and_clock_evicts() {
        let (pool, metrics) = pool(3);
        let spill = no_spill();
        for page in 0..10u64 {
            let read = move || Ok(vec![page as u8; 8]);
            let g = pool
                .get_or_fault(key(page), &read, &spill)
                .unwrap_or_else(|e| panic!("{e}"));
            drop(g);
            assert!(pool.occupied_frames() <= 3, "budget ceiling violated");
        }
        let snap = metrics.snapshot();
        assert!(snap.evictions_total() >= 7, "{snap:?}");
        assert!(snap.bufferpool_bytes <= snap.bufferpool_capacity_bytes);
    }

    #[test]
    fn pinned_frames_survive_eviction_pressure() {
        let (pool, _) = pool(3);
        let spill = no_spill();
        let read1 = || Ok(vec![1u8; 8]);
        let pinned = pool
            .get_or_fault(key(1), &read1, &spill)
            .unwrap_or_else(|e| panic!("{e}"));
        for page in 2..20u64 {
            let read = move || Ok(vec![page as u8; 8]);
            drop(
                pool.get_or_fault(key(page), &read, &spill)
                    .unwrap_or_else(|e| panic!("{e}")),
            );
        }
        // The pinned page is still resident and correct (a hit, no re-read).
        let read_fail =
            || -> Result<Vec<u8>> { Err(FluxumError::Storage("pinned page was evicted".into())) };
        let again = pool
            .get_or_fault(key(1), &read_fail, &spill)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(again.image(), &[1u8; 8]);
        drop((pinned, again));
    }

    #[test]
    fn all_pinned_returns_buffer_pool_exhausted_never_oom() {
        let (pool, _) = pool(2);
        let spill = no_spill();
        let read1 = || Ok(vec![1u8; 8]);
        let read2 = || Ok(vec![2u8; 8]);
        let read3 = || Ok(vec![3u8; 8]);
        let _g1 = pool
            .get_or_fault(key(1), &read1, &spill)
            .unwrap_or_else(|e| panic!("{e}"));
        let _g2 = pool
            .get_or_fault(key(2), &read2, &spill)
            .unwrap_or_else(|e| panic!("{e}"));
        match pool.get_or_fault(key(3), &read3, &spill) {
            Err(FluxumError::BufferPoolExhausted { capacity: 2 }) => {}
            other => panic!("expected BufferPoolExhausted, got {other:?}"),
        }
        // Releasing a pin makes the fault succeed.
        drop(_g1);
        let g3 = pool
            .get_or_fault(key(3), &read3, &spill)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(g3.image(), &[3u8; 8]);
    }

    #[test]
    fn clean_frames_are_preferred_over_dirty_under_the_hand() {
        let (pool, metrics) = pool(4);
        let spilled = Mutex::new(Vec::new());
        let spill = |k: PageKey, image: &[u8]| {
            spilled
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((k, image.to_vec()));
            Ok(())
        };
        // One dirty frame among clean ones.
        drop(
            pool.install(key(1), vec![0xD1; 8], false, &spill)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        for page in 2..=4u64 {
            let read = move || Ok(vec![page as u8; 8]);
            drop(
                pool.get_or_fault(key(page), &read, &spill)
                    .unwrap_or_else(|e| panic!("{e}")),
            );
        }
        // Pressure: clean frames must be the victims; the dirty frame
        // stays resident and nothing is spilled.
        let read5 = || Ok(vec![0x05; 8]);
        drop(
            pool.get_or_fault(key(5), &read5, &spill)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        assert!(
            spilled
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty(),
            "clean frames should be evicted before any dirty spill"
        );
        assert!(metrics.snapshot().evictions_clean >= 1);
        let still_resident = pool.lookup(key(1)).map(|g| g.image().to_vec());
        assert_eq!(
            still_resident.as_deref(),
            Some(&[0xD1; 8][..]),
            "the dirty frame must have survived clean-preference eviction"
        );
    }

    #[test]
    fn dirty_victims_spill_before_drop() {
        let (pool, metrics) = pool(2);
        let spilled = Mutex::new(Vec::new());
        let spill = |k: PageKey, image: &[u8]| {
            spilled
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((k, image.to_vec()));
            Ok(())
        };
        // Both frames dirty: any victim must be spilled, never discarded.
        drop(
            pool.install(key(1), vec![0xD1; 8], false, &spill)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        drop(
            pool.install(key(2), vec![0xD2; 8], false, &spill)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        let read3 = || Ok(vec![0x03; 8]);
        drop(
            pool.get_or_fault(key(3), &read3, &spill)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        let log = spilled.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(
            log.iter()
                .any(|(k, img)| (*k == key(1) && img == &[0xD1; 8])
                    || (*k == key(2) && img == &[0xD2; 8])),
            "dirty frame dropped without spill: {log:?}"
        );
        assert!(metrics.snapshot().evictions_spill >= 1);
    }

    #[test]
    fn concurrent_misses_coalesce_into_one_read() {
        let (pool, metrics) = pool(4);
        let reads = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let pool = Arc::clone(&pool);
                let reads = &reads;
                scope.spawn(move || {
                    let read = || {
                        reads.fetch_add(1, Ordering::SeqCst);
                        // Give the other threads time to pile onto the
                        // single-flight wait.
                        std::thread::sleep(std::time::Duration::from_millis(20));
                        Ok(vec![0xAB; 8])
                    };
                    let spill = |_: PageKey, _: &[u8]| Ok(());
                    let g = pool
                        .get_or_fault(key(42), &read, &spill)
                        .unwrap_or_else(|e| panic!("{e}"));
                    assert_eq!(g.image(), &[0xAB; 8]);
                });
            }
        });
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "exactly one physical read for coalesced misses (TIER-032)"
        );
        assert_eq!(metrics.snapshot().misses, 1);
        assert_eq!(metrics.snapshot().hits, 7);
    }

    #[test]
    fn write_page_marks_dirty_and_flush_spills_once() {
        let (pool, _) = pool(2);
        let spill_count = AtomicUsize::new(0);
        let spill = |_: PageKey, _: &[u8]| {
            spill_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        };
        let read = || Ok(vec![0u8; 8]);
        let mut g = pool
            .get_or_fault(key(1), &read, &spill)
            .unwrap_or_else(|e| panic!("{e}"));
        pool.write_page(&mut g, vec![9u8; 8])
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(g.image(), &[9u8; 8]);
        drop(g);
        pool.flush(&spill).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(spill_count.load(Ordering::SeqCst), 1);
        // Now clean: a second flush writes nothing.
        pool.flush(&spill).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(spill_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn evict_all_empties_unpinned_and_faults_reread() {
        let (pool, metrics) = pool(4);
        let spill = no_spill();
        let read = || Ok(vec![5u8; 8]);
        drop(
            pool.get_or_fault(key(1), &read, &spill)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        pool.evict_all(&spill).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(pool.occupied_frames(), 0);
        assert_eq!(metrics.snapshot().bufferpool_bytes, 0);
        drop(
            pool.get_or_fault(key(1), &read, &spill)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        assert_eq!(metrics.snapshot().misses, 2, "re-read after evict_all");
    }
}
