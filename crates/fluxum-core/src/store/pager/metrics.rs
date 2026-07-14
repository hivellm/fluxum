//! Buffer-pool and cold-tier metrics (SPEC-015 TIER-080).
//!
//! Lock-free atomic counters updated on the pager hot paths and consumed by
//! the SPEC-012 Prometheus exporter (T5.6). `fluxum_page_reads_total` is the
//! observable witness of the TIER-014 zero-disk-I/O invariant: it MUST NOT
//! increase while a workload is served entirely from the pool.

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared pager metric counters (one set per [`super::Pager`], i.e. per
/// budget domain).
#[derive(Debug, Default)]
pub struct PagerMetrics {
    /// Gauge: bytes currently held in pool frames.
    pub(crate) bufferpool_bytes: AtomicU64,
    /// Gauge: configured pool capacity (TIER-003).
    pub(crate) bufferpool_capacity_bytes: AtomicU64,
    /// Counter: pool hits.
    pub(crate) hits: AtomicU64,
    /// Counter: pool misses (each miss implies one fault-in).
    pub(crate) misses: AtomicU64,
    /// Counter: clean-frame evictions (dropped instantly).
    pub(crate) evictions_clean: AtomicU64,
    /// Counter: dirty-frame evictions (spilled to the cold tier first).
    pub(crate) evictions_spill: AtomicU64,
    /// Counter: physical page reads of data-leaf pages.
    pub(crate) page_reads_data: AtomicU64,
    /// Counter: physical page reads of index-flagged pages (TIER-050
    /// witness: index pages demonstrably fault under pressure).
    pub(crate) page_reads_index: AtomicU64,
    /// Counter: physical page writes (spill + flush).
    pub(crate) page_writes: AtomicU64,
    /// Counter: uncompressed payload bytes offered to the spill path
    /// (`fluxum_page_compression_ratio` numerator input, TIER-043/080).
    pub(crate) compression_raw_bytes: AtomicU64,
    /// Counter: payload bytes actually stored by the spill path (raw when
    /// compression was skipped or discarded, TIER-040).
    pub(crate) compression_stored_bytes: AtomicU64,
}

/// A point-in-time copy of every counter, for assertions and export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// `fluxum_bufferpool_bytes` gauge.
    pub bufferpool_bytes: u64,
    /// `fluxum_bufferpool_capacity_bytes` gauge.
    pub bufferpool_capacity_bytes: u64,
    /// `fluxum_bufferpool_hits_total` counter.
    pub hits: u64,
    /// `fluxum_bufferpool_misses_total` counter.
    pub misses: u64,
    /// `fluxum_bufferpool_evictions_total{kind="clean"}` counter.
    pub evictions_clean: u64,
    /// `fluxum_bufferpool_evictions_total{kind="spill"}` counter.
    pub evictions_spill: u64,
    /// `fluxum_page_reads_total{index="false"}` counter.
    pub page_reads_data: u64,
    /// `fluxum_page_reads_total{index="true"}` counter.
    pub page_reads_index: u64,
    /// `fluxum_page_writes_total` counter.
    pub page_writes: u64,
    /// `fluxum_page_compression_raw_bytes_total` counter.
    pub compression_raw_bytes: u64,
    /// `fluxum_page_compression_stored_bytes_total` counter.
    pub compression_stored_bytes: u64,
}

impl MetricsSnapshot {
    /// Total physical page reads, data + index.
    pub fn page_reads_total(&self) -> u64 {
        self.page_reads_data + self.page_reads_index
    }

    /// The `fluxum_page_compression_ratio` gauge value — raw ÷ stored
    /// payload bytes over everything spilled so far (TIER-043/080); `None`
    /// before the first spill. The T5.6 exporter derives the per-table
    /// series from per-pager snapshots.
    pub fn compression_ratio(&self) -> Option<f64> {
        (self.compression_stored_bytes > 0)
            .then(|| self.compression_raw_bytes as f64 / self.compression_stored_bytes as f64)
    }

    /// Total evictions, clean + spill.
    pub fn evictions_total(&self) -> u64 {
        self.evictions_clean + self.evictions_spill
    }

    /// The counters as `(prometheus_series, value)` samples in SPEC-012
    /// catalog form (TIER-080), ready for the T5.6 exporter.
    pub fn samples(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("fluxum_bufferpool_bytes", self.bufferpool_bytes),
            (
                "fluxum_bufferpool_capacity_bytes",
                self.bufferpool_capacity_bytes,
            ),
            ("fluxum_bufferpool_hits_total", self.hits),
            ("fluxum_bufferpool_misses_total", self.misses),
            (
                "fluxum_bufferpool_evictions_total{kind=\"clean\"}",
                self.evictions_clean,
            ),
            (
                "fluxum_bufferpool_evictions_total{kind=\"spill\"}",
                self.evictions_spill,
            ),
            (
                "fluxum_page_reads_total{index=\"false\"}",
                self.page_reads_data,
            ),
            (
                "fluxum_page_reads_total{index=\"true\"}",
                self.page_reads_index,
            ),
            ("fluxum_page_writes_total", self.page_writes),
            (
                "fluxum_page_compression_raw_bytes_total",
                self.compression_raw_bytes,
            ),
            (
                "fluxum_page_compression_stored_bytes_total",
                self.compression_stored_bytes,
            ),
        ]
    }
}

impl PagerMetrics {
    /// Copy every counter at once.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            bufferpool_bytes: self.bufferpool_bytes.load(Ordering::Relaxed),
            bufferpool_capacity_bytes: self.bufferpool_capacity_bytes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions_clean: self.evictions_clean.load(Ordering::Relaxed),
            evictions_spill: self.evictions_spill.load(Ordering::Relaxed),
            page_reads_data: self.page_reads_data.load(Ordering::Relaxed),
            page_reads_index: self.page_reads_index.load(Ordering::Relaxed),
            page_writes: self.page_writes.load(Ordering::Relaxed),
            compression_raw_bytes: self.compression_raw_bytes.load(Ordering::Relaxed),
            compression_stored_bytes: self.compression_stored_bytes.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn sub(counter: &AtomicU64, n: u64) {
        counter.fetch_sub(n, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_and_samples_carry_the_tier_080_series() {
        let metrics = PagerMetrics::default();
        PagerMetrics::add(&metrics.hits, 3);
        PagerMetrics::add(&metrics.page_reads_index, 2);
        PagerMetrics::add(&metrics.bufferpool_bytes, 100);
        PagerMetrics::sub(&metrics.bufferpool_bytes, 25);

        let snap = metrics.snapshot();
        assert_eq!(snap.hits, 3);
        assert_eq!(snap.page_reads_index, 2);
        assert_eq!(snap.page_reads_total(), 2);
        assert_eq!(snap.bufferpool_bytes, 75);

        let samples = snap.samples();
        let get = |name: &str| {
            samples
                .iter()
                .find(|(n, _)| *n == name)
                .unwrap_or_else(|| panic!("missing series {name}"))
                .1
        };
        assert_eq!(get("fluxum_bufferpool_hits_total"), 3);
        assert_eq!(get("fluxum_page_reads_total{index=\"true\"}"), 2);
        assert_eq!(get("fluxum_page_reads_total{index=\"false\"}"), 0);
        assert_eq!(get("fluxum_bufferpool_bytes"), 75);
    }

    #[test]
    fn compression_ratio_is_raw_over_stored() {
        let metrics = PagerMetrics::default();
        assert_eq!(metrics.snapshot().compression_ratio(), None);
        PagerMetrics::add(&metrics.compression_raw_bytes, 9_000);
        PagerMetrics::add(&metrics.compression_stored_bytes, 3_000);
        let snap = metrics.snapshot();
        assert_eq!(snap.compression_ratio(), Some(3.0));
        let samples = snap.samples();
        assert!(
            samples.contains(&("fluxum_page_compression_raw_bytes_total", 9_000)),
            "{samples:?}"
        );
        assert!(
            samples.contains(&("fluxum_page_compression_stored_bytes_total", 3_000)),
            "{samples:?}"
        );
    }
}
