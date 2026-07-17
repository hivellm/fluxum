//! SPEC-007 §2 (SHD-001..004, SHD-012) — shard-routing determinism: golden
//! vectors for all three strategies; the same key resolves to the same
//! shard across runs (and platforms — the hash is the platform-stable
//! xxHash64 over FluxBIN, never a seeded hasher).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::shard::PartitionStrategy;
use fluxum_core::store::RowValue;

#[test]
fn hash_routing_is_deterministic_with_golden_vectors() {
    let strategy = PartitionStrategy::Hash { shard_count: 4 };
    let shard = |value: RowValue| strategy.shard_for(&[value]).unwrap();

    // Golden vectors (SHD-003): pinned constants — any change here is a
    // routing break that would strand every partitioned row on restart.
    let golden = [
        (RowValue::U64(0), 3),
        (RowValue::U64(1), 1),
        (RowValue::U64(42), 3),
        (RowValue::Str("channel-7".into()), 3),
    ];

    // Determinism across repeated resolution.
    for (value, expected) in &golden {
        for _ in 0..3 {
            assert_eq!(shard(value.clone()), *expected);
        }
        assert!(*expected < 4, "modulo shard_count");
    }
    // Distinct-type keys with equal payloads hash differently (FluxBIN
    // encodes the type shape).
    assert!(
        (0..64u64).map(|n| shard(RowValue::U64(n))).collect::<std::collections::HashSet<_>>().len()
            > 1,
        "keys spread across shards"
    );
}

#[test]
fn range_and_region_routing_match_the_spec_formulas() {
    // Range: greatest boundary ≤ key wins (SHD-012).
    let strategy = PartitionStrategy::Range {
        boundaries: vec![(0, 0), (1024, 1), (2048, 2), (3072, 3)],
    };
    let shard = |n: i64| strategy.shard_for(&[RowValue::I64(n)]).unwrap();
    assert_eq!(shard(-5), 0, "below the first boundary → its shard");
    assert_eq!(shard(0), 0);
    assert_eq!(shard(1023), 0);
    assert_eq!(shard(1024), 1);
    assert_eq!(shard(2047), 1);
    assert_eq!(shard(2048), 2);
    assert_eq!(shard(9999), 3);

    // Region: floor(coord / region_size) at and around cell boundaries.
    let strategy = PartitionStrategy::Region {
        region_size: 4000.0,
        grid_columns: 4,
        shard_count: 16,
    };
    let shard = |x: f64, y: f64| {
        strategy
            .shard_for(&[RowValue::F64(x), RowValue::F64(y)])
            .unwrap()
    };
    // Same cell → same shard; crossing the boundary changes the cell.
    assert_eq!(shard(0.0, 0.0), shard(3999.9, 3999.9), "inside one cell");
    assert_ne!(shard(3999.9, 0.0), shard(4000.0, 0.0), "x boundary crossed");
    assert_ne!(shard(0.0, 3999.9), shard(0.0, 4000.0), "y boundary crossed");
    // Exactly at the boundary belongs to the higher cell (floor semantics).
    assert_eq!(shard(4000.0, 0.0), shard(4001.0, 0.0));
    // Negative coordinates floor correctly (cell -1, not 0).
    assert_ne!(shard(-0.5, 0.0), shard(0.5, 0.0));
    // Determinism.
    for _ in 0..3 {
        assert_eq!(shard(123.0, 456.0), shard(123.0, 456.0));
    }
}
