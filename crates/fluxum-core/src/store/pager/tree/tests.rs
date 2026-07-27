use super::*;
use crate::config::PageCompression;
use crate::store::pager::{Pager, PagerOptions};

const PAGE_SIZE: usize = 256; // budget 223, max_key 101, inline cap 55

fn fixture() -> (tempfile::TempDir, Arc<Pager>, TableId) {
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let pager = Pager::open(
        dir.path(),
        PagerOptions {
            shard_id: 0,
            page_size: PAGE_SIZE,
            pool_capacity_bytes: (64 * PAGE_SIZE) as u64,
            high_watermark: 0.95,
            low_watermark: 0.90,
            compression: PageCompression::None,
            compression_min_bytes: 1024,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    (dir, pager, TableId::from_raw(1))
}

/// Overwrite the tree's root page with an arbitrary node payload
/// (single-writer corruption seam for the malformed-node error paths).
fn corrupt_root(pager: &Arc<Pager>, table: TableId, tree: &PagedTree, payload: &[u8]) {
    let root = tree.root_page_id();
    let header = PageHeader::new(root, table.as_u32(), 0, FLAG_INDEX);
    let image = encode_page(&header, payload).unwrap_or_else(|e| panic!("{e}"));
    let mut guard = pager.fault(table, root).unwrap_or_else(|e| panic!("{e}"));
    pager
        .write_pinned(&mut guard, image)
        .unwrap_or_else(|e| panic!("{e}"));
}

fn get_err(tree: &PagedTree, key: &[u8]) -> String {
    match tree.get(key) {
        Ok(v) => panic!("corrupt node served: {v:?}"),
        Err(e) => e.to_string(),
    }
}

fn scan_err(tree: &PagedTree) -> String {
    match tree.scan(&[], None, &mut |_, _| Ok(true)) {
        Ok(done) => panic!("corrupt node scanned to {done}"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn empty_keys_are_rejected_but_long_keys_are_not() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));

    let err = match tree.insert(b"", b"v") {
        Ok(()) => panic!("empty key accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("keys must be non-empty"), "{err}");

    // Keys far past the inline bound (and past a whole page) round-trip
    // exactly — they overflow instead of erroring.
    let long = vec![b'k'; PAGE_SIZE * 5];
    tree.insert(&long, b"v").unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        tree.get(&long).unwrap_or_else(|e| panic!("{e}")),
        Some(b"v".to_vec())
    );
}

/// Keys sharing a long common prefix (so the routing prefix cannot
/// decide) must still order, find, and scan exactly — the property the
/// whole overflow-key design turns on.
#[test]
fn overflow_keys_sharing_a_prefix_are_exact() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    let shared = vec![b'p'; PAGE_SIZE]; // >> max_inline_key

    // Keys: shared ++ suffix, plus the bare shared prefix itself (which
    // is a proper prefix of every other key — the ordering edge case).
    let mut keys: Vec<Vec<u8>> = vec![shared.clone()];
    for i in 0..24u32 {
        let mut k = shared.clone();
        k.extend_from_slice(format!("-{i:03}").as_bytes());
        keys.push(k);
    }
    for (i, k) in keys.iter().enumerate() {
        tree.insert(k, format!("v{i}").as_bytes())
            .unwrap_or_else(|e| panic!("{e}"));
    }
    // Every key resolves to its own value (no prefix collision).
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(
            tree.get(k).unwrap_or_else(|e| panic!("{e}")),
            Some(format!("v{i}").into_bytes()),
            "key {i} lost"
        );
    }
    // A probe that extends the shared prefix but was never inserted.
    let mut missing = shared.clone();
    missing.extend_from_slice(b"-999");
    assert_eq!(tree.get(&missing).unwrap_or_else(|e| panic!("{e}")), None);

    // A full scan yields every key in sorted order.
    let mut seen: Vec<Vec<u8>> = Vec::new();
    tree.scan(&[], None, &mut |k, _| {
        seen.push(k.to_vec());
        Ok(true)
    })
    .unwrap_or_else(|e| panic!("{e}"));
    let mut expect = keys.clone();
    expect.sort();
    assert_eq!(seen, expect);

    // A bounded scan over the overflow-key range.
    let mut lo = shared.clone();
    lo.extend_from_slice(b"-005");
    let mut hi = shared.clone();
    hi.extend_from_slice(b"-010");
    let mut ranged: Vec<Vec<u8>> = Vec::new();
    tree.scan(&lo, Some(&hi), &mut |k, _| {
        ranged.push(k.to_vec());
        Ok(true)
    })
    .unwrap_or_else(|e| panic!("{e}"));
    let expect_ranged: Vec<Vec<u8>> = expect
        .iter()
        .filter(|k| k.as_slice() >= lo.as_slice() && k.as_slice() < hi.as_slice())
        .cloned()
        .collect();
    assert_eq!(ranged, expect_ranged);

    // Deleting an overflow key removes exactly that entry.
    assert!(tree.delete(&keys[3]).unwrap_or_else(|e| panic!("{e}")));
    assert_eq!(tree.get(&keys[3]).unwrap_or_else(|e| panic!("{e}")), None);
    assert_eq!(
        tree.get(&keys[4]).unwrap_or_else(|e| panic!("{e}")),
        Some(b"v4".to_vec())
    );
}

/// A CoW update of an overflow-key entry keeps an older version readable
/// (the key chain is shared, the value chain is retired per version).
#[test]
fn cow_over_overflow_keys_preserves_snapshots() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for i in 0..12u32 {
        let mut k = vec![b'z'; PAGE_SIZE / 2];
        k.extend_from_slice(format!("{i:03}").as_bytes());
        tree.insert(&k, b"before").unwrap_or_else(|e| panic!("{e}"));
        keys.push(k);
    }
    let snap = tree.clone();

    let mut superseded = Vec::new();
    for k in &keys {
        tree.insert_cow(k, b"after", &mut superseded)
            .unwrap_or_else(|e| panic!("{e}"));
    }
    for k in &keys {
        assert_eq!(
            tree.get(k).unwrap_or_else(|e| panic!("{e}")),
            Some(b"after".to_vec())
        );
        assert_eq!(
            snap.get(k).unwrap_or_else(|e| panic!("{e}")),
            Some(b"before".to_vec()),
            "the pinned snapshot must still read the old version"
        );
    }
}

/// `bulk_load` over long keys builds a balanced tree (the level loop
/// terminates — the livelock the inline bound exists to prevent).
#[test]
fn bulk_load_handles_uniformly_long_keys() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..64u32)
        .map(|i| {
            let mut k = vec![b'L'; PAGE_SIZE * 2];
            k.extend_from_slice(format!("{i:04}").as_bytes());
            (k, i.to_le_bytes().to_vec())
        })
        .collect();
    tree.bulk_load(entries.clone())
        .unwrap_or_else(|e| panic!("{e}"));
    for (k, v) in &entries {
        assert_eq!(
            tree.get(k).unwrap_or_else(|e| panic!("{e}")),
            Some(v.clone())
        );
    }
    let mut count = 0usize;
    tree.scan(&[], None, &mut |_, _| {
        count += 1;
        Ok(true)
    })
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(count, entries.len());
}

#[test]
fn cow_writes_leave_a_pinned_old_root_readable() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    // Enough entries to grow an interior level over 256-byte pages.
    for i in 0..40u32 {
        tree.insert(format!("key-{i:04}").as_bytes(), &i.to_le_bytes())
            .unwrap_or_else(|e| panic!("{e}"));
    }
    // A snapshot handle pinned at the current version's root.
    let snap = tree.clone();
    let snap_root = snap.root_page_id();

    // Copy-on-write updates (change every key) + additions, retaining the
    // superseded pages rather than freeing them — the snapshot still reads
    // them.
    let mut superseded = Vec::new();
    for i in 0..40u32 {
        tree.insert_cow(
            format!("key-{i:04}").as_bytes(),
            &(i + 1000).to_le_bytes(),
            &mut superseded,
        )
        .unwrap_or_else(|e| panic!("{e}"));
    }
    for i in 40..80u32 {
        tree.insert_cow(
            format!("key-{i:04}").as_bytes(),
            &i.to_le_bytes(),
            &mut superseded,
        )
        .unwrap_or_else(|e| panic!("{e}"));
    }

    // The write published a new root and reported superseded pages.
    assert_ne!(
        tree.root_page_id(),
        snap_root,
        "a CoW write must publish a fresh root"
    );
    assert!(!superseded.is_empty(), "CoW writes report superseded pages");

    // The new version sees the updates and additions…
    assert_eq!(
        tree.get(b"key-0007").unwrap_or_else(|e| panic!("{e}")),
        Some(1007u32.to_le_bytes().to_vec())
    );
    assert_eq!(
        tree.get(b"key-0050").unwrap_or_else(|e| panic!("{e}")),
        Some(50u32.to_le_bytes().to_vec())
    );
    // …while the pinned snapshot still reads the original tree, unfreed.
    assert_eq!(
        snap.get(b"key-0007").unwrap_or_else(|e| panic!("{e}")),
        Some(7u32.to_le_bytes().to_vec())
    );
    assert_eq!(
        snap.get(b"key-0050").unwrap_or_else(|e| panic!("{e}")),
        None
    );

    // Reclaiming the superseded pages leaves the new version intact — it
    // references only fresh pages.
    tree.free_superseded(&superseded)
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        tree.get(b"key-0007").unwrap_or_else(|e| panic!("{e}")),
        Some(1007u32.to_le_bytes().to_vec())
    );
    assert_eq!(
        tree.get(b"key-0079").unwrap_or_else(|e| panic!("{e}")),
        Some(79u32.to_le_bytes().to_vec())
    );
}

#[test]
fn cow_delete_reports_superseded_and_keeps_the_old_root() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    for i in 0..40u32 {
        tree.insert(format!("key-{i:04}").as_bytes(), &i.to_le_bytes())
            .unwrap_or_else(|e| panic!("{e}"));
    }
    let snap = tree.clone();
    let mut superseded = Vec::new();
    // Delete a spread of keys under CoW.
    for i in (0..40u32).step_by(3) {
        assert!(
            tree.delete_cow(format!("key-{i:04}").as_bytes(), &mut superseded)
                .unwrap_or_else(|e| panic!("{e}"))
        );
    }
    // A missing key rewrites nothing and reports no supersession.
    let before = superseded.len();
    assert!(
        !tree
            .delete_cow(b"key-9999", &mut superseded)
            .unwrap_or_else(|e| panic!("{e}"))
    );
    assert_eq!(
        superseded.len(),
        before,
        "a no-op delete supersedes nothing"
    );

    // The new version no longer has the deleted keys; the snapshot still does.
    assert_eq!(
        tree.get(b"key-0000").unwrap_or_else(|e| panic!("{e}")),
        None
    );
    assert_eq!(
        snap.get(b"key-0000").unwrap_or_else(|e| panic!("{e}")),
        Some(0u32.to_le_bytes().to_vec())
    );
    // A surviving key is still readable on both.
    assert_eq!(
        tree.get(b"key-0001").unwrap_or_else(|e| panic!("{e}")),
        Some(1u32.to_le_bytes().to_vec())
    );
}

#[test]
fn delete_of_a_missing_key_reports_false() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    tree.insert(b"present", b"v")
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(!tree.delete(b"absent").unwrap_or_else(|e| panic!("{e}")));
    assert!(tree.delete(b"present").unwrap_or_else(|e| panic!("{e}")));
    assert!(!tree.delete(b"present").unwrap_or_else(|e| panic!("{e}")));
}

#[test]
fn scans_stop_early_when_the_visitor_returns_false() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    // Enough entries to force leaf splits and an interior level, so the
    // early stop propagates through both scan_node arms.
    for i in 0..64u32 {
        let key = format!("key-{i:04}");
        tree.insert(key.as_bytes(), &i.to_le_bytes())
            .unwrap_or_else(|e| panic!("{e}"));
    }
    let mut seen = 0usize;
    let completed = tree
        .scan(&[], None, &mut |_, _| {
            seen += 1;
            Ok(seen < 5)
        })
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(!completed, "an early-stopped scan must report false");
    assert_eq!(seen, 5);
}

#[test]
fn keys_below_every_separator_route_to_the_first_child() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    // bulk_load builds interior entries keyed by real first keys (no
    // low sentinel), so a probe below the smallest key exercises the
    // first-child routing fallback.
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (10..74u32)
        .map(|i| (format!("k-{i:04}").into_bytes(), i.to_le_bytes().to_vec()))
        .collect();
    tree.bulk_load(entries).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        tree.get(b"a-below-everything")
            .unwrap_or_else(|e| panic!("{e}")),
        None
    );
    assert_eq!(
        tree.get(b"k-0010").unwrap_or_else(|e| panic!("{e}")),
        Some(10u32.to_le_bytes().to_vec())
    );
}

#[test]
fn bulk_load_rejects_bad_keys_and_unsorted_input() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));

    let err = match tree.bulk_load(vec![(Vec::new(), b"v".to_vec())]) {
        Ok(()) => panic!("empty bulk_load key accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("empty or exceeds"), "{err}");

    let err = match tree.bulk_load(vec![
        (b"b".to_vec(), b"1".to_vec()),
        (b"a".to_vec(), b"2".to_vec()),
    ]) {
        Ok(()) => panic!("unsorted bulk_load accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("strictly sorted"), "{err}");
}

#[test]
fn replacing_an_overflow_value_frees_the_superseded_chain() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    // Values above the inline cap (55 bytes at 256-byte pages) go to
    // overflow chains spanning multiple pages.
    let v1 = vec![0xA1u8; 700];
    let v2 = vec![0xB2u8; 500];
    tree.insert(b"big", &v1).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(tree.get(b"big").unwrap_or_else(|e| panic!("{e}")), Some(v1));
    // Replace: the old chain is freed page by page, the new one reads
    // back exactly.
    tree.insert(b"big", &v2).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(tree.get(b"big").unwrap_or_else(|e| panic!("{e}")), Some(v2));
    // Delete frees the remaining chain.
    assert!(tree.delete(b"big").unwrap_or_else(|e| panic!("{e}")));
    assert_eq!(tree.get(b"big").unwrap_or_else(|e| panic!("{e}")), None);
}

#[test]
fn unknown_node_kinds_are_reported_not_served() {
    let (_dir, pager, table) = fixture();
    let tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    corrupt_root(&pager, table, &tree, &[0xFF, 1, 2, 3]);
    // The allocation-free get path names the kind byte…
    let err = get_err(&tree, b"k");
    assert!(err.contains("unknown node kind 0xff"), "{err}");
    // …and the parsing scan path reports the same corruption.
    let err = scan_err(&tree);
    assert!(err.contains("unknown node kind 0xff"), "{err}");
}

#[test]
fn empty_and_truncated_node_payloads_are_reported() {
    let (_dir, pager, table) = fixture();
    let tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));

    corrupt_root(&pager, table, &tree, &[]);
    let err = scan_err(&tree);
    assert!(err.contains("empty node payload"), "{err}");

    // A leaf entry declaring a 5-byte key with 1 byte present.
    corrupt_root(&pager, table, &tree, &[NODE_LEAF, 5, 0, 0, b'k']);
    let err = scan_err(&tree);
    assert!(err.contains("truncated node entry"), "{err}");
}

#[test]
fn unknown_leaf_value_tags_are_reported_on_both_paths() {
    let (_dir, pager, table) = fixture();
    let tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    // One leaf entry: key_len=1, tag=4 (unknown — tags 0..=3 are the
    // inline/overflow key×value matrix), key 'k'.
    corrupt_root(&pager, table, &tree, &[NODE_LEAF, 1, 0, 4, b'k']);
    let err = get_err(&tree, b"k");
    assert!(err.contains("malformed leaf"), "{err}");
    let err = scan_err(&tree);
    assert!(err.contains("unknown leaf value tag 4"), "{err}");
}

#[test]
fn an_interior_node_with_zero_entries_is_reported() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
    corrupt_root(&pager, table, &tree, &[NODE_INTERIOR]);
    // get: the raw router finds no entry → malformed interior.
    let err = get_err(&tree, b"k");
    assert!(err.contains("malformed interior"), "{err}");
    // delete parses the node and routes through route_index.
    let err = match tree.delete(b"k") {
        Ok(hit) => panic!("corrupt interior deleted: {hit}"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("interior node with zero entries"), "{err}");
}

#[test]
fn corrupt_overflow_chains_are_reported() {
    let (_dir, pager, table) = fixture();
    let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));

    // A short "overflow" page (payload < 8 bytes: no next pointer).
    let short_id = pager.allocate_page_id(table);
    let header = PageHeader::new(short_id, table.as_u32(), 0, FLAG_OVERFLOW);
    let image = encode_page(&header, &[1, 2, 3]).unwrap_or_else(|e| panic!("{e}"));
    drop(
        pager
            .install(table, short_id, image)
            .unwrap_or_else(|e| panic!("{e}")),
    );
    // A terminated chain page (next = NIL, no data bytes).
    let empty_id = pager.allocate_page_id(table);
    let header = PageHeader::new(empty_id, table.as_u32(), 0, FLAG_OVERFLOW);
    let image = encode_page(&header, &NIL.to_le_bytes()).unwrap_or_else(|e| panic!("{e}"));
    drop(
        pager
            .install(table, empty_id, image)
            .unwrap_or_else(|e| panic!("{e}")),
    );

    // Leaf with two overflow entries: "a" → short page, "b" → empty
    // chain claiming 10 bytes.
    let mut payload = vec![NODE_LEAF];
    for (key, head) in [(b'a', short_id), (b'b', empty_id)] {
        payload.extend_from_slice(&1u16.to_le_bytes());
        payload.push(1); // overflow tag
        payload.push(key);
        payload.extend_from_slice(&10u64.to_le_bytes()); // total_len
        payload.extend_from_slice(&head.to_le_bytes());
    }
    corrupt_root(&pager, table, &tree, &payload);

    let err = get_err(&tree, b"a");
    assert!(err.contains("overflow page too short"), "{err}");
    let err = get_err(&tree, b"b");
    assert!(err.contains("overflow chain length mismatch"), "{err}");
    // free_value walks the same chain on delete.
    let err = match tree.delete(b"a") {
        Ok(hit) => panic!("corrupt chain freed: {hit}"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("overflow page too short"), "{err}");
}

// --- ordering equivalence vs a resident oracle ----------------------

/// Arbitrary keys built from a small alphabet with lengths spanning the
/// inline bound (`max_inline_key` is 27 at 256-byte pages), so a run
/// mixes inline keys, overflow keys, shared prefixes, and keys that are
/// proper prefixes of others.
fn key_strategy() -> impl proptest::strategy::Strategy<Value = Vec<u8>> {
    use proptest::prelude::*;
    prop::collection::vec(prop::sample::select(vec![b'a', b'b', b'p']), 1..90)
}

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// The paged tree answers `get` and ordered `scan` exactly like a
    /// resident `BTreeMap` over the same key/value stream — including
    /// updates, deletes, and probes for keys never inserted.
    #[test]
    fn matches_a_btreemap_oracle_over_mixed_key_lengths(
        ops in prop::collection::vec(
            (key_strategy(), any::<u8>(), any::<bool>()),
            1..40,
        ),
        probes in prop::collection::vec(key_strategy(), 1..10),
    ) {
        use std::collections::BTreeMap;
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false)
            .unwrap_or_else(|e| panic!("{e}"));
        let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for (key, value, is_delete) in ops {
            if is_delete {
                let hit = tree.delete(&key).unwrap_or_else(|e| panic!("{e}"));
                prop_assert_eq!(hit, oracle.remove(&key).is_some());
            } else {
                let bytes = vec![value; 1 + usize::from(value % 7)];
                tree.insert(&key, &bytes).unwrap_or_else(|e| panic!("{e}"));
                oracle.insert(key, bytes);
            }
        }

        // Point lookups: inserted keys and arbitrary probes agree.
        for key in oracle.keys() {
            let got = tree.get(key).unwrap_or_else(|e| panic!("{e}"));
            prop_assert_eq!(got.as_ref(), oracle.get(key));
        }
        for probe in &probes {
            let got = tree.get(probe).unwrap_or_else(|e| panic!("{e}"));
            prop_assert_eq!(got.as_ref(), oracle.get(probe));
        }

        // Full scan: same pairs, same order.
        let mut scanned: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        tree.scan(&[], None, &mut |k, v| {
            scanned.push((k.to_vec(), v.to_vec()));
            Ok(true)
        })
        .unwrap_or_else(|e| panic!("{e}"));
        let expected: Vec<(Vec<u8>, Vec<u8>)> =
            oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        prop_assert_eq!(scanned, expected);

        // Bounded scans agree for every probe pair used as [start, end).
        for probe in &probes {
            let mut ranged: Vec<Vec<u8>> = Vec::new();
            tree.scan(probe, None, &mut |k, _| {
                ranged.push(k.to_vec());
                Ok(true)
            })
            .unwrap_or_else(|e| panic!("{e}"));
            let expect: Vec<Vec<u8>> = oracle
                .range(probe.clone()..)
                .map(|(k, _)| k.clone())
                .collect();
            prop_assert_eq!(ranged, expect);
        }
    }
}
