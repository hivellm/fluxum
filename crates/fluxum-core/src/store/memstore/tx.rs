//! The write transaction over [`MemStore`] (STG-005, TXN-040..042) — the
//! eager-validation write API and the copy-on-write commit merge. Split
//! from the parent module to honour the file-size convention; a child
//! module, so the single-writer internals stay private to `memstore`.

#[allow(clippy::wildcard_imports)]
use super::*;

/// A single in-flight transaction (holds the shard's writer lock).
///
/// Reads ([`Tx::query_pk`], [`Tx::scan`]) see only the committed snapshot
/// captured at [`MemStore::begin`] — never this transaction's own pending
/// writes (STG-004 / TXN-050; the explicit `scan_pending`/`scan_all` family
/// is SPEC-004 / T3.x surface). Dropping the handle without calling
/// [`Tx::commit`] rolls back (STG-006).
#[derive(Debug)]
pub struct Tx<'a> {
    pub(super) store: &'a MemStore,
    pub(super) writer: MutexGuard<'a, WriterState>,
    pub(super) base: Arc<CommittedState>,
    pub(super) state: TxState,
}

impl Tx<'_> {
    /// The id this transaction receives if it commits (TXN-030).
    pub fn tx_id(&self) -> u64 {
        self.state.tx_id
    }

    /// The store's column-transform executor, if attached (SPEC-017 §5) —
    /// the read boundary decrypts `#[encrypted]` columns through it.
    pub(crate) fn transform_engine(
        &self,
    ) -> Option<Arc<crate::transform::engine::TransformEngine>> {
        self.store.transforms.get().cloned()
    }

    /// Insert a row (column values in declaration order).
    ///
    /// Enforces PK uniqueness (TXN-040) and every `#[unique]` constraint
    /// (TXN-041) eagerly against the STG-007 overlay: committed rows
    /// tx-deleted in this transaction do not conflict; pending inserts do.
    /// For `#[auto_inc]` tables, a `0` placeholder in the auto-inc column is
    /// replaced with the next counter value (TXN-042: assigned at insert
    /// time); an explicit non-zero id is kept and the counter jumps past it.
    /// Returns the row as stored, with the assigned id.
    pub fn insert(&mut self, table: TableId, values: Vec<RowValue>) -> Result<Row> {
        self.write(table, values, false)
    }

    /// Insert a row, replacing any existing row with the same primary key
    /// (the TXN-040 exception: an occupied PK replaces instead of erroring).
    /// `#[unique]` constraints against *other* rows still apply (TXN-041),
    /// and auto-inc placeholders are assigned exactly as in [`Tx::insert`].
    /// Returns the row as stored.
    pub fn upsert(&mut self, table: TableId, values: Vec<RowValue>) -> Result<Row> {
        self.write(table, values, true)
    }

    /// Shared insert/upsert path; `replace` selects the TXN-040 semantics
    /// for an occupied primary key.
    fn write(&mut self, table: TableId, mut values: Vec<RowValue>, replace: bool) -> Result<Row> {
        let schema = self.store.schema_of(table)?;
        // SPEC-007 SHD-031: global tables are writable only on the
        // authoritative shard; replicas receive mutations via replication.
        if schema.access == crate::schema::TableAccess::Global
            && !self
                .store
                .global_writes_allowed
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(FluxumError::Reducer(
                "global table writes must execute on the authoritative shard".into(),
            ));
        }
        // RV-050: derive every `#[computed]` column from the other columns,
        // overwriting whatever the reducer set (the column is read-only). Runs
        // before validation so the derived value is what gets type-checked,
        // stored, indexed, and fanned out — and before encryption, so a
        // derivation over a plaintext sibling sees the plaintext.
        if let Some(cols) = self.store.computed.get(&table) {
            for (ordinal, compute) in cols {
                let derived = compute(&values)?;
                if let Some(slot) = values.get_mut(usize::from(*ordinal)) {
                    *slot = derived;
                }
            }
        }
        // DMX-040: a `Blob` value must reference a stored object — validated
        // here (write time) so the commit merge's incref can never miss.
        for &ordinal in &blob_ordinals(schema) {
            if let Some(RowValue::Blob(blob)) = values.get(usize::from(ordinal)) {
                let Some(blobs) = self.store.blobs.get() else {
                    return Err(FluxumError::Storage(format!(
                        "table `{}`: Blob column write with no blob store attached (DMX-040)",
                        schema.name
                    )));
                };
                let hash = crate::commitlog::BlobHash::from_bytes(*blob.as_bytes());
                if !blobs.contains(&hash) {
                    return Err(FluxumError::Storage(format!(
                        "table `{}`: Blob reference {hash} names no stored object — upload                          it first (DMX-040)",
                        schema.name
                    )));
                }
            }
        }
        check_row(schema, &values)?;
        // SPX-010 eager constraint (like PK uniqueness below): an R-tree row
        // must satisfy min <= max per axis, so the commit merge stays
        // infallible.
        if let Some(spatial) = &self.base.table(table)?.spatial {
            spatial.check_insert(schema.name, &values)?;
        }
        self.assign_auto_inc(table, schema, &mut values)?;
        // RV-030: declarative `#[not_null]` / `#[check]` / `#[references]`
        // constraints, validated eagerly at write time (like TXN-040/041) so
        // the commit merge stays infallible — on the plaintext row, after
        // computed derivation and auto-inc assignment, before `#[encrypted]`
        // sealing.
        self.check_constraints(table, schema, &values)?;
        let pk = encode_pk_of_row(schema, &values)?;

        // SPEC-017 CT-011/030: seal `#[encrypted]` columns after validation and
        // pk derivation (key columns are never encrypted, CT-013), before the
        // row is stored — so the commit log, cold pages, checkpoints, and
        // replication stream only ever see ciphertext. Non-encrypted tables
        // pay nothing (the engine fast-skips).
        if let Some(engine) = self.store.transforms.get()
            && engine.touches(table)
        {
            engine.on_write_row(table, &mut values, pk.as_bytes())?;
        }

        let committed_row = self.base.table(table)?.row_at(&pk)?;
        // Overlay occupancy (STG-007): a pending insert/reinsert holds the
        // key; a committed row holds it unless tx-deleted. `PendingOp` rows
        // are `Arc`-shared, so this clone is a pointer bump.
        let pending = self
            .state
            .tables
            .get(&table)
            .and_then(|ops| ops.get(&pk))
            .cloned();
        let occupied = matches!(pending, Some(PendingOp::Insert(_) | PendingOp::Update(_)))
            || (pending.is_none() && committed_row.is_some());
        if occupied && !replace {
            return Err(FluxumError::Storage(format!(
                "primary key conflict: table={} pk={}",
                schema.name,
                display_pk_of_row(schema, &values)
            )));
        }

        // TXN-041: every `#[unique]` constraint, eagerly, over the same
        // overlay — so the commit merge stays validated by construction.
        self.check_unique(table, schema, &values, &pk)?;

        // RV-031: the visible row this write replaces, captured before the
        // buffer mutation so a trigger's `old` argument is the pre-write
        // view. Only tables with registered triggers pay for the clone.
        let wants_events = crate::reducer::has_triggers(table);
        let old_visible: Option<Row> = if wants_events {
            match (&pending, &committed_row) {
                (Some(PendingOp::Insert(row) | PendingOp::Update(row)), _) => Some(row.clone()),
                (Some(PendingOp::Delete), _) => None,
                (None, committed) => committed.clone(),
            }
        } else {
            None
        };

        let ops = self.state.tables.entry(table).or_default();
        let stored = match pending {
            // Tx-deleted committed row: reinsert is allowed (STG-007).
            Some(PendingOp::Delete) => {
                let committed = committed_row.ok_or_else(|| {
                    FluxumError::Storage(format!(
                        "internal invariant violated: Delete op for pk absent from \
                         CommittedState (table={})",
                        schema.name
                    ))
                })?;
                if committed.values() == values.as_slice() {
                    // Delete-then-reinsert of identical content cancels to a
                    // no-op; the committed row identity is preserved
                    // (STG-007 rule 1).
                    ops.remove(&pk);
                    committed
                } else {
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Update(row.clone()));
                    row
                }
            }
            // Upsert over this transaction's own pending write: the pending
            // row is replaced in place, keeping its Insert/Update flavor
            // (the flavor records whether a *committed* row underlies the
            // key, which has not changed).
            Some(PendingOp::Insert(_)) => {
                let row = Row::new(values);
                ops.insert(pk, PendingOp::Insert(row.clone()));
                row
            }
            Some(PendingOp::Update(_)) => {
                let row = Row::new(values);
                ops.insert(pk, PendingOp::Update(row.clone()));
                row
            }
            None => {
                if let Some(committed) = committed_row {
                    // Upsert over a committed row (TXN-040 exception).
                    if committed.values() == values.as_slice() {
                        // Identical content: structural no-op, committed row
                        // identity preserved (STG-007 rule 1) — no trigger
                        // event either, nothing changed.
                        return Ok(committed);
                    }
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Update(row.clone()));
                    row
                } else {
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Insert(row.clone()));
                    row
                }
            }
        };
        if wants_events {
            let kind = if old_visible.is_some() {
                TriggerKind::Update
            } else {
                TriggerKind::Insert
            };
            self.state.trigger_events.push(TriggerEvent {
                table,
                kind,
                old: old_visible,
                new: Some(stored.clone()),
            });
        }
        Ok(stored)
    }

    /// SPEC-022 RV-030: declarative constraints, validated eagerly at write
    /// time. `#[not_null]` rejects `None`, `#[check]` predicates must hold,
    /// and every `#[references]` value must name an overlay-visible parent
    /// row. Violations abort the transaction with a typed error.
    fn check_constraints(
        &self,
        table: TableId,
        schema: &'static TableSchema,
        values: &[RowValue],
    ) -> Result<()> {
        for def in self.store.not_null.get(&table).into_iter().flatten() {
            if let Some(RowValue::Optional(None)) = values.get(usize::from(def.ordinal)) {
                return Err(FluxumError::query(
                    codes::TXN_NOT_NULL_VIOLATION,
                    format!(
                        "table `{}` column `{}`: None violates #[not_null] (RV-030)",
                        def.table, def.column
                    ),
                ));
            }
        }
        for def in self.store.checks.get(&table).into_iter().flatten() {
            if !(def.check)(values)? {
                return Err(FluxumError::query(
                    codes::TXN_CHECK_VIOLATION,
                    format!(
                        "table `{}` column `{}`: #[check({})] violated (RV-030)",
                        def.table, def.column, def.expr
                    ),
                ));
            }
        }
        for fk in self.store.fks_out.get(&table).into_iter().flatten() {
            let Some(value) = values.get(usize::from(fk.child_ordinal)) else {
                continue; // arity already rejected by check_row
            };
            let target = match value {
                // A None reference is explicitly unlinked — valid.
                RowValue::Optional(None) => continue,
                RowValue::Optional(Some(inner)) => inner.as_ref(),
                other => other,
            };
            if !self.parent_exists(fk, target)? {
                return Err(FluxumError::query(
                    codes::TXN_FK_VIOLATION,
                    format!(
                        "table `{}` column `{}`: referenced `{}` row {target:?} does \
                         not exist (#[references], RV-030)",
                        schema.name, fk.child_column, fk.parent_schema.name
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Overlay-aware parent existence for an RV-030 foreign-key write: a
    /// parent row this transaction inserted satisfies the reference; one it
    /// deleted does not.
    fn parent_exists(&self, fk: &ResolvedFk, value: &RowValue) -> Result<bool> {
        let pk = encode_pk_values(fk.parent_schema, std::slice::from_ref(value))?;
        match self
            .state
            .tables
            .get(&fk.parent)
            .and_then(|ops| ops.get(&pk))
        {
            Some(PendingOp::Insert(_) | PendingOp::Update(_)) => Ok(true),
            Some(PendingOp::Delete) => Ok(false),
            None => self.base.table(fk.parent)?.contains_pk(&pk),
        }
    }

    /// TXN-041: reject `values` if any `#[unique]` constraint value is held
    /// by a *visible* row other than the one at `pk` — visible per the
    /// STG-007 overlay: pending inserts/replacements count, committed rows
    /// count unless tx-deleted or tx-replaced, and the row being written
    /// never conflicts with itself.
    fn check_unique(
        &self,
        table: TableId,
        schema: &'static TableSchema,
        values: &[RowValue],
        pk: &PkBytes,
    ) -> Result<()> {
        let base = self.base.table(table)?;
        if base.unique.is_empty() {
            return Ok(());
        }
        let ops = self.state.tables.get(&table);
        for constraint in &base.unique {
            let key = constraint.key_of_values(values)?;
            // Pending overlay: another pending row carrying this value.
            if let Some(ops) = ops {
                for (other_pk, op) in ops {
                    if other_pk == pk {
                        continue;
                    }
                    let row = match op {
                        PendingOp::Insert(row) | PendingOp::Update(row) => row,
                        PendingOp::Delete => continue,
                    };
                    if constraint.key_of_values(row.values())? == key {
                        return Err(unique::violation_error(
                            schema,
                            constraint.columns(),
                            values,
                        ));
                    }
                }
            }
            // Committed owner — unless it is the row being written, or this
            // transaction tx-deleted/tx-replaced it (an Update whose new row
            // still carries the value was already caught just above).
            if let Some(owner) = constraint.owner(&key)? {
                let shadowed = owner == *pk
                    || ops
                        .and_then(|m| m.get(&owner))
                        .is_some_and(|op| matches!(op, PendingOp::Delete | PendingOp::Update(_)));
                if !shadowed {
                    return Err(unique::violation_error(
                        schema,
                        constraint.columns(),
                        values,
                    ));
                }
            }
        }
        Ok(())
    }

    /// Delete by primary key values (in `primary_key` declaration order).
    ///
    /// Returns whether a (committed or pending) row was deleted. Deleting a
    /// row inserted by this same transaction cancels the insert entirely.
    ///
    /// SPEC-022 RV-032: deleting a row referenced by `#[references]` child
    /// rows applies the declared referential action atomically within this
    /// same transaction — `restrict` (the default) rejects the delete with a
    /// typed 3102 while any child row still references it, `cascade` deletes
    /// the referencing rows (transitively, via an explicit worklist — cycles
    /// terminate because re-deleting an already-deleted row is a no-op), and
    /// `set_null` clears the referencing column.
    pub fn delete(&mut self, table: TableId, pk_values: &[RowValue]) -> Result<bool> {
        let Some(old) = self.delete_one(table, pk_values)? else {
            return Ok(false);
        };
        let store = self.store;
        // RV-032 worklist: every deleted row whose table has incoming
        // references fans out its action; cascade pushes further entries.
        let mut deleted: Vec<(TableId, Row)> = vec![(table, old)];
        while let Some((parent, old_row)) = deleted.pop() {
            let Some(fks) = store.fks_in.get(&parent) else {
                continue;
            };
            for fk in fks {
                // Single-column PK validated at store assembly.
                let &[pk_ord] = fk.parent_schema.primary_key else {
                    continue;
                };
                let parent_value = old_row.values()[usize::from(pk_ord)].clone();
                self.apply_ref_action(fk, &parent_value, &mut deleted)?;
            }
        }
        Ok(true)
    }

    /// Apply one RV-032 referential action: find every overlay-visible child
    /// row referencing `parent_value` and restrict / cascade / set-null it.
    fn apply_ref_action(
        &mut self,
        fk: &ResolvedFk,
        parent_value: &RowValue,
        deleted: &mut Vec<(TableId, Row)>,
    ) -> Result<()> {
        let children: Vec<Row> = self
            .scan_all(fk.child)?
            .into_iter()
            .filter(|row| {
                fk_value_matches(
                    row.values().get(usize::from(fk.child_ordinal)),
                    parent_value,
                )
            })
            .collect();
        for child in children {
            match fk.on_delete {
                crate::schema::RefAction::Restrict => {
                    return Err(FluxumError::query(
                        codes::TXN_FK_VIOLATION,
                        format!(
                            "delete from `{}` restricted: `{}`.`{}` rows still reference \
                             {parent_value:?} (on_delete = restrict, RV-032)",
                            fk.parent_schema.name, fk.child_schema.name, fk.child_column
                        ),
                    ));
                }
                crate::schema::RefAction::Cascade => {
                    let pk_vals: Vec<RowValue> = fk
                        .child_schema
                        .primary_key
                        .iter()
                        .map(|&ord| child.values()[usize::from(ord)].clone())
                        .collect();
                    if let Some(old) = self.delete_one(fk.child, &pk_vals)? {
                        deleted.push((fk.child, old));
                    }
                }
                crate::schema::RefAction::SetNull => {
                    let mut vals = child.values().to_vec();
                    // Stored rows carry `#[encrypted]` columns sealed —
                    // unseal before the rewrite so `write` doesn't seal
                    // ciphertext twice (SPEC-017 CT-011).
                    if let Some(engine) = self.store.transforms.get()
                        && engine.touches(fk.child)
                    {
                        let cpk = encode_pk_of_row(fk.child_schema, &vals)?;
                        engine.on_read_row(fk.child, &mut vals, cpk.as_bytes(), true)?;
                    }
                    vals[usize::from(fk.child_ordinal)] = RowValue::Optional(None);
                    self.write(fk.child, vals, true)?;
                }
            }
        }
        Ok(())
    }

    /// Apply the buffer mutation for one delete, returning the previously
    /// visible row (`None` = nothing to delete). Records the RV-031 trigger
    /// event; RV-032 referential actions are [`Tx::delete`]'s worklist.
    fn delete_one(&mut self, table: TableId, pk_values: &[RowValue]) -> Result<Option<Row>> {
        let schema = self.store.schema_of(table)?;
        // SPEC-007 SHD-031: deletes are writes too.
        if schema.access == crate::schema::TableAccess::Global
            && !self
                .store
                .global_writes_allowed
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(FluxumError::Reducer(
                "global table writes must execute on the authoritative shard".into(),
            ));
        }
        let pk = encode_pk_values(schema, pk_values)?;
        let committed_row = self.base.table(table)?.row_at(&pk)?;
        let ops = self.state.tables.entry(table).or_default();
        let old = match ops.get(&pk).cloned() {
            // Insert-then-delete of a pending row cancels to a no-op.
            Some(PendingOp::Insert(row)) => {
                ops.remove(&pk);
                Some(row)
            }
            // The committed row stays deleted; the pending replacement dies.
            Some(PendingOp::Update(row)) => {
                ops.insert(pk, PendingOp::Delete);
                Some(row)
            }
            Some(PendingOp::Delete) => None,
            None => {
                if committed_row.is_some() {
                    ops.insert(pk, PendingOp::Delete);
                }
                committed_row
            }
        };
        if let Some(old_row) = &old
            && crate::reducer::has_triggers(table)
        {
            self.state.trigger_events.push(TriggerEvent {
                table,
                kind: TriggerKind::Delete,
                old: Some(old_row.clone()),
                new: None,
            });
        }
        Ok(old)
    }

    /// SHD-041 steps 2–3: read every row of the entity's row set — each
    /// `(table, key ordinals)` domain member's committed rows whose
    /// partition key equals `key` — and serialize it as the opaque handoff
    /// buffer (`rmp` of `(table id, FluxBIN rows)` pairs, deterministic
    /// table order).
    pub fn handoff_export(
        &self,
        tables: &[(TableId, Vec<u16>)],
        key: &[RowValue],
    ) -> Result<Vec<u8>> {
        let mut export: Vec<(u32, Vec<Vec<u8>>)> = Vec::new();
        let mut sorted: Vec<&(TableId, Vec<u16>)> = tables.iter().collect();
        sorted.sort_by_key(|(table, _)| table.as_u32());
        for (table, key_ordinals) in sorted {
            let mut rows = Vec::new();
            for row in self.scan(*table)? {
                let matches = key_ordinals
                    .iter()
                    .zip(key)
                    .all(|(&ordinal, want)| row.value(ordinal) == Some(want));
                if matches {
                    rows.push(crate::store::row::encode_row(row.values())?);
                }
            }
            export.push((table.as_u32(), rows));
        }
        rmp_serde::to_vec(&export)
            .map_err(|e| FluxumError::Storage(format!("handoff export encode failed: {e}")))
    }

    /// SHD-041 step 7: deserialize a handoff buffer and insert every row of
    /// the row set into THIS shard's buffered transaction. Rows re-validate
    /// through the ordinary write path (constraints, indexes on commit).
    pub fn handoff_import(&mut self, buffer: &[u8]) -> Result<u64> {
        let export: Vec<(u32, Vec<Vec<u8>>)> = rmp_serde::from_slice(buffer)
            .map_err(|e| FluxumError::Storage(format!("handoff import decode failed: {e}")))?;
        let mut imported = 0u64;
        for (table_raw, rows) in export {
            let table = TableId::from_raw(table_raw);
            let schema = self.store.schema_of(table)?;
            for bytes in rows {
                let row = crate::store::row::decode_row(schema, &bytes)?;
                self.upsert(table, row.values().to_vec())?;
                imported += 1;
            }
        }
        Ok(imported)
    }

    /// SHD-041 step 10: delete the entity's rows from this shard (the
    /// cleanup half). Returns how many rows were deleted.
    pub fn handoff_delete(
        &mut self,
        tables: &[(TableId, Vec<u16>)],
        key: &[RowValue],
    ) -> Result<u64> {
        let mut deleted = 0u64;
        for (table, key_ordinals) in tables {
            let schema = self.store.schema_of(*table)?;
            let victims: Vec<Vec<RowValue>> = self
                .scan(*table)?
                .into_iter()
                .filter(|row| {
                    key_ordinals
                        .iter()
                        .zip(key)
                        .all(|(&ordinal, want)| row.value(ordinal) == Some(want))
                })
                .map(|row| {
                    schema
                        .primary_key
                        .iter()
                        .map(|&ordinal| row.values()[usize::from(ordinal)].clone())
                        .collect()
                })
                .collect();
            for pk in victims {
                if self.delete(*table, &pk)? {
                    deleted += 1;
                }
            }
        }
        Ok(deleted)
    }

    /// Drain the RV-031 trigger events recorded so far, in mutation order.
    /// The reducer layer's `TxHandle` calls this after every mutation and
    /// dispatches registered `#[fluxum::on_*]` hooks inside this same
    /// transaction.
    pub fn take_trigger_events(&mut self) -> Vec<TriggerEvent> {
        std::mem::take(&mut self.state.trigger_events)
    }

    /// Decrypt a stored row's `#[encrypted]` columns (SPEC-017 CT-031) for
    /// the RV-031 trigger dispatch path; a no-op without a transform engine
    /// or when the table has no encrypted column.
    pub(crate) fn decrypt_stored(&self, table: TableId, row: &Row) -> Result<Row> {
        let Some(engine) = self.store.transforms.get() else {
            return Ok(row.clone());
        };
        if !engine.touches(table) {
            return Ok(row.clone());
        }
        let schema = self.store.schema_of(table)?;
        let mut values = row.values().to_vec();
        let pk = encode_pk_of_row(schema, &values)?;
        engine.on_read_row(table, &mut values, pk.as_bytes(), true)?;
        Ok(Row::new(values))
    }

    /// Point lookup against the committed snapshot captured at `begin`
    /// (STG-004: pending writes of this transaction are never visible).
    pub fn query_pk(&self, table: TableId, pk_values: &[RowValue]) -> Result<Option<Row>> {
        let t = self.base.table(table)?;
        let pk = encode_pk_values(t.schema, pk_values)?;
        t.row_at(&pk)
    }

    /// Scan the committed snapshot captured at `begin`, in encoded-PK byte
    /// order (STG-004: pending writes are never visible). Materialized from the
    /// paged tree.
    pub fn scan(&self, table: TableId) -> Result<Vec<Row>> {
        self.base.table(table)?.all_rows()
    }

    /// The rows a SPEC-010 (T3.6) migration rewrite operates on: every
    /// committed row keyed by its exact encoded PK bytes, seen **through**
    /// this transaction's pending replacements — a row already rewritten by
    /// an earlier [`Tx::migrate_replace`] of this same step yields its
    /// pending (new-layout) values, so consecutive DDL operations compose.
    /// Rows the transaction deleted or freshly inserted are excluded
    /// (inserts already carry the compiled layout).
    ///
    /// Keying by stored PK bytes — never re-deriving them from the compiled
    /// schema's ordinals — is what keeps rows in an old column layout
    /// addressable.
    pub(crate) fn migrate_rows(&self, table: TableId) -> Result<Vec<(PkBytes, Row)>> {
        let base = self.base.table(table)?;
        let ops = self.state.tables.get(&table);
        let mut out = Vec::with_capacity(base.row_count());
        base.for_each_row(|pk, row| {
            let effective = match ops.and_then(|pending| pending.get(&pk)) {
                Some(PendingOp::Update(replacement)) => Some(replacement.clone()),
                Some(PendingOp::Delete | PendingOp::Insert(_)) => None,
                None => Some(row),
            };
            if let Some(effective) = effective {
                out.push((pk, effective));
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// SPEC-010 (T3.6) migration seam: buffer an in-place replacement of the
    /// committed row at `pk` **without** validating `values` against the
    /// compiled schema.
    ///
    /// Mid-migration rows live in intermediate layouts (the stored catalog's
    /// layout plus the DDL steps applied so far), which only match the
    /// compiled schema after the last migration step — `check_row` would
    /// reject them. Safety rests on the migration runner's contract instead:
    /// `values` are derived from the committed row itself by appending or
    /// renaming columns, so PK bytes and every existing column ordinal
    /// (indexes, `#[unique]`, spatial coordinates) are preserved by
    /// construction, and the commit merge's remove(old)/insert(new) pairs
    /// stay symmetric.
    pub(crate) fn migrate_replace(
        &mut self,
        table: TableId,
        pk: PkBytes,
        values: Vec<RowValue>,
    ) -> Result<()> {
        let schema = self.store.schema_of(table)?;
        if !self.base.table(table)?.contains_pk(&pk)? {
            return Err(FluxumError::Storage(format!(
                "migrate_replace: table `{}` has no committed row at pk {pk} (SPEC-010 \
                 rewrites address committed rows only)",
                schema.name
            )));
        }
        let ops = self.state.tables.entry(table).or_default();
        ops.insert(pk, PendingOp::Update(Row::new(values)));
        Ok(())
    }

    /// Scan a B-tree index of the committed snapshot captured at `begin`
    /// (STG-004: pending writes are never visible). Same shapes and
    /// ordering as [`super::Snapshot::index_scan`].
    pub fn index_scan(
        &self,
        table: TableId,
        index: IndexId,
        prefix: &[RowValue],
        lower: Bound<&RowValue>,
        upper: Bound<&RowValue>,
    ) -> Result<Vec<Row>> {
        self.base
            .table(table)?
            .index_scan(index, prefix, lower, upper)
    }

    /// Equality lookup on a B-tree index of the committed snapshot captured
    /// at `begin` (see [`super::Snapshot::index_eq`]).
    pub fn index_eq(&self, table: TableId, index: IndexId, key: &[RowValue]) -> Result<Vec<Row>> {
        self.index_scan(table, index, key, Bound::Unbounded, Bound::Unbounded)
    }

    /// Spatial region query over the committed snapshot captured at `begin`
    /// (STG-004: pending writes are never visible). See
    /// [`super::Snapshot::spatial_region`].
    pub fn spatial_region(&self, table: TableId, region: Rect) -> Result<Vec<Row>> {
        self.base.table(table)?.spatial_region(region)
    }

    /// Spatial radius query over the committed snapshot captured at `begin`.
    /// See [`super::Snapshot::spatial_radius`].
    pub fn spatial_radius(&self, table: TableId, x: f64, y: f64, r: f64) -> Result<Vec<Row>> {
        self.base.table(table)?.spatial_radius(x, y, r)
    }

    /// Spatial point query over the committed snapshot captured at `begin`.
    /// See [`super::Snapshot::spatial_point`].
    pub fn spatial_point(&self, table: TableId, x: f64, y: f64) -> Result<Vec<Row>> {
        self.base.table(table)?.spatial_point(x, y)
    }

    /// Evaluate an `IN REGION` / `WITHIN RADIUS` predicate over the
    /// committed snapshot captured at `begin` (SPX-020/021). See
    /// [`super::Snapshot::eval_spatial`] for the 400/503 error contract.
    pub fn eval_spatial(&self, table: TableId, predicate: &SpatialPredicate) -> Result<Vec<Row>> {
        self.base.table(table)?.eval_spatial(predicate)
    }

    /// Rows written by THIS transaction — pending inserts and the new
    /// content of upsert replacements — in encoded-PK byte order. Pending
    /// deletes contribute nothing. This is the explicit TXN-051
    /// read-your-own-writes seam that SPEC-004 surfaces to reducers as
    /// `scan_pending` (FR-17); the default reads above never see these rows
    /// (TXN-050).
    pub fn scan_pending(&self, table: TableId) -> Result<impl Iterator<Item = &Row>> {
        // Unknown-table error parity with `scan`.
        self.base.table(table)?;
        Ok(self
            .state
            .tables
            .get(&table)
            .into_iter()
            .flat_map(|ops| ops.values())
            .filter_map(|op| match op {
                PendingOp::Insert(row) | PendingOp::Update(row) => Some(row),
                PendingOp::Delete => None,
            }))
    }

    /// Combined view (TXN-051): the committed snapshot overlaid with this
    /// transaction's pending effects, deduplicated by primary key — a
    /// pending insert or upsert replacement wins over the committed row
    /// with the same key, and a pending delete removes it. Order: committed
    /// keys in encoded-PK byte order (replacements in place), followed by
    /// the rows newly inserted by this transaction in encoded-PK byte order.
    pub fn scan_all(&self, table: TableId) -> Result<Vec<Row>> {
        let base = self.base.table(table)?;
        let pending = self.state.tables.get(&table);
        let mut out = Vec::with_capacity(base.row_count());
        base.for_each_row(|pk, row| {
            match pending.and_then(|ops| ops.get(&pk)) {
                None => out.push(row),
                Some(PendingOp::Update(replacement)) => out.push(replacement.clone()),
                // Delete: shadowed. Insert over a committed key cannot happen
                // (the write path turns it into Update/Delete), but skipping it
                // here keeps the dedup-by-PK contract even then — the pending
                // pass below yields it once.
                Some(PendingOp::Delete | PendingOp::Insert(_)) => {}
            }
            Ok(())
        })?;
        if let Some(ops) = pending {
            for op in ops.values() {
                if let PendingOp::Insert(row) = op {
                    out.push(row.clone());
                }
            }
        }
        Ok(out)
    }

    /// Commit: merge `TxState` into a new `CommittedState` and swap it in
    /// atomically (STG-005). Constraints were enforced eagerly at write
    /// time, so the merge itself is infallible under the single-writer
    /// guarantee. Returns the [`TxDiff`] for the commit log (T2.2) and
    /// subscription evaluation (SPEC-005).
    pub fn commit(mut self) -> Result<TxDiff> {
        // 1. Build the new state off to the side (readers stay on the old
        //    snapshot; nothing is observable until the swap).
        let mut tables = self.base.tables.clone(); // Arc bumps only
        let mut diffs: Vec<TableDiff> = Vec::with_capacity(self.state.tables.len());
        // Pages the copy-on-write merge superseded, per table — retired
        // against the new version once it is published (TIER-061).
        let mut superseded: Vec<(TableId, u64)> = Vec::new();
        let tx_tables = std::mem::take(&mut self.state.tables);
        for (table_id, ops) in tx_tables {
            if ops.is_empty() {
                continue;
            }
            let slot = tables.get_mut(&table_id).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "internal invariant violated: touched table {table_id} missing from \
                     CommittedState"
                ))
            })?;
            let table = Arc::make_mut(slot); // deep-clones only touched tables
            let mut sup: Vec<u64> = Vec::new();
            let mut diff = TableDiff {
                table_id,
                inserts: Vec::new(),
                deletes: Vec::new(),
            };
            // Unique-map maintenance is two-pass across the table's ops:
            // every vacated value is released before any new value is
            // claimed, so a transaction that moves a `#[unique]` value
            // between rows (validated eagerly at write time, TXN-041)
            // merges regardless of pk iteration order.
            for (pk, op) in &ops {
                if matches!(op, PendingOp::Delete | PendingOp::Update(_)) {
                    let old = table.row_at(pk)?.ok_or_else(invariant_missing_row)?;
                    for constraint in &mut table.unique {
                        constraint.remove(&old, pk, &mut sup)?;
                    }
                }
            }
            // Rows (paged, CoW) and secondary indexes are updated together on
            // this private pre-swap copy (STG-005 steps 2–4), so the published
            // snapshot's rows and indexes are mutually consistent and rollback
            // never has index state to revert (STG-007 rule 2).
            for (pk, op) in ops {
                match op {
                    PendingOp::Insert(row) => {
                        for index in table.indexes.values_mut() {
                            index.insert(&row, pk.clone(), &mut sup)?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            spatial.insert_row(&row, pk.clone(), &mut sup)?;
                        }
                        for fulltext in &mut table.fulltext {
                            fulltext.insert_row(&row, pk.clone(), &mut sup)?;
                        }
                        for constraint in &mut table.unique {
                            constraint.insert(&row, pk.clone(), &mut sup)?;
                        }
                        table.put_row(&pk, &row, &mut sup)?;
                        diff.inserts.push(row);
                    }
                    PendingOp::Delete => {
                        let old = table
                            .take_row(&pk, &mut sup)?
                            .ok_or_else(invariant_missing_row)?;
                        for index in table.indexes.values_mut() {
                            index.remove(&old, &pk, &mut sup)?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            spatial.remove_row(&old, &pk, &mut sup)?;
                        }
                        for fulltext in &mut table.fulltext {
                            fulltext.remove_row(&old, &pk, &mut sup)?;
                        }
                        diff.deletes.push((pk, old));
                    }
                    PendingOp::Update(row) => {
                        let old = table
                            .put_row(&pk, &row, &mut sup)?
                            .ok_or_else(invariant_missing_row)?;
                        for index in table.indexes.values_mut() {
                            index.remove(&old, &pk, &mut sup)?;
                            index.insert(&row, pk.clone(), &mut sup)?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            // SPX-032: old coordinates out, new coordinates
                            // in — atomic with the row swap on this private
                            // pre-swap copy, so no stale entry can publish.
                            spatial.remove_row(&old, &pk, &mut sup)?;
                            spatial.insert_row(&row, pk.clone(), &mut sup)?;
                        }
                        for fulltext in &mut table.fulltext {
                            // FTS-021: re-analyze the old text out, the new
                            // text in — atomic with the row swap.
                            fulltext.remove_row(&old, &pk, &mut sup)?;
                            fulltext.insert_row(&row, pk.clone(), &mut sup)?;
                        }
                        // The old row's unique values were released in the
                        // two-pass removal above; claim the new row's.
                        for constraint in &mut table.unique {
                            constraint.insert(&row, pk.clone(), &mut sup)?;
                        }
                        diff.deletes.push((pk, old));
                        diff.inserts.push(row);
                    }
                }
            }
            superseded.extend(sup.into_iter().map(|p| (table_id, p)));
            diffs.push(diff);
        }

        // 2. Publish auto-inc high-water advances (including ones made by
        //    earlier rolled-back transactions) so T2.2 can log them.
        let dirty = std::mem::take(&mut self.writer.high_water_dirty);
        let mut auto_inc = Vec::with_capacity(dirty.len());
        for table_id in dirty {
            let high_water = self
                .writer
                .counters
                .get(&table_id)
                .map_or(0, |c| c.high_water);
            let slot = tables.get_mut(&table_id).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "internal invariant violated: auto-inc table {table_id} missing from \
                     CommittedState"
                ))
            })?;
            Arc::make_mut(slot).auto_inc_high_water = high_water;
            auto_inc.push((table_id, high_water));
        }

        // 3. Consume the tx id and publish the new version — atomic for
        //    readers — retiring the pages the CoW merge superseded (TIER-061).
        let tx_id = self.state.tx_id;
        self.writer.next_tx_id = tx_id.saturating_add(1);
        let published = self.store.publish(&mut self.writer, tables, superseded)?;

        // SPEC-022 RV-020: retain this commit's snapshot in the bounded
        // temporal window. Untouched tables are `Arc`-shared with the
        // neighbors, so retention costs only the copies the commit already
        // made. Metadata only — never reducer-visible (SEC-020 unaffected).
        self.store.retain_in_temporal_window(
            tx_id,
            crate::types::Timestamp::now().as_micros(),
            published,
        );

        // 4. Blob refcounts (DMX-040): row references drive GC. Applied
        //    under the writer lock, increments before decrements so an
        //    intra-commit move never dips a count to a reclaimable zero.
        if let Some(blobs) = self.store.blobs.get() {
            apply_blob_refcounts(&self.store.catalog, blobs, &diffs);
        }

        Ok(TxDiff {
            tx_id,
            tables: diffs,
            auto_inc,
        })
    }

    /// Roll back: revert eagerly applied effects (STG-007; none exist in
    /// T2.1) and discard `TxState` (STG-006). Committed state is untouched
    /// by construction; the tx id is not consumed (TXN-030). Dropping the
    /// handle without committing is equivalent (see the [`Drop`] impl).
    pub fn rollback(self) {
        // Drop performs the STG-007 revert; being explicit here documents
        // intent at call sites.
    }

    /// Assign / observe the auto-inc column (STG-040, TXN-042).
    pub(super) fn assign_auto_inc(
        &mut self,
        table: TableId,
        schema: &'static TableSchema,
        values: &mut [RowValue],
    ) -> Result<()> {
        let Some(ordinal) = schema.auto_inc else {
            return Ok(());
        };
        let idx = usize::from(ordinal);
        // DM-004 (registry-validated): the auto-inc column is u64.
        let RowValue::U64(current) = values[idx] else {
            return Err(FluxumError::Storage(format!(
                "table `{}`: #[auto_inc] column must be u64 (FluxType::{:?} found)",
                schema.name, schema.columns[idx].ty
            )));
        };
        let counter = self
            .writer
            .counters
            .entry(table)
            .or_insert_with(AutoIncCounter::new);
        let advanced = if current == 0 {
            let (assigned, advanced) =
                counter.allocate(self.store.options.auto_inc_allocation_step);
            values[idx] = RowValue::U64(assigned);
            advanced
        } else {
            counter.observe_explicit(current)
        };
        if advanced {
            self.writer.high_water_dirty.insert(table);
        }
        Ok(())
    }
}

impl Drop for Tx<'_> {
    /// A dropped (committed or not) transaction always replays its undo log
    /// (STG-007 rule 3) — a reducer panic unwinding through the handle gets
    /// the same revert as an explicit [`Tx::rollback`]. After a successful
    /// [`Tx::commit`] the log is empty, so this is a no-op.
    fn drop(&mut self) {
        self.state.revert_eager_effects();
    }
}
