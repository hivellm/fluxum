//! `POST /rows` — the console's row editor (DEV-030 v1): schema-directed
//! JSON→RowValue conversion and the audited pipeline commit. Split from
//! the parent module to honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

// --- POST /rows (console row editor, DEV-030 v1) ----------------------------------

/// Upsert or delete one row as the admin operator — the console's
/// phpMyAdmin-style edit surface.
///
/// Body: `{ "table": "Task", "op": "upsert" | "delete", "row": {col: json} }`.
/// The row object carries **every** column for `upsert`; for `delete` it may
/// be the full row as rendered by the grid (extra columns beyond the PK are
/// ignored for the lookup but used for shard routing).
///
/// This is not a bypass of the storage model: the edit commits through the
/// shard's own `TxPipeline` — single-writer, constraints enforced eagerly
/// (`#[unique]`, `#[check]`, FKs, PK typing), subscriptions fanned out, the
/// commit logged — exactly like a reducer's write, with
/// `CommitMeta { caller: admin identity, reducer_name: "__console.row_edit" }`
/// so the audit trail and the live watch attribute it to the console. On a
/// multi-shard deployment the edit routes to the shard owning the row
/// (SHD-012).
pub(super) async fn row_edit(ctx: &Arc<ShardContext>, body: &[u8]) -> AdminResponse {
    let (request_id, payload) = match parse_request(body) {
        Ok(pair) => pair,
        Err(e) => return AdminResponse::err(400, None, e),
    };
    let rid = request_id.as_deref();
    let Some(table_name) = payload.get("table").and_then(Value::as_str) else {
        return AdminResponse::err(400, rid, "payload.table (string) required");
    };
    let op = payload
        .get("op")
        .and_then(Value::as_str)
        .unwrap_or("upsert");
    let Some(row_obj) = payload.get("row").and_then(Value::as_object) else {
        return AdminResponse::err(400, rid, "payload.row (object of column: value) required");
    };
    let Some(table_id) = ctx.store().table_id(table_name) else {
        return AdminResponse::err(404, rid, format!("unknown table `{table_name}`"));
    };
    let Some(schema) = ctx.store().table_schema(table_id) else {
        return AdminResponse::err(404, rid, format!("unknown table `{table_name}`"));
    };

    // Schema-directed conversion: every column, in declaration order. For a
    // delete only the PK columns are *required*; anything else present is
    // still converted (the shard router may need the partition key).
    let pk_ordinals: std::collections::HashSet<usize> = schema
        .primary_key
        .iter()
        .map(|&ordinal| usize::from(ordinal))
        .collect();
    let mut values: Vec<Option<fluxum_core::store::RowValue>> =
        Vec::with_capacity(schema.columns.len());
    for (ordinal, column) in schema.columns.iter().enumerate() {
        match row_obj.get(column.name) {
            Some(json) => match json_to_row_value(&column.ty, json) {
                Ok(value) => values.push(Some(value)),
                Err(e) => {
                    return AdminResponse::err(400, rid, format!("column `{}`: {e}", column.name));
                }
            },
            None if op == "delete" && !pk_ordinals.contains(&ordinal) => values.push(None),
            None => {
                return AdminResponse::err(
                    400,
                    rid,
                    format!("column `{}` missing from payload.row", column.name),
                );
            }
        }
    }

    // The pipeline this edit commits on: the shard owning the row (SHD-012)
    // on a multi-shard deployment, this shard otherwise.
    let target = match ctx.coord() {
        Some(coord) => {
            let routing_row: Vec<fluxum_core::store::RowValue> = values
                .iter()
                .map(|v| {
                    v.clone()
                        .unwrap_or(fluxum_core::store::RowValue::Optional(None))
                })
                .collect();
            match coord.shard_of_row(table_id, &routing_row) {
                Ok(shard) => match coord.host(shard) {
                    Some(host) => Arc::clone(host),
                    None => {
                        return AdminResponse::err(500, rid, format!("unknown shard {shard}"));
                    }
                },
                Err(e) => return AdminResponse::err(status_of(&e), rid, e.to_string()),
            }
        }
        None => Arc::clone(ctx),
    };

    let meta = fluxum_core::txn::CommitMeta {
        caller: ctx.admin_identity,
        reducer_name: "__console.row_edit".to_owned(),
    };
    let outcome = match op {
        "upsert" => {
            let row: Vec<fluxum_core::store::RowValue> =
                match values.into_iter().collect::<Option<Vec<_>>>() {
                    Some(row) => row,
                    None => return AdminResponse::err(400, rid, "upsert requires every column"),
                };
            target
                .engine
                .pipeline()
                .call_with(
                    meta,
                    Box::new(move |tx| {
                        tx.upsert(table_id, row.clone())?;
                        Ok(())
                    }),
                )
                .await
                .map(|receipt| json!({ "op": "upsert", "tx_id": receipt.tx_id }))
        }
        "delete" => {
            let pk: Vec<fluxum_core::store::RowValue> = schema
                .primary_key
                .iter()
                .filter_map(|&ordinal| values.get(usize::from(ordinal)).cloned().flatten())
                .collect();
            if pk.len() != schema.primary_key.len() {
                return AdminResponse::err(400, rid, "delete requires every primary-key column");
            }
            target
                .engine
                .pipeline()
                .call_with(
                    meta,
                    Box::new(move |tx| {
                        if !tx.delete(table_id, &pk)? {
                            return Err(FluxumError::query(
                                fluxum_protocol::codes::SQL_MALFORMED,
                                "no row with that primary key",
                            ));
                        }
                        Ok(())
                    }),
                )
                .await
                .map(|receipt| json!({ "op": "delete", "tx_id": receipt.tx_id }))
        }
        other => {
            return AdminResponse::err(
                400,
                rid,
                format!("payload.op must be `upsert` or `delete`, got `{other}`"),
            );
        }
    };
    match outcome {
        Ok(result) => AdminResponse::ok(rid, result),
        Err(e) => AdminResponse::err(status_of(&e), rid, e.to_string()),
    }
}

/// Convert one JSON value to a [`fluxum_core::store::RowValue`] under the
/// column's declared [`fluxum_core::schema::FluxType`] — the inverse of
/// [`fluxum_core::subscription::row_value_to_json`], accepting exactly the
/// shapes that renderer emits (hex for bytes/identity, micros for
/// timestamps, stringified numbers tolerated for the 64-bit types the grid
/// shows as strings).
///
/// Structured columns (`Enum`, `Struct`, `Blob`, `CrdtText`) are refused
/// with a pointer at reducers: their invariants live in module code, and a
/// console must not guess at them.
pub(super) fn json_to_row_value(
    ty: &fluxum_core::schema::FluxType,
    json: &Value,
) -> Result<fluxum_core::store::RowValue, String> {
    use fluxum_core::schema::FluxType as T;
    use fluxum_core::store::RowValue as V;
    let want_i64 = |json: &Value| -> Result<i64, String> {
        match json {
            Value::Number(n) => n.as_i64().ok_or_else(|| format!("{n} is not an integer")),
            Value::String(s) => s
                .trim()
                .parse()
                .map_err(|_| format!("`{s}` is not an integer")),
            other => Err(format!("expected an integer, got {other}")),
        }
    };
    let want_u64 = |json: &Value| -> Result<u64, String> {
        match json {
            Value::Number(n) => n
                .as_u64()
                .ok_or_else(|| format!("{n} is not an unsigned integer")),
            Value::String(s) => s
                .trim()
                .parse()
                .map_err(|_| format!("`{s}` is not an unsigned integer")),
            other => Err(format!("expected an unsigned integer, got {other}")),
        }
    };
    let narrow = |v: i64, lo: i64, hi: i64| -> Result<i64, String> {
        if v < lo || v > hi {
            return Err(format!("{v} is outside [{lo}, {hi}]"));
        }
        Ok(v)
    };
    match ty {
        T::Bool => match json {
            Value::Bool(b) => Ok(V::Bool(*b)),
            other => Err(format!("expected a boolean, got {other}")),
        },
        T::I8 => Ok(V::I8(
            narrow(want_i64(json)?, i64::from(i8::MIN), i64::from(i8::MAX))? as i8,
        )),
        T::I16 => Ok(V::I16(
            narrow(want_i64(json)?, i64::from(i16::MIN), i64::from(i16::MAX))? as i16,
        )),
        T::I32 => Ok(V::I32(
            narrow(want_i64(json)?, i64::from(i32::MIN), i64::from(i32::MAX))? as i32,
        )),
        T::I64 => Ok(V::I64(want_i64(json)?)),
        T::U8 => {
            let v = want_u64(json)?;
            u8::try_from(v)
                .map(V::U8)
                .map_err(|_| format!("{v} overflows u8"))
        }
        T::U16 => {
            let v = want_u64(json)?;
            u16::try_from(v)
                .map(V::U16)
                .map_err(|_| format!("{v} overflows u16"))
        }
        T::U32 => {
            let v = want_u64(json)?;
            u32::try_from(v)
                .map(V::U32)
                .map_err(|_| format!("{v} overflows u32"))
        }
        T::U64 => Ok(V::U64(want_u64(json)?)),
        T::F32 => match json {
            Value::Number(n) => n
                .as_f64()
                .map(|f| V::F32(f as f32))
                .ok_or_else(|| format!("{n} is not a number")),
            other => Err(format!("expected a number, got {other}")),
        },
        T::F64 => match json {
            Value::Number(n) => n
                .as_f64()
                .map(V::F64)
                .ok_or_else(|| format!("{n} is not a number")),
            other => Err(format!("expected a number, got {other}")),
        },
        T::Str => match json {
            Value::String(s) => Ok(V::Str(s.clone())),
            other => Err(format!("expected a string, got {other}")),
        },
        T::Bytes => match json {
            Value::String(s) => hex_bytes(s).map(V::Bytes),
            other => Err(format!("expected a hex string, got {other}")),
        },
        T::Identity => match json {
            Value::String(s) => s
                .parse::<fluxum_core::types::Identity>()
                .map(V::Identity)
                .map_err(|e| format!("bad identity: {e}")),
            other => Err(format!(
                "expected a 64-hex-char identity string, got {other}"
            )),
        },
        T::ConnectionId => match json {
            Value::String(s) => s
                .trim()
                .parse::<u128>()
                .map(|v| V::ConnectionId(ConnectionId::new(v)))
                .map_err(|_| format!("`{s}` is not a connection id")),
            Value::Number(_) => {
                want_u64(json).map(|v| V::ConnectionId(ConnectionId::new(u128::from(v))))
            }
            other => Err(format!("expected a connection id, got {other}")),
        },
        T::EntityId => want_u64(json).map(|v| V::EntityId(fluxum_core::types::EntityId::new(v))),
        T::Timestamp => want_i64(json).map(|v| V::Timestamp(Timestamp::from_micros(v))),
        T::Decimal => match json {
            Value::String(s) => parse_decimal(s.trim()).map(V::Decimal),
            Value::Number(n) => parse_decimal(&n.to_string()).map(V::Decimal),
            other => Err(format!("expected a decimal string, got {other}")),
        },
        T::Option(inner) => match json {
            Value::Null => Ok(V::Optional(None)),
            other => Ok(V::Optional(Some(Box::new(json_to_row_value(
                inner, other,
            )?)))),
        },
        T::List(inner) => match json {
            Value::Array(items) => items
                .iter()
                .map(|item| json_to_row_value(inner, item))
                .collect::<Result<Vec<_>, _>>()
                .map(V::List),
            other => Err(format!("expected an array, got {other}")),
        },
        T::Enum(_) | T::Struct(_) | T::Blob | T::CrdtText => Err(
            "structured column types (enum/struct/blob/crdt) are edited through reducers, \
             not the console"
                .to_owned(),
        ),
    }
}

/// Parse a plain decimal literal (`-12.34`) into an exact
/// [`fluxum_core::types::Decimal`] — digits become the unscaled integer,
/// the fraction length the scale. No exponent form; the console has no
/// business inventing one.
pub(super) fn parse_decimal(s: &str) -> Result<fluxum_core::types::Decimal, String> {
    let bad = || format!("`{s}` is not a decimal literal");
    let (sign, digits) = match s.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(bad());
    }
    let scale = u8::try_from(frac_part.len()).map_err(|_| bad())?;
    let mut unscaled: i128 = 0;
    for c in int_part.chars().chain(frac_part.chars()) {
        let digit = c.to_digit(10).ok_or_else(bad)?;
        unscaled = unscaled
            .checked_mul(10)
            .and_then(|u| u.checked_add(i128::from(digit)))
            .ok_or_else(|| format!("`{s}` overflows the decimal range"))?;
    }
    Ok(fluxum_core::types::Decimal::from_parts(
        sign * unscaled,
        scale,
    ))
}

/// Decode a hex string (the `row_value_to_json` rendering of bytes).
pub(super) fn hex_bytes(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return Err("hex string must have an even length".to_owned());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| format!("bad hex at offset {i}")))
        .collect()
}
