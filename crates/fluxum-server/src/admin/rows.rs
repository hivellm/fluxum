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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use fluxum_core::schema::FluxType as T;
    use fluxum_core::store::RowValue as V;
    use serde_json::json;

    use super::*;

    fn conv(ty: &T, v: Value) -> Result<fluxum_core::store::RowValue, String> {
        json_to_row_value(ty, &v)
    }

    /// Every scalar arm accepts exactly the shape `row_value_to_json`
    /// renders — numbers as numbers, 64-bit values also as strings (the
    /// grid shows them that way), hex for bytes/identity, micros for
    /// timestamps.
    #[test]
    fn every_scalar_type_round_trips_its_rendered_shape() {
        assert_eq!(conv(&T::Bool, json!(true)).unwrap(), V::Bool(true));
        assert_eq!(conv(&T::I8, json!(-5)).unwrap(), V::I8(-5));
        assert_eq!(conv(&T::I16, json!(-300)).unwrap(), V::I16(-300));
        assert_eq!(conv(&T::I32, json!(70_000)).unwrap(), V::I32(70_000));
        assert_eq!(conv(&T::I64, json!(-9)).unwrap(), V::I64(-9));
        assert!(conv(&T::I64, json!("‑1")).is_err()); // non-ASCII minus refused
        assert_eq!(conv(&T::I64, json!(" -12 ")).unwrap(), V::I64(-12));
        assert_eq!(conv(&T::U8, json!(200)).unwrap(), V::U8(200));
        assert_eq!(conv(&T::U16, json!(60_000)).unwrap(), V::U16(60_000));
        assert_eq!(conv(&T::U32, json!(4_000_000)).unwrap(), V::U32(4_000_000));
        assert_eq!(
            conv(&T::U64, json!("18446744073709551615")).unwrap(),
            V::U64(u64::MAX),
            "64-bit values ride as strings"
        );
        assert!(matches!(conv(&T::F32, json!(1.5)).unwrap(), V::F32(_)));
        assert!(matches!(conv(&T::F64, json!(2.25)).unwrap(), V::F64(_)));
        assert_eq!(conv(&T::Str, json!("hi")).unwrap(), V::Str("hi".into()));
        assert_eq!(
            conv(&T::Bytes, json!("0aff")).unwrap(),
            V::Bytes(vec![0x0a, 0xff])
        );
        let identity = "11".repeat(32);
        assert!(matches!(
            conv(&T::Identity, json!(identity)).unwrap(),
            V::Identity(_)
        ));
        assert!(matches!(
            conv(&T::ConnectionId, json!("7")).unwrap(),
            V::ConnectionId(_)
        ));
        assert!(matches!(
            conv(&T::ConnectionId, json!(7)).unwrap(),
            V::ConnectionId(_)
        ));
        assert!(matches!(
            conv(&T::EntityId, json!(42)).unwrap(),
            V::EntityId(_)
        ));
        assert!(matches!(
            conv(&T::Timestamp, json!(1_785_196_924_389_486_i64)).unwrap(),
            V::Timestamp(_)
        ));
    }

    /// Range and shape violations name the offending value — the editor
    /// shows these verbatim, so they must be precise.
    #[test]
    fn narrowing_and_shape_errors_name_the_value() {
        assert!(conv(&T::I8, json!(128)).unwrap_err().contains("outside"));
        assert!(
            conv(&T::I16, json!(40_000))
                .unwrap_err()
                .contains("outside")
        );
        assert!(
            conv(&T::I32, json!(i64::MAX))
                .unwrap_err()
                .contains("outside")
        );
        assert!(
            conv(&T::U8, json!(256))
                .unwrap_err()
                .contains("overflows u8")
        );
        assert!(
            conv(&T::U16, json!(70_000))
                .unwrap_err()
                .contains("overflows u16")
        );
        assert!(
            conv(&T::U32, json!(5_000_000_000_u64))
                .unwrap_err()
                .contains("overflows u32")
        );
        assert!(conv(&T::U64, json!(-1)).unwrap_err().contains("unsigned"));
        assert!(
            conv(&T::U64, json!("nope"))
                .unwrap_err()
                .contains("unsigned")
        );
        assert!(
            conv(&T::I64, json!("x"))
                .unwrap_err()
                .contains("not an integer")
        );
        assert!(
            conv(&T::I64, json!(true))
                .unwrap_err()
                .contains("expected an integer")
        );
        assert!(
            conv(&T::Bool, json!(1))
                .unwrap_err()
                .contains("expected a boolean")
        );
        assert!(
            conv(&T::F64, json!("1"))
                .unwrap_err()
                .contains("expected a number")
        );
        assert!(
            conv(&T::F32, json!(null))
                .unwrap_err()
                .contains("expected a number")
        );
        assert!(
            conv(&T::Str, json!(1))
                .unwrap_err()
                .contains("expected a string")
        );
        assert!(
            conv(&T::Bytes, json!(1))
                .unwrap_err()
                .contains("hex string")
        );
        assert!(
            conv(&T::Identity, json!("zz"))
                .unwrap_err()
                .contains("bad identity")
        );
        assert!(
            conv(&T::Identity, json!(1))
                .unwrap_err()
                .contains("64-hex-char")
        );
        assert!(
            conv(&T::ConnectionId, json!("x"))
                .unwrap_err()
                .contains("not a connection id")
        );
        assert!(
            conv(&T::ConnectionId, json!(true))
                .unwrap_err()
                .contains("connection id")
        );
    }

    /// Option and List recurse through the same converter; structured
    /// types refuse with the pointer at reducers.
    #[test]
    fn containers_recurse_and_structured_types_refuse() {
        assert_eq!(
            conv(&T::Option(&T::U32), json!(null)).unwrap(),
            V::Optional(None)
        );
        assert_eq!(
            conv(&T::Option(&T::U32), json!(7)).unwrap(),
            V::Optional(Some(Box::new(V::U32(7))))
        );
        assert!(conv(&T::Option(&T::U32), json!("x")).is_err());
        assert_eq!(
            conv(&T::List(&T::I32), json!([1, 2])).unwrap(),
            V::List(vec![V::I32(1), V::I32(2)])
        );
        assert!(
            conv(&T::List(&T::I32), json!("no"))
                .unwrap_err()
                .contains("expected an array")
        );
        assert!(
            conv(&T::List(&T::I32), json!([1, "x"])).is_err(),
            "a bad element fails the whole list"
        );
        for ty in [T::Blob, T::CrdtText] {
            assert!(
                conv(&ty, json!("x"))
                    .unwrap_err()
                    .contains("through reducers")
            );
        }
    }

    #[test]
    fn decimal_literals_parse_exactly_and_refuse_noise() {
        let d = parse_decimal("-12.34").unwrap();
        assert_eq!(format!("{d}"), "-12.34");
        assert_eq!(format!("{}", parse_decimal("0.5").unwrap()), "0.5");
        assert_eq!(format!("{}", parse_decimal("+7").unwrap()), "7");
        assert!(parse_decimal("").unwrap_err().contains("not a decimal"));
        assert!(parse_decimal(".").unwrap_err().contains("not a decimal"));
        assert!(parse_decimal("1e5").unwrap_err().contains("not a decimal"));
        assert!(parse_decimal("12a").unwrap_err().contains("not a decimal"));
        assert!(
            parse_decimal(&"9".repeat(60))
                .unwrap_err()
                .contains("overflows the decimal range")
        );
        // Both JSON shapes reach the same parser.
        assert!(matches!(
            conv(&T::Decimal, json!("1.25")).unwrap(),
            V::Decimal(_)
        ));
        assert!(matches!(
            conv(&T::Decimal, json!(3)).unwrap(),
            V::Decimal(_)
        ));
        assert!(
            conv(&T::Decimal, json!(true))
                .unwrap_err()
                .contains("decimal string")
        );
    }

    #[test]
    fn hex_decoding_is_strict() {
        assert_eq!(hex_bytes("00ff10").unwrap(), vec![0x00, 0xff, 0x10]);
        assert_eq!(hex_bytes(" 0a ").unwrap(), vec![0x0a], "whitespace trimmed");
        assert!(hex_bytes("abc").unwrap_err().contains("even length"));
        assert!(hex_bytes("zz").unwrap_err().contains("bad hex at offset 0"));
        assert!(hex_bytes("00qq").unwrap_err().contains("offset 2"));
    }
}
