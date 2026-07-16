//! Column transforms (SPEC-017): the DTO-in-schema write path.
//!
//! This module holds the deterministic value **normalizers** (CT-021/022/023)
//! — money to exact fixed-point, timestamps to canonical UTC — that the
//! `#[normalize(...)]` column attribute applies before a value is stored. The
//! `ColumnTransform` trait, the crypto transforms (`#[encrypted]`/`#[signed]`),
//! field-level security (`#[masked]`/`#[column_grant]`), and the link-time
//! transform registry land in later increments of this task; string/Unicode
//! normalization (CT-023) needs a Unicode-normalization dependency and is
//! deferred with them.

pub mod normalize;

pub use normalize::{datetime_utc, money_from_minor_units, money_from_str};
