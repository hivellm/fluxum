//! Reducer argument decoding (SPEC-004 RED-001, T3.3): the typed bridge from
//! a `ReducerCall`'s dynamic [`FluxValue`] argument list to the parameter
//! types a `#[fluxum::reducer]` function declares.
//!
//! The `#[fluxum::reducer]` macro generates two functions from a reducer's
//! signature: an argument **check** (arity + per-parameter decode, run by the
//! engine *before* any transaction is started — RED-001) and the dispatch
//! **glue** (the same decode feeding the user function inside the
//! transaction). Both are plain compositions of [`check_arity`] and
//! [`decode_arg`], so the two paths can never disagree.
//!
//! Decoding is strict per the RPC-010/RPC-011 value model: an `I64` wire
//! integer inhabits any integer parameter it fits into (range-checked), a
//! `u64`/`EntityId` parameter additionally accepts the tagged `EntityId`
//! form, floats only come from `F64`, and no cross-kind coercion exists
//! (no string→number, no bool→int). `Option<T>` accepts `Null` or a `T`;
//! list-typed parameters are not part of the T3.3 surface (SPEC-011 SDK
//! codegen owns richer argument shapes).

use fluxum_protocol::{FluxValue, codes};

use crate::error::{FluxumError, Result};
use crate::types::{EntityId, Identity, Timestamp};

/// A Rust type that can be decoded from one [`FluxValue`] reducer argument.
///
/// Implemented for the argument-shaped subset of the SPEC-001 §3 universe;
/// `#[fluxum::reducer]` bounds every declared parameter type with this trait,
/// so an unsupported parameter type is a compile error at the reducer.
pub trait ReducerArg: Sized {
    /// Human-readable type name for mismatch diagnostics.
    const TYPE_NAME: &'static str;

    /// Decode from a wire value; `None` on any kind or range mismatch.
    fn from_flux(value: &FluxValue) -> Option<Self>;
}

macro_rules! impl_int_arg {
    ($($ty:ty),+ $(,)?) => {
        $(impl ReducerArg for $ty {
            const TYPE_NAME: &'static str = stringify!($ty);

            fn from_flux(value: &FluxValue) -> Option<Self> {
                match value {
                    FluxValue::I64(n) => <$ty>::try_from(*n).ok(),
                    _ => None,
                }
            }
        })+
    };
}

impl_int_arg!(i8, i16, i32, i64, u8, u16, u32);

impl ReducerArg for u64 {
    const TYPE_NAME: &'static str = "u64";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::I64(n) => u64::try_from(*n).ok(),
            // Above i64::MAX the wire form is the tagged EntityId (RPC-011).
            FluxValue::EntityId(n) => Some(*n),
            _ => None,
        }
    }
}

impl ReducerArg for bool {
    const TYPE_NAME: &'static str = "bool";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl ReducerArg for f32 {
    const TYPE_NAME: &'static str = "f32";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            #[allow(clippy::cast_possible_truncation)] // f64→f32 rounds, by contract
            FluxValue::F64(f) => Some(*f as f32),
            _ => None,
        }
    }
}

impl ReducerArg for f64 {
    const TYPE_NAME: &'static str = "f64";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::F64(f) => Some(*f),
            _ => None,
        }
    }
}

impl ReducerArg for String {
    const TYPE_NAME: &'static str = "String";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::Str(s) => Some(s.clone()),
            _ => None,
        }
    }
}

impl ReducerArg for Vec<u8> {
    const TYPE_NAME: &'static str = "Vec<u8>";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::Bytes(b) => Some(b.clone()),
            _ => None,
        }
    }
}

impl ReducerArg for Identity {
    const TYPE_NAME: &'static str = "Identity";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::Identity(bytes) => Some(Identity::from_bytes(*bytes)),
            _ => None,
        }
    }
}

impl ReducerArg for EntityId {
    const TYPE_NAME: &'static str = "EntityId";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::EntityId(n) => Some(EntityId::new(*n)),
            FluxValue::I64(n) => u64::try_from(*n).ok().map(EntityId::new),
            _ => None,
        }
    }
}

impl ReducerArg for Timestamp {
    const TYPE_NAME: &'static str = "Timestamp";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::Timestamp(us) => Some(Timestamp::from_micros(*us)),
            FluxValue::I64(us) => Some(Timestamp::from_micros(*us)),
            _ => None,
        }
    }
}

impl<T: ReducerArg> ReducerArg for Option<T> {
    const TYPE_NAME: &'static str = "Option<_>";

    fn from_flux(value: &FluxValue) -> Option<Self> {
        match value {
            FluxValue::Null => Some(None),
            other => T::from_flux(other).map(Some),
        }
    }
}

/// Reject an argument-count mismatch with a wire-ready 400 (RED-001; the
/// engine runs this before any transaction exists).
pub fn check_arity(reducer: &str, args: &[FluxValue], expected: usize) -> Result<()> {
    if args.len() != expected {
        return Err(FluxumError::query(
            codes::MALFORMED,
            format!(
                "reducer `{reducer}` takes {expected} argument(s), got {} (RED-001)",
                args.len()
            ),
        ));
    }
    Ok(())
}

/// Decode the argument at `index` (parameter `name`) as `T`, or reject with
/// a wire-ready 400 naming the parameter and the expected type (RED-001).
pub fn decode_arg<T: ReducerArg>(
    reducer: &str,
    args: &[FluxValue],
    index: usize,
    name: &str,
) -> Result<T> {
    let value = args.get(index).ok_or_else(|| {
        FluxumError::query(
            codes::MALFORMED,
            format!("reducer `{reducer}`: missing argument {index} (`{name}`) (RED-001)"),
        )
    })?;
    T::from_flux(value).ok_or_else(|| {
        FluxumError::query(
            codes::MALFORMED,
            format!(
                "reducer `{reducer}`: argument {index} (`{name}`) expects {}, got {value:?} \
                 (RED-001)",
                T::TYPE_NAME
            ),
        )
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn integers_decode_with_range_checks() {
        assert_eq!(i8::from_flux(&FluxValue::I64(-5)), Some(-5));
        assert_eq!(i8::from_flux(&FluxValue::I64(200)), None);
        assert_eq!(u32::from_flux(&FluxValue::I64(-1)), None);
        assert_eq!(u64::from_flux(&FluxValue::I64(7)), Some(7));
        assert_eq!(
            u64::from_flux(&FluxValue::EntityId(u64::MAX)),
            Some(u64::MAX)
        );
        assert_eq!(i64::from_flux(&FluxValue::Str("9".into())), None);
    }

    #[test]
    fn floats_strings_bytes_and_bools_decode_strictly() {
        assert_eq!(f64::from_flux(&FluxValue::F64(1.5)), Some(1.5));
        assert_eq!(f32::from_flux(&FluxValue::F64(0.25)), Some(0.25));
        assert_eq!(f64::from_flux(&FluxValue::I64(1)), None, "no int→float");
        assert_eq!(bool::from_flux(&FluxValue::Bool(true)), Some(true));
        assert_eq!(bool::from_flux(&FluxValue::I64(1)), None);
        assert_eq!(
            String::from_flux(&FluxValue::Str("hi".into())),
            Some("hi".into())
        );
        assert_eq!(
            Vec::<u8>::from_flux(&FluxValue::Bytes(vec![1, 2])),
            Some(vec![1, 2])
        );
        assert_eq!(Vec::<u8>::from_flux(&FluxValue::Str("no".into())), None);
    }

    #[test]
    fn domain_newtypes_and_options_decode() {
        let id = Identity::from_bytes([9u8; 32]);
        assert_eq!(
            Identity::from_flux(&FluxValue::Identity([9u8; 32])),
            Some(id)
        );
        assert_eq!(
            EntityId::from_flux(&FluxValue::EntityId(4)),
            Some(EntityId::new(4))
        );
        assert_eq!(
            Timestamp::from_flux(&FluxValue::Timestamp(17)),
            Some(Timestamp::from_micros(17))
        );
        assert_eq!(
            Option::<u32>::from_flux(&FluxValue::Null),
            Some(None),
            "Null inhabits Option"
        );
        assert_eq!(Option::<u32>::from_flux(&FluxValue::I64(3)), Some(Some(3)));
        assert_eq!(Option::<u32>::from_flux(&FluxValue::Bool(true)), None);
    }

    #[test]
    fn arity_and_decode_errors_are_wire_ready_400s() {
        let args = [FluxValue::I64(1), FluxValue::Str("x".into())];
        check_arity("send", &args, 2).unwrap();
        let err = check_arity("send", &args, 3).unwrap_err();
        assert_eq!(err.query_code(), Some(codes::MALFORMED));

        let n: u32 = decode_arg("send", &args, 0, "channel").unwrap();
        assert_eq!(n, 1);
        let err = decode_arg::<u32>("send", &args, 1, "channel").unwrap_err();
        assert_eq!(err.query_code(), Some(codes::MALFORMED));
        assert!(err.to_string().contains("`channel`"), "{err}");
        let err = decode_arg::<u32>("send", &args, 9, "missing").unwrap_err();
        assert!(err.to_string().contains("missing argument"), "{err}");
    }
}
