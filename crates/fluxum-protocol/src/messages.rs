//! FluxRPC message types (SPEC-006 §4 client → server, §5 server → client).
//!
//! The envelope layer is MessagePack (`rmp-serde`): [`ClientMessage`] and
//! [`ServerMessage`] encode as the RPC-011 tagged pattern —
//! `fixarray[2]` of `[tag: str, payload]` — with each payload struct encoded
//! positionally (fields in declaration order as a MessagePack array).
//! Byte-carrying fields (`token`, `identity`, `caller`, `rows_data`) use the
//! MessagePack `bin` format. Row data inside [`TableUpdate`] uses FluxBIN
//! ([`crate::fluxbin`]), not MessagePack.
//!
//! Every request carries an `id: u32` chosen by the sender; responses echo it
//! (RPC-002 multiplexing). `TxUpdate` / `TxUpdateLight` are server-initiated
//! and carry no `id`.

use serde::{Deserialize, Serialize};

use crate::rowlist::RowList;
use crate::tagged::tagged_enum;

/// Serde adapter: `[u8; 32]` as a MessagePack `bin 32`.
mod bin32 {
    use std::fmt;

    use serde::de::{Deserializer, Visitor};
    use serde::ser::Serializer;

    pub fn serialize<S: Serializer>(bytes: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        struct Bin32Visitor;

        impl Visitor<'_> for Bin32Visitor {
            type Value = [u8; 32];

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("exactly 32 raw bytes")
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                v.try_into().map_err(|_| E::invalid_length(v.len(), &self))
            }
        }

        deserializer.deserialize_bytes(Bin32Visitor)
    }
}

/// Serde adapter: `Result<(), String>` as `["Ok", nil]` / `["Err", message]`
/// (RPC-031).
mod outcome {
    use std::fmt;

    use serde::de::{Deserializer, SeqAccess, Visitor};
    use serde::ser::{SerializeTuple, Serializer};

    pub fn serialize<S: Serializer>(
        outcome: &Result<(), String>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(2)?;
        match outcome {
            Ok(()) => {
                tuple.serialize_element("Ok")?;
                tuple.serialize_element(&())?;
            }
            Err(message) => {
                tuple.serialize_element("Err")?;
                tuple.serialize_element(message)?;
            }
        }
        tuple.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Result<(), String>, D::Error> {
        struct OutcomeVisitor;

        impl<'de> Visitor<'de> for OutcomeVisitor {
            type Value = Result<(), String>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("[\"Ok\", nil] or [\"Err\", message]")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let tag: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                match tag.as_str() {
                    "Ok" => {
                        seq.next_element::<()>()?
                            .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                        Ok(Ok(()))
                    }
                    "Err" => Ok(Err(seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?)),
                    other => Err(serde::de::Error::unknown_variant(other, &["Ok", "Err"])),
                }
            }
        }

        deserializer.deserialize_tuple(2, OutcomeVisitor)
    }
}

// ---------------------------------------------------------------------------
// Client → Server (§4)
// ---------------------------------------------------------------------------

/// RPC-020 — first message on every connection; sets per-connection options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authenticate {
    /// Request id (echoed by `AuthResult`).
    pub id: u32,
    /// Opaque auth token (JWT, API key, or custom token).
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
    /// `"none"` | `"gzip"` | `"brotli"` (RPC-008); `None` means `"none"`.
    pub compression: Option<String>,
    /// `"full"` | `"light"` (RPC-035); `None` means `"full"`.
    pub tx_updates: Option<String>,
}

/// RPC-021 — execute a named reducer atomically.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReducerCall {
    /// Request id (echoed by `ReducerResult`).
    pub id: u32,
    /// Reducer function name, e.g. `"send_chat"`.
    pub reducer: String,
    /// Reducer version (`None` for latest).
    pub version: Option<u32>,
    /// Positional arguments (after `ReducerContext`).
    pub args: Vec<crate::value::FluxValue>,
}

/// RPC-022 — register a batch of subscription queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscribe {
    /// Request id (echoed by `InitialData`).
    pub id: u32,
    /// One or more SQL query strings (SPEC-005 SQL subset).
    pub queries: Vec<String>,
}

/// RPC-023 — register a single subscription query without re-sending the
/// batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeSingle {
    /// Request id (echoed by `InitialData`).
    pub id: u32,
    /// Exactly one SQL query string.
    pub query: String,
}

/// RPC-024 — drop previously registered subscription queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unsubscribe {
    /// Request id.
    pub id: u32,
    /// Server-assigned query IDs from `InitialData.tables[n].query_id`.
    pub query_ids: Vec<u32>,
}

/// RPC-025 — one-shot read-only query; no subscription is registered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneOffQuery {
    /// Request id (echoed by `InitialData`).
    pub id: u32,
    /// Read-only SQL query.
    pub sql: String,
}

// ---------------------------------------------------------------------------
// Server → Client (§5)
// ---------------------------------------------------------------------------

/// RPC-030 — response to `Authenticate`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthResult {
    /// Echoes `Authenticate.id`.
    pub id: u32,
    /// Derived 256-bit identity (SPEC-009).
    #[serde(with = "bin32")]
    pub identity: [u8; 32],
    /// Refreshed/rotated token (MAY be the same as the input).
    #[serde(with = "serde_bytes")]
    pub token: Vec<u8>,
}

/// RPC-031 — response to `ReducerCall`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReducerResult {
    /// Echoes `ReducerCall.id`.
    pub id: u32,
    /// Encoded as `["Ok", nil]` or `["Err", "message"]`.
    #[serde(with = "outcome")]
    pub outcome: Result<(), String>,
}

/// RPC-032 — snapshot response to `Subscribe` / `SubscribeSingle` /
/// `OneOffQuery`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitialData {
    /// Echoes the request id.
    pub id: u32,
    /// Server's current schema version (RPC-043).
    pub schema_version: u32,
    /// One entry per query.
    pub tables: Vec<TableUpdate>,
}

/// RPC-032 — per-table row diff carried by `InitialData` and `TxUpdate`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableUpdate {
    /// Stable table id.
    pub table_id: u32,
    /// Table name.
    pub table_name: String,
    /// Server-assigned ID for this subscription query (used by
    /// `Unsubscribe`).
    pub query_id: u32,
    /// FluxBIN-encoded inserted rows, flat (RPC-041).
    pub inserts: RowList,
    /// Deleted rows: `rows_data` holds FluxBIN primary-key field(s) only
    /// (RPC-042).
    pub deletes: RowList,
}

/// RPC-033 — server-initiated commit broadcast with full metadata (no `id`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxUpdate {
    /// Monotonically increasing per shard; clients use it to detect missed
    /// updates (RPC-062).
    pub tx_id: u64,
    /// Reducer commit time, µs since the Unix epoch.
    pub timestamp: i64,
    /// Name of the reducer that caused this commit; `""` for
    /// system-initiated commits.
    pub reducer_name: String,
    /// Identity of the calling client (32 zero bytes for system commits).
    #[serde(with = "bin32")]
    pub caller: [u8; 32],
    /// Reducer execution time in microseconds.
    pub duration_us: u32,
    /// Row diffs, one entry per affected subscribed table.
    pub tables: Vec<TableUpdate>,
}

/// RPC-035 — metadata-stripped commit broadcast for connections that opted
/// into `tx_updates: light`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxUpdateLight {
    /// As [`TxUpdate::tx_id`].
    pub tx_id: u64,
    /// As [`TxUpdate::timestamp`].
    pub timestamp: i64,
    /// As [`TxUpdate::tables`].
    pub tables: Vec<TableUpdate>,
}

/// RPC-034 — error response (wire tag `"Error"`); codes in [`crate::codes`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorMessage {
    /// Echoes the request id if applicable; `None` for server-initiated
    /// errors.
    pub id: Option<u32>,
    /// HTTP-compatible status code (RPC-034 table).
    pub code: u16,
    /// Human-readable description.
    pub message: String,
}

// ---------------------------------------------------------------------------
// Envelopes
// ---------------------------------------------------------------------------

tagged_enum! {
    /// Every client → server message (§4), as the `[tag, payload]` envelope.
    pub enum ClientMessage {
        /// RPC-020.
        "Authenticate" => Authenticate(Authenticate),
        /// RPC-021.
        "ReducerCall" => ReducerCall(ReducerCall),
        /// RPC-022.
        "Subscribe" => Subscribe(Subscribe),
        /// RPC-023.
        "SubscribeSingle" => SubscribeSingle(SubscribeSingle),
        /// RPC-024.
        "Unsubscribe" => Unsubscribe(Unsubscribe),
        /// RPC-025.
        "OneOffQuery" => OneOffQuery(OneOffQuery),
    }
}

tagged_enum! {
    /// Every server → client message (§5), as the `[tag, payload]` envelope.
    pub enum ServerMessage {
        /// RPC-030.
        "AuthResult" => AuthResult(AuthResult),
        /// RPC-031.
        "ReducerResult" => ReducerResult(ReducerResult),
        /// RPC-032.
        "InitialData" => InitialData(InitialData),
        /// RPC-033.
        "TxUpdate" => TxUpdate(TxUpdate),
        /// RPC-035.
        "TxUpdateLight" => TxUpdateLight(TxUpdateLight),
        /// RPC-034.
        "Error" => Error(ErrorMessage),
    }
}
