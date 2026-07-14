//! Fluxum pure wire layer (SPEC-006, no storage deps) — shared by the server
//! and every SDK. T1.2 deliverable; the format freezes at gate G5.
//!
//! Two-layer encoding model:
//!
//! | Layer | Encoding | Module |
//! |---|---|---|
//! | Frame | `u32 LE length + body` (HiveLLM wire standard) | [`frame`] |
//! | Message envelope | MessagePack (`rmp-serde`), `[tag, payload]` variants | [`messages`], [`value`] |
//! | Row data in `TableUpdate` | FluxBIN (schema-driven, hand-rolled) | [`fluxbin`], [`rowlist`] |
//!
//! A complete frame therefore is: `u32 LE length` + MessagePack
//! [`messages::ClientMessage`] / [`messages::ServerMessage`] envelope, with
//! any row batches inside carried as flat FluxBIN buffers
//! ([`rowlist::RowList`]).

pub mod codes;
pub mod fluxbin;
pub mod frame;
pub mod messages;
pub mod rowlist;
mod tagged;
pub mod value;

pub use fluxbin::{FluxBinError, FluxBinReader, FluxBinWriter};
pub use frame::{DEFAULT_MAX_FRAME_BYTES, FRAME_HEADER_LEN, Frame, FrameCodec, FrameError};
pub use messages::{
    AuthResult, Authenticate, ClientMessage, ErrorMessage, InitialData, OneOffQuery, ReducerCall,
    ReducerResult, ServerMessage, Subscribe, SubscribeSingle, TableUpdate, TxUpdate, TxUpdateLight,
    Unsubscribe,
};
pub use rowlist::{RowList, RowListBuilder, RowListError, RowSizeHint};
pub use value::FluxValue;
