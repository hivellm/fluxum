//! The Fluxum wire layer, vendored so this SDK publishes on its own.
//!
//! # Why there are two copies
//!
//! The server reads this protocol from `crates/fluxum-protocol`, an internal
//! workspace crate that is `publish = false`. A published crate cannot depend
//! on an unpublished one, so a shared crate would have to go to crates.io
//! purely to satisfy the SDK — releasing an internal server crate as a public
//! artifact, and coupling every SDK release to it. `fluxum-sdk` is the only
//! crate this project publishes (SPEC-011 SDK-071), so the SDK carries its own
//! copy instead and depends on nothing internal.
//!
//! # Why the copies cannot drift
//!
//! Every `.rs` file beside this one is a **verbatim** copy of the matching
//! file in `crates/fluxum-protocol/src/`. Nothing here is edited by hand —
//! edit the server-side file and re-sync.
//!
//! `tests/protocol_sync.rs` fails the gate when a byte differs, and also
//! checks that this directory holds exactly the vendored set — a module added
//! or deleted without updating the list would otherwise slip past a
//! file-by-file comparison. That test is
//! the whole safety argument: duplicating a wire format between a server and
//! its own client is normally how the two silently diverge, and an encoding
//! disagreement does not fail one message — it desynchronizes the connection.
//! A build that cannot compile with the copies out of step turns that class of
//! bug into a red test.
//!
//! To re-sync after changing the server-side protocol:
//!
//! ```text
//! SYNC_PROTOCOL=1 cargo test -p fluxum-sdk --test protocol_sync
//! ```
//!
//! # What is not here
//!
//! `plugin_rpc` stays server-only: it is the sidecar transport
//! (SPEC-016), not something a client speaks.

pub mod codes;
pub mod fluxbin;
pub mod frame;
pub mod messages;
pub mod rowlist;
// `pub(crate)`, not private as in the source crate: the re-export in `lib.rs`
// that makes `crate::tagged` resolve for the vendored files has to be able to
// see it.
pub(crate) mod tagged;
pub mod value;

pub use fluxbin::{FluxBinError, FluxBinReader, FluxBinWriter};
pub use frame::{DEFAULT_MAX_FRAME_BYTES, FRAME_HEADER_LEN, Frame, FrameCodec, FrameError};
pub use messages::{
    AuthResult, Authenticate, ClientMessage, ErrorMessage, InitialData, OneOffQuery, ReducerCall,
    ReducerError, ReducerResult, Resume, ServerMessage, Subscribe, SubscribeSingle, TableUpdate,
    TxUpdate, TxUpdateLight, Unsubscribe,
};
pub use rowlist::{RowList, RowListBuilder, RowListError, RowSizeHint};
pub use value::FluxValue;
