//! Plugin RPC (SPEC-020 PLG-031): the wire Fluxum speaks to an
//! out-of-process **sidecar** plugin.
//!
//! This is a *separate* protocol from the client wire in [`messages`], and
//! deliberately so: a sidecar is an operator-deployed peer on the same host
//! or network, not an untrusted client, and the two evolve on different
//! clocks. It reuses only the framing family — `u32 LE` length + MessagePack
//! ([`Frame`]/[`FrameCodec`]) — so a sidecar author can lean on the same
//! codec without inheriting the client message set.
//!
//! # Scope
//!
//! ReadPath capabilities only ([`Capability::ScoreReranker`],
//! `Retriever`, `Fusion`): the calls whose failure can degrade to a base
//! result instead of an error (PLG-031). The OffPath `StreamSink` wire
//! belongs to the CDC task that builds the sink, and the WritePath
//! `KeyProvider`-to-KMS exception (PLG-021) needs key caching rather than a
//! per-call round trip — neither is modelled here.
//!
//! [`messages`]: crate::messages
//! [`Frame`]: crate::frame::Frame
//! [`FrameCodec`]: crate::frame::FrameCodec
//! [`Capability::ScoreReranker`]: https://docs.rs/fluxum-core
//!
//! # Wire compatibility
//!
//! Every payload here is encoded by `rmp_serde::to_vec` — **compact**
//! MessagePack, which writes a struct as an *array* in declaration order,
//! with no field names on the wire. Two consequences bind every future edit:
//!
//! 1. A new field is only backward-compatible at the **tail**, marked
//!    `#[serde(default)]`, so a frame written before it existed still
//!    decodes.
//! 2. Inserting or reordering a field mid-struct shifts every field after
//!    it and silently makes old frames undecodable — or worse, decodable as
//!    the wrong values.
//!
//! `plugin_rpc_additive.rs` pins this against regression.

use serde::{Deserialize, Serialize};

use crate::tagged::tagged_enum;

/// The Plugin RPC version this build speaks ([`Hello::version`]). A sidecar
/// that does not recognize it must answer [`PluginRpcError`] rather than
/// guess: a version mismatch is a deployment bug, and failing it loudly at
/// handshake beats mis-decoding scores at query time.
pub const PLUGIN_RPC_VERSION: u32 = 1;

/// One scored candidate row: an encoded primary key and its relevance
/// score. Mirrors `fluxum_core::plugin::Scored` — the core type is not used
/// directly because `fluxum-protocol` must not depend on `fluxum-core`.
///
/// `pk` is opaque here: the sidecar echoes back the exact bytes it was
/// given. That keeps the sidecar out of the business of decoding FluxBIN row
/// keys, and means a re-ranker cannot invent a row that was not a candidate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    /// The row's encoded primary key, opaque to the sidecar.
    #[serde(with = "serde_bytes")]
    pub pk: Vec<u8>,
    /// The candidate's score; higher is more relevant.
    pub score: f64,
}

/// The `MATCH` query a ReadPath call is scoped to. Mirrors
/// `fluxum_core::plugin::FtQuery`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchQuery {
    /// The queried table's struct name.
    pub table: String,
    /// The `#[fulltext]` column queried.
    pub column: String,
    /// The raw match query text.
    pub query: String,
    /// The requested result limit.
    pub limit: u64,
}

/// Opening handshake (PLG-031): identifies the caller and proves it may
/// call this sidecar (PLG-061 — a sidecar is authenticated like any server
/// peer; it is granted no identity beyond what the manifest says).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The [`PLUGIN_RPC_VERSION`] the host speaks.
    pub version: u32,
    /// The manifest name of the plugin being called.
    pub plugin: String,
    /// The capability the host expects this sidecar to implement, as its
    /// `Capability::name()` string (`score_reranker`, `retriever`,
    /// `fusion`). Sent so a sidecar can refuse a binding it does not
    /// implement at *handshake* rather than mis-serving calls.
    pub capability: String,
    /// The shared secret the sidecar authenticates the host with, when the
    /// manifest configures one. `None` for an unauthenticated sidecar (a
    /// same-pod loopback deployment); never logged or echoed.
    pub token: Option<String>,
}

/// `ScoreReranker::rerank` over the wire: re-order the base candidates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RerankRequest {
    /// Correlates the response; unique per connection.
    pub call_id: u64,
    /// The query being served.
    pub query: MatchQuery,
    /// The base (BM25) candidates, in base order.
    pub candidates: Vec<Candidate>,
}

/// `Retriever::retrieve` over the wire: contribute external candidates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrieveRequest {
    /// Correlates the response; unique per connection.
    pub call_id: u64,
    /// The query being served.
    pub query: MatchQuery,
    /// How many candidates the host wants back.
    pub k: u64,
}

/// `Fusion::fuse` over the wire: merge two ranked lists.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FuseRequest {
    /// Correlates the response; unique per connection.
    pub call_id: u64,
    /// The lexical (BM25) list, in rank order.
    pub lexical: Vec<Candidate>,
    /// The dense/retriever list, in rank order.
    pub dense: Vec<Candidate>,
}

/// The sidecar accepted the handshake.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ready {
    /// The [`PLUGIN_RPC_VERSION`] the sidecar speaks. The host refuses a
    /// mismatch rather than negotiating down: one version is in flight per
    /// deployment, and a silent downgrade is how scores go subtly wrong.
    pub version: u32,
    /// The sidecar's self-reported name, for the `GET /plugins` row and
    /// logs. Advisory — the host trusts its own manifest, not this.
    pub name: String,
}

/// A capability call's result: the scored list.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candidates {
    /// Echoes the request's `call_id`.
    pub call_id: u64,
    /// The resulting candidates, in the sidecar's chosen order.
    pub candidates: Vec<Candidate>,
}

/// The sidecar could not serve the call.
///
/// A ReadPath error is **not** a client error: the host logs it, counts it
/// against the breaker, and serves the base result (PLG-031). It exists so
/// the host can distinguish "the sidecar said no" from "the sidecar hung",
/// which are different failure signals even though both degrade the same.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginRpcError {
    /// Echoes the request's `call_id`; `0` for a handshake failure, which
    /// has no call to correlate to.
    pub call_id: u64,
    /// A human-readable reason, for the host's log. Never shown to a
    /// database client.
    pub message: String,
}

tagged_enum! {
    /// Host → sidecar (PLG-031), as the `[tag, payload]` envelope.
    pub enum PluginRequest {
        /// Opening handshake; always the first message on a connection.
        "Hello" => Hello(Hello),
        /// ReadPath: re-score/re-order the base candidates.
        "Rerank" => Rerank(RerankRequest),
        /// ReadPath: contribute external candidates.
        "Retrieve" => Retrieve(RetrieveRequest),
        /// ReadPath: fuse two ranked lists.
        "Fuse" => Fuse(FuseRequest),
    }
}

tagged_enum! {
    /// Sidecar → host (PLG-031), as the `[tag, payload]` envelope.
    pub enum PluginResponse {
        /// Handshake accepted.
        "Ready" => Ready(Ready),
        /// A capability call's scored result.
        "Candidates" => Candidates(Candidates),
        /// The call failed; the host degrades to the base result.
        "Error" => Error(PluginRpcError),
    }
}

impl PluginResponse {
    /// The `call_id` this response answers, or `None` for a handshake reply
    /// (which correlates to the connection, not a call).
    pub fn call_id(&self) -> Option<u64> {
        match self {
            Self::Ready(_) => None,
            Self::Candidates(c) => Some(c.call_id),
            Self::Error(e) => Some(e.call_id),
        }
    }
}

impl PluginRequest {
    /// The `call_id` of a capability call, or `None` for the handshake.
    pub fn call_id(&self) -> Option<u64> {
        match self {
            Self::Hello(_) => None,
            Self::Rerank(r) => Some(r.call_id),
            Self::Retrieve(r) => Some(r.call_id),
            Self::Fuse(r) => Some(r.call_id),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn candidate(pk: u8, score: f64) -> Candidate {
        Candidate {
            pk: vec![pk],
            score,
        }
    }

    fn query() -> MatchQuery {
        MatchQuery {
            table: "Doc".into(),
            column: "body".into(),
            query: "rust database".into(),
            limit: 10,
        }
    }

    #[test]
    fn every_request_round_trips_through_its_envelope() {
        let requests = vec![
            PluginRequest::Hello(Hello {
                version: PLUGIN_RPC_VERSION,
                plugin: "reranker".into(),
                capability: "score_reranker".into(),
                token: Some("s3cret".into()),
            }),
            PluginRequest::Rerank(RerankRequest {
                call_id: 1,
                query: query(),
                candidates: vec![candidate(1, 2.5), candidate(2, 1.0)],
            }),
            PluginRequest::Retrieve(RetrieveRequest {
                call_id: 2,
                query: query(),
                k: 50,
            }),
            PluginRequest::Fuse(FuseRequest {
                call_id: 3,
                lexical: vec![candidate(1, 2.5)],
                dense: vec![candidate(9, 0.9)],
            }),
        ];
        for request in requests {
            let bytes = rmp_serde::to_vec(&request).unwrap();
            let back: PluginRequest = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(back, request);
        }
    }

    #[test]
    fn every_response_round_trips_through_its_envelope() {
        let responses = vec![
            PluginResponse::Ready(Ready {
                version: PLUGIN_RPC_VERSION,
                name: "reranker".into(),
            }),
            PluginResponse::Candidates(Candidates {
                call_id: 7,
                candidates: vec![candidate(2, 9.5), candidate(1, 0.5)],
            }),
            PluginResponse::Error(PluginRpcError {
                call_id: 7,
                message: "model not loaded".into(),
            }),
        ];
        for response in responses {
            let bytes = rmp_serde::to_vec(&response).unwrap();
            let back: PluginResponse = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(back, response);
        }
    }

    #[test]
    fn call_ids_correlate_requests_to_responses() {
        let request = PluginRequest::Rerank(RerankRequest {
            call_id: 42,
            query: query(),
            candidates: vec![],
        });
        let response = PluginResponse::Candidates(Candidates {
            call_id: 42,
            candidates: vec![],
        });
        assert_eq!(request.call_id(), response.call_id());
        // The handshake pair correlates to the connection, not a call.
        assert_eq!(
            PluginResponse::Ready(Ready {
                version: PLUGIN_RPC_VERSION,
                name: "x".into(),
            })
            .call_id(),
            None
        );
    }

    #[test]
    fn a_candidate_pk_is_opaque_bytes_not_a_string() {
        // FluxBIN keys are arbitrary bytes; encoding them as a msgpack `str`
        // would corrupt any key that is not valid UTF-8. `serde_bytes` pins
        // the `bin` family.
        let candidate = Candidate {
            pk: vec![0xff, 0x00, 0xfe],
            score: 1.0,
        };
        let bytes = rmp_serde::to_vec(&candidate).unwrap();
        let back: Candidate = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back.pk, vec![0xff, 0x00, 0xfe]);
    }

    #[test]
    fn payloads_are_positional_arrays_so_only_tail_fields_are_additive() {
        // This is the whole reason for the module's tail rule, pinned rather
        // than asserted in a doc comment: compact MessagePack writes a struct
        // as an ARRAY in declaration order, with no field names. A `Candidate`
        // is `[pk, score]` — 0x92 is fixarray(2).
        let bytes = rmp_serde::to_vec(&candidate(7, 1.5)).unwrap();
        assert_eq!(bytes[0], 0x92, "Candidate must encode as a 2-array");

        // So a field appended at the TAIL with `#[serde(default)]` still
        // decodes a frame written before it existed...
        #[derive(Deserialize)]
        struct FutureCandidate {
            #[serde(with = "serde_bytes")]
            pk: Vec<u8>,
            #[allow(dead_code)]
            score: f64,
            #[serde(default)]
            explain: Option<String>,
        }
        let grown: FutureCandidate = rmp_serde::from_slice(&bytes)
            .expect("a tail field with a default decodes an older frame");
        assert_eq!(grown.pk, vec![7]);
        assert_eq!(grown.explain, None);

        // ...while a field inserted in the MIDDLE shifts every field after it
        // and mis-decodes the same bytes. This is a decode of `[pk, score]`
        // into `[pk, explain, score]`: `score` lands in `explain`.
        #[derive(Deserialize)]
        struct BrokenCandidate {
            #[serde(with = "serde_bytes")]
            #[allow(dead_code)]
            pk: Vec<u8>,
            #[serde(default)]
            #[allow(dead_code)]
            explain: Option<String>,
            #[allow(dead_code)]
            score: f64,
        }
        assert!(
            rmp_serde::from_slice::<BrokenCandidate>(&bytes).is_err(),
            "a mid-struct insert must break loudly here, not silently in production"
        );
    }

    #[test]
    fn an_unknown_tag_is_refused_rather_than_guessed() {
        // A sidecar speaking a newer protocol must not have its message
        // silently coerced into a known variant.
        let frame = rmp_serde::to_vec(&("Teleport", (1u64,))).unwrap();
        assert!(rmp_serde::from_slice::<PluginResponse>(&frame).is_err());
    }
}
