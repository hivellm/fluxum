//! HTTP-compatible wire error codes carried by `Error.code` (SPEC-006
//! RPC-034). Others (404, 500, …) MAY be used where their HTTP meaning
//! applies.

/// 400 — malformed frame or message body (RPC-001).
pub const MALFORMED: u16 = 400;

/// 401 — `unauthenticated`: message before a successful `Authenticate`
/// (RPC-020).
pub const UNAUTHENTICATED: u16 = 401;

/// 404 — unknown resource, e.g. an expired Streamable-HTTP session (RPC-007).
pub const NOT_FOUND: u16 = 404;

/// 408 — `idle timeout`: sent before closing an idle connection (RPC-060).
pub const IDLE_TIMEOUT: u16 = 408;

/// 413 — `frame too large`: frame exceeds `max_frame_bytes` (RPC-061).
pub const FRAME_TOO_LARGE: u16 = 413;

/// 429 — rate limit exceeded / inbound queue overflow (RPC-021, RPC-064).
pub const RATE_LIMITED: u16 = 429;

/// 500 — internal server error.
pub const INTERNAL: u16 = 500;

/// 503 — shard unavailable, e.g. during entity handoff (SPEC-007).
pub const SHARD_UNAVAILABLE: u16 = 503;
