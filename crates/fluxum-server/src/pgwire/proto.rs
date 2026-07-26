//! The PostgreSQL v3 frontend/backend message codec (SPEC-027) — hand-rolled,
//! independent of the FluxRPC `FrameCodec` (which is `u32 LE + MessagePack`).
//!
//! Postgres framing is its own shape: a **startup** packet with no type tag
//! (`i32 length | i32 protocol | key\0value\0… \0`), then **tagged** messages
//! (`u8 tag | i32 length-inclusive-of-itself | body`). The same tag byte means
//! different things by direction (`D` is Describe from the client but DataRow
//! to it), so this module is split: [`Frontend`] is only ever *read*, the
//! `backend` builders are only ever *written*.
//!
//! Every message body is length-prefixed, so reads are `read_exact(header)`
//! then `read_exact(body)` — no incremental buffering, and the body is parsed
//! synchronously by [`Cursor`], which keeps the wire parsing unit-testable
//! without a socket.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt};

// --- protocol constants -----------------------------------------------------------

/// The v3.0 protocol number carried in the startup packet.
pub const PROTOCOL_V3: i32 = 196_608;
/// `SSLRequest` magic (RFC: 1234 << 16 | 5679).
pub const SSL_REQUEST: i32 = 80_877_103;
/// `GSSENCRequest` magic.
pub const GSS_REQUEST: i32 = 80_877_104;
/// `CancelRequest` magic.
pub const CANCEL_REQUEST: i32 = 80_877_102;

// Type OIDs (pg_type) we surface — the closed set FluxType maps onto.
/// `bool`.
pub const OID_BOOL: i32 = 16;
/// `bytea`.
pub const OID_BYTEA: i32 = 17;
/// `int8` (bigint).
pub const OID_INT8: i32 = 20;
/// `int2` (smallint).
pub const OID_INT2: i32 = 21;
/// `int4` (integer).
pub const OID_INT4: i32 = 23;
/// `text`.
pub const OID_TEXT: i32 = 25;
/// `float4` (real).
pub const OID_FLOAT4: i32 = 700;
/// `float8` (double precision).
pub const OID_FLOAT8: i32 = 701;
/// `numeric`.
pub const OID_NUMERIC: i32 = 1700;

// --- frontend (client → server) ---------------------------------------------------

/// A message read from the client. Only the subset this read-only endpoint
/// acts on is modelled; anything else is surfaced as [`Frontend::Unknown`] so
/// the driver can answer a clean error instead of desynchronizing.
#[derive(Debug, Clone, PartialEq)]
pub enum Frontend {
    /// `SSLRequest` — answered with a single `N` (no TLS) then re-read.
    SslRequest,
    /// `GSSENCRequest` — answered with a single `N`.
    GssRequest,
    /// `CancelRequest` — no query to cancel here; the driver drops it.
    CancelRequest {
        /// The backend process id the client wants to cancel.
        pid: i32,
        /// That backend's secret key.
        secret: i32,
    },
    /// The real startup packet (protocol 3.0) with its parameters.
    Startup {
        /// The `key`/`value` parameters (`user`, `database`, …).
        params: Vec<(String, String)>,
    },
    /// `p` — the password (cleartext here); the endpoint reads it as a token.
    Password(Vec<u8>),
    /// `Q` — a simple query string.
    Query(String),
    /// `P` — parse a (possibly named) statement.
    Parse {
        /// Destination statement name (empty = the unnamed statement).
        name: String,
        /// The SQL text.
        sql: String,
        /// Client-specified parameter type OIDs (0 = unspecified).
        param_types: Vec<i32>,
    },
    /// `B` — bind a portal to a parsed statement.
    Bind {
        /// Destination portal name (empty = the unnamed portal).
        portal: String,
        /// Source statement name.
        statement: String,
        /// Number of bound parameters (this endpoint supports only zero).
        param_count: i16,
    },
    /// `D` — describe a statement (`S`) or portal (`P`).
    Describe {
        /// `S` for a statement, `P` for a portal.
        kind: u8,
        /// The name being described.
        name: String,
    },
    /// `E` — execute a portal.
    Execute {
        /// The portal name (empty = unnamed).
        portal: String,
        /// Row cap (0 = unlimited); this endpoint always returns all rows.
        max_rows: i32,
    },
    /// `S` — sync: end the extended-query batch, expect `ReadyForQuery`.
    Sync,
    /// `H` — flush.
    Flush,
    /// `C` — close a statement/portal.
    Close {
        /// `S` or `P`.
        kind: u8,
        /// The name being closed.
        name: String,
    },
    /// `X` — terminate the connection.
    Terminate,
    /// A tagged message this endpoint does not act on.
    Unknown(u8),
}

/// Read the startup/SSL/cancel packet (no type tag): `i32 len | i32 code | …`.
///
/// # Errors
/// EOF or a malformed length/oversized packet.
pub async fn read_startup<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Frontend> {
    let len = r.read_i32().await?;
    let body_len = frame_body_len(len)?;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body).await?;
    let mut cur = Cursor::new(&body);
    let code = cur.get_i32()?;
    match code {
        SSL_REQUEST => Ok(Frontend::SslRequest),
        GSS_REQUEST => Ok(Frontend::GssRequest),
        CANCEL_REQUEST => Ok(Frontend::CancelRequest {
            pid: cur.get_i32()?,
            secret: cur.get_i32()?,
        }),
        PROTOCOL_V3 => {
            let mut params = Vec::new();
            loop {
                let key = cur.get_cstr()?;
                if key.is_empty() {
                    break;
                }
                let value = cur.get_cstr()?;
                params.push((key, value));
            }
            Ok(Frontend::Startup { params })
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported startup protocol code {other}"),
        )),
    }
}

/// Read one tagged frontend message.
///
/// # Errors
/// EOF or a malformed length.
pub async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Frontend> {
    let tag = r.read_u8().await?;
    let len = r.read_i32().await?;
    let body_len = frame_body_len(len)?;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body).await?;
    let mut cur = Cursor::new(&body);
    Ok(match tag {
        b'p' => Frontend::Password(body.clone()),
        b'Q' => Frontend::Query(cur.get_cstr()?),
        b'P' => {
            let name = cur.get_cstr()?;
            let sql = cur.get_cstr()?;
            let n = cur.get_i16()?;
            let mut param_types = Vec::with_capacity(n.max(0) as usize);
            for _ in 0..n.max(0) {
                param_types.push(cur.get_i32()?);
            }
            Frontend::Parse {
                name,
                sql,
                param_types,
            }
        }
        b'B' => {
            let portal = cur.get_cstr()?;
            let statement = cur.get_cstr()?;
            // format codes
            let fc = cur.get_i16()?;
            for _ in 0..fc.max(0) {
                cur.get_i16()?;
            }
            let param_count = cur.get_i16()?;
            Frontend::Bind {
                portal,
                statement,
                param_count,
            }
        }
        b'D' => Frontend::Describe {
            kind: cur.get_u8()?,
            name: cur.get_cstr()?,
        },
        b'E' => Frontend::Execute {
            portal: cur.get_cstr()?,
            max_rows: cur.get_i32()?,
        },
        b'S' => Frontend::Sync,
        b'H' => Frontend::Flush,
        b'C' => Frontend::Close {
            kind: cur.get_u8()?,
            name: cur.get_cstr()?,
        },
        b'X' => Frontend::Terminate,
        other => Frontend::Unknown(other),
    })
}

/// The PG length field counts itself (4 bytes); the body is the remainder.
/// Cap at 64 MiB — a startup/query larger than that is a fault, not a query.
fn frame_body_len(len: i32) -> io::Result<usize> {
    if !(4..=64 * 1024 * 1024).contains(&len) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid pg message length {len}"),
        ));
    }
    Ok((len - 4) as usize)
}

/// A synchronous reader over a message body.
pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// Wrap a body buffer.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn need(&self, n: usize) -> io::Result<()> {
        if self.pos + n > self.buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short pg message body",
            ));
        }
        Ok(())
    }

    /// One byte.
    pub fn get_u8(&mut self) -> io::Result<u8> {
        self.need(1)?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// A big-endian `i16`.
    pub fn get_i16(&mut self) -> io::Result<i16> {
        self.need(2)?;
        let v = i16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    /// A big-endian `i32`.
    pub fn get_i32(&mut self) -> io::Result<i32> {
        self.need(4)?;
        let v = i32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    /// A `\0`-terminated UTF-8 string (lossy on invalid bytes).
    pub fn get_cstr(&mut self) -> io::Result<String> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unterminated pg string",
            ));
        }
        let s = String::from_utf8_lossy(&self.buf[start..self.pos]).into_owned();
        self.pos += 1; // skip the NUL
        Ok(s)
    }
}

// --- backend (server → client) ----------------------------------------------------

/// One `RowDescription` field: a column name and its resolved type OID.
#[derive(Debug, Clone)]
pub struct FieldDesc {
    /// Column name.
    pub name: String,
    /// pg_type OID.
    pub type_oid: i32,
    /// Type size in bytes, or `-1` for a variable-length type.
    pub type_size: i16,
}

/// A growable backend-message builder. Each `put_*` appends; `finish` back-fills
/// the length field. Messages accumulate into one `Vec` and are flushed as a
/// single write (a whole query reply — description, rows, completion, ready —
/// goes out in one syscall).
#[derive(Default)]
pub struct Out {
    buf: Vec<u8>,
}

impl Out {
    /// A fresh buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// The accumulated bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Whether anything has been written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Begin a tagged message; returns the index of its length placeholder.
    fn begin(&mut self, tag: u8) -> usize {
        self.buf.push(tag);
        let at = self.buf.len();
        self.buf.extend_from_slice(&[0; 4]);
        at
    }

    /// Back-fill the length (counts the 4 length bytes + body, excludes tag).
    fn end(&mut self, at: usize) {
        let len = (self.buf.len() - at) as i32;
        self.buf[at..at + 4].copy_from_slice(&len.to_be_bytes());
    }

    fn put_i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    fn put_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    fn put_cstr(&mut self, s: &str) {
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
    }

    /// `R` AuthenticationCleartextPassword (code 3).
    pub fn auth_cleartext(&mut self) -> &mut Self {
        let at = self.begin(b'R');
        self.put_i32(3);
        self.end(at);
        self
    }

    /// `R` AuthenticationOk (code 0).
    pub fn auth_ok(&mut self) -> &mut Self {
        let at = self.begin(b'R');
        self.put_i32(0);
        self.end(at);
        self
    }

    /// `S` ParameterStatus.
    pub fn param_status(&mut self, name: &str, value: &str) -> &mut Self {
        let at = self.begin(b'S');
        self.put_cstr(name);
        self.put_cstr(value);
        self.end(at);
        self
    }

    /// `K` BackendKeyData.
    pub fn backend_key(&mut self, pid: i32, secret: i32) -> &mut Self {
        let at = self.begin(b'K');
        self.put_i32(pid);
        self.put_i32(secret);
        self.end(at);
        self
    }

    /// `Z` ReadyForQuery (`I` idle, `T` in-txn, `E` failed).
    pub fn ready_for_query(&mut self, status: u8) -> &mut Self {
        let at = self.begin(b'Z');
        self.buf.push(status);
        self.end(at);
        self
    }

    /// `T` RowDescription. All fields are text format (code 0).
    pub fn row_description(&mut self, fields: &[FieldDesc]) -> &mut Self {
        let at = self.begin(b'T');
        self.put_i16(i16::try_from(fields.len()).unwrap_or(i16::MAX));
        for (i, f) in fields.iter().enumerate() {
            self.put_cstr(&f.name);
            self.put_i32(0); // table OID: unknown
            self.put_i16(i16::try_from(i + 1).unwrap_or(0)); // column attr number
            self.put_i32(f.type_oid);
            self.put_i16(f.type_size);
            self.put_i32(-1); // type modifier
            self.put_i16(0); // text format
        }
        self.end(at);
        self
    }

    /// `D` DataRow. `None` cells are SQL NULL (length `-1`); values are the
    /// text-format bytes.
    pub fn data_row(&mut self, cells: &[Option<Vec<u8>>]) -> &mut Self {
        let at = self.begin(b'D');
        self.put_i16(i16::try_from(cells.len()).unwrap_or(i16::MAX));
        for cell in cells {
            match cell {
                None => self.put_i32(-1),
                Some(bytes) => {
                    self.put_i32(i32::try_from(bytes.len()).unwrap_or(0));
                    self.buf.extend_from_slice(bytes);
                }
            }
        }
        self.end(at);
        self
    }

    /// `C` CommandComplete with a command tag (e.g. `SELECT 5`).
    pub fn command_complete(&mut self, tag: &str) -> &mut Self {
        let at = self.begin(b'C');
        self.put_cstr(tag);
        self.end(at);
        self
    }

    /// `I` EmptyQueryResponse.
    pub fn empty_query(&mut self) -> &mut Self {
        let at = self.begin(b'I');
        self.end(at);
        self
    }

    /// `E` ErrorResponse: severity, SQLSTATE, and a message (PG SQLSTATE in
    /// `code`, e.g. `25006` read-only, `28P01` bad password, `42601` syntax).
    pub fn error(&mut self, severity: &str, code: &str, message: &str) -> &mut Self {
        self.diagnostic(b'E', severity, code, message)
    }

    /// `N` NoticeResponse (same shape as an error, non-fatal).
    pub fn notice(&mut self, severity: &str, code: &str, message: &str) -> &mut Self {
        self.diagnostic(b'N', severity, code, message)
    }

    fn diagnostic(&mut self, tag: u8, severity: &str, code: &str, message: &str) -> &mut Self {
        let at = self.begin(tag);
        self.buf.push(b'S');
        self.put_cstr(severity);
        self.buf.push(b'V');
        self.put_cstr(severity);
        self.buf.push(b'C');
        self.put_cstr(code);
        self.buf.push(b'M');
        self.put_cstr(message);
        self.buf.push(0); // field terminator
        self.end(at);
        self
    }

    /// `1` ParseComplete.
    pub fn parse_complete(&mut self) -> &mut Self {
        let at = self.begin(b'1');
        self.end(at);
        self
    }

    /// `2` BindComplete.
    pub fn bind_complete(&mut self) -> &mut Self {
        let at = self.begin(b'2');
        self.end(at);
        self
    }

    /// `3` CloseComplete.
    pub fn close_complete(&mut self) -> &mut Self {
        let at = self.begin(b'3');
        self.end(at);
        self
    }

    /// `n` NoData (a described statement/portal with no result columns).
    pub fn no_data(&mut self) -> &mut Self {
        let at = self.begin(b'n');
        self.end(at);
        self
    }

    /// `t` ParameterDescription — always empty (no bind parameters supported).
    pub fn parameter_description(&mut self) -> &mut Self {
        let at = self.begin(b't');
        self.put_i16(0);
        self.end(at);
        self
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    async fn read_one(bytes: &[u8]) -> Frontend {
        let mut cur = std::io::Cursor::new(bytes.to_vec());
        read_message(&mut cur).await.unwrap()
    }

    fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    #[tokio::test]
    async fn reads_a_simple_query() {
        let msg = tagged(b'Q', b"SELECT * FROM Item\0");
        assert_eq!(
            read_one(&msg).await,
            Frontend::Query("SELECT * FROM Item".into())
        );
    }

    #[tokio::test]
    async fn reads_startup_params_and_ssl_request() {
        // SSLRequest: len=8, code=SSL_REQUEST.
        let mut ssl = Vec::new();
        ssl.extend_from_slice(&8i32.to_be_bytes());
        ssl.extend_from_slice(&SSL_REQUEST.to_be_bytes());
        let mut cur = std::io::Cursor::new(ssl);
        assert_eq!(read_startup(&mut cur).await.unwrap(), Frontend::SslRequest);

        // Startup with user+database.
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        body.extend_from_slice(b"user\0analytics\0database\0fluxum\0\0");
        let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body);
        let mut cur = std::io::Cursor::new(packet);
        let Frontend::Startup { params } = read_startup(&mut cur).await.unwrap() else {
            panic!("expected startup");
        };
        assert_eq!(params[0], ("user".into(), "analytics".into()));
        assert_eq!(params[1], ("database".into(), "fluxum".into()));
    }

    #[tokio::test]
    async fn reads_parse_bind_describe_execute() {
        let mut parse_body = Vec::new();
        parse_body.extend_from_slice(b"st1\0SELECT * FROM Item\0");
        parse_body.extend_from_slice(&0i16.to_be_bytes());
        let Frontend::Parse { name, sql, .. } = read_one(&tagged(b'P', &parse_body)).await else {
            panic!("parse");
        };
        assert_eq!(name, "st1");
        assert_eq!(sql, "SELECT * FROM Item");

        let mut bind_body = Vec::new();
        bind_body.extend_from_slice(b"\0st1\0");
        bind_body.extend_from_slice(&0i16.to_be_bytes()); // format codes
        bind_body.extend_from_slice(&0i16.to_be_bytes()); // params
        bind_body.extend_from_slice(&0i16.to_be_bytes()); // result formats
        let Frontend::Bind {
            statement,
            param_count,
            ..
        } = read_one(&tagged(b'B', &bind_body)).await
        else {
            panic!("bind");
        };
        assert_eq!(statement, "st1");
        assert_eq!(param_count, 0);

        assert_eq!(
            read_one(&tagged(b'D', b"S\0")).await,
            Frontend::Describe {
                kind: b'S',
                name: String::new()
            }
        );
        assert_eq!(read_one(&tagged(b'S', b"")).await, Frontend::Sync);
        assert_eq!(read_one(&tagged(b'X', b"")).await, Frontend::Terminate);
    }

    #[test]
    fn builds_a_row_description_and_data_row() {
        let mut out = Out::new();
        out.row_description(&[FieldDesc {
            name: "id".into(),
            type_oid: OID_INT8,
            type_size: 8,
        }])
        .data_row(&[Some(b"42".to_vec())])
        .command_complete("SELECT 1")
        .ready_for_query(b'I');
        let bytes = out.into_bytes();
        assert_eq!(bytes[0], b'T'); // RowDescription first
        // The last five bytes are ReadyForQuery: 'Z', len=5, 'I'.
        let z = bytes.len() - 6;
        assert_eq!(bytes[z], b'Z');
        assert_eq!(bytes[bytes.len() - 1], b'I');
    }

    #[test]
    fn an_error_carries_severity_code_and_message() {
        let mut out = Out::new();
        out.error("ERROR", "25006", "read-only endpoint");
        let bytes = out.into_bytes();
        assert_eq!(bytes[0], b'E');
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("25006"));
        assert!(text.contains("read-only endpoint"));
    }
}
