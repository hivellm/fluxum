//! Blocking Streamable HTTP transport plumbing (SPEC-006 §3, RPC-004..007).
//!
//! The `/rpc` surface in two halves, mirrored from the server:
//!
//! - `POST /rpc` carries FluxRPC frames in the request body; the response
//!   body (Content-Length) carries the id-correlated answers (RPC-005). The
//!   first successful `Authenticate` mints a session and returns it in the
//!   `Fluxum-Session` header (RPC-007); every later POST and the GET stream
//!   echo it back. A stale/expired session is a `404`.
//! - `GET /rpc` opens the server-initiated push stream: an HTTP/1.1 chunked
//!   body of concatenated FluxRPC frames, including zero-length keep-alives
//!   (RPC-006).
//!
//! Hand-rolled HTTP/1.1 over `TcpStream`, deliberately: the SDK's dependency
//! surface stays the vendored wire layer alone (no HTTP client crate), the
//! peer is always a Fluxum server, and the subset needed — one request shape,
//! Content-Length responses, chunked streams — is small and pinned by the
//! server's own loopback tests. One TCP connection per POST
//! (`Connection: close`); the push stream holds its own connection open.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::protocol::{Frame, FrameCodec, ServerMessage};

/// The one wire content type for `/rpc` (RPC-004 — binary, never JSON).
const CONTENT_TYPE: &str = "application/x-fluxum";

/// A `/rpc` endpoint: `host:port`, no scheme.
pub(crate) struct HttpEndpoint {
    pub addr: String,
}

/// A parsed POST response: HTTP status, the session token if the server
/// issued one on this response, and the decoded server messages in the body.
pub(crate) struct PostResponse {
    pub status: u16,
    pub session: Option<String>,
    pub messages: Vec<ServerMessage>,
}

impl HttpEndpoint {
    /// One `POST /rpc` round-trip: send `body` (concatenated FluxRPC frames),
    /// read the full response. Bounded reads — a wedged server surfaces as an
    /// I/O timeout, never a hung caller.
    pub fn post(&self, session: Option<&str>, body: &[u8]) -> std::io::Result<PostResponse> {
        let mut stream = TcpStream::connect(&self.addr)?;
        let _ = stream.set_nodelay(true);
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;

        let mut request = format!(
            "POST /rpc HTTP/1.1\r\nHost: {}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n",
            self.addr,
            body.len()
        );
        if let Some(token) = session {
            request.push_str(&format!("Fluxum-Session: {token}\r\n"));
        }
        request.push_str("Connection: close\r\n\r\n");
        stream.write_all(request.as_bytes())?;
        stream.write_all(body)?;
        stream.flush()?;

        let head = read_head(&mut stream)?;
        let mut body = head.leftover;
        match head.content_length {
            Some(length) => {
                let mut chunk = [0u8; 4096];
                while body.len() < length {
                    let n = stream.read(&mut chunk)?;
                    if n == 0 {
                        break;
                    }
                    body.extend_from_slice(&chunk[..n]);
                }
                body.truncate(length);
            }
            None => {
                // `Connection: close` — the body runs to EOF.
                let _ = stream.read_to_end(&mut body);
            }
        }
        Ok(PostResponse {
            status: head.status,
            session: head.session,
            messages: decode_frames(&body),
        })
    }

    /// Open the `GET /rpc` push stream (RPC-006). Returns the HTTP status and,
    /// on 200, the live chunked stream. A non-200 status is data, not an
    /// error: `404` means the session is gone (re-establish), `409` that the
    /// server still counts the previous stream (retry shortly).
    pub fn open_stream(&self, session: &str) -> std::io::Result<(u16, Option<ChunkedStream>)> {
        let mut stream = TcpStream::connect(&self.addr)?;
        let _ = stream.set_nodelay(true);
        // Bounded while we wait for the response head; cleared once live —
        // the stream then idles between keep-alives and must block freely.
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        let request = format!(
            "GET /rpc HTTP/1.1\r\nHost: {}\r\nFluxum-Session: {session}\r\n\r\n",
            self.addr
        );
        stream.write_all(request.as_bytes())?;
        stream.flush()?;

        let head = read_head(&mut stream)?;
        if head.status != 200 {
            return Ok((head.status, None));
        }
        stream.set_read_timeout(None)?;
        Ok((200, Some(ChunkedStream::new(stream, head.leftover))))
    }
}

/// The server-initiated push stream: an HTTP/1.1 chunked body decoded
/// incrementally into server messages. The blocking sibling of the TCP
/// `MessageStream` — same contract: `next()` blocks for the next decodable
/// message, `None` means the stream is over.
pub(crate) struct ChunkedStream {
    stream: TcpStream,
    codec: FrameCodec,
    /// Undecoded chunked-transfer bytes.
    raw: Vec<u8>,
    /// De-chunked FluxRPC frame bytes.
    frames: Vec<u8>,
    /// The terminal zero-size chunk arrived (or the chunking desynchronized):
    /// no more reads, but frames already de-chunked still drain out.
    done: bool,
}

impl ChunkedStream {
    fn new(stream: TcpStream, leftover: Vec<u8>) -> Self {
        Self {
            stream,
            codec: FrameCodec::default(),
            raw: leftover,
            frames: Vec::new(),
            done: false,
        }
    }

    /// A handle on the underlying socket, so `Drop` can shut it down and
    /// unblock a reader parked in `next()`.
    pub fn socket(&self) -> std::io::Result<TcpStream> {
        self.stream.try_clone()
    }

    /// The next decodable server message; `None` on EOF, socket error, or —
    /// once every already-received frame has drained out — the terminal
    /// zero-size chunk or malformed chunking. Draining first matters: the
    /// server ends the stream cleanly right AFTER a final frame (the `408`
    /// idle-expiry error, for one), and that frame must not be lost to the
    /// terminator that follows it. Zero-length keep-alive frames are consumed
    /// silently.
    pub fn next(&mut self) -> Option<ServerMessage> {
        let mut chunk = [0u8; 8192];
        loop {
            // De-chunk everything complete in `raw`.
            while !self.done {
                let Some(line_end) = find(&self.raw, b"\r\n") else {
                    break;
                };
                let size_text = String::from_utf8_lossy(&self.raw[..line_end]).into_owned();
                let Ok(size) = usize::from_str_radix(size_text.trim(), 16) else {
                    self.done = true; // desynchronized — stop reading
                    break;
                };
                if size == 0 {
                    self.done = true; // terminal chunk: the stream is over
                    break;
                }
                if self.raw.len() < line_end + 2 + size + 2 {
                    break; // incomplete chunk body
                }
                self.frames
                    .extend_from_slice(&self.raw[line_end + 2..line_end + 2 + size]);
                self.raw.drain(..line_end + 2 + size + 2);
            }
            // Decode any whole FluxRPC frames de-chunked so far.
            loop {
                match self.codec.decode(&self.frames) {
                    Ok(Some((Frame::Body(body), consumed))) => {
                        let message = ServerMessage::decode(body).ok();
                        self.frames.drain(..consumed);
                        if let Some(message) = message {
                            return Some(message);
                        }
                    }
                    Ok(Some((Frame::KeepAlive, consumed))) => {
                        self.frames.drain(..consumed);
                    }
                    Ok(None) => break,
                    Err(_) => return None,
                }
            }
            if self.done {
                return None;
            }
            match self.stream.read(&mut chunk) {
                Ok(0) => return None,
                Ok(n) => self.raw.extend_from_slice(&chunk[..n]),
                Err(_) => return None,
            }
        }
    }
}

/// A parsed response head plus whatever body bytes arrived with it.
struct Head {
    status: u16,
    session: Option<String>,
    content_length: Option<usize>,
    leftover: Vec<u8>,
}

/// Read and parse the status line and headers (case-insensitive names).
fn read_head(stream: &mut TcpStream) -> std::io::Result<Head> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos;
        }
        // A head larger than this is not a Fluxum server talking.
        if buf.len() > 64 * 1024 {
            return Err(std::io::Error::other("oversized response head"));
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::Error::other("connection closed before response head"));
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let head_text = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head_text.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| std::io::Error::other("malformed status line"))?;

    let mut session = None;
    let mut content_length = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "fluxum-session" if !value.is_empty() => session = Some(value.to_owned()),
            "content-length" => content_length = value.parse().ok(),
            _ => {}
        }
    }
    Ok(Head {
        status,
        session,
        content_length,
        leftover: buf[head_end + 4..].to_vec(),
    })
}

/// Decode the concatenated FluxRPC frames of a finite body, skipping
/// keep-alives and anything undecodable.
fn decode_frames(body: &[u8]) -> Vec<ServerMessage> {
    let codec = FrameCodec::default();
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < body.len() {
        let Ok(Some((frame, consumed))) = codec.decode(&body[offset..]) else {
            break;
        };
        if let Frame::Body(bytes) = frame
            && let Ok(message) = ServerMessage::decode(bytes)
        {
            out.push(message);
        }
        offset += consumed;
    }
    out
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_head_parses_status_session_and_length() {
        // A loopback pair: write a canned response into a socket, parse it.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let writer = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            peer.write_all(
                b"HTTP/1.1 200 OK\r\nFluxum-Session: abc123\r\nContent-Length: 4\r\n\r\nBODY",
            )
            .unwrap();
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        let head = read_head(&mut stream).unwrap();
        writer.join().unwrap();
        assert_eq!(head.status, 200);
        assert_eq!(head.session.as_deref(), Some("abc123"));
        assert_eq!(head.content_length, Some(4));
        assert_eq!(head.leftover, b"BODY");
    }

    #[test]
    fn a_chunked_stream_yields_messages_across_chunk_boundaries() {
        use crate::protocol::ReducerResult;
        // A real encoded frame, split mid-frame across two HTTP chunks, with a
        // keep-alive in front — the decoder must reassemble and skip it.
        let message = ServerMessage::ReducerResult(ReducerResult {
            id: 9,
            outcome: Ok(()),
        });
        let body = message.encode().unwrap();
        let framed = FrameCodec::default().encode(&body).unwrap();
        let keepalive = FrameCodec::default().encode(&[]).unwrap();

        let mut wire: Vec<u8> = Vec::new();
        let split = framed.len() / 2;
        for part in [&keepalive[..], &framed[..split], &framed[split..]] {
            wire.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
            wire.extend_from_slice(part);
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\n");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let writer = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            peer.write_all(&wire).unwrap();
        });
        let stream = TcpStream::connect(addr).unwrap();
        let mut chunked = ChunkedStream::new(stream, Vec::new());
        let first = chunked.next();
        writer.join().unwrap();
        match first {
            Some(ServerMessage::ReducerResult(result)) => assert_eq!(result.id, 9),
            other => panic!("expected the reducer result, got {other:?}"),
        }
        assert!(chunked.next().is_none(), "terminal chunk ends the stream");
    }
}
