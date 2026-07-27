//! HTTP/1.1 wire plumbing (request parsing, response/chunk writing, the
//! static-file route and small session-state helpers) — split from the
//! parent module to honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

// --- HTTP/1.1 request parsing --------------------------------------------------

/// A parsed request: method, path, lowercased headers, and the body.
pub(super) struct Request {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
}

impl Request {
    pub(super) fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }

    pub(super) fn header_eq(&self, name: &str, value: &str) -> bool {
        self.header(name).is_some_and(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case(value)
        })
    }
}

/// Read one HTTP/1.1 request; `None` on a clean connection close.
pub(super) async fn read_request(
    stream: &mut MaybeTls,
    buf: &mut Vec<u8>,
) -> io::Result<Option<Request>> {
    // Read until the end of the header block.
    let headers_end = loop {
        if let Some(pos) = find_subslice(buf, b"\r\n\r\n") {
            break pos;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header block too large",
            ));
        }
    };

    let head = String::from_utf8_lossy(&buf[..headers_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_owned();
    let path = parts.next().unwrap_or("").to_owned();

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().to_owned();
            if name == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
    }

    // Consume the header block and read the body.
    let body_start = headers_end + 4;
    buf.drain(..body_start);
    while buf.len() < content_length {
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf.drain(..content_length).collect();

    Ok(Some(Request {
        method,
        path,
        headers,
        body,
    }))
}

// --- HTTP/1.1 response writing -------------------------------------------------

pub(super) async fn write_response(
    stream: &mut MaybeTls,
    code: u16,
    reason: &str,
    session: Option<&str>,
    body: &[u8],
) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(token) = session {
        head.push_str(&format!("Fluxum-Session: {token}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

/// Write a non-FluxRPC response (admin JSON / metrics text) with an explicit
/// content type.
pub(super) async fn write_json(
    stream: &mut MaybeTls,
    code: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {code} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",
        reason_phrase(code),
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

/// A minimal reason-phrase table for the admin responses.
pub(super) fn reason_phrase(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

/// Serve a file from `static_dir` (see [`crate::statics`]).
///
/// A path that would escape the root is answered 404 rather than 403: telling
/// a prober which traversals were *rejected* rather than *absent* maps out the
/// filesystem for them.
pub(super) async fn handle_static(
    state: &Arc<HttpState>,
    stream: &mut MaybeTls,
    path: &str,
) -> io::Result<()> {
    let Some(root) = state.options.static_dir.as_deref() else {
        return write_simple(stream, 404, "Not Found").await;
    };
    let Some(file) = crate::statics::resolve(root, path) else {
        return write_simple(stream, 404, "Not Found").await;
    };
    let Ok(body) = tokio::fs::read(&file).await else {
        return write_simple(stream, 404, "Not Found").await;
    };

    // `Connection: close`, deliberately.
    //
    // A browser allows ~6 concurrent connections per origin on HTTP/1.1, and
    // Fluxum's push stream (RPC-006) holds one of them for the session's whole
    // life. Keeping asset connections alive means the page's own HTML, CSS and
    // JS sit in that pool competing with it — and a page served from the same
    // origin as `/rpc` can exhaust the pool before it ever opens the stream,
    // leaving `GET /rpc` queued in the browser with no request on the wire and
    // no error anywhere. Closing after each file returns the socket at once.
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\n\
         Cache-Control: no-cache\r\nConnection: close\r\n\r\n",
        crate::statics::content_type(&file),
        body.len(),
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

pub(super) async fn write_simple(stream: &mut MaybeTls, code: u16, reason: &str) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: 0\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await
}

/// Write the `GET /rpc` streaming response head (RPC-006).
///
/// `X-Content-Type-Options: nosniff` is load-bearing, not hygiene. Browsers
/// MIME-sniff an unrecognised `Content-Type`, and sniffing needs bytes — so
/// Chrome holds the `fetch()` promise unresolved until the first chunk
/// arrives. A push stream is idle by definition until the first commit, which
/// made every browser client hang at connect while Node (which does not sniff)
/// worked fine. `nosniff` tells the browser there is nothing to guess.
pub(super) async fn write_stream_header(stream: &mut MaybeTls) -> io::Result<()> {
    // `nosniff` because the content type is not one a browser recognises, and
    // sniffing an unrecognised type means waiting for bytes a push stream has
    // no reason to send yet. (It is not what caused the slow-open bug — see
    // `handle_get` — but it is still the right header.)
    const HEAD: &str = concat!(
        "HTTP/1.1 200 OK\r\nContent-Type: ",
        "application/x-fluxum",
        "\r\nTransfer-Encoding: chunked\r\n",
        "X-Content-Type-Options: nosniff\r\nCache-Control: no-cache\r\n\r\n",
    );
    stream.write_all(HEAD.as_bytes()).await?;
    stream.flush().await
}

pub(super) async fn write_chunk(stream: &mut MaybeTls, data: &[u8]) -> io::Result<()> {
    let header = format!("{:x}\r\n", data.len());
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}

pub(super) async fn write_last_chunk(stream: &mut MaybeTls) -> io::Result<()> {
    stream.write_all(b"0\r\n\r\n").await?;
    stream.flush().await
}

// --- helpers -------------------------------------------------------------------

pub(super) fn connection_id_of(state: &SessionState) -> Option<u128> {
    match state {
        SessionState::Authenticated { caller, .. } => Some(caller.connection_id.as_u128()),
        SessionState::Unauthenticated => None,
    }
}

/// The caller identity (hex) of an authenticated session, for the SEC-053
/// admin directory; empty for an unauthenticated state.
pub(super) fn identity_hex_of(state: &SessionState) -> String {
    match state {
        SessionState::Authenticated { caller, .. } => {
            use std::fmt::Write as _;
            caller.identity.as_bytes().iter().fold(
                String::with_capacity(caller.identity.as_bytes().len() * 2),
                |mut acc, b| {
                    let _ = write!(acc, "{b:02x}");
                    acc
                },
            )
        }
        SessionState::Unauthenticated => String::new(),
    }
}

/// Resolve a presented raw token to the at-rest id of a *live* session
/// (SEC-050/052): the token's own hash if it keys a session, else a
/// still-in-window grace mapping from a just-rotated token, else `None`
/// (unknown — never adopted). Expired grace entries are purged here.
/// Whether the request carries a `Fluxum-Session` that resolves to a live,
/// authenticated session (F-002 blob gate). A revoked session does not count.
pub(super) async fn is_authed_session(state: &Arc<HttpState>, request: &Request) -> bool {
    let Some(raw) = request.header("fluxum-session") else {
        return false;
    };
    let Some(id) = resolve_id(state, &raw).await else {
        return false;
    };
    let sessions = state.sessions.lock().await;
    sessions.get(&id).is_some_and(|s| {
        !s.revoked.load(std::sync::atomic::Ordering::SeqCst)
            && matches!(s.state, SessionState::Authenticated { .. })
    })
}

pub(super) async fn resolve_id(state: &Arc<HttpState>, raw_token: &str) -> Option<String> {
    let id = token_id(raw_token);
    if state.sessions.lock().await.contains_key(&id) {
        return Some(id);
    }
    let mut grace = state.grace.lock().await;
    let now = Instant::now();
    grace.retain(|_, (_, deadline)| now < *deadline);
    grace.get(&id).map(|(current, _)| current.clone())
}

/// Decode every FluxRPC frame in a POST body into client messages. A frame
/// larger than the limit is `413`; a malformed envelope is `400`.
pub(super) fn decode_frames(codec: &FrameCodec, body: &[u8]) -> Result<Vec<ClientMessage>, u16> {
    let mut messages = Vec::new();
    let mut offset = 0usize;
    while offset < body.len() {
        match codec.decode(&body[offset..]) {
            Ok(Some((frame, consumed))) => {
                if let Frame::Body(bytes) = frame {
                    let message =
                        ClientMessage::decode(bytes).map_err(|_| codes::PROTO_MALFORMED)?;
                    messages.push(message);
                }
                offset += consumed;
            }
            Ok(None) => break, // trailing partial frame — ignore
            Err(_too_large) => return Err(codes::PROTO_FRAME_TOO_LARGE),
        }
    }
    Ok(messages)
}

pub(super) fn frame_message(codec: &FrameCodec, message: &ServerMessage) -> Result<Vec<u8>, ()> {
    let body = message.encode().map_err(|_| ())?;
    codec.encode(&body).map_err(|_| ())
}

pub(super) fn error_frame(codec: &FrameCodec, code: u16, message: &str) -> Vec<u8> {
    let msg = ServerMessage::Error(fluxum_protocol::ErrorMessage::from_catalog(
        None,
        code,
        message,
        Vec::new(),
    ));
    frame_message(codec, &msg).unwrap_or_default()
}

/// The HTTP status a catalog code derives for this transport (SPEC-028 §7).
pub(super) fn http_status(code: u16) -> u16 {
    codes::entry(code).map_or(500, |entry| entry.http_status)
}

pub(super) fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Sleep until `deadline`, or forever when `None` (idle disabled).
pub(super) async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}
