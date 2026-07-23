//! `fluxum logs` (SPEC-024 DEV-012): stream the server's structured log
//! lines from `GET /logs` with level/format filters.
//!
//! The server side is dependency-light NDJSON over chunked HTTP/1.1, so the
//! client side is too: one `TcpStream`, a tiny incremental chunked decoder,
//! and `serde_json` for the per-line filtering the flag surface promises.
//! Rendering:
//!
//! - `--format json` (default): the tap's lines verbatim — pipeable to `jq`.
//! - `--format pretty`: `HH:MM:SS LEVEL target message {extra fields}`.
//!
//! `--level warn` shows `warn` and above. The stream is already governed by
//! the server's configured level (OBS-082); this filter only narrows further.

use std::io::{Read, Write};
use std::net::TcpStream;

use crate::CliError;

/// A log severity, ordered so `Warn > Info` comparisons read naturally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// TRACE.
    Trace,
    /// DEBUG.
    Debug,
    /// INFO.
    Info,
    /// WARN.
    Warn,
    /// ERROR.
    Error,
}

impl Level {
    /// Parse a level name, any case. `None` for garbage — the caller turns
    /// that into a usage error naming the valid set.
    pub fn parse(text: &str) -> Option<Level> {
        Some(match text.to_ascii_lowercase().as_str() {
            "trace" => Level::Trace,
            "debug" => Level::Debug,
            "info" => Level::Info,
            "warn" | "warning" => Level::Warn,
            "error" => Level::Error,
            _ => return None,
        })
    }
}

/// How a line is printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// The tap's JSON verbatim.
    Json,
    /// Human-readable one-liner.
    Pretty,
}

impl Format {
    /// Parse a format name. `None` for garbage.
    pub fn parse(text: &str) -> Option<Format> {
        Some(match text.to_ascii_lowercase().as_str() {
            "json" => Format::Json,
            "pretty" => Format::Pretty,
            _ => return None,
        })
    }
}

/// An incremental HTTP/1.1 chunked-transfer decoder: feed raw socket bytes,
/// take out complete payload lines. Keep-alive blank lines are dropped here,
/// so callers only ever see log lines.
#[derive(Debug, Default)]
pub struct ChunkDecoder {
    raw: Vec<u8>,
    payload: Vec<u8>,
    /// The final `0\r\n\r\n` chunk arrived — the server ended the stream.
    done: bool,
}

impl ChunkDecoder {
    /// Feed socket bytes.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.raw.extend_from_slice(bytes);
        self.decode();
    }

    /// Whether the terminating chunk arrived.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Move every complete decoded chunk into the payload buffer.
    fn decode(&mut self) {
        loop {
            let Some(header_end) = find(&self.raw, b"\r\n") else {
                return;
            };
            let Ok(size_text) = std::str::from_utf8(&self.raw[..header_end]) else {
                // A malformed size line desynchronizes the stream: stop.
                self.done = true;
                return;
            };
            // Chunk extensions (`;`) are allowed by HTTP; the size is first.
            let size_text = size_text.split(';').next().unwrap_or_default().trim();
            let Ok(size) = usize::from_str_radix(size_text, 16) else {
                self.done = true;
                return;
            };
            if size == 0 {
                self.done = true;
                return;
            }
            // header + payload + trailing CRLF must be complete.
            let need = header_end + 2 + size + 2;
            if self.raw.len() < need {
                return;
            }
            self.payload
                .extend_from_slice(&self.raw[header_end + 2..header_end + 2 + size]);
            self.raw.drain(..need);
        }
    }

    /// The next complete payload line, blank keep-alives skipped.
    pub fn next_line(&mut self) -> Option<String> {
        loop {
            let end = find(&self.payload, b"\n")?;
            let line: Vec<u8> = self.payload.drain(..=end).collect();
            let text = String::from_utf8_lossy(&line).trim_end().to_owned();
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
}

/// First index of `needle` in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Whether `line` (a tap JSON object) passes the `--level` floor. Lines
/// without a parseable level (e.g. the dropped-lines marker) always pass —
/// hiding them would misrepresent the stream.
#[must_use]
pub fn passes(line: &str, floor: Option<Level>) -> bool {
    let Some(floor) = floor else { return true };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        return true;
    };
    let Some(level) = value
        .get("level")
        .and_then(serde_json::Value::as_str)
        .and_then(Level::parse)
    else {
        return true;
    };
    level >= floor
}

/// Render one tap line for `format`. `None` when the line should not be
/// printed at all (unparseable non-JSON noise in pretty mode only).
#[must_use]
pub fn render(line: &str, format: Format) -> Option<String> {
    match format {
        Format::Json => Some(line.to_owned()),
        Format::Pretty => {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                return Some(line.to_owned());
            };
            let timestamp = value
                .get("timestamp")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            // `2026-07-23T12:34:56.789Z` → `12:34:56` (best effort).
            let clock = timestamp
                .split_once('T')
                .map(|(_, t)| t.split('.').next().unwrap_or(t))
                .unwrap_or(timestamp);
            let level = value
                .get("level")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?");
            let target = value
                .get("target")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let message = value
                .pointer("/fields/message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let mut extras = String::new();
            if let Some(fields) = value.get("fields").and_then(serde_json::Value::as_object) {
                for (key, val) in fields {
                    if key != "message" {
                        use std::fmt::Write as _;
                        let _ = write!(extras, " {key}={val}");
                    }
                }
            }
            Some(format!("{clock} {level:>5} {target}: {message}{extras}"))
        }
    }
}

/// Options for [`stream`].
#[derive(Debug, Clone)]
pub struct LogsOptions {
    /// `host:port` of the server's HTTP port.
    pub server: String,
    /// Keep following (`-f`) instead of dumping the recent ring and exiting.
    pub follow: bool,
    /// Minimum level to print.
    pub level: Option<Level>,
    /// Output rendering.
    pub format: Format,
}

/// `fluxum logs`: connect, request `/logs`, print lines until the stream (or
/// the pipe) ends. Writes to `out` so tests capture the rendering.
pub fn stream(options: &LogsOptions, out: &mut dyn Write) -> Result<(), CliError> {
    let addr = crate::host_port(&options.server);
    let mut socket =
        TcpStream::connect(addr).map_err(|e| CliError::Connect(format!("{addr}: {e}")))?;
    let path = if options.follow {
        "/logs?follow=1"
    } else {
        "/logs"
    };
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: application/x-ndjson\r\n\
         Connection: close\r\n\r\n"
    );
    socket
        .write_all(request.as_bytes())
        .map_err(|e| CliError::Connect(e.to_string()))?;

    // Read up to the header/body split, checking the status line.
    let mut head = Vec::new();
    let mut chunk = [0u8; 4096];
    let body_start = loop {
        let n = socket
            .read(&mut chunk)
            .map_err(|e| CliError::Connect(e.to_string()))?;
        if n == 0 {
            return Err(CliError::Response(
                "connection closed before headers".into(),
            ));
        }
        head.extend_from_slice(&chunk[..n]);
        if let Some(split) = find(&head, b"\r\n\r\n") {
            break split + 4;
        }
    };
    let header_text = String::from_utf8_lossy(&head[..body_start]);
    let status = header_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| CliError::Response("no status line".into()))?;
    if status != "200" {
        // The refusal body (SEC-054 envelope) is small; read what is there.
        let mut rest = head[body_start..].to_vec();
        let _ = socket.read_to_end(&mut rest);
        let body = String::from_utf8_lossy(&rest);
        let message = serde_json::from_str::<serde_json::Value>(body.trim())
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_owned))
            .unwrap_or_else(|| body.trim().to_owned());
        return Err(CliError::Response(format!(
            "server answered {status}: {message}"
        )));
    }

    let mut decoder = ChunkDecoder::default();
    decoder.feed(&head[body_start..]);
    loop {
        while let Some(line) = decoder.next_line() {
            if passes(&line, options.level)
                && let Some(rendered) = render(&line, options.format)
                && writeln!(out, "{rendered}").is_err()
            {
                return Ok(()); // downstream pipe closed (e.g. `| head`)
            }
        }
        if decoder.is_done() {
            return Ok(());
        }
        let n = socket
            .read(&mut chunk)
            .map_err(|e| CliError::Connect(e.to_string()))?;
        if n == 0 {
            return Ok(()); // server went away — the stream simply ends
        }
        decoder.feed(&chunk[..n]);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn levels_order_and_parse() {
        assert!(Level::parse("WARN").unwrap() > Level::parse("info").unwrap());
        assert_eq!(Level::parse("warning"), Some(Level::Warn));
        assert_eq!(Level::parse("loud"), None);
        assert_eq!(Format::parse("PRETTY"), Some(Format::Pretty));
        assert_eq!(Format::parse("xml"), None);
    }

    #[test]
    fn chunk_decoder_reassembles_split_chunks_and_stops_at_the_last() {
        let mut decoder = ChunkDecoder::default();
        // One line split across two chunks, fed byte-dribbled.
        let wire = b"c\r\n{\"level\":\"IN\r\n7\r\nFO\"}\nx\n\r\n0\r\n\r\n";
        for byte in wire {
            decoder.feed(&[*byte]);
        }
        assert_eq!(decoder.next_line().unwrap(), "{\"level\":\"INFO\"}");
        assert_eq!(decoder.next_line().unwrap(), "x");
        assert!(decoder.next_line().is_none());
        assert!(decoder.is_done());
    }

    #[test]
    fn keepalive_blank_lines_are_swallowed() {
        let mut decoder = ChunkDecoder::default();
        decoder.feed(b"1\r\n\n\r\n3\r\nhi\n\r\n");
        assert_eq!(decoder.next_line().unwrap(), "hi");
        assert!(!decoder.is_done());
    }

    #[test]
    fn level_floor_filters_but_never_hides_markers() {
        let info = r#"{"level":"INFO","fields":{"message":"m"}}"#;
        let error = r#"{"level":"ERROR","fields":{"message":"m"}}"#;
        let marker = r#"{"fluxum_logs_dropped":7}"#;
        assert!(!passes(info, Some(Level::Warn)));
        assert!(passes(error, Some(Level::Warn)));
        assert!(passes(marker, Some(Level::Error)), "markers always pass");
        assert!(passes(info, None));
    }

    /// A one-shot fake `/logs` endpoint answering with a fixed head+body.
    fn serve_logs_once(head: &'static str, body: &'static [u8]) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            let Ok((mut socket, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut socket, &mut buf);
            let _ = socket.write_all(head.as_bytes());
            let _ = socket.write_all(body);
        });
        addr
    }

    #[test]
    fn stream_prints_filtered_rendered_lines_until_the_last_chunk() {
        const HEAD: &str = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\n\
                            Transfer-Encoding: chunked\r\n\r\n";
        // info (filtered out), warn (kept), keep-alive, terminator.
        const BODY: &[u8] = b"2a\r\n{\"level\":\"INFO\",\"fields\":{\"message\":\"a\"}}\n\r\n\
                              2a\r\n{\"level\":\"WARN\",\"fields\":{\"message\":\"b\"}}\n\r\n\
                              1\r\n\n\r\n0\r\n\r\n";
        let addr = serve_logs_once(HEAD, BODY);
        let options = LogsOptions {
            server: addr,
            follow: false,
            level: Some(Level::Warn),
            format: Format::Pretty,
        };
        let mut out = Vec::new();
        stream(&options, &mut out).unwrap();
        let printed = String::from_utf8(out).unwrap();
        assert!(printed.contains("WARN"), "{printed}");
        assert!(printed.contains(": b"), "{printed}");
        assert!(
            !printed.contains(": a"),
            "info was below the floor: {printed}"
        );
    }

    #[test]
    fn stream_surfaces_a_guard_refusal_with_its_message() {
        const HEAD: &str = "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\n\
                            Content-Length: 41\r\n\r\n";
        const BODY: &[u8] = br#"{"success":false,"error":"not from here"}"#;
        let addr = serve_logs_once(HEAD, BODY);
        let options = LogsOptions {
            server: addr,
            follow: true,
            level: None,
            format: Format::Json,
        };
        let err = stream(&options, &mut Vec::new()).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("403"), "{text}");
        assert!(text.contains("not from here"), "{text}");
    }

    #[test]
    fn pretty_rendering_shows_clock_level_target_message_and_extras() {
        let line = r#"{"timestamp":"2026-07-23T12:34:56.789Z","level":"WARN","target":"fluxum::server","fields":{"message":"slow reducer","duration_us":1500}}"#;
        let rendered = render(line, Format::Pretty).unwrap();
        assert_eq!(
            rendered,
            "12:34:56  WARN fluxum::server: slow reducer duration_us=1500"
        );
        // JSON mode is verbatim.
        assert_eq!(render(line, Format::Json).unwrap(), line);
        // Non-JSON noise passes through rather than vanishing.
        assert_eq!(render("plain", Format::Pretty).unwrap(), "plain");
    }
}
