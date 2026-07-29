//! `fluxum-bench fanout` — the burst-mode fan-out bench (phase9 F-015).
//!
//! The parity `e2e` workload paces one writer at 10–20 msg/s, where the
//! per-connection writer queue is empty by construction and any write
//! coalescing measures as a no-op. This command drives the queue NON-empty:
//! `--clients W` writer identities fire one `send_chat` each, simultaneously,
//! every `W / rate` seconds — so each round lands a W-commit burst and every
//! subscriber's outbound queue sees W frames back to back, while the
//! aggregate rate stays at `--rate` and each identity stays under the demo
//! module's 20/s admission ceiling.
//!
//! Measured per delivery: e2e latency (send instant embedded in the message
//! content, same machine, same clock — the parity e2e's trick). Reported:
//! deliveries/s, p50/p95/p99, and — once the OBS-024 counters exist — the
//! server's frames-per-write batch factor scraped from `/metrics` (`null`
//! on a server that predates them: that IS the baseline).

use super::*;

use std::io::{Read as _, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fluxum_sdk::protocol::{FluxBinReader, FluxValue};
use fluxum_sdk::{Connection, TableSchema};

#[derive(serde::Serialize)]
struct FanoutReport {
    /// Bench schema version for the report file.
    harness_version: u32,
    /// Subscriber sessions receiving every commit.
    subscribers: usize,
    /// Writer identities (the per-round burst factor).
    writers: usize,
    /// Offered aggregate commit rate, commits/s.
    rate_target: u32,
    /// Measured window, seconds.
    duration_secs: u64,
    /// Commits actually sent inside the window.
    sent: u64,
    /// Deliveries observed (sent × subscribers when nothing was lost).
    deliveries: u64,
    /// Deliveries per second over the window.
    deliveries_per_sec: f64,
    /// Per-delivery e2e latency, µs.
    e2e_us: LatencySummary,
    /// OBS-024 mean frames per socket write, from `/metrics`; `None` on a
    /// server without the counters (the pre-coalescing baseline).
    frames_per_write: Option<f64>,
    /// OBS-024 coalesced frames + writes raw counters, when present.
    writer_frames: Option<u64>,
    /// See `writer_frames`.
    writer_writes: Option<u64>,
}

#[derive(serde::Serialize)]
struct LatencySummary {
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let ix = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[ix.min(sorted.len() - 1)]
}

/// ChatMessage row: (id u64, sender identity, channel u32, content str, …).
fn chat_content(row: &[u8]) -> Option<&str> {
    let mut reader = FluxBinReader::new(row);
    reader.read_u64().ok()?;
    reader.read_identity().ok()?;
    reader.read_u32().ok()?;
    reader.read_str().ok()
}

fn chat_schema() -> TableSchema {
    TableSchema {
        name: "ChatMessage".into(),
        pk_of_row: Box::new(|row| row[..8].to_vec()),
        pk_of_delete: Box::new(|entry| entry[..8].to_vec()),
    }
}

fn now_micros() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros(),
    )
    .unwrap_or(u64::MAX)
}

/// Scrape `/metrics` for the OBS-024 writer-coalescing counters. `None`
/// when the exposition does not carry them — the baseline server.
///
/// Content-Length read, never read-to-EOF: the server keeps the connection
/// alive regardless of `Connection: close`, so an EOF read blocks forever
/// (the phase8 console-smoke lesson, relearned once is enough).
fn scrape_frames_per_write(http_port: u16) -> Option<(u64, u64)> {
    let mut stream = TcpStream::connect(("127.0.0.1", http_port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: bench\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut raw = Vec::new();
    let mut chunk = [0u8; 16384];
    let (headers_end, content_length) = loop {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        raw.extend_from_slice(&chunk[..n]);
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&raw[..pos]);
            let len = headers.lines().find_map(|l| {
                let (name, value) = l.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })?;
            break (pos + 4, len);
        }
    };
    while raw.len() < headers_end + content_length {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&chunk[..n]);
    }
    let body = String::from_utf8_lossy(&raw[headers_end..]).into_owned();
    let value_of = |name: &str| -> Option<u64> {
        body.lines()
            .filter(|l| l.starts_with(name) && !l.starts_with('#'))
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .fold(None, |acc, v| {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                Some(acc.unwrap_or(0) + v as u64)
            })
    };
    let frames = value_of("fluxum_writer_coalesced_frames_total")?;
    let writes = value_of("fluxum_writer_writes_total")?;
    Some((frames, writes))
}

pub(super) fn run_fanout_burst_command(opts: &Opts) -> Result<(), String> {
    let subscribers = opts.subscribers.max(1);
    let writers = opts.clients.max(1);
    let rate = opts.rate.max(1);
    let duration = Duration::from_secs(opts.duration_secs.max(5));
    // RED-050: send_chat admits 20/s per identity; the aggregate rate is
    // spread across the writer identities, so the fleet must be big enough.
    let per_writer = f64::from(rate) / writers as f64;
    if per_writer > 19.0 {
        return Err(format!(
            "rate {rate}/s across {writers} writers is {per_writer:.1}/s per identity — \
             over the demo module's 20/s send_chat admission; raise --clients"
        ));
    }

    let server = BenchServer::start_with(None)?;
    println!(
        "== fanout: {rate} commits/s in {writers}-commit bursts, {subscribers} subscribers, {}s ==",
        duration.as_secs()
    );

    // Subscribers first, so every commit inside the window fans out to all.
    let latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let deliveries = Arc::new(AtomicU64::new(0));
    let mut subs = Vec::with_capacity(subscribers);
    for i in 0..subscribers {
        let conn = Connection::connect(
            &server.url,
            format!("fanout-sub-{i}").as_bytes(),
            [chat_schema()],
        )
        .map_err(|e| format!("subscriber {i} connect: {e}"))?;
        let latencies = Arc::clone(&latencies);
        let deliveries = Arc::clone(&deliveries);
        conn.on(
            "ChatMessage:insert",
            Box::new(move |row, _old| {
                if let Some(sent) = chat_content(row).and_then(|c| c.parse::<u64>().ok()) {
                    let e2e = now_micros().saturating_sub(sent);
                    deliveries.fetch_add(1, Ordering::Relaxed);
                    latencies
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(e2e);
                }
            }),
        );
        conn.subscribe(&["SELECT * FROM ChatMessage WHERE channel = 1"])
            .map_err(|e| format!("subscriber {i} subscribe: {e}"))?;
        subs.push(conn);
    }

    let mut writer_conns = Vec::with_capacity(writers);
    for i in 0..writers {
        writer_conns.push(
            Connection::connect(&server.url, format!("fanout-writer-{i}").as_bytes(), [])
                .map_err(|e| format!("writer {i} connect: {e}"))?,
        );
    }

    // Rounds: all writers fire simultaneously (pipelined), then the round's
    // acks drain — a W-commit burst per round, `rate / W` rounds per second.
    let round_interval = Duration::from_secs_f64(writers as f64 / f64::from(rate));
    let started = Instant::now();
    let mut sent = 0u64;
    let mut round_start = started;
    while started.elapsed() < duration {
        let stamp = now_micros().to_string();
        let mut pending = Vec::with_capacity(writers);
        for conn in &writer_conns {
            pending.push(
                conn.call_reducer_async(
                    "send_chat",
                    vec![FluxValue::I64(1), FluxValue::Str(stamp.clone())],
                )
                .map_err(|e| format!("send_chat (send): {e}"))?,
            );
        }
        for handle in pending {
            handle.wait().map_err(|e| format!("send_chat (ack): {e}"))?;
            sent += 1;
        }
        round_start += round_interval;
        if let Some(sleep) = round_start.checked_duration_since(Instant::now()) {
            std::thread::sleep(sleep);
        }
    }
    let window = started.elapsed();

    // Let the tail of the fan-out drain before reading the counters.
    let expected = sent * subscribers as u64;
    let drain_deadline = Instant::now() + Duration::from_secs(10);
    while deliveries.load(Ordering::Relaxed) < expected && Instant::now() < drain_deadline {
        std::thread::sleep(Duration::from_millis(25));
    }

    let mut sorted = latencies
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    sorted.sort_unstable();
    let delivered = deliveries.load(Ordering::Relaxed);
    let counters = scrape_frames_per_write(server.http_port);
    #[allow(clippy::cast_precision_loss)]
    let report = FanoutReport {
        harness_version: 1,
        subscribers,
        writers,
        rate_target: rate,
        duration_secs: window.as_secs(),
        sent,
        deliveries: delivered,
        deliveries_per_sec: delivered as f64 / window.as_secs_f64(),
        e2e_us: LatencySummary {
            p50: percentile(&sorted, 0.50),
            p95: percentile(&sorted, 0.95),
            p99: percentile(&sorted, 0.99),
            max: sorted.last().copied().unwrap_or(0),
        },
        #[allow(clippy::cast_precision_loss)]
        frames_per_write: counters.map(|(frames, writes)| frames as f64 / writes.max(1) as f64),
        writer_frames: counters.map(|(frames, _)| frames),
        writer_writes: counters.map(|(_, writes)| writes),
    };

    let rendered = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
    println!(
        "fanout: {sent} sent, {delivered}/{expected} delivered ({:.0}/s) | e2e p50 {} µs p95 {} µs p99 {} µs | frames/write {}",
        report.deliveries_per_sec,
        report.e2e_us.p50,
        report.e2e_us.p95,
        report.e2e_us.p99,
        report
            .frames_per_write
            .map_or("n/a (baseline)".to_string(), |f| format!("{f:.2}")),
    );
    if let Some(out) = &opts.out {
        std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
        let path = out.join("fanout-report.json");
        std::fs::write(&path, rendered).map_err(|e| e.to_string())?;
        println!("report at {}", path.display());
    } else {
        println!("{rendered}");
    }
    if delivered < expected {
        return Err(format!(
            "fanout: only {delivered} of {expected} deliveries arrived within the drain window"
        ));
    }
    Ok(())
}
