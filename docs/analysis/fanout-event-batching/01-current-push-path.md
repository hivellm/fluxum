# 01 — The realtime push path as it exists today

*Findings F-001..F-004.*

## The pipeline, end to end

```
single writer commits
  └─ CommitHook → broadcast::channel(256)          boot.rs:533
      └─ fan-out task (one per namespace)          lib.rs:1217-1356
          ├─ SubscriptionManager::on_commit        manager.rs:706-792   → Vec<QueryDelta>
          ├─ per delta, per query_id group:
          │    TxUpdate envelope  → MessagePack    manager.rs:800-813
          │    FrameCodec::encode → Arc<Vec<u8>>   lib.rs:1325-1332
          └─ per subscriber: sink.try_send(OutFrame)  lib.rs:1340-1343, 1192-1214
              └─ mpsc::channel(send_queue_depth = 1024)  tcp.rs:354 / http.rs:1082
                  └─ writer task → socket
                       TCP : write_all(frame)               tcp.rs:679-694
                       HTTP: write_chunk(frame)             http.rs:1285 → http/wire.rs:227-233
```

Everything above the socket is already well factored: the query plan is compiled once
(SUB-020), the delta is evaluated once per unique query (`manager.rs:713-746`), and the
framed bytes are shared across every subscriber of a group by cloning an `Arc`, never the
buffer (`lib.rs:1332, 1341`). The cost this analysis is about lives strictly **below** that
line — in how many times those shared bytes are handed to the kernel.

---

## F-001 — One socket write per frame per subscriber; nothing coalesces, anywhere

**Evidence**

`crates/fluxum-server/src/tcp.rs:679-694` — the whole TCP writer:

```rust
while let Some(frame) = out_rx.recv().await {
    metrics.note_fanout_stage(FanoutStage::QueueWait, us(frame.enqueued_at.elapsed()));
    let began = std::time::Instant::now();
    if write_half.write_all(&frame.bytes).await.is_err() { break; }
    metrics.note_fanout_stage(FanoutStage::Flush, us(began.elapsed()));
}
```

`crates/fluxum-server/src/http.rs:1278-1291` is the same shape for the `GET /rpc` push
stream: `out_rx.recv()` → one `write_chunk` → back to `select!`.

There is no `BufWriter`, no `recv_many`, no vectored write, no scratch buffer, and no
`is_write_vectored` implementation on `MaybeTls` (`crates/fluxum-server/src/tls.rs:90-107`
forwards `poll_write`/`poll_flush` only). `recv()` returns exactly one frame per await even
when 50 are already queued behind it.

**Impact.** Kernel-facing work scales as
`writes = commits × subscribers × matched_queries_per_subscriber`, with no amortization
term. On the reference box this leg was measured at **41–46 µs per frame** for the socket
write plus **107–115 µs per frame** of queue+wake latency (see F-004). A burst of queued
frames costs exactly N times a single frame; the design has no way to spend one wake and
one syscall on N.

**Confidence:** high — read directly from both writer loops.

---

## F-002 — The HTTP push stream issues *three* writes and a flush per frame

**Evidence** — `crates/fluxum-server/src/http/wire.rs:227-233`:

```rust
pub(super) async fn write_chunk(stream: &mut MaybeTls, data: &[u8]) -> io::Result<()> {
    let header = format!("{:x}\r\n", data.len());
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}
```

Every `TxUpdate` on the browser/Streamable-HTTP transport therefore costs **3 `write_all`
calls + 1 `flush`**, not 1. It also allocates a `String` per frame for the chunk header.

Two multipliers make this worse than a 3× syscall count:

- `TCP_NODELAY` is enabled on the HTTP listener (`crates/fluxum-server/src/http.rs:330`),
  so the kernel is explicitly told not to merge those three writes — each can leave as its
  own segment, and the `flush()` removes the last opportunity for the socket to hold them.
- Under TLS (`MaybeTls::Tls`, `tls.rs:90-99`), each `poll_write` feeds rustls separately;
  a small plaintext write becomes its own TLS record with its own 5-byte header and 16-byte
  AEAD tag. Three records per frame ≈ **63 bytes of TLS overhead** on a frame whose payload
  may be ~200 bytes.

**Impact.** The transport intended for browsers — the one that cannot fall back to raw TCP
— pays the highest per-event overhead in the system. Fixing this alone is a 3× reduction in
writes on that path with no protocol change and no latency trade.

**Confidence:** high for the write count (direct read); medium for the exact TLS record
accounting (depends on rustls' internal fragmentation of successive small writes).

---

## F-003 — `TCP_NODELAY` everywhere means userspace batching is the *only* lever

**Evidence** — Nagle is disabled on every listener:

- `crates/fluxum-server/src/tcp.rs:146` — `stream.set_nodelay(true)`
- `crates/fluxum-server/src/http.rs:330` — same
- `crates/fluxum-server/src/pgwire/mod.rs:110` — same
- `crates/fluxum-core/src/plugin/sidecar.rs:479` — same

**Impact.** This is the *correct* default for a latency-first realtime database — Nagle
interacting with delayed-ACK is a classic 40 ms tail generator, and the project's headline
metric is change→subscriber p99. But it also means the kernel will never coalesce small
frames on our behalf: **one `write_all` is one segment**. Any packet-count reduction has to
be produced deliberately, in userspace, before the bytes reach the socket. This finding is
not a defect; it is the constraint that makes F-001/F-002 actionable rather than
academic.

**Confidence:** high.

---

## F-004 — The measured stage split already blames the per-frame legs

The predecessor task `phase0_parity-fanout-latency` (archived
`.rulebook/archive/2026-07-23-phase0_parity-fanout-latency/tasks.md`, items 1.1 and 1.5)
instrumented exactly this pipeline via `fluxum_fanout_stage_us{stage}` (OBS-023). Measured
on the reference box, release build, **50 subscribers @ 10 msg/s**:

| Stage | Scope | Mean |
|---|---|---|
| `recv_lag` | commit broadcast → fan-out wake | 12.7–15.9 µs / commit |
| `eval` | manager lock + `on_commit` | 12.8–13.1 µs / commit |
| `enqueue` | envelope encode + all 50 `try_send`s | 34.7–36.5 µs / commit |
| **`queue_wait`** | frame enqueued → writer task dequeued it | **107.5–115.4 µs / frame** |
| **`flush`** | one `write_all` on one socket | **40.9–46.1 µs / frame** |
| `server_total` | commit → all subscribers enqueued | 73.8–79.5 µs / commit |

Fan-out slope by subscriber count (client-observed): 1 sub p50 310 µs / p99 484 µs; 10 subs
341/542; 50 subs 498/771. The **1→50 slope is +188 µs p50 / +287 µs p99** — pure delivery
serialization.

**Impact.** The two *per-frame* stages dominate and the two *per-commit* stages do not.
That is the arithmetic signature of a workload that batching helps: reducing the frame
count (F-005) or the write count per frame batch (F-001/F-002) attacks 150+ µs/frame, while
further optimizing evaluation attacks ~13 µs/commit.

The same task also records two attempts that were **tried and honestly reverted** — chunked
parallel enqueue (measured no better; p99 734 µs vs 686 µs) and a direct-socket write path
(discarded by arithmetic: 50 sequential `try_write`s serialize what the writer tasks spread
across 32 workers). Neither of those was batching: both kept **one write per frame** and
only moved *who* performed it. Coalescing is the untried axis.

**Impact on framing of this analysis.** NFR-11's `e2e_p99 ≥ 10×` is currently **not
claimed** on the reference box (measured 5.25× / 9.56× / 10.8× across three runs). The task
attributes ~0.29 ms of the 0.68 ms p99 to `subscribers × ~41 µs Windows-loopback write` —
i.e. to precisely the term that coalescing divides.

**Confidence:** high — these are the project's own committed measurements, reproduced
across two runs a day apart.

---

Next: [02 — Why there are more frames than there need to be](02-frame-multiplication.md)
