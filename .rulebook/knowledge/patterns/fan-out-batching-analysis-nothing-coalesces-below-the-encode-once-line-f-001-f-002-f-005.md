# Fan-out batching analysis: nothing coalesces below the encode-once line (F-001/F-002/F-005)

**Category**: performance
**Tags**: analysis:fanout-event-batching, performance, subscriptions, transport, batching

## Description

Analysis at docs/analysis/fanout-event-batching/ (README + 01..06, findings F-001..F-020). The fan-out path ABOVE the socket is already optimal (plan compiled once SUB-020, delta evaluated once per unique query manager.rs:713-746, framed bytes Arc-shared lib.rs:1332/1341). Every problem is BELOW that line.

Headline gaps:
- F-001: one `write_all` per frame per subscriber — tcp.rs:679-694 and http.rs:1278-1291 both `recv()` a single frame per await. No BufWriter, no recv_many, no vectored write; MaybeTls (tls.rs:90-115) has no poll_write_vectored.
- F-002: http/wire.rs:227-233 `write_chunk` = 3 × write_all + flush per frame (hex header, data, CRLF) → up to 3 segments/TLS records per TxUpdate on the browser transport. Ten-line fix, highest certainty-to-effort item.
- F-003: TCP_NODELAY is on everywhere (tcp.rs:146, http.rs:330, pgwire/mod.rs:110) — correct for latency, but it means the kernel never coalesces; userspace batching is the ONLY lever.
- F-005: one TxUpdate frame per (query delta × connection) — lib.rs:1289-1344 loops `for delta { for query_id group { for conn } }` and manager.rs:811 hardcodes `tables: vec![one]`. RPC-033 already declares `tables: Vec<TableUpdate>` and RPC-032 makes query_id the correlation handle → merged form is legal TODAY. Merge per equivalence class (connections with identical matched (delta,query_id) set) to keep SUB-024 encode-once.
- F-009: SubscriberBuffer (SUB-042 3-tier byte-budgeted policy, sendbuffer.rs) is implemented, tested and DISCONNECTED — only test + doc-link references. Live path is mpsc::channel(send_queue_depth=1024 FRAMES, hardcoded tcp.rs:56/http.rs:96) with binary deliver-or-kill (lib.rs:1199-1213). `subscriptions.send_buffer_bytes` is parsed/plumbed/hot-reloadable/asserted and read by nobody on the delivery path. Real ceiling = 1024 × frame_size (up to 16 GB at max_frame_bytes).
- F-012: `Err(Lagged(_)) => continue` at lib.rs:1237 drops commits for ALL subscribers with no metric and no log; capacity hardcoded 256 (boot.rs:533) = ~4 ms slack at the 64k commits/s ceiling.
- F-017: transport coalescing is wire-transparent to all 5 SDKs (all use incremental buffer-draining frame readers: rust client/runtime.rs:384-407, ts protocol.ts:74-101, py client.py:257-262, go client.go:267/306, c# Connection.cs:216/235) and RPC-004/005 already mandate back-to-back frames. No negotiation, no SDK release needed.

Plan: P1 phase0_fanout-write-coalescing (buffered chunk write + opportunistic recv_many drain + OBS-024 batch metrics + BURST bench) → gates P2 phase0_fanout-txupdate-merge and P3 phase8_subscriber-send-buffer-policy.
