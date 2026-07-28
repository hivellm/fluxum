// Chat-latency sample: TWO real Fluxum clients in one page, one clock.
//
// A (the sender) calls the `send_chat` reducer; B (the subscriber) holds a
// live `SELECT * FROM ChatMessage` subscription. Every timestamp is
// `performance.now()` from the same page, so there is no clock skew:
//
//   ack — send until the reducer's commit acknowledgment reaches A
//   e2e — send until B's cache has APPLIED the TxUpdate (commit + fan-out +
//         push over the GET stream + FluxBIN decode)
//
// Payloads are tagged `lat:<run>:<seq>` on channel 42, so ordinary chat
// traffic flows through the same pane without polluting the measurement.

import { FluxumClient, RowReader } from './fluxum.min.js';

const $ = (id) => document.getElementById(id);
const CHANNEL = 42;
const RUN_ID = Math.random().toString(36).slice(2, 8);
const TIMEOUT_MS = 5000;

const decodeChat = (row) => {
  const r = new RowReader(row);
  return {
    id: r.read('U64'),
    sender: r.read('Identity'),
    channel: r.read('U32'),
    content: r.read('Str'),
    sentAt: r.read('Timestamp'),
  };
};
const pkU64 = (b) => String(new RowReader(b).read('U64'));
const TABLES = [{ name: 'ChatMessage', pkOfRow: pkU64, pkOfDelete: pkU64 }];

// --- measurement state -------------------------------------------------------

const acks = [];
const e2es = [];
let timeouts = 0;
let rejected = 0;
let seq = 0;
const pending = new Map(); // seq -> { t0, resolve }

// The demo module declares `send_chat` at 20 calls/s (RED admission rate);
// the sequential runner paces itself under it, or the tail of a run is
// nothing but rejections.
const PACE_MS = 1000 / 15;

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx];
}
const fmt = (v) => (v == null ? '—' : v.toFixed(1));

function renderStats() {
  const rows = [
    ['a', acks],
    ['e', e2es],
  ];
  for (const [prefix, samples] of rows) {
    const sorted = [...samples].sort((x, y) => x - y);
    $(prefix + '-n').textContent = String(samples.length);
    $(prefix + '-min').textContent = fmt(sorted[0]);
    $(prefix + '-mean').textContent = fmt(
      samples.length ? samples.reduce((a, b) => a + b, 0) / samples.length : null,
    );
    $(prefix + '-p50').textContent = fmt(percentile(sorted, 50));
    $(prefix + '-p95').textContent = fmt(percentile(sorted, 95));
    $(prefix + '-p99').textContent = fmt(percentile(sorted, 99));
    $(prefix + '-max').textContent = fmt(sorted[sorted.length - 1]);
  }
  const sortedE = [...e2es].sort((x, y) => x - y);
  const sortedA = [...acks].sort((x, y) => x - y);
  $('s-ack').textContent = acks.length
    ? `${fmt(percentile(sortedA, 50))} / ${fmt(percentile(sortedA, 99))} ms`
    : '—';
  $('s-e2e').textContent = e2es.length
    ? `${fmt(percentile(sortedE, 50))} / ${fmt(percentile(sortedE, 99))} ms`
    : '—';
  $('s-n').textContent = String(e2es.length);
  $('s-to').textContent = `${timeouts} / ${rejected}`;
  renderHist(sortedE);
}

// A plain bucketed bar strip: one series, values on the axis ends only.
function renderHist(sorted) {
  const box = $('hist');
  box.textContent = '';
  if (sorted.length < 5) {
    $('hx-hi').textContent = '—';
    return;
  }
  const lo = 0;
  const hi = Math.max(1, (percentile(sorted, 99) ?? 1) * 1.2);
  const buckets = new Array(24).fill(0);
  for (const v of sorted) {
    const idx = Math.min(buckets.length - 1, Math.floor(((v - lo) / (hi - lo)) * buckets.length));
    buckets[idx] += 1;
  }
  const peak = Math.max(...buckets, 1);
  for (const count of buckets) {
    const bar = document.createElement('div');
    bar.className = 'b';
    bar.style.height = `${Math.max(1, (count / peak) * 100)}%`;
    bar.title = `${count} sample(s)`;
    box.appendChild(bar);
  }
  $('hx-lo').textContent = '0 ms';
  $('hx-hi').textContent = `${hi.toFixed(0)} ms`;
}

function addLine(pane, text, ms, foreign) {
  const li = document.createElement('li');
  const label = document.createElement('span');
  label.textContent = text;
  li.appendChild(label);
  if (ms != null) {
    const badge = document.createElement('span');
    badge.className = 'ms ' + (ms <= 25 ? 'fast' : ms <= 100 ? '' : 'slow');
    badge.textContent = `${ms.toFixed(1)} ms`;
    li.appendChild(badge);
  }
  if (foreign) li.className = 'foreign';
  const box = $(pane);
  box.prepend(li);
  while (box.childElementCount > 30) box.removeChild(box.lastChild);
}

// --- the two clients ---------------------------------------------------------

const short = (v) =>
  (typeof v === 'string' ? v : [...v].map((b) => b.toString(16).padStart(2, '0')).join('')).slice(0, 8);

let a = null;
let b = null;

async function connect() {
  $('s-status').textContent = 'connecting…';
  $('dot').dataset.state = 'connecting';
  a = await FluxumClient.connect({
    url: window.location.origin,
    token: new TextEncoder().encode('lat-sender'),
    tables: TABLES,
  });
  b = await FluxumClient.connect({
    url: window.location.origin,
    token: new TextEncoder().encode('lat-subscriber'),
    tables: TABLES,
  });
  b.on('ChatMessage:insert', (row) => {
    const now = performance.now();
    const m = decodeChat(row);
    const match = /^lat:([^:]+):(\d+)$/.exec(m.content);
    if (match && match[1] === RUN_ID) {
      const entry = pending.get(Number(match[2]));
      if (entry) {
        pending.delete(Number(match[2]));
        const ms = now - entry.t0;
        e2es.push(ms);
        addLine('pane-b', `#${match[2]}`, ms, false);
        entry.resolve();
        renderStats();
      }
      return;
    }
    // Ordinary chat riding the same subscription — shown, not measured.
    addLine('pane-b', `${short(m.sender)}: ${m.content}`, null, true);
  });
  b.onError(() => ($('dot').dataset.state = 'error'));
  a.onError(() => ($('dot').dataset.state = 'error'));
  await b.subscribe(['SELECT * FROM ChatMessage']);
  $('id-a').textContent = short(a.identity);
  $('id-b').textContent = short(b.identity);
  $('dot').dataset.state = 'connected';
  $('s-status').textContent = 'connected — A sends, B listens';
}

// One measured ping: resolves when B applied it (or times out).
async function ping() {
  const mySeq = ++seq;
  const content = `lat:${RUN_ID}:${mySeq}`;
  let resolve;
  const applied = new Promise((r) => (resolve = r));
  const t0 = performance.now();
  pending.set(mySeq, { t0, resolve });
  try {
    await a.callReducer('send_chat', [CHANNEL, content]);
    const ackMs = performance.now() - t0;
    acks.push(ackMs);
    addLine('pane-a', `#${mySeq}`, ackMs, false);
  } catch (err) {
    pending.delete(mySeq);
    rejected += 1;
    renderStats();
    $('s-status').textContent = `send_chat rejected (admission rate?): ${err}`;
    return;
  }
  const timer = setTimeout(() => {
    if (pending.delete(mySeq)) {
      timeouts += 1;
      renderStats();
    }
    resolve();
  }, TIMEOUT_MS);
  await applied;
  clearTimeout(timer);
  renderStats();
}

// --- run modes ---------------------------------------------------------------

let running = false;
let streamTimer = null;

async function runN(n) {
  if (running) return;
  running = true;
  $('run').disabled = true;
  for (let i = 0; i < n && running; i += 1) {
    $('s-status').textContent = `run ${i + 1}/${n}…`;
    const t0 = performance.now();
    await ping(); // sequential: each ping waits for its own e2e
    const spent = performance.now() - t0;
    if (spent < PACE_MS) await new Promise((r) => setTimeout(r, PACE_MS - spent));
  }
  $('s-status').textContent = 'done';
  $('run').disabled = false;
  running = false;
}

function toggleStream() {
  if (streamTimer) {
    clearInterval(streamTimer);
    streamTimer = null;
    $('stream').textContent = 'stream /s';
    $('s-status').textContent = 'stream stopped';
    return;
  }
  const rate = Math.max(1, Math.min(200, Number($('rate').value) || 20));
  $('stream').textContent = 'stop';
  $('s-status').textContent = `streaming ${rate}/s…`;
  streamTimer = setInterval(() => void ping(), 1000 / rate);
}

$('ping').onclick = () => void ping();
$('run').onclick = () => void runN(Math.max(1, Math.min(5000, Number($('n').value) || 100)));
$('stream').onclick = toggleStream;
$('reset').onclick = () => {
  acks.length = 0;
  e2es.length = 0;
  timeouts = 0;
  pending.clear();
  $('pane-a').textContent = '';
  $('pane-b').textContent = '';
  renderStats();
  $('s-status').textContent = 'reset';
};

window.addEventListener('pagehide', () => {
  if (streamTimer) clearInterval(streamTimer);
  running = false;
  a?.close();
  b?.close();
});

try {
  await connect();
} catch (err) {
  $('dot').dataset.state = 'error';
  $('s-status').textContent = `connect failed: ${err}`;
  console.error('[latency]', err);
}
