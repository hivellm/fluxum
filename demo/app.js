// The Fluxum demo: a table updating in real time, straight off the wire.
//
// Plain JavaScript loaded with <script type="module">, no build step
// (SDK-081), importing the packaged browser runtime — the same bundle npm
// ships. Everything on this page is binary end to end: MessagePack envelopes,
// FluxBIN rows, Streamable HTTP. No JSON, no gateway.

import { FluxumClient, RowReader } from './fluxum.min.js';

// --- Row decoding ------------------------------------------------------------
//
// A generated SDK emits these from the schema (SDK-041); hand-written here
// because the demo does not run codegen. FluxBIN is positional, so the order
// below IS the wire format and has to match the module's declaration order.

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

const decodeTask = (row) => {
  const r = new RowReader(row);
  return { id: r.read('U64'), owner: r.read('Identity'), title: r.read('Str'), done: r.read('Bool') };
};

const decodePresence = (row) => {
  const r = new RowReader(row);
  return { identity: r.read('Identity'), connectedAt: r.read('Timestamp') };
};

// `read('Identity')` returns hex already, not bytes.
const pkU64 = (b) => String(new RowReader(b).read('U64'));
const pkIdentity = (b) => new RowReader(b).read('Identity');

const TABLES = [
  { name: 'ChatMessage', pkOfRow: pkU64, pkOfDelete: pkU64 },
  { name: 'Task', pkOfRow: pkU64, pkOfDelete: pkU64 },
  { name: 'OnlineUser', pkOfRow: pkIdentity, pkOfDelete: pkIdentity },
];

const QUERIES = [
  'SELECT * FROM ChatMessage',
  'SELECT * FROM Task',
  'SELECT * FROM OnlineUser',
];

// --- Helpers -----------------------------------------------------------------

const $ = (id) => document.getElementById(id);

/** Accepts both shapes: `read('Identity')` gives hex, `client.identity` gives bytes. */
const toHex = (v) =>
  typeof v === 'string' ? v : [...v].map((b) => b.toString(16).padStart(2, '0')).join('');
const short = (v) => toHex(v).slice(0, 8);

const clock = (micros) => {
  const d = new Date(Number(micros / 1000n));
  return d.toLocaleTimeString([], { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
};

function setState(state) {
  $('dot').dataset.state = state;
}

// --- Live metrics ------------------------------------------------------------

const metrics = { events: 0, window: [] };

function countEvent() {
  metrics.events += 1;
  const now = performance.now();
  metrics.window.push(now);
  // Keep a one-second sliding window so the rate reflects now, not the average
  // since page load — which would flatten out exactly when a burst arrives.
  while (metrics.window.length && now - metrics.window[0] > 1000) metrics.window.shift();
}

setInterval(() => {
  const now = performance.now();
  while (metrics.window.length && now - metrics.window[0] > 1000) metrics.window.shift();
  $('s-rate').textContent = String(metrics.window.length);
}, 250);

// --- Rendering ---------------------------------------------------------------

/** Ids already on screen, so only genuinely new rows flash. */
let seenChat = new Set();
let seenTasks = new Set();

function renderChat(client) {
  const rows = client.cache.rows('ChatMessage').map(decodeChat).sort((a, b) => Number(b.id - a.id));
  const body = $('chat-body');
  const me = toHex(client.identity);

  if (rows.length === 0) {
    body.innerHTML = '<tr class="empty"><td colspan="5">waiting for rows…</td></tr>';
    seenChat = new Set();
    return;
  }

  const next = new Set();
  body.replaceChildren(
    ...rows.slice(0, 200).map((m) => {
      const id = String(m.id);
      next.add(id);
      const tr = document.createElement('tr');
      if (!seenChat.has(id) && seenChat.size > 0) tr.className = 'fresh';
      const mine = m.sender === me;
      tr.innerHTML = `
        <td class="num">${id}</td>
        <td class="${mine ? 'you' : ''}">${short(m.sender)}${mine ? ' ·' : ''}</td>
        <td class="num">${m.channel}</td>
        <td></td>
        <td class="num">${clock(m.sentAt)}</td>`;
      // textContent, not innerHTML: row content is user input and goes nowhere
      // near the parser.
      tr.children[3].textContent = m.content;
      return tr;
    }),
  );
  seenChat = next;
}

function renderTasks(client) {
  const rows = client.cache.rows('Task').map(decodeTask).sort((a, b) => Number(a.id - b.id));
  const body = $('task-body');

  if (rows.length === 0) {
    body.innerHTML = '<li class="empty">no tasks yet</li>';
    seenTasks = new Set();
    return;
  }

  const next = new Set();
  body.replaceChildren(
    ...rows.map((t) => {
      const key = `${t.id}:${t.done}`;
      next.add(key);
      const li = document.createElement('li');
      if (!seenTasks.has(key) && seenTasks.size > 0) li.className = 'fresh';
      const label = document.createElement('span');
      label.textContent = t.title;
      if (t.done) label.className = 'done';
      li.append(label);
      if (!t.done) {
        const btn = document.createElement('button');
        btn.textContent = 'complete';
        btn.onclick = () => call('complete_task', [t.id]);
        li.append(btn);
      }
      return li;
    }),
  );
  seenTasks = next;
}

function renderPresence(client) {
  const me = toHex(client.identity);
  const rows = client.cache.rows('OnlineUser').map(decodePresence);
  $('presence').replaceChildren(
    ...rows.map((p) => {
      const li = document.createElement('li');
      li.textContent = short(p.identity);
      if (p.identity === me) li.className = 'self';
      return li;
    }),
  );
}

// --- Session -----------------------------------------------------------------

let db = null;
let storm = null;

async function connectAs(name) {
  stopStorm();
  if (db) {
    const previous = db;
    db = null;
    await previous.close();
  }

  setState('connecting');
  seenChat = new Set();
  seenTasks = new Set();

  const client = await FluxumClient.connect({
    // Same origin as this page — the server sends no CORS headers, which is
    // exactly why it serves these files itself.
    url: window.location.origin,
    token: new TextEncoder().encode(name),
    tables: TABLES,
  });
  db = client;

  const redraw = () => {
    renderChat(client);
    renderTasks(client);
    renderPresence(client);
    $('s-rows').textContent = String(client.cache.size);
    $('s-events').textContent = String(metrics.events);
  };

  for (const table of ['ChatMessage', 'Task', 'OnlineUser']) {
    for (const kind of ['insert', 'delete', 'update']) {
      client.on(`${table}:${kind}`, () => {
        countEvent();
        // The cache finished applying the whole transaction before this ran
        // (SDK-045), so reading it here always shows a consistent state.
        redraw();
      });
    }
  }

  client.onError(() => setState('error'));

  await client.subscribe(QUERIES);
  redraw();
  $('s-id').textContent = short(client.identity);
  setState('connected');
}

/** Run a reducer and time the round trip. */
async function call(reducer, args) {
  if (!db) return;
  const t0 = performance.now();
  try {
    await db.callReducer(reducer, args);
    $('s-rtt').textContent = `${(performance.now() - t0).toFixed(1)} ms`;
  } catch (err) {
    $('s-rtt').textContent = 'rejected';
    setState('error');
    console.error(`[fluxum] ${reducer}:`, err);
  }
}

// --- Traffic generator -------------------------------------------------------

const WORDS = ['commit', 'shard', 'replica', 'index', 'frame', 'subscribe', 'reduce', 'row'];

function startStorm() {
  const btn = $('storm');
  btn.dataset.on = 'true';
  btn.textContent = 'stop stream';
  let n = 0;
  storm = setInterval(() => {
    const word = WORDS[n % WORDS.length];
    call('send_chat', [1 + (n % 3), `${word} #${++n}`]);
  }, 100);
}

function stopStorm() {
  if (!storm) return;
  clearInterval(storm);
  storm = null;
  const btn = $('storm');
  btn.dataset.on = 'false';
  btn.textContent = 'stream 10/s';
}

// --- Wiring ------------------------------------------------------------------

$('send').onclick = () => {
  const input = $('msg');
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  call('send_chat', [1, text]);
};

$('msg').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') $('send').click();
});

$('add').onclick = () => {
  const input = $('task');
  const title = input.value.trim();
  if (!title) return;
  input.value = '';
  call('add_task', [title]);
};

$('task').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') $('add').click();
});

$('storm').onclick = () => (storm ? stopStorm() : startStorm());

$('switch').onclick = async () => {
  try {
    await connectAs($('identity').value.trim() || 'anonymous');
  } catch (err) {
    setState('error');
    console.error('[fluxum] connect:', err);
  }
};

// Close the session when the page goes away.
//
// Not politeness: the GET /rpc push stream is long-lived, and a browser allows
// only ~6 connections per origin over HTTP/1.1. A page that reloads without
// closing leaks one each time, and after six every request to this origin
// queues forever — which looks exactly like a hung server.
window.addEventListener('pagehide', () => {
  stopStorm();
  db?.close();
});

window.addEventListener('error', () => setState('error'));
window.addEventListener('unhandledrejection', () => setState('error'));

try {
  await connectAs($('identity').value.trim() || 'alice');
} catch (err) {
  setState('error');
  console.error('[fluxum] connect:', err);
}
