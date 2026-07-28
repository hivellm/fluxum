// MMO position sync over the packaged browser SDK: every avatar is one row
// in the ephemeral `Player` table, moved by the `move_player` reducer and
// despawned by the engine when its connection closes.
//
// The canvas is deliberately decoupled from the network: TxUpdates only move
// TARGETS in a keyed map, and a requestAnimationFrame loop interpolates
// render positions toward them — so 1,000 row-updates/s and 60 fps never
// fight. Render-side costs are kept flat: one clear per frame, a
// pre-rendered background grid, no shadows, no per-frame allocations on the
// hot path, and name labels are a toggle.

import { FluxumClient, RowReader } from './fluxum.min.js';

const $ = (id) => document.getElementById(id);
const WORLD_W = 2000;
const WORLD_H = 1200;
const MOVE_HZ = 15; // < the reducer's 60/s admission rate
const SPEED = 260; // world units per second (keyboard)

const decodePlayer = (row) => {
  const r = new RowReader(row);
  return {
    connection: r.read('ConnectionId'),
    identity: r.read('Identity'),
    name: r.read('Str'),
    x: r.read('I32'),
    y: r.read('I32'),
    hue: r.read('U32'),
  };
};
const pkConnection = (b) => String(new RowReader(b).read('ConnectionId'));
const TABLES = [{ name: 'Player', pkOfRow: pkConnection, pkOfDelete: pkConnection }];

// --- world state -------------------------------------------------------------
// pk -> { x, y (render), tx, ty (target), name, hue, fill, mine }
const players = new Map();
let myIdentity = null; // hex, set after connect; marks my avatar's ring

const updates = { count: 0, window: [] };
function noteUpdate() {
  updates.count += 1;
  updates.window.push(performance.now());
}

function upsertPlayer(row) {
  const m = decodePlayer(row);
  const pk = String(m.connection);
  const existing = players.get(pk);
  if (existing) {
    existing.tx = m.x;
    existing.ty = m.y;
  } else {
    players.set(pk, {
      x: m.x, y: m.y, tx: m.x, ty: m.y,
      name: m.name, hue: m.hue,
      fill: `hsl(${m.hue} 70% 55%)`,
      edge: `hsl(${m.hue} 70% 35%)`,
      mine: m.identity === myIdentity,
    });
  }
  noteUpdate();
}

// --- canvas ------------------------------------------------------------------
const canvas = $('world');
const ctx = canvas.getContext('2d', { alpha: false });
let scale = 1, offX = 0, offY = 0, grid = null;

function resize() {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  canvas.width = Math.max(1, Math.round(w * dpr));
  canvas.height = Math.max(1, Math.round(h * dpr));
  // Fit the world, letterboxed, in device pixels.
  scale = Math.min(canvas.width / WORLD_W, canvas.height / WORLD_H);
  offX = (canvas.width - WORLD_W * scale) / 2;
  offY = (canvas.height - WORLD_H * scale) / 2;
  // Pre-render the background once per resize, not per frame.
  grid = document.createElement('canvas');
  grid.width = canvas.width;
  grid.height = canvas.height;
  const g = grid.getContext('2d');
  g.fillStyle = '#0b0e14';
  g.fillRect(0, 0, grid.width, grid.height);
  g.strokeStyle = 'rgba(120,140,180,0.08)';
  g.lineWidth = 1;
  g.beginPath();
  for (let x = 0; x <= WORLD_W; x += 100) {
    g.moveTo(offX + x * scale, offY);
    g.lineTo(offX + x * scale, offY + WORLD_H * scale);
  }
  for (let y = 0; y <= WORLD_H; y += 100) {
    g.moveTo(offX, offY + y * scale);
    g.lineTo(offX + WORLD_W * scale, offY + y * scale);
  }
  g.stroke();
  g.strokeStyle = 'rgba(120,140,180,0.35)';
  g.strokeRect(offX, offY, WORLD_W * scale, WORLD_H * scale);
}
new ResizeObserver(resize).observe(canvas);

let showNames = true;
$('names').onclick = () => {
  showNames = !showNames;
  $('names').textContent = `names: ${showNames ? 'on' : 'off'}`;
};

const fps = { frames: 0, last: performance.now() };
let lastFrame = performance.now();

function frame(now) {
  const dt = Math.min(0.1, (now - lastFrame) / 1000);
  lastFrame = now;
  // Exponential approach: converges on the target without overshoot and
  // stays smooth at any update rate.
  const k = 1 - Math.exp(-dt * 12);
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  if (grid) ctx.drawImage(grid, 0, 0);
  const r = Math.max(3, 7 * scale);
  for (const p of players.values()) {
    p.x += (p.tx - p.x) * k;
    p.y += (p.ty - p.y) * k;
    const cx = offX + p.x * scale;
    const cy = offY + p.y * scale;
    ctx.fillStyle = p.fill;
    ctx.beginPath();
    ctx.arc(cx, cy, r, 0, 6.2832);
    ctx.fill();
    ctx.strokeStyle = p.edge;
    ctx.lineWidth = 1;
    ctx.stroke();
    if (p.mine) {
      ctx.strokeStyle = '#fff';
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(cx, cy, r + 3, 0, 6.2832);
      ctx.stroke();
    }
  }
  if (showNames && players.size <= 200) {
    ctx.fillStyle = 'rgba(220,230,255,0.75)';
    ctx.font = `${Math.max(9, Math.round(10 * scale * 1.4))}px ui-monospace, monospace`;
    ctx.textAlign = 'center';
    for (const p of players.values()) {
      ctx.fillText(p.name, offX + p.x * scale, offY + p.y * scale - r - 4);
    }
  }
  fps.frames += 1;
  if (now - fps.last >= 1000) {
    $('s-fps').textContent = String(fps.frames);
    fps.frames = 0;
    fps.last = now;
    while (updates.window.length && now - updates.window[0] > 1000) updates.window.shift();
    $('s-ups').textContent = String(updates.window.length);
    $('s-players').textContent = String(players.size);
  }
  requestAnimationFrame(frame);
}

// --- input + movement --------------------------------------------------------
const keys = new Set();
let me = { x: WORLD_W / 2, y: WORLD_H / 2 };
let waypoint = null;
let moved = true; // send the spawn move immediately

window.addEventListener('keydown', (e) => {
  if (['w', 'a', 's', 'd', 'ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(e.key)) {
    keys.add(e.key);
    waypoint = null;
    e.preventDefault();
  }
});
window.addEventListener('keyup', (e) => keys.delete(e.key));
canvas.addEventListener('pointerdown', (e) => {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const wx = ((e.clientX - rect.left) * dpr - offX) / scale;
  const wy = ((e.clientY - rect.top) * dpr - offY) / scale;
  waypoint = { x: Math.max(0, Math.min(WORLD_W, wx)), y: Math.max(0, Math.min(WORLD_H, wy)) };
});

setInterval(() => {
  // Local simulation tick + throttled network send (15 Hz < 60/s cap).
  const dt = 1 / MOVE_HZ;
  let vx = 0, vy = 0;
  if (keys.has('w') || keys.has('ArrowUp')) vy -= 1;
  if (keys.has('s') || keys.has('ArrowDown')) vy += 1;
  if (keys.has('a') || keys.has('ArrowLeft')) vx -= 1;
  if (keys.has('d') || keys.has('ArrowRight')) vx += 1;
  if (waypoint) {
    const dx = waypoint.x - me.x, dy = waypoint.y - me.y;
    const d = Math.hypot(dx, dy);
    if (d < 4) waypoint = null;
    else { vx = dx / d; vy = dy / d; }
  }
  if (vx || vy) {
    const n = Math.hypot(vx, vy);
    me.x = Math.max(0, Math.min(WORLD_W, me.x + (vx / n) * SPEED * dt));
    me.y = Math.max(0, Math.min(WORLD_H, me.y + (vy / n) * SPEED * dt));
    moved = true;
  }
  if (moved && db) {
    moved = false;
    const t0 = performance.now();
    db.callReducer('move_player', [Math.round(me.x), Math.round(me.y)])
      .then(() => { $('s-ack').textContent = `${(performance.now() - t0).toFixed(1)} ms`; })
      .catch((err) => { $('s-status').textContent = `move rejected: ${err}`; });
  }
}, 1000 / MOVE_HZ);

// --- session -----------------------------------------------------------------
let db = null;

try {
  $('dot').dataset.state = 'connecting';
  db = await FluxumClient.connect({
    url: window.location.origin,
    token: new TextEncoder().encode('mmo-' + Math.random().toString(36).slice(2, 8)),
    tables: TABLES,
  });
  for (const kind of ['insert', 'update']) db.on(`Player:${kind}`, upsertPlayer);
  db.on('Player:delete', (row) => {
    players.delete(pkConnection(row));
    noteUpdate();
  });
  db.onError(() => ($('dot').dataset.state = 'error'));
  myIdentity = [...db.identity].map((b) => b.toString(16).padStart(2, '0')).join('');
  await db.subscribe(['SELECT * FROM Player']);
  // Seed from the InitialData snapshot — players already in the world when
  // this page loaded; the per-row events keep the map fresh from here on.
  for (const row of db.cache.rows('Player')) upsertPlayer(row);
  // Spawn immediately; the avatar row comes back through the subscription
  // like everyone else's, and `mine` matches on identity.
  await db.callReducer('move_player', [Math.round(me.x), Math.round(me.y)]);
  $('dot').dataset.state = 'connected';
  $('s-status').textContent = 'connected — steer with WASD or click';
} catch (err) {
  $('dot').dataset.state = 'error';
  $('s-status').textContent = `connect failed: ${err}`;
  console.error('[mmo]', err);
}

window.addEventListener('pagehide', () => db?.close());
resize();
requestAnimationFrame(frame);
