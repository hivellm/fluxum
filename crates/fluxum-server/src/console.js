"use strict";
const $ = (id) => document.getElementById(id);
let token = sessionStorage.getItem("fluxum_operator") || "";
let schemaDoc = null;
let healthDoc = null;

function headers() {
  return token ? { "Fluxum-Operator": token } : {};
}
async function api(path, init) {
  const opts = Object.assign({ cache: "no-store" }, init || {});
  opts.headers = Object.assign({}, headers(), opts.headers || {});
  return fetch(path, opts);
}
async function apiJson(path, init) {
  const r = await api(path, init);
  const body = await r.json().catch(() => ({}));
  if (body && body.success === false) throw new Error(body.error || ("HTTP " + r.status));
  if (!r.ok) throw new Error("HTTP " + r.status);
  return body.payload !== undefined ? body.payload : body;
}
function toast(kind, msg) {
  const t = document.createElement("div");
  t.className = "toast " + kind;
  t.textContent = msg;
  $("toasts").appendChild(t);
  setTimeout(() => t.remove(), 3200);
}

// --- view switching ----------------------------------------------------------
const VIEW_TITLES = { overview: "Overview", data: "Data", query: "Query",
  reducers: "Reducers", live: "Live", logs: "Logs", metrics: "Metrics",
  schema: "Schema", designer: "New table" };
function showView(view) {
  document.querySelectorAll("nav.views button").forEach((b) => b.classList.toggle("on", b.dataset.view === view));
  document.querySelectorAll(".view").forEach((s) => s.classList.toggle("on", s.id === "view-" + view));
  $("view-title").textContent = VIEW_TITLES[view] || view;
  // The topbar mirrors the active nav item's icon (route icon + title).
  const active = document.querySelector("nav.views button.on svg");
  $("route-ic").innerHTML = active ? active.outerHTML : "";
  if (view === "overview") { renderOverview(); refreshOverview(); }
  if (view === "metrics" && !metricRows.length) loadMetrics();
  if (view === "designer") renderDesigner();
}
$("nav").addEventListener("click", (e) => {
  const btn = e.target.closest("button");
  if (btn && btn.dataset.view) showView(btn.dataset.view);
});

// --- health header -----------------------------------------------------------
function fmtUp(s) {
  if (s < 90) return s + "s";
  if (s < 5400) return Math.round(s / 60) + "m";
  if (s < 129600) return Math.round(s / 3600) + "h";
  return Math.round(s / 86400) + "d";
}
async function pollHealth() {
  try {
    const r = await api("/health");
    const h = await r.json();
    healthDoc = h;
    const state = (h.shards && h.shards[0] && h.shards[0].state) || h.status || "?";
    $("h-state").textContent = state;
    $("h-dot").className = "dot " + (h.status === "ok" ? "ok" : h.status === "degraded" ? "warn" : "err");
    $("h-up").textContent = fmtUp(h.uptime_s || 0);
    $("h-conns").textContent = h.connections != null ? h.connections : "–";
    $("h-tx").textContent = (h.shards && h.shards[0] && h.shards[0].tx_id) != null ? h.shards[0].tx_id : "–";
    $("h-tls").textContent = h.tls ? "tls on" : "tls off";
    $("conn-name").textContent = location.host || "localhost";
  } catch { $("h-dot").className = "dot err"; $("h-state").textContent = "unreachable"; }
  // A visible overview rides the same 5 s cadence (metrics + re-render).
  if (document.querySelector("#view-overview.on")) refreshOverview();
}

// --- auth boot (DEV-031) ------------------------------------------------------
async function boot() {
  let state = { console_open: true, authed: false };
  try { state = await apiJson("/console/state"); } catch { /* network gate refusals land here */ }
  $("h-mode").textContent = state.console_open ? "development" : "production";
  const locked = !state.console_open && !state.authed;
  $("login").classList.toggle("on", locked);
  if (!locked) {
    pollHealth();
    setInterval(pollHealth, 5000);
    loadSchema();
  }
}
$("login-save").addEventListener("click", async () => {
  token = $("login-token").value.trim();
  sessionStorage.setItem("fluxum_operator", token);
  const state = await apiJson("/console/state").catch(() => null);
  if (state && (state.authed || state.console_open)) { $("login-err").textContent = ""; boot(); }
  else $("login-err").textContent = "not accepted";
});
$("btn-token").addEventListener("click", () => {
  $("login-token").value = token;
  $("login").classList.add("on");
});
$("login").addEventListener("click", (e) => { if (e.target === $("login")) $("login").classList.remove("on"); });

// --- overview (health + metrics panels) --------------------------------------
function dom(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}
function fmtBytes(n) {
  if (n == null || !isFinite(Number(n))) return "–";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = Number(n), i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
  return (i ? v.toFixed(1) : String(v)) + " " + units[i];
}
// Sum a Prometheus series across its label sets (e.g. per-shard gauges).
function metric(name) {
  let sum = null;
  for (const r of metricRows) {
    if (r.series === name || r.series.startsWith(name + "{")) sum = (sum || 0) + Number(r.value);
  }
  return sum;
}
// label value -> summed number, for series like fluxum_table_rows{shard,table}.
function metricByLabel(name, label) {
  const out = new Map();
  const re = new RegExp("^" + name + "\\{.*" + label + '="([^"]*)"');
  for (const r of metricRows) {
    const m = re.exec(r.series);
    if (m) out.set(m[1], (out.get(m[1]) || 0) + Number(r.value));
  }
  return out;
}
function kvRow(k, v, src) {
  const row = dom("div", "kv");
  const key = dom("span", "k", k);
  key.title = k;
  const val = dom("span", "v", v);
  val.title = v; // full value on hover — long paths ellipsize
  row.append(key, val);
  if (src) row.appendChild(dom("span", "src", src));
  return row;
}
function panel(title) {
  const p = dom("div", "panel");
  p.appendChild(dom("h3", null, title));
  return p;
}
function renderOverview() {
  const tiles = $("o-tiles");
  const panels = $("o-panels");
  tiles.textContent = "";
  panels.textContent = "";
  const h = healthDoc;
  if (!h) { tiles.appendChild(dom("div", "muted", "waiting for /health…")); return; }
  const shard = (h.shards && h.shards[0]) || {};
  const tile = (k, v, sub) => {
    const t = dom("div", "tile");
    t.appendChild(dom("div", "k", k));
    const val = dom("div", "v", v);
    if (sub) val.appendChild(dom("span", "sub", sub));
    t.appendChild(val);
    tiles.appendChild(t);
  };
  tile("Status", h.status || "?", shard.state);
  tile("Uptime", fmtUp(h.uptime_s || 0));
  tile("Connections", String(h.connections != null ? h.connections : "–"));
  tile("Last tx", String(shard.tx_id != null ? shard.tx_id : "–"));
  tile("Queue depth", String(shard.queue_depth != null ? shard.queue_depth : "–"));
  tile("TLS", h.tls ? "on" : "off");

  const cfg = h.config || null;
  // Memory & buffer pool (TIER-002..005): budget vs the pool's own accounting.
  const mem = panel("Memory");
  if (cfg && cfg.memory_budget_bytes) {
    mem.appendChild(kvRow("memory.budget", fmtBytes(cfg.memory_budget_bytes.value), cfg.memory_budget_bytes.source));
    mem.appendChild(kvRow("buffer-pool capacity", fmtBytes(cfg.bufferpool_capacity_bytes.value), cfg.bufferpool_capacity_bytes.source));
  }
  const used = metric("fluxum_bufferpool_bytes");
  const cap = metric("fluxum_bufferpool_capacity_bytes");
  if (used != null && cap) {
    mem.appendChild(kvRow("pool resident", fmtBytes(used) + " / " + fmtBytes(cap)));
    const bar = dom("div", "bar");
    const fill = document.createElement("i");
    fill.style.width = Math.min(100, (used / cap) * 100).toFixed(1) + "%";
    bar.appendChild(fill);
    mem.appendChild(bar);
  }
  const est = metric("fluxum_memstore_bytes");
  if (est != null) mem.appendChild(kvRow("memstore estimate", fmtBytes(est)));
  const reclaim = metric("fluxum_reclaim_pending_pages");
  if (reclaim != null) mem.appendChild(kvRow("reclaim pending pages", String(reclaim)));
  panels.appendChild(mem);

  // Shard + replication (OBS-060, REP-080).
  const sh = panel("Shard");
  sh.appendChild(kvRow("id", String(shard.id != null ? shard.id : "–")));
  sh.appendChild(kvRow("state", shard.state || "–"));
  sh.appendChild(kvRow("queue depth", String(shard.queue_depth != null ? shard.queue_depth : "–")));
  const repl = shard.replication;
  if (repl) {
    sh.appendChild(kvRow("role", repl.role + " (epoch " + repl.epoch + ")"));
    if (repl.role === "primary") {
      sh.appendChild(kvRow("connected replicas", String(repl.connected_replicas != null ? repl.connected_replicas : 0)));
      sh.appendChild(kvRow("zero-loss guarantee", repl.degraded ? "suspended (degraded)" : "in force"));
    } else {
      if (repl.primary) sh.appendChild(kvRow("primary", repl.primary));
      if (repl.acked_tx_id != null) sh.appendChild(kvRow("acked tx", String(repl.acked_tx_id)));
      if (repl.lag_tx != null) sh.appendChild(kvRow("lag (tx)", String(repl.lag_tx)));
      sh.appendChild(kvRow("reads", repl.stale ? "stale" : "fresh"));
    }
  } else {
    sh.appendChild(kvRow("replication", "standalone"));
  }
  panels.appendChild(sh);

  // Hardware probe + derived values with provenance (HWA-013).
  if (cfg) {
    const hw = panel("Hardware & derived config");
    const probe = cfg.hardware || {};
    hw.appendChild(kvRow("cores", probe.logical_cores + " logical / " + probe.physical_cores + " physical"));
    hw.appendChild(kvRow("RAM", fmtBytes(probe.total_ram_bytes) + " total, " + fmtBytes(probe.available_ram_bytes) + " free"));
    if (probe.cgroup_cpu_quota != null) hw.appendChild(kvRow("cgroup CPU quota", String(probe.cgroup_cpu_quota)));
    if (probe.cgroup_memory_limit_bytes != null) hw.appendChild(kvRow("cgroup memory limit", fmtBytes(probe.cgroup_memory_limit_bytes)));
    const derived = [
      ["worker threads", "worker_threads", String],
      ["shards", "shards", String],
      ["fan-out concurrency", "fanout_concurrency", String],
      ["commit-log write buffer", "commit_log_write_buffer_bytes", fmtBytes],
      ["checkpoint interval (tx)", "checkpoint_interval_tx", String],
      ["SIMD", "simd", String],
    ];
    for (const [label, key, fmt] of derived) {
      const d = cfg[key];
      if (d && d.value !== undefined) hw.appendChild(kvRow(label, fmt(d.value), d.source));
    }
    panels.appendChild(hw);
  }

  // Resolved on-disk locations (the only place they are readable back).
  // Entries are `{value, source}` like the derived config values.
  if (h.storage && typeof h.storage === "object") {
    const st = panel("Storage");
    for (const [k, v] of Object.entries(h.storage)) {
      if (v && typeof v === "object" && v.value !== undefined) {
        st.appendChild(kvRow(k, String(v.value), v.source));
      } else {
        st.appendChild(kvRow(k, typeof v === "object" ? JSON.stringify(v) : String(v)));
      }
    }
    panels.appendChild(st);
  }

  // Committed rows per table (OBS-030), summed across shards.
  const rows = metricByLabel("fluxum_table_rows", "table");
  if (rows.size) {
    const tp = panel("Table rows");
    for (const [name, n] of [...rows.entries()].sort((a, b) => b[1] - a[1])) {
      tp.appendChild(kvRow(name, String(n)));
    }
    panels.appendChild(tp);
  }
}
async function refreshOverview() {
  await fetchMetrics().catch(() => {});
  renderOverview();
}

// --- column-type helpers ------------------------------------------------------
// Schema "type" strings are the Rust FluxType debug form: "U64", "Str",
// "Option(Str)", "List(U32)", "Enum(...)", "Struct(...)".
function parseTy(s) {
  let optional = false, list = false, inner = s;
  if (inner.startsWith("Option(") && inner.endsWith(")")) { optional = true; inner = inner.slice(7, -1); }
  if (inner.startsWith("List(") && inner.endsWith(")")) { list = true; inner = inner.slice(5, -1); }
  return { optional, list, base: inner };
}
function baseEditable(base) {
  return !(base.startsWith("Enum(") || base.startsWith("Struct(") || base === "Blob" || base === "CrdtText");
}
function tyEditable(s) { return baseEditable(parseTy(s).base); }
const INT_TYPES = ["I8", "I16", "I32", "I64", "U8", "U16", "U32", "U64", "EntityId", "ConnectionId", "Timestamp"];
// Turn one input string into the JSON the /rows endpoint expects.
function inputToJson(base, raw) {
  if (base === "Bool") return raw === true;
  const s = String(raw).trim();
  if (INT_TYPES.includes(base)) {
    if (!/^[+-]?\d+$/.test(s)) throw new Error("expected an integer");
    const n = Number(s);
    return Number.isSafeInteger(n) ? n : s; // big 64-bit values ride as strings
  }
  if (base === "F32" || base === "F64") {
    const n = Number(s);
    if (!Number.isFinite(n)) throw new Error("expected a number");
    return n;
  }
  return s; // Str, Bytes(hex), Identity(hex), Decimal — strings on the wire
}
function fieldToJson(ty, field) {
  const t = parseTy(ty);
  if (t.optional && field.isNull()) return null;
  if (t.list) {
    const parsed = JSON.parse(field.value() || "[]");
    if (!Array.isArray(parsed)) throw new Error("expected a JSON array");
    return parsed.map((item) => (t.base === "Bool" ? item === true : inputToJson(t.base, item)));
  }
  if (t.base === "Bool") return field.checked();
  return inputToJson(t.base, field.value());
}

// --- data grid -----------------------------------------------------------------
function renderGrid(el, columns, rows, opts) {
  const meta = (opts && opts.meta) || null;
  el.textContent = "";
  const t = document.createElement("table");
  t.className = "grid";
  const thead = t.createTHead().insertRow();
  for (const c of columns) {
    const th = document.createElement("th");
    th.textContent = c;
    if (meta && meta[c]) {
      const ty = document.createElement("span");
      ty.className = "ty";
      ty.textContent = meta[c];
      th.appendChild(ty);
    }
    thead.appendChild(th);
  }
  const tb = t.createTBody();
  for (const row of rows) {
    const tr = tb.insertRow();
    if (opts && opts.onRow) {
      tr.className = "click";
      tr.addEventListener("click", () => opts.onRow(row));
      tr.title = "click to edit";
    }
    for (const c of columns) {
      const v = row[c];
      const cell = tr.insertCell();
      if (v === null || v === undefined) { cell.textContent = "null"; cell.className = "null"; }
      else cell.textContent = typeof v === "object" ? JSON.stringify(v) : String(v);
    }
  }
  el.appendChild(t);
}

// --- tables sidebar + data view --------------------------------------------------
let currentTable = null;
function tableSchema(name) {
  return (schemaDoc && schemaDoc.tables || []).find((t) => t.name === name) || null;
}
async function loadSchema() {
  try {
    schemaDoc = await apiJson("/schema");
    const list = $("tbl-list");
    list.textContent = "";
    const sel = $("w-table");
    while (sel.options.length > 1) sel.remove(1);
    const tables = schemaDoc.tables || [];
    $("tbl-n").textContent = tables.length;
    for (const t of tables) {
      const item = document.createElement("div");
      item.className = "item";
      const glyph = document.createElement("span");
      glyph.className = "glyph";
      glyph.textContent = "▦";
      const name = document.createElement("span");
      name.className = "tname";
      name.textContent = t.name;
      const count = document.createElement("span");
      count.className = "count";
      count.textContent = t.columns.length + " col";
      item.append(glyph, name, count);
      item.addEventListener("click", () => { currentTable = t.name; showView("data"); browse(); });
      list.appendChild(item);
      sel.add(new Option(t.name, t.name));
    }
    $("s-json").textContent = JSON.stringify(schemaDoc, null, 2);
    renderSchemaDoc();
    renderReducerList();
    renderAuditTables();
    if (!currentTable && tables.length) { currentTable = tables[0].name; browse(); }
  } catch (e) { $("tbl-list").textContent = String(e.message || e); }
}
async function browse() {
  if (!currentTable) return;
  document.querySelectorAll("#tbl-list .item").forEach((i) =>
    i.classList.toggle("on", i.querySelector(".tname").textContent === currentTable));
  $("tbl-name").textContent = currentTable;
  const schema = tableSchema(currentTable);
  const editableTable = schema && schema.columns.every((c) => tyEditable(c.type));
  $("tbl-new").disabled = !editableTable;
  $("tbl-hint").textContent = editableTable ? "" : "structured columns — edit via reducers";
  const limit = parseInt($("tbl-limit").value, 10) || 50;
  try {
    const p = await apiJson("/query", { method: "POST", body: JSON.stringify({ sql: "SELECT * FROM " + currentTable + " LIMIT " + limit }) });
    const meta = {};
    if (schema) for (const c of schema.columns) meta[c.name] = c.type;
    renderGrid($("tbl-grid"), p.columns, p.rows, {
      meta,
      onRow: editableTable ? (row) => openEditor(row) : null,
    });
    $("tbl-count").textContent = p.rows.length + " row(s)";
  } catch (e) { $("tbl-grid").textContent = ""; $("tbl-count").textContent = String(e.message || e); }
}
$("tbl-refresh").addEventListener("click", browse);
$("tbl-new").addEventListener("click", () => openEditor(null));

// --- row editor (POST /rows) ------------------------------------------------------
let editorFields = [];   // [{name, ty, isNull(), value(), checked()}]
let editorOriginal = null; // the pre-edit row (delete uses it)
function openEditor(row) {
  const schema = tableSchema(currentTable);
  if (!schema) return;
  editorOriginal = row;
  editorFields = [];
  $("ed-title").textContent = row ? "Edit row" : "New row";
  $("ed-table").textContent = currentTable;
  $("ed-delete").style.display = row ? "" : "none";
  $("ed-delete").textContent = "Delete";
  $("ed-err").textContent = "";
  const box = $("ed-fields");
  box.textContent = "";
  const pkOrdinals = new Set(schema.primary_key || []);
  const autoInc = schema.auto_inc || null;
  schema.columns.forEach((col, ordinal) => {
    const t = parseTy(col.type);
    const field = document.createElement("div");
    field.className = "field";
    const label = document.createElement("label");
    label.textContent = col.name + " ";
    const ty = document.createElement("span");
    ty.className = "ty";
    ty.textContent = col.type;
    label.appendChild(ty);
    if (pkOrdinals.has(ordinal)) { const b = document.createElement("span"); b.className = "badge pk"; b.textContent = "pk"; label.appendChild(b); }
    if (autoInc === col.name) { const b = document.createElement("span"); b.className = "badge auto"; b.textContent = "auto"; label.appendChild(b); }
    if (t.optional) { const b = document.createElement("span"); b.className = "badge opt"; b.textContent = "opt"; label.appendChild(b); }
    field.appendChild(label);

    const current = row ? row[col.name] : undefined;
    let getValue, getChecked, getNull = () => false;
    if (!baseEditable(t.base)) {
      const locked = document.createElement("div");
      locked.className = "locked";
      locked.textContent = "structured type — edit via reducers";
      field.appendChild(locked);
      getValue = () => { throw new Error("uneditable column"); };
    } else if (t.base === "Bool" && !t.list) {
      const wrap = document.createElement("label");
      wrap.className = "checkwrap";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = current === true;
      wrap.append(cb, document.createTextNode(" true"));
      field.appendChild(wrap);
      getChecked = () => cb.checked;
      getValue = () => cb.checked;
    } else {
      const input = document.createElement("input");
      input.type = "text";
      input.spellcheck = false;
      let initial = "";
      if (current !== undefined && current !== null) {
        initial = typeof current === "object" ? JSON.stringify(current) : String(current);
      } else if (!row && autoInc === col.name) {
        initial = "0";
        input.placeholder = "0 = assign automatically";
      } else if (t.list) {
        initial = row ? "" : "[]";
      }
      input.value = initial;
      if (t.optional) {
        const wrap = document.createElement("div");
        wrap.className = "nullrow";
        const nullCb = document.createElement("input");
        nullCb.type = "checkbox";
        nullCb.checked = row ? current === null : false;
        input.disabled = nullCb.checked;
        nullCb.addEventListener("change", () => { input.disabled = nullCb.checked; });
        const nullLabel = document.createElement("label");
        nullLabel.className = "checkwrap";
        nullLabel.append(nullCb, document.createTextNode(" null"));
        wrap.append(input, nullLabel);
        field.appendChild(wrap);
        getNull = () => nullCb.checked;
      } else {
        field.appendChild(input);
      }
      getValue = () => input.value;
    }
    editorFields.push({ name: col.name, ty: col.type, value: getValue, checked: getChecked || (() => false), isNull: getNull });
    box.appendChild(field);
  });
  $("overlay").classList.add("on");
}
function closeEditor() { $("overlay").classList.remove("on"); }
$("ed-close").addEventListener("click", closeEditor);
$("overlay").addEventListener("click", (e) => { if (e.target === $("overlay")) closeEditor(); });

async function rowsCall(op, rowObj) {
  return apiJson("/rows", {
    method: "POST",
    body: JSON.stringify({ table: currentTable, op, row: rowObj }),
  });
}
$("ed-save").addEventListener("click", async () => {
  $("ed-err").textContent = "";
  const rowObj = {};
  try {
    for (const f of editorFields) rowObj[f.name] = fieldToJson(f.ty, f);
  } catch (e) { $("ed-err").textContent = String(e.message || e); return; }
  try {
    const r = await rowsCall("upsert", rowObj);
    toast("ok", "Row saved (tx " + r.tx_id + ")");
    closeEditor();
    browse();
  } catch (e) { $("ed-err").textContent = String(e.message || e); }
});
$("ed-delete").addEventListener("click", async () => {
  // Two-step confirm, phpMyAdmin style but inline.
  if ($("ed-delete").textContent === "Delete") { $("ed-delete").textContent = "Confirm delete?"; return; }
  $("ed-err").textContent = "";
  try {
    const r = await rowsCall("delete", editorOriginal || {});
    toast("ok", "Row deleted (tx " + r.tx_id + ")");
    closeEditor();
    browse();
  } catch (e) { $("ed-err").textContent = String(e.message || e); }
});

// --- query view (SQL console: EXPLAIN, keyset paging, live mode, inspector) ---------
let lastQuery = null; // { sql, table, columns, rows, orderCol, limit }
function parseQuerySql(sql) {
  const order = /\bORDER\s+BY\s+([A-Za-z_][A-Za-z0-9_]*)/i.exec(sql);
  const limit = /\bLIMIT\s+(\d+)/i.exec(sql);
  return { orderCol: order ? order[1] : null, limit: limit ? parseInt(limit[1], 10) : null };
}
// One primary-key column name, or null — the AFTER cursor needs it (QP-041).
function singlePk(table) {
  const schema = tableSchema(table);
  const pk = schema ? schema.primary_key || [] : [];
  return pk.length === 1 ? schema.columns[pk[0]].name : null;
}
// JSON value → SQL literal, typed by the schema column (64-bit values ride
// as JSON strings but are numeric literals in SQL).
function sqlLiteral(table, column, value) {
  if (value === null || value === undefined) return "NULL";
  const schema = tableSchema(table);
  const col = schema && schema.columns.find((c) => c.name === column);
  const base = col ? parseTy(col.type).base : "Str";
  if (base === "Bool") return value ? "TRUE" : "FALSE";
  if (INT_TYPES.includes(base) || base === "F32" || base === "F64") return String(value);
  return "'" + String(value).replace(/'/g, "''") + "'";
}
async function runQuery(sql) {
  if (typeof sql !== "string") sql = $("q-sql").value;
  $("q-err").textContent = ""; $("q-count").textContent = "";
  try {
    const p = await apiJson("/query", { method: "POST", body: JSON.stringify({ sql }) });
    const parsed = parseQuerySql(sql);
    lastQuery = { sql, table: p.table, columns: p.columns, rows: p.rows,
      orderCol: parsed.orderCol, limit: parsed.limit };
    const meta = {};
    const schema = tableSchema(p.table);
    if (schema) for (const c of schema.columns) meta[c.name] = c.type;
    renderGrid($("q-grid"), p.columns, p.rows, { meta, onRow: (row) => openInspector(p.table, row) });
    $("q-count").textContent = p.rows.length + " row(s)";
    // Next page: a full page, an ORDER BY column, and a single-column pk.
    $("q-next").disabled = !(parsed.orderCol && parsed.limit && p.rows.length >= parsed.limit
      && singlePk(p.table));
  } catch (e) {
    lastQuery = null;
    $("q-next").disabled = true;
    $("q-grid").textContent = ""; $("q-err").textContent = String(e.message || e);
  }
  return lastQuery !== null;
}
$("q-run").addEventListener("click", () => { liveStop(); runQuery(); });
$("q-sql").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { liveStop(); runQuery(); }
});
$("q-next").addEventListener("click", () => {
  const q = lastQuery;
  if (!q || !q.rows.length || !q.orderCol) return;
  const pk = singlePk(q.table);
  if (!pk) return;
  const last = q.rows[q.rows.length - 1];
  const cursor = " AFTER (" + sqlLiteral(q.table, q.orderCol, last[q.orderCol]) + ", "
    + sqlLiteral(q.table, pk, last[pk]) + ")";
  const sql = q.sql.replace(/\s*AFTER\s*\([^)]*\)\s*$/i, "").trimEnd() + cursor;
  $("q-sql").value = sql;
  runQuery(sql);
});

// EXPLAIN (QP-051): compile-only — access path, bounds, residual, order.
function explainRows(rep) {
  const rows = [["table", rep.table]];
  const access = rep.access || {};
  if (access.kind === "index_scan") {
    rows.push(["access", "index_scan on (" + (access.index || []).join(", ") + ")"]);
    rows.push(["probes", String(access.probes) + " (equality prefix " + access.equality_prefix_len + ")"]);
    if (access.lower) rows.push(["lower bound", String(access.lower)]);
    if (access.upper) rows.push(["upper bound", String(access.upper)]);
  } else {
    rows.push(["access", access.kind || "?"]);
  }
  if ((rep.residual || []).length) rows.push(["residual filter", rep.residual.join(" AND ")]);
  if (rep.order_by) {
    rows.push(["order", rep.order_by.column + (rep.order_by.descending ? " DESC" : " ASC")
      + (rep.ordered_by_index ? " — served by the index" : " — sorted at execution")]);
  }
  if (rep.limit != null) rows.push(["limit", String(rep.limit)]);
  if (rep.cursor) rows.push(["cursor", "AFTER (" + rep.cursor.order_value + ", " + rep.cursor.pk_value + ")"]);
  rows.push(["normalized", rep.normalized || ""]);
  return rows;
}
$("q-explain").addEventListener("click", async () => {
  const box = $("q-explain-box");
  box.textContent = "";
  box.style.display = "";
  const head = dom("h3", null, "Explain");
  const close = dom("span", "src", "close");
  close.style.cursor = "pointer";
  close.style.marginLeft = "auto";
  close.addEventListener("click", () => { box.style.display = "none"; });
  const hd = dom("div", "row");
  hd.append(head, close);
  box.appendChild(hd);
  try {
    const rep = await apiJson("/query/explain", {
      method: "POST", body: JSON.stringify({ sql: $("q-sql").value }),
    });
    for (const [k, v] of explainRows(rep)) box.appendChild(kvRow(k, v));
  } catch (e) { box.appendChild(dom("div", "err-box", String(e.message || e))); }
});

// Live mode: the query re-executes on every commit touching its table, via
// the /console/watch stream (DEV-031 lock discipline) — TxUpdate-driven
// refresh, debounced. A dropped-commit marker also re-runs: it means missed
// events, and re-execution reads current truth anyway.
let liveCtl = null, liveTimer = 0, liveSql = null, liveEvents = 0;
function liveStop(msg) {
  if (liveCtl) { const c = liveCtl; liveCtl = null; c.abort(); }
  liveSql = null;
  clearTimeout(liveTimer);
  $("q-live").textContent = "Go live";
  $("q-live-dot").className = "dot";
  $("q-live-status").textContent = msg || "";
}
$("q-live").addEventListener("click", async () => {
  if (liveCtl) { liveStop(); return; }
  const sql = $("q-sql").value;
  if (!(await runQuery(sql))) return; // surface the query error first
  const table = lastQuery.table;
  liveSql = sql; liveEvents = 0;
  liveCtl = new AbortController();
  $("q-live").textContent = "Stop";
  $("q-live-dot").className = "dot ok";
  $("q-live-status").textContent = "live on " + table;
  try {
    await streamLines("/console/watch?table=" + encodeURIComponent(table), liveCtl.signal, (s) => {
      const evt = JSON.parse(s);
      if (evt.watching !== undefined) return; // hello event
      liveEvents += 1;
      $("q-live-status").textContent = "live on " + table + " · " + liveEvents + " event(s)";
      clearTimeout(liveTimer);
      liveTimer = setTimeout(() => { if (liveSql) runQuery(liveSql); }, 250);
    });
    if (liveCtl) liveStop("stream ended");
  } catch (e) {
    if (e.name !== "AbortError") liveStop(String(e.message || e));
  }
});

// Row inspector: read-only typed view of one result row (edits stay in the
// Data view, which routes through POST /rows).
function openInspector(table, row) {
  $("in-table").textContent = table || "";
  const box = $("in-fields");
  box.textContent = "";
  const schema = table ? tableSchema(table) : null;
  for (const [k, v] of Object.entries(row)) {
    const col = schema && schema.columns.find((c) => c.name === k);
    const rendered = v === null ? "null" : typeof v === "object" ? JSON.stringify(v, null, 1) : String(v);
    box.appendChild(kvRow(k + (col ? " (" + col.type + ")" : ""), rendered));
  }
  $("ioverlay").classList.add("on");
}
$("in-close").addEventListener("click", () => $("ioverlay").classList.remove("on"));
$("ioverlay").addEventListener("click", (e) => { if (e.target === $("ioverlay")) $("ioverlay").classList.remove("on"); });

// --- reducer console (signature-driven forms, RPC-051 invoke, audit) ----------------
let currentReducer = null; // the selected /schema reducer descriptor
let reducerFields = [];    // [{ name, build() -> JSON arg (throws on bad input) }]
const invokeHistory = [];  // this session's invocations, latest first
// "Option<u64>" / "Vec<String>" / "u64" → { opt, vec, base } (Rust source types, SDK-001).
function parseRustTy(ty) {
  let opt = false, vec = false, base = (ty || "").trim();
  if (base.startsWith("Option<") && base.endsWith(">")) { opt = true; base = base.slice(7, -1).trim(); }
  if (base.startsWith("Vec<") && base.endsWith(">")) { vec = true; base = base.slice(4, -1).trim(); }
  return { opt, vec, base };
}
const RUST_INTS = ["u8", "u16", "u32", "u64", "usize", "i8", "i16", "i32", "i64", "isize"];
// One scalar input string → the JSON argument (json_to_flux universe: the
// admin surface maps numbers to I64/F64 and strings to Str).
function rustScalarToJson(base, raw) {
  const s = String(raw).trim();
  if (RUST_INTS.includes(base)) {
    if (!/^[+-]?\d+$/.test(s)) throw new Error("expected an integer");
    const n = Number(s);
    if (!Number.isSafeInteger(n)) throw new Error("integer too large for the JSON admin surface");
    return n;
  }
  if (base === "f32" || base === "f64") {
    const n = Number(s);
    if (!Number.isFinite(n)) throw new Error("expected a number");
    return n;
  }
  if (base === "bool") return s === "true";
  if (base === "String" || base === "&str" || base === "str") return String(raw);
  // Identity/Timestamp/module types: accept JSON if it parses, else a string.
  if (s === "") return "";
  try { return JSON.parse(s); } catch { return String(raw); }
}
function reducerField(param) {
  const t = parseRustTy(param.ty !== undefined ? param.ty : param.type);
  const field = dom("div", "field");
  const label = dom("label", null, param.name + " ");
  label.appendChild(dom("span", "ty", param.type || param.ty));
  field.appendChild(label);
  let build;
  if (t.base === "bool" && !t.vec && !t.opt) {
    const wrap = dom("label", "checkwrap");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    wrap.append(cb, document.createTextNode(" true"));
    field.appendChild(wrap);
    build = () => cb.checked;
  } else {
    const input = document.createElement("input");
    input.type = "text";
    input.spellcheck = false;
    input.style.fontFamily = "var(--mono)";
    if (t.vec) input.placeholder = "JSON array, e.g. [1, 2]";
    else if (!RUST_INTS.includes(t.base) && !["f32", "f64", "String", "&str", "str", "bool"].includes(t.base)) {
      input.placeholder = "JSON value or string";
    }
    let nullCb = null;
    if (t.opt) {
      const wrap = dom("div", "nullrow");
      nullCb = document.createElement("input");
      nullCb.type = "checkbox";
      nullCb.addEventListener("change", () => { input.disabled = nullCb.checked; });
      const nl = dom("label", "checkwrap");
      nl.append(nullCb, document.createTextNode(" null"));
      wrap.append(input, nl);
      field.appendChild(wrap);
    } else {
      field.appendChild(input);
    }
    build = () => {
      if (nullCb && nullCb.checked) return null;
      if (t.vec) {
        const parsed = JSON.parse(input.value || "[]");
        if (!Array.isArray(parsed)) throw new Error("expected a JSON array");
        return parsed.map((item) => (typeof item === "string" ? rustScalarToJson(t.base, item) : item));
      }
      if (t.base === "bool") return input.value.trim() === "true";
      return rustScalarToJson(t.base, input.value);
    };
  }
  return { field, build, name: param.name };
}
function renderReducerList() {
  const list = $("r-list");
  if (!list) return;
  list.textContent = "";
  const reducers = (schemaDoc && schemaDoc.reducers) || [];
  $("r-n").textContent = reducers.length;
  for (const r of reducers) {
    const item = dom("div", "item");
    item.appendChild(dom("span", null, r.name));
    const rate = dom("span", "rate badge " + (r.client_callable ? "auto" : "opt"),
      r.client_callable ? (r.max_rate_per_sec ? r.max_rate_per_sec + "/s" : "open") : "sched");
    item.appendChild(rate);
    item.addEventListener("click", () => selectReducer(r));
    list.appendChild(item);
  }
  if (currentReducer && !reducers.some((r) => r.name === currentReducer.name)) {
    currentReducer = null;
    $("r-title").textContent = "—";
    $("r-args").textContent = "";
    $("r-invoke").disabled = true;
  }
}
function selectReducer(r) {
  currentReducer = r;
  document.querySelectorAll("#r-list .item").forEach((i) =>
    i.classList.toggle("on", i.firstChild.textContent === r.name));
  const params = r.params || [];
  $("r-title").textContent = r.name + "(" + params.map((p) => p.name).join(", ") + ")";
  const meta = $("r-meta");
  meta.textContent = "";
  meta.appendChild(dom("span", "badge " + (r.client_callable ? "auto" : "opt"),
    r.client_callable ? "client callable" : "schedule-only"));
  meta.appendChild(dom("span", "badge opt",
    r.max_rate_per_sec ? "rate " + r.max_rate_per_sec + "/s" : "no rate limit"));
  if (r.return_type) meta.appendChild(dom("span", "mut2", "returns " + r.return_type));
  const box = $("r-args");
  box.textContent = "";
  reducerFields = [];
  for (const p of params) {
    const f = reducerField(p);
    reducerFields.push(f);
    box.appendChild(f.field);
  }
  if (!params.length) box.appendChild(dom("div", "muted", "no arguments"));
  $("r-invoke").disabled = !r.client_callable; // the server refuses 403 anyway (F-004)
  $("r-err").textContent = r.client_callable ? "" : "schedule-only — not invocable over HTTP";
}
function pushHistory(entry) {
  invokeHistory.unshift(entry);
  if (invokeHistory.length > 20) invokeHistory.pop();
  const box = $("r-history");
  $("r-history-head").style.display = "";
  box.textContent = "";
  for (const h of invokeHistory) {
    const line = dom("div", "hist-line " + (h.ok ? "ok" : "fail"),
      h.at + "  " + h.reducer + "(" + h.args + ") — " + h.outcome);
    box.appendChild(line);
  }
}
$("r-invoke").addEventListener("click", async () => {
  if (!currentReducer) return;
  $("r-err").textContent = "";
  const args = [];
  try {
    for (const f of reducerFields) args.push(f.build());
  } catch (e) {
    $("r-err").textContent = String(e.message || e);
    return;
  }
  const preview = args.map((a) => JSON.stringify(a)).join(", ");
  const at = new Date().toTimeString().slice(0, 8);
  try {
    await apiJson("/reducer/" + encodeURIComponent(currentReducer.name), {
      method: "POST", body: JSON.stringify(args),
    });
    toast("ok", currentReducer.name + " committed");
    pushHistory({ at, reducer: currentReducer.name, args: preview, ok: true, outcome: "committed" });
    if (document.querySelector("#view-data.on")) browse();
  } catch (e) {
    const msg = String(e.message || e);
    $("r-err").textContent = msg;
    pushHistory({ at, reducer: currentReducer.name, args: preview, ok: false, outcome: msg });
  }
});

// The audit-trail panel (OPS-020/021): commit provenance for a table —
// tx, time, caller, reducer, row deltas. Metadata only, and it requires a
// server-peer operator token; without one the panel says so instead of
// pretending to be empty.
function renderAuditTables() {
  const sel = $("a-table");
  if (!sel) return;
  const was = sel.value;
  sel.textContent = "";
  for (const t of (schemaDoc && schemaDoc.tables) || []) sel.add(new Option(t.name, t.name));
  if (was) sel.value = was;
}
$("a-refresh").addEventListener("click", async () => {
  const note = $("a-note");
  const grid = $("a-grid");
  grid.textContent = "";
  if (!token) {
    note.textContent = "audit requires a server-peer operator token (OPS-021) — set one via the Operator token button";
    return;
  }
  note.textContent = "";
  const table = $("a-table").value;
  const limit = parseInt($("a-limit").value, 10) || 20;
  try {
    const p = await apiJson("/audit", {
      method: "POST",
      body: JSON.stringify({ token, table, limit }),
    });
    const rows = (p.entries || []).map((e) => ({
      tx: e.tx_id,
      time: e.timestamp ? new Date(e.timestamp / 1000).toISOString().replace("T", " ").slice(0, 19) : "",
      caller: (e.caller || "").slice(0, 12),
      reducer: e.reducer_name || "(internal)",
      // inserted/deleted are booleans: both = an in-place update.
      change: e.inserted && e.deleted ? "update" : e.inserted ? "insert" : e.deleted ? "delete" : "?",
    }));
    renderGrid(grid, ["tx", "time", "caller", "reducer", "change"], rows, null);
    note.textContent = p.count + " entr" + (p.count === 1 ? "y" : "ies");
  } catch (e) { note.textContent = String(e.message || e); }
});

// --- NDJSON stream reader -----------------------------------------------------------
async function streamLines(path, signal, onLine) {
  const r = await api(path, { signal });
  if (!r.ok) throw new Error("HTTP " + r.status + (r.status === 401 ? " — operator token required" : ""));
  const reader = r.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    const parts = buf.split("\n");
    buf = parts.pop();
    for (const line of parts) {
      const s = line.trim();
      if (s) onLine(s);
    }
  }
}

// --- live view (DEV-030 diff viewer) -------------------------------------------------
let watchCtl = null, watchCount = 0;
function watchStopped(msg) {
  watchCtl = null;
  $("w-toggle").textContent = "Start";
  $("w-dot").className = "dot";
  $("w-status").textContent = msg || "stopped";
}
function addEvent(evt) {
  const feed = $("w-feed");
  const card = document.createElement("div");
  card.className = "evt";
  const hd = document.createElement("div");
  hd.className = "hd";
  const caller = (evt.caller || "").slice(0, 12);
  hd.innerHTML = "<b>tx " + Number(evt.tx_id) + "</b> · " +
    (evt.reducer ? "reducer <b>" + escHtml(evt.reducer) + "</b>" : "internal") +
    (caller ? " · caller " + escHtml(caller) + "…" : "");
  card.appendChild(hd);
  const body = document.createElement("div");
  body.className = "body";
  for (const t of evt.tables || []) {
    for (const row of t.inserts || []) {
      const d = document.createElement("div");
      d.className = "rowline ins";
      d.textContent = "+ " + t.table + " " + JSON.stringify(row);
      body.appendChild(d);
    }
    for (const row of t.deletes || []) {
      const d = document.createElement("div");
      d.className = "rowline del";
      d.textContent = "− " + t.table + " " + JSON.stringify(row);
      body.appendChild(d);
    }
  }
  card.appendChild(body);
  feed.appendChild(card);
  while (feed.childElementCount > 300) feed.removeChild(feed.firstChild);
  feed.scrollTop = feed.scrollHeight;
  watchCount += 1;
  $("w-count").textContent = watchCount + " event(s)";
}
function escHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
$("w-toggle").addEventListener("click", async () => {
  if (watchCtl) { watchCtl.abort(); watchStopped(); return; }
  watchCtl = new AbortController();
  $("w-toggle").textContent = "Stop";
  $("w-dot").className = "dot ok";
  const table = $("w-table").value;
  $("w-status").textContent = "watching " + (table || "all tables");
  try {
    await streamLines("/console/watch" + (table ? "?table=" + encodeURIComponent(table) : ""), watchCtl.signal, (s) => {
      const evt = JSON.parse(s);
      if (evt.fluxum_watch_dropped) {
        const d = document.createElement("div");
        d.className = "muted";
        d.style.padding = "4px 10px";
        d.textContent = "⚠ " + evt.fluxum_watch_dropped + " commit(s) dropped (slow consumer)";
        $("w-feed").appendChild(d);
        return;
      }
      if (evt.watching !== undefined) return; // hello event
      addEvent(evt);
    });
    watchStopped("stream ended");
  } catch (e) {
    if (e.name !== "AbortError") watchStopped(String(e.message || e));
  }
});
$("w-clear").addEventListener("click", () => { $("w-feed").textContent = ""; watchCount = 0; $("w-count").textContent = ""; });

// --- logs view (DEV-032) --------------------------------------------------------------
let logCtl = null;
function logLineMatches(obj, raw) {
  const filter = $("l-filter").value.toLowerCase();
  if (filter && !raw.toLowerCase().includes(filter)) return false;
  if ($("l-reducers").checked) {
    const f = obj.fields || {};
    if (f.reducer === undefined && f.reducer_name === undefined) return false;
  }
  return true;
}
function addLogLine(raw) {
  let obj = {};
  try { obj = JSON.parse(raw); } catch { /* keepalive or marker */ }
  if (obj.fluxum_logs_dropped) {
    const d = document.createElement("div");
    d.className = "log-line";
    d.textContent = "⚠ " + obj.fluxum_logs_dropped + " line(s) dropped";
    $("l-feed").appendChild(d);
    return;
  }
  if (!logLineMatches(obj, raw)) return;
  const f = obj.fields || {};
  const slow = f.event === "slow_reducer";
  const div = document.createElement("div");
  div.className = "log-line" + (slow ? " slow" : "");
  const lvl = document.createElement("span");
  lvl.className = "lvl lvl-" + (obj.level || "");
  lvl.textContent = (obj.level || "?").padEnd(5);
  const tgt = document.createElement("span");
  tgt.className = "tgt";
  tgt.textContent = " " + (obj.timestamp || "").slice(11, 23) + " " + (obj.target || "") + " ";
  const extras = Object.keys(f).filter((k) => k !== "message").map((k) => k + "=" + JSON.stringify(f[k])).join(" ");
  const msg = document.createElement("span");
  msg.textContent = (f.message || "") + (extras ? "  " + extras : "");
  div.append(lvl, tgt, msg);
  const feed = $("l-feed");
  feed.appendChild(div);
  while (feed.childElementCount > 1000) feed.removeChild(feed.firstChild);
  feed.scrollTop = feed.scrollHeight;
}
$("l-toggle").addEventListener("click", async () => {
  if (logCtl) { logCtl.abort(); logCtl = null; $("l-toggle").textContent = "Follow"; return; }
  logCtl = new AbortController();
  $("l-toggle").textContent = "Stop";
  try {
    await streamLines("/logs?follow=1", logCtl.signal, addLogLine);
  } catch (e) {
    if (e.name !== "AbortError") addLogLine(JSON.stringify({ level: "ERROR", fields: { message: String(e.message || e) } }));
  }
  logCtl = null;
  $("l-toggle").textContent = "Follow";
});
$("l-clear").addEventListener("click", () => { $("l-feed").textContent = ""; });

// --- metrics view -----------------------------------------------------------------------
let metricRows = [];
function renderMetrics() {
  const filter = $("m-filter").value.toLowerCase();
  const rows = metricRows.filter((r) => !filter || r.series.toLowerCase().includes(filter));
  const el = $("m-grid");
  el.textContent = "";
  const t = document.createElement("table");
  t.className = "grid";
  const hd = t.createTHead().insertRow();
  for (const h of ["series", "value"]) { const th = document.createElement("th"); th.textContent = h; hd.appendChild(th); }
  const tb = t.createTBody();
  for (const r of rows) {
    const tr = tb.insertRow();
    tr.insertCell().textContent = r.series;
    tr.insertCell().textContent = r.value;
  }
  el.appendChild(t);
  $("m-count").textContent = rows.length + " series";
}
async function fetchMetrics() {
  const r = await api("/metrics");
  const text = await r.text();
  metricRows = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim(); // exposition continuation lines carry indentation
    if (!line || line.startsWith("#")) continue;
    const at = line.lastIndexOf(" ");
    if (at > 0) metricRows.push({ series: line.slice(0, at), value: line.slice(at + 1) });
  }
}
async function loadMetrics() {
  try {
    await fetchMetrics();
    renderMetrics();
  } catch (e) { $("m-grid").textContent = String(e.message || e); }
}
$("m-refresh").addEventListener("click", loadMetrics);
$("m-filter").addEventListener("input", renderMetrics);

// --- schema view (rendered browser + raw JSON toggle) -------------------------------------
function badge(text, kind) { return dom("span", "badge " + kind, text); }
function gridTable(headers) {
  const t = dom("table", "grid");
  const hd = t.createTHead().insertRow();
  for (const h of headers) hd.appendChild(dom("th", null, h));
  return t;
}
function indexLabel(x) {
  let extra = "";
  if (x.kind === "fulltext") extra = ", " + x.language + (x.stemming ? ", stemming" : "");
  return x.kind + "(" + (x.columns || []).join(", ") + extra + ")";
}
function schemaTableCard(t) {
  const card = dom("div", "scard");
  const hd = dom("div", "hd");
  hd.appendChild(dom("b", null, t.name));
  hd.appendChild(badge(t.access === "Public" ? "public" : "private", t.access === "Public" ? "pk" : "opt"));
  if (t.partition_by) hd.appendChild(badge("partition: " + t.partition_by, "opt"));
  const vis = t.visibility || {};
  if (vis.kind && vis.kind !== "public_all") {
    let label = vis.kind;
    if (vis.column) label += "(" + vis.column + ")";
    if (vis.predicate) label += "(" + vis.predicate + ")";
    if (vis.table) label += "(" + vis.table + ")";
    hd.appendChild(badge("visibility: " + label, "opt"));
  }
  card.appendChild(hd);
  const pkOrdinals = new Set(t.primary_key || []);
  const uniqueCols = new Set((t.unique || []).flat());
  const tbl = gridTable(["column", "type", "flags"]);
  const tb = tbl.createTBody();
  (t.columns || []).forEach((c, ordinal) => {
    const tr = tb.insertRow();
    tr.insertCell().textContent = c.name;
    tr.insertCell().textContent = c.type;
    const flags = tr.insertCell();
    if (pkOrdinals.has(ordinal)) flags.appendChild(badge("pk", "pk"));
    if (t.auto_inc === c.name) flags.appendChild(badge("auto", "auto"));
    if (uniqueCols.has(c.name)) flags.appendChild(badge("unique", "opt"));
    if (c.transforms) flags.appendChild(badge(c.transforms.map((x) => x.kind).join(", "), "opt"));
  });
  card.appendChild(tbl);
  const idx = t.indexes || [];
  if (idx.length) card.appendChild(dom("div", "ft", "indexes: " + idx.map(indexLabel).join(" · ")));
  return card;
}
function schemaReducerCard(reducers) {
  const card = dom("div", "scard");
  const hd = dom("div", "hd");
  hd.appendChild(dom("b", null, "reducers"));
  hd.appendChild(dom("span", "mut2", reducers.length + " registered"));
  card.appendChild(hd);
  const tbl = gridTable(["name", "signature", "client callable", "rate/s"]);
  const tb = tbl.createTBody();
  for (const r of reducers) {
    const tr = tb.insertRow();
    tr.insertCell().textContent = r.name;
    const params = (r.params || []).map((p) => p.name + ": " + p.type).join(", ");
    tr.insertCell().textContent = "(" + params + ")" + (r.return_type ? " -> " + r.return_type : "");
    tr.insertCell().textContent = r.client_callable ? "yes" : "no";
    tr.insertCell().textContent = r.max_rate_per_sec ? String(r.max_rate_per_sec) : "unlimited";
  }
  card.appendChild(tbl);
  return card;
}
function renderSchemaDoc() {
  const box = $("s-doc");
  box.textContent = "";
  const doc = schemaDoc;
  if (!doc) return;
  $("s-meta").textContent = "schema v" + doc.schema_version + " · document v" + doc.document_version;
  for (const t of doc.tables || []) box.appendChild(schemaTableCard(t));
  if ((doc.reducers || []).length) box.appendChild(schemaReducerCard(doc.reducers));
  if ((doc.views || []).length) {
    const card = dom("div", "scard");
    const hd = dom("div", "hd");
    hd.appendChild(dom("b", null, "views"));
    card.appendChild(hd);
    card.appendChild(dom("div", "ft", doc.views.join(" · ")));
    box.appendChild(card);
  }
}
let schemaRaw = false;
$("s-raw").addEventListener("click", () => {
  schemaRaw = !schemaRaw;
  $("s-raw").textContent = schemaRaw ? "Rendered" : "Raw JSON";
  $("s-doc").style.display = schemaRaw ? "none" : "";
  $("s-json-wrap").style.display = schemaRaw ? "" : "none";
});
$("s-refresh").addEventListener("click", loadSchema);

// --- table designer (generates the #[fluxum::table] snippet) -------------------------------
const RUST_TYPES = ["u64", "u32", "u16", "u8", "i64", "i32", "i16", "i8", "f64", "f32",
  "bool", "String", "Vec<u8>", "Identity", "Timestamp", "Decimal"];
let designCols = [
  { name: "id", ty: "u64", opt: false, vec: false, pk: true, auto: true },
  { name: "name", ty: "String", opt: false, vec: false, pk: false, auto: false },
];
function renderDesigner() {
  const box = $("d-cols");
  box.textContent = "";
  const partSel = $("d-part");
  const partWas = partSel.value;
  while (partSel.options.length > 1) partSel.remove(1);
  designCols.forEach((col, i) => {
    partSel.add(new Option(col.name, col.name));
    const row = document.createElement("div");
    row.className = "col-row";
    const name = document.createElement("input");
    name.type = "text"; name.value = col.name; name.style.fontFamily = "var(--mono)";
    name.addEventListener("input", () => { col.name = name.value; emitRust(); });
    const ty = document.createElement("select");
    for (const t of RUST_TYPES) ty.add(new Option(t, t));
    ty.value = col.ty;
    ty.addEventListener("change", () => { col.ty = ty.value; emitRust(); });
    const mk = (label, key) => {
      const wrap = document.createElement("label");
      wrap.className = "mini";
      const cb = document.createElement("input");
      cb.type = "checkbox"; cb.checked = col[key];
      cb.addEventListener("change", () => { col[key] = cb.checked; emitRust(); });
      wrap.append(cb, document.createTextNode(label));
      return wrap;
    };
    const del = document.createElement("button");
    del.className = "btn ghost"; del.textContent = "✕";
    del.addEventListener("click", () => { designCols.splice(i, 1); renderDesigner(); });
    row.append(name, ty, mk("pk", "pk"), mk("auto", "auto"), mk("Option", "opt"), del);
    box.appendChild(row);
  });
  partSel.value = partWas;
  emitRust();
}
function emitRust() {
  const table = ($("d-name").value || "MyTable").trim();
  const access = $("d-access").value;
  const part = $("d-part").value;
  const attrs = [access];
  if (part) attrs.push("partition_by(" + part + ")");
  let out = "#[fluxum::table(" + attrs.join(", ") + ")]\n";
  out += "#[derive(Debug, Clone, PartialEq)]\n";
  out += "pub struct " + table + " {\n";
  for (const col of designCols) {
    if (col.pk) out += "    #[primary_key]\n";
    if (col.auto) out += "    #[auto_inc]\n";
    const ty = col.opt ? "Option<" + col.ty + ">" : col.ty;
    out += "    pub " + (col.name || "_") + ": " + ty + ",\n";
  }
  out += "}\n";
  $("d-out").textContent = out;
}
$("d-add").addEventListener("click", () => {
  designCols.push({ name: "field" + designCols.length, ty: "String", opt: false, vec: false, pk: false, auto: false });
  renderDesigner();
});
$("d-name").addEventListener("input", emitRust);
$("d-access").addEventListener("change", emitRust);
$("d-part").addEventListener("change", emitRust);
$("d-copy").addEventListener("click", async () => {
  try { await navigator.clipboard.writeText($("d-out").textContent); toast("ok", "Copied"); }
  catch { toast("err", "Clipboard unavailable"); }
});

showView("overview");
boot();
