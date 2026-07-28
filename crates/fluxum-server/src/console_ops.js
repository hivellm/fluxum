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
  const body = { token, table, limit };
  // OPS-020 filters: a row key (comma-separated for a composite pk, each
  // value parsed as JSON when it parses, a string otherwise) and a tx range.
  const pkRaw = $("a-pk").value.trim();
  if (pkRaw) {
    body.pk = pkRaw.split(",").map((s) => {
      const t = s.trim();
      try { return JSON.parse(t); } catch { return t; }
    });
  }
  const txFrom = parseInt($("a-txfrom").value, 10);
  if (Number.isFinite(txFrom)) body.tx_from = txFrom;
  const txTo = parseInt($("a-txto").value, 10);
  if (Number.isFinite(txTo)) body.tx_to = txTo;
  try {
    const p = await apiJson("/audit", {
      method: "POST",
      body: JSON.stringify(body),
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

// --- sessions & bans (SEC-053 directory, SUB-042 queues, SEC-033 bans) --------------
function sessionCell(tr, text, mono) {
  const cell = tr.insertCell();
  cell.textContent = text;
  if (mono === false) cell.style.fontFamily = "system-ui, sans-serif";
  return cell;
}
async function loadSessions() {
  const note = $("ss-note");
  const grid = $("ss-grid");
  try {
    const p = await apiJson("/sessions");
    const sessions = p.sessions || [];
    $("ss-count").textContent = sessions.length + " session(s)";
    grid.textContent = "";
    note.textContent = sessions.length ? "" :
      "no live HTTP sessions — clients appear here once they authenticate over /rpc";
    if (!sessions.length) return;
    const tbl = gridTable(["session", "identity", "connection", "age", "client ip", "queue", "subscriptions", ""]);
    const tb = tbl.createTBody();
    for (const s of sessions) {
      const tr = tb.insertRow();
      tr.className = "click";
      tr.title = "click for details";
      sessionCell(tr, (s.id || "").slice(0, 12) + "…");
      sessionCell(tr, (s.identity || "").slice(0, 12) + "…");
      sessionCell(tr, s.connection_id);
      sessionCell(tr, fmtUp(s.age_secs || 0));
      sessionCell(tr, s.client_ip || "–");
      sessionCell(tr, s.queue ? s.queue.queued + " / " + s.queue.capacity : "–");
      const subs = s.subscriptions || [];
      sessionCell(tr, subs.length ? subs.length + " quer" + (subs.length === 1 ? "y" : "ies") : "none");
      const actions = tr.insertCell();
      const kick = dom("button", "btn sm danger", "Kick");
      kick.addEventListener("click", async (e) => {
        e.stopPropagation();
        // Two-step confirm, like row delete.
        if (kick.textContent === "Kick") { kick.textContent = "Confirm?"; return; }
        try {
          await apiJson("/sessions/" + encodeURIComponent(s.id), { method: "DELETE" });
          toast("ok", "session terminated");
          loadSessions();
        } catch (err) { toast("err", String(err.message || err)); }
      });
      actions.appendChild(kick);
      tr.addEventListener("click", () => openInspector("session", {
        id: s.id,
        identity: s.identity,
        connection_id: s.connection_id,
        age: fmtUp(s.age_secs || 0),
        client_ip: s.client_ip || null,
        queue: s.queue ? s.queue.queued + " / " + s.queue.capacity : null,
        subscriptions: subs.length
          ? subs.map((x) => "#" + x.query_id + " " + x.sql).join("  ·  ")
          : null,
      }));
    }
    grid.appendChild(tbl);
  } catch (e) { grid.textContent = ""; note.textContent = String(e.message || e); }
}
$("ss-refresh").addEventListener("click", loadSessions);

async function loadBans() {
  const note = $("b-note");
  const grid = $("b-grid");
  try {
    const p = await apiJson("/bans");
    grid.textContent = "";
    const statics = p.static || [];
    const runtime = p.runtime || [];
    note.textContent = (statics.length || runtime.length) ? "" : "no bans in force";
    const tbl = gridTable(["entry", "kind", "expires", ""]);
    const tb = tbl.createTBody();
    for (const entry of statics) {
      const tr = tb.insertRow();
      sessionCell(tr, entry);
      sessionCell(tr, "static (config blocklist)");
      sessionCell(tr, "never — edit config + reload to lift");
      tr.insertCell();
    }
    for (const ban of runtime) {
      const tr = tb.insertRow();
      sessionCell(tr, ban.entry);
      sessionCell(tr, "runtime");
      sessionCell(tr, ban.remaining_ttl_ms == null ? "no expiry"
        : "in " + Math.ceil(ban.remaining_ttl_ms / 1000) + " s");
      const actions = tr.insertCell();
      const lift = dom("button", "btn sm ghost", "Unban");
      lift.addEventListener("click", async () => {
        try {
          // A CIDR entry carries `/` — the server rejoins path segments, so
          // the raw entry rides in the path unencoded.
          await apiJson("/bans/" + ban.entry, { method: "DELETE" });
          toast("ok", "unbanned " + ban.entry);
          loadBans();
        } catch (e) { toast("err", String(e.message || e)); }
      });
      actions.appendChild(lift);
    }
    if (statics.length || runtime.length) grid.appendChild(tbl);
  } catch (e) { grid.textContent = ""; note.textContent = String(e.message || e); }
}
$("b-refresh").addEventListener("click", loadBans);
$("b-ban").addEventListener("click", async () => {
  const entry = $("b-entry").value.trim();
  if (!entry) { $("b-note").textContent = "enter an IP or CIDR to ban"; return; }
  const ttl = parseInt($("b-ttl").value, 10);
  const body = { entry };
  if (Number.isFinite(ttl) && ttl > 0) body.ttl_secs = ttl;
  try {
    await apiJson("/bans", { method: "POST", body: JSON.stringify(body) });
    toast("ok", "banned " + entry);
    $("b-entry").value = "";
    loadBans();
  } catch (e) { $("b-note").textContent = String(e.message || e); }
});

// --- ops view (OPS-030/040, REP-060/064/080, OPS-060) --------------------------------
function renderOps() {
  // Reloadable values in force, with provenance (OPS-040).
  const rel = $("op-reloadable");
  rel.textContent = "";
  const reloadable = healthDoc && healthDoc.reloadable;
  if (reloadable && typeof reloadable === "object") {
    for (const [k, v] of Object.entries(reloadable)) {
      if (v && typeof v === "object" && v.value !== undefined) {
        rel.appendChild(kvRow(k, typeof v.value === "object" ? JSON.stringify(v.value) : String(v.value), v.source));
      } else {
        rel.appendChild(kvRow(k, typeof v === "object" ? JSON.stringify(v) : String(v)));
      }
    }
  } else {
    rel.appendChild(dom("div", "muted", "waiting for /health…"));
  }
  // Replication posture (REP-080) — read-only here; promote rides the
  // election/CLI, there is no HTTP promote endpoint to call.
  const rp = $("op-repl");
  rp.textContent = "";
  const shard = healthDoc && healthDoc.shards && healthDoc.shards[0];
  const repl = shard && shard.replication;
  if (repl) {
    rp.appendChild(kvRow("role", repl.role + " (epoch " + repl.epoch + ")"));
    if (repl.role === "primary") {
      rp.appendChild(kvRow("connected replicas", String(repl.connected_replicas != null ? repl.connected_replicas : 0)));
      rp.appendChild(kvRow("zero-loss guarantee", repl.degraded ? "suspended (degraded)" : "in force"));
    } else {
      if (repl.primary) rp.appendChild(kvRow("primary", repl.primary));
      if (repl.lag_tx != null) rp.appendChild(kvRow("lag (tx)", String(repl.lag_tx)));
      rp.appendChild(kvRow("reads", repl.stale ? "stale" : "fresh"));
    }
  } else {
    rp.appendChild(kvRow("mode", "standalone — no replication configured"));
  }
  const pending = metric("fluxum_archive_segments_pending");
  if (pending != null) rp.appendChild(kvRow("archive segments pending", String(pending)));
  // Namespaces & quotas from the per-tenant metric series (OPS-051/061).
  const memByNs = metricByLabel("fluxum_tenant_memory_bytes", "namespace");
  const ns = $("ns-grid");
  ns.textContent = "";
  if (!memByNs.size) {
    $("ns-note").textContent = "no namespaces configured — tenants bind by name at "
      + "Authenticate; quotas live in the config (OPS-060) and hot-reload";
    return;
  }
  $("ns-note").textContent = "";
  const storageByNs = metricByLabel("fluxum_tenant_storage_bytes", "namespace");
  const subsByNs = metricByLabel("fluxum_tenant_subscriptions_active", "namespace");
  const tbl = gridTable(["namespace", "memory", "storage", "subscriptions"]);
  const tb = tbl.createTBody();
  for (const [name, memory] of [...memByNs.entries()].sort()) {
    const tr = tb.insertRow();
    tr.insertCell().textContent = name;
    tr.insertCell().textContent = fmtBytes(memory);
    tr.insertCell().textContent = fmtBytes(storageByNs.get(name) || 0);
    tr.insertCell().textContent = String(subsByNs.get(name) || 0);
  }
  ns.appendChild(tbl);
}
async function opsTick() {
  await fetchMetrics().catch(() => {});
  renderOps();
}
$("op-reload").addEventListener("click", async () => {
  $("op-reload-out").textContent = "";
  try {
    const p = await apiJson("/config/reload", { method: "POST", body: "{}" });
    $("op-reload-out").textContent = (p.changed && p.changed.length)
      ? "changed: " + p.changed.join(", ")
      : "reloaded — nothing changed";
    toast("ok", "config reloaded");
    pollHealth();
  } catch (e) { $("op-reload-out").textContent = String(e.message || e); }
});
$("op-checkpoint").addEventListener("click", async () => {
  $("op-checkpoint-out").textContent = "";
  try {
    const p = await apiJson("/checkpoint", { method: "POST", body: "{}" });
    $("op-checkpoint-out").textContent = p.fresh
      ? "checkpointed at tx " + p.last_tx_id
      : "already covered at tx " + p.last_tx_id;
    toast("ok", "checkpoint done");
  } catch (e) { $("op-checkpoint-out").textContent = String(e.message || e); }
});
$("op-drain").addEventListener("click", async () => {
  const b = $("op-drain");
  if (b.textContent === "Drain") { b.textContent = "Confirm drain?"; return; }
  b.textContent = "Drain";
  try {
    const p = await apiJson("/drain", { method: "POST", body: "{}" });
    $("op-drain-out").textContent = "draining — state " + p.state
      + ", queue " + p.queue_depth + ", last tx " + p.last_tx_id;
    toast("info", "shard draining; restart the process to serve again");
  } catch (e) { $("op-drain-out").textContent = String(e.message || e); }
});
$("bk-create").addEventListener("click", async () => {
  const out = $("bk-out").value.trim();
  const msg = $("bk-msg");
  if (!out) { msg.textContent = "enter an output directory (on the server's filesystem)"; return; }
  msg.textContent = "backing up…";
  $("bk-report").textContent = "";
  try {
    const p = await apiJson("/backup", { method: "POST", body: JSON.stringify({ out }) });
    msg.textContent = "";
    const box = $("bk-report");
    box.appendChild(kvRow("backup id", p.backup_id));
    box.appendChild(kvRow("manifest", p.manifest));
    box.appendChild(kvRow("shards / segments", p.shards + " / " + p.segments));
    box.appendChild(kvRow("head tx", String(p.head_tx_id)));
    $("bk-dir").value = out;
    toast("ok", "backup created");
  } catch (e) { msg.textContent = String(e.message || e); }
});
$("bk-verify").addEventListener("click", async () => {
  const dir = $("bk-dir").value.trim();
  const msg = $("bk-msg");
  if (!dir) { msg.textContent = "enter the backup directory to verify"; return; }
  msg.textContent = "verifying…";
  try {
    const p = await apiJson("/backup/verify", { method: "POST", body: JSON.stringify({ dir }) });
    msg.textContent = "";
    const box = $("bk-report");
    box.textContent = "";
    box.appendChild(kvRow("files checked", String(p.checked)));
    box.appendChild(kvRow("verdict", p.ok ? "OK — every hash matches" : (p.errors || []).length + " failure(s)"));
    for (const err of p.errors || []) box.appendChild(dom("div", "err-box", err.file + ": " + err.error));
    toast(p.ok ? "ok" : "err", p.ok ? "backup verified" : "verification failed");
  } catch (e) { msg.textContent = String(e.message || e); }
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
const LOG_RANK = { TRACE: 0, DEBUG: 1, INFO: 2, WARN: 3, ERROR: 4 };
function logLineMatches(obj, raw) {
  // Minimum-severity filter: the chosen level and above.
  const min = $("l-level").value;
  if (min && (LOG_RANK[obj.level] === undefined || LOG_RANK[obj.level] < LOG_RANK[min])) {
    return false;
  }
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
// --- metrics dashboard: sampled history + sparklines --------------------------------
// One sample per 5 s tick while the view is open; each tile is a single
// series (its title names it — no legend), drawn as a thin accent line.
const MDASH_CAP = 60; // ~5 min of history at the 5 s cadence
const mhist = new Map(); // series name -> [values], capped
const MDASH_SERIES = [
  "fluxum_shard_last_tx_id", "fluxum_connections_active",
  "fluxum_subscriptions_active", "fluxum_reducer_queue_depth",
  "fluxum_fanout_messages_total", "fluxum_fanout_stage_us_sum",
  "fluxum_fanout_stage_us_count", "fluxum_bufferpool_hits_total",
  "fluxum_bufferpool_misses_total", "fluxum_bufferpool_bytes",
  "fluxum_bufferpool_capacity_bytes", "fluxum_memstore_bytes",
];
function pushSample() {
  for (const name of MDASH_SERIES) {
    const v = metric(name);
    if (v == null) continue;
    const arr = mhist.get(name) || [];
    arr.push(v);
    if (arr.length > MDASH_CAP) arr.shift();
    mhist.set(name, arr);
  }
}
function hist(name) { return mhist.get(name) || []; }
// Cumulative counter -> per-second rates between consecutive samples.
function rateSeries(arr) {
  const out = [];
  for (let i = 1; i < arr.length; i += 1) out.push(Math.max(0, (arr[i] - arr[i - 1]) / 5));
  return out;
}
// Per-interval ratio of two counters (e.g. avg latency, hit rate); NaN-safe.
function deltaRatio(numArr, denArr, scale) {
  const out = [];
  const n = Math.min(numArr.length, denArr.length);
  for (let i = 1; i < n; i += 1) {
    const den = denArr[i] - denArr[i - 1];
    out.push(den > 0 ? ((numArr[i] - numArr[i - 1]) / den) * scale : 0);
  }
  return out;
}
// Min-max normalized polyline points for a 100x28 viewBox. The HTML parser
// resolves the SVG namespace, so no namespace URI string is needed (the
// self-containment test forbids absolute URLs anywhere in the shell).
function sparkPoints(values) {
  const lo = Math.min(...values), hi = Math.max(...values);
  const span = hi - lo || 1;
  const step = values.length > 1 ? 100 / (values.length - 1) : 0;
  return values
    .map((v, i) => (i * step).toFixed(1) + "," + (26 - ((v - lo) / span) * 24).toFixed(1))
    .join(" ");
}
function dashTile(label, value, sparkVals, sub) {
  const t = dom("div", "tile");
  t.appendChild(dom("div", "k", label));
  const v = dom("div", "v", value);
  if (sub) v.appendChild(dom("span", "sub", sub));
  t.appendChild(v);
  if (sparkVals && sparkVals.length > 1) {
    const wrap = dom("div", "spark");
    wrap.innerHTML = '<svg viewBox="0 0 100 28" preserveAspectRatio="none"><polyline points="'
      + sparkPoints(sparkVals) + '"/></svg>';
    wrap.title = sparkVals.length + " samples, 5 s apart";
    t.appendChild(wrap);
  }
  return t;
}
function fmtNum(n) {
  if (n == null || !isFinite(n)) return "–";
  if (Math.abs(n) >= 100) return String(Math.round(n));
  return (Math.round(n * 10) / 10).toString();
}
function renderMetricsDash() {
  const box = $("m-dash");
  box.textContent = "";
  const samples = hist("fluxum_shard_last_tx_id").length;
  $("m-dash-note").textContent = samples < 2 ? "collecting samples…" : "";
  const last = (arr) => (arr.length ? arr[arr.length - 1] : null);

  const txRates = rateSeries(hist("fluxum_shard_last_tx_id"));
  box.appendChild(dashTile("Tx rate", fmtNum(last(txRates)), txRates, "/s"));
  const conns = hist("fluxum_connections_active");
  box.appendChild(dashTile("Connections", fmtNum(last(conns)), conns));
  const subs = hist("fluxum_subscriptions_active");
  box.appendChild(dashTile("Subscriptions", fmtNum(last(subs)), subs));
  const queue = hist("fluxum_reducer_queue_depth");
  box.appendChild(dashTile("Queue depth", fmtNum(last(queue)), queue));
  const fanout = rateSeries(hist("fluxum_fanout_messages_total"));
  box.appendChild(dashTile("Fan-out msgs", fmtNum(last(fanout)), fanout, "/s"));
  // Avg fan-out stage latency over each interval, µs -> ms.
  const lat = deltaRatio(hist("fluxum_fanout_stage_us_sum"), hist("fluxum_fanout_stage_us_count"), 0.001);
  box.appendChild(dashTile("Fan-out latency", fmtNum(last(lat)), lat, "ms avg"));
  const hits = deltaRatio(hist("fluxum_bufferpool_hits_total"),
    hist("fluxum_bufferpool_hits_total").map((v, i) => v + (hist("fluxum_bufferpool_misses_total")[i] || 0)), 100);
  box.appendChild(dashTile("Pool hit rate", fmtNum(last(hits)), hits, "%"));
  // Pool occupancy: current value + gauge bar (not a time series).
  const used = metric("fluxum_bufferpool_bytes");
  const cap = metric("fluxum_bufferpool_capacity_bytes");
  const pool = dashTile("Pool resident", fmtBytes(used), null, cap ? "of " + fmtBytes(cap) : "");
  if (used != null && cap) {
    const bar = dom("div", "bar");
    bar.style.marginTop = "10px";
    const fill = document.createElement("i");
    fill.style.width = Math.min(100, (used / cap) * 100).toFixed(1) + "%";
    bar.appendChild(fill);
    pool.appendChild(bar);
  }
  box.appendChild(pool);
  const mem = hist("fluxum_memstore_bytes");
  box.appendChild(dashTile("Memstore", fmtBytes(last(mem)), mem));
}
let metricsMode = "dash";
$("m-mode").addEventListener("click", () => {
  metricsMode = metricsMode === "dash" ? "series" : "dash";
  $("m-mode").textContent = metricsMode === "dash" ? "Series" : "Dashboard";
  $("m-dash").style.display = metricsMode === "dash" ? "" : "none";
  $("m-grid").style.display = metricsMode === "dash" ? "none" : "";
  if (metricsMode === "series") renderMetrics();
});
async function metricsTick() {
  try { await fetchMetrics(); } catch (e) { $("m-count").textContent = String(e.message || e); return; }
  pushSample();
  if (metricsMode === "dash") renderMetricsDash();
  else renderMetrics();
}
$("m-refresh").addEventListener("click", metricsTick);
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
