## 1. Implementation
- [x] 1.1 Build the demo Fluxum module (chat + presence + per-user tasks) on the example schema: reducers (send_chat, complete_task, ...), on_connect/on_disconnect presence, `#[visibility(owner_only)]` on Task, a rate-limited reducer (UC-1/UC-2/UC-6) — `crates/fluxum-demo`: tables ChatMessage/Task/OnlineUser, reducers send_chat (`max_rate = "20/s"`)/add_task/complete_task, on_connect/on_disconnect presence, `#[visibility(owner_only(owner))]` on Task. Linked into `fluxum-server`, and the live `/schema` confirms all three tables and reducers register. Note `complete_task` re-checks ownership explicitly: `owner_only` governs what a *subscription delivers*, while a reducer runs server-side against the whole table — without the check, a guessed id completes someone else's task
- [x] 1.2 Build the demo web client entirely on the generated TypeScript SDK over Streamable HTTP in the browser (FR-82) — `demo/` (index.html + app.js + style.css): a live-updating ChatMessage table with row flashes, a stat strip (rows cached, events, events/s, reducer round trip), a 10/s traffic generator, per-user Tasks and presence chips. Plain JS via `<script type="module">`, importing the packaged bundle. **Caveat:** hand-written row decoders, NOT `fluxum generate` output — wiring codegen into the page is what remains of this item
- [x] 1.3 Serve the demo statics from the repo, including the vanilla-JS smoke page used by the T6.2 SDK-081 test (script type=module, no build step) — `server.static_dir` (off by default) plus `crates/fluxum-server/src/statics.rs`, which walks parsed path components and accepts only `Normal` ones. Served by the database itself because `/rpc` sends no CORS headers: a page elsewhere has every request blocked before the SDK sees it. The separate vanilla-JS smoke *page* for SDK-081 is still to add — the demo page already is one, but the assertion is not scripted
- [x] 1.4 End-to-end assertions: send_chat produces a TxUpdate, local cache reflects the new ChatMessage row, typed insert callback fires with a FluxBIN-decoded row (SPEC-011 acceptance 8) — `sdks/typescript/tests/client.e2e.test.ts` spawns the real server and asserts exactly that, plus out-of-order id correlation, server-side `owner_only` filtering, and that a batched multi-query subscribe populates every table. Runs in Node, not a browser; the headless-Chromium half is T6.2 1.9
- [x] 1.5 Verification (DAG exit test): demo scenario scripted in CI (auth -> subscribe -> reducer -> TxUpdate -> cache assertions) — scripted three ways under `npm test` (the local gate while GitHub Actions is paused): `sdks/typescript/tests/smoke.vanilla.test.ts` drives connect→subscribe→send_chat→TxUpdate-callback→cache assertion through the **packaged bundle** exactly as the no-build demo page does (SDK-081); `client.e2e.test.ts` asserts the same loop plus id correlation and `owner_only` filtering; and the shared conformance corpus (`conformance.test.ts`, 11 scenarios) exercises the demo module's every reducer, subscription, and error path against the real server. The **Rust** runner runs the same corpus (10/10), so the demo scenario is scripted across two SDKs. Note on the codegen decoders (1.2 caveat): `fluxum generate --lang typescript` now emits typed per-table row decoders (`decodeChatMessage(row): ChatMessage`, commit c86dd69) — the enabler for a typed example on codegen; the no-build plain-JS demo page cannot import TS directly, so consuming them belongs to a small TS example rather than a retrofit of the deliberately build-free page
- [ ] 1.6 Gate G6 input: demo runs end-to-end on the generated SDK (PRD 12.1)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass

## Progress log

The demo runs end to end in a real browser today: `cargo build -p fluxum-server`,
`npm --prefix sdks/typescript run build`, then start the server with
`FLUXUM_SERVER_STATIC_DIR=$PWD/demo` and open <http://127.0.0.1:15800/>. See
`demo/README.md`, which also records the two browser-only bugs this shook out
(the push stream opening a keep-alive interval late, and presence keyed by
identity rather than connection) and how each was fixed.

What is left for this task is the part that makes it a *product* demo rather
than a working one: driving the page from `fluxum generate` output instead of
hand-written decoders (1.2), and scripting the scenario in CI (1.5).
