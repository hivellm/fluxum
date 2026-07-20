# SDK conformance corpus (SPEC-013 TST-052)

A versioned, declarative scenario corpus executed by a runner in **every** SDK against the same
server build. Identical observable results are required from all runners: the corpus — not any
one SDK's test suite — is what "this client implements the Fluxum protocol" means, and each SDK
is release-blocked until its runner is green (SDK-064).

- `corpus.json` — the manifest: corpus version, the demo-module schema fixture (column names and
  FluxBIN types per table, in declaration order), and the scenario list.
- `scenarios/*.json` — one scenario per file, named by the manifest.

New cases may be added freely; **changing an expected value carries the same review bar as a wire
format change** (TST-052).

## Runner protocol

A runner is a per-SDK program (a test-suite entry in that SDK's native harness) that, for each
scenario in the manifest:

1. Boots a **fresh** `fluxum-server` with the demo module — fresh ports, fresh data directory.
   The demo tables are global to a server, so scenarios never share one.
2. Executes the scenario's `steps` in order, maintaining a set of named client sessions.
3. Fails the scenario on the first step whose expectation does not hold.

Runners connect over the transport(s) native to their platform. A scenario makes no assumptions
about transport beyond the protocol itself; running the corpus once per transport is encouraged
where an SDK carries more than one.

## Step vocabulary (corpus_version 1)

| Step | Meaning |
| --- | --- |
| `{"connect": {"client", "token"?}}` | Open + authenticate a named session. `token` is UTF-8; absent = empty token. Resolves only once authenticated. |
| `{"close": {"client"}}` | Close the session. |
| `{"subscribe": {"client", "queries": [..], "as"?}}` | Register queries and await every `InitialData` (one per query, RPC-032), applied to the local cache. With `as`, bind the returned server-assigned `query_id`s under that label for a later `unsubscribe`. |
| `{"unsubscribe": {"client", "handles"}}` | Drop the subscriptions whose `query_id`s were bound under the `handles` label. Rows only those queries held leave the cache; rows still covered by another subscription survive (SDK-044). |
| `{"call": {"client", "reducer", "args", "expect_error"?: {"contains"}}}` | Call a reducer. Without `expect_error` the call must succeed; with it, the call must fail and the error message must contain the substring. |
| `{"call_until_error": {"client", "reducer", "args", "attempts", "expect_error": {"contains"}}}` | Call the reducer up to `attempts` times, stopping at the first failure — which must match `expect_error`. For admission behavior (rate limits) where the exact rejection point depends on timing. Fails if every attempt succeeds. |
| `{"await_row": {"client", "table", "where"}}` | Poll the local cache (≤ 5 s) until a row matches `where`. This is how a runner observes a `TxUpdate` landing without racing the push stream. |
| `{"await_gone": {"client", "table", "where"}}` | Poll (≤ 5 s) until **no** row matches. |
| `{"await_count": {"client", "table", "where"?, "count"}}` | Poll (≤ 5 s) until exactly `count` rows match `where` (`where` absent = all rows). For rows distinguishable only by a nondeterministic column — e.g. two connections sharing one identity. |
| `{"expect_cache": {"client", "table", "rows": [..]}}` | Exact set equality: every expected row matches exactly one cached row and nothing is left over. Order-independent. |
| `{"expect_distinct_identities": {"clients": [..]}}` | The listed sessions all report pairwise distinct identities. |
| `{"restart_server": {}}` | Kill the server process and start it again on the SAME ports and data directory — a crash-and-recover. Live clients are expected to reconnect, resubscribe and resync on their own; a scenario follows this with `await_row`/`expect_cache` to prove they did. |

## Value language

Rows cross the wire as FluxBIN; a runner decodes them with the manifest's column types and
canonicalizes for comparison:

- `U64` / `I64` / `Timestamp` / `EntityId` → **decimal string** (64-bit precision does not
  survive every language's number type; a string does).
- `Identity` → 64-char lowercase hex; `ConnectionId` → 32-char lowercase hex.
- `U8`–`U32`, `I8`–`I32`, `F32`/`F64`, `Bool`, `Str` → native JSON value.

Expected values (in `where` and `rows`) use the same forms, plus two escapes:

- `"*"` matches anything — for server-assigned values that are not deterministic (timestamps,
  connection ids).
- `"$identity:NAME"` resolves to the identity the server derived for session NAME — determined
  at runtime, so scenarios do not bake in a particular auth provider's derivation.

Auto-increment primary keys ARE deterministic on a fresh server (1, 2, 3…) and scenarios assert
them literally — **until a restart**: recovery resumes the allocator from its reserved
high-water block (STG-040), so ids allocated after a `restart_server` may jump. Post-restart
rows match on content and use `"*"` for the id.

## What belongs here vs. an SDK's own suite

The corpus asserts **protocol-observable behavior**: what any correct client must see. How a
particular SDK surfaces it — callback signatures, error classes, reconnect pacing — belongs to
that SDK's own tests. If a scenario can only be expressed in terms of one SDK's API, it does not
belong in the corpus.

## Runners

| SDK | Runner | Status |
| --- | --- | --- |
| TypeScript | `sdks/typescript/tests/conformance.test.ts` (Node; Chromium via the same interpreter is T6.2's 1.9) | reference |
| Rust | with `phase6_rust-sdk` (T6.4) | pending |
| Python / Go / C# | with T7.4–T7.6 | pending |
