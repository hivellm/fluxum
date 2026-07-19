# Fluxum demo

A browser talking the binary FluxRPC wire straight to the database: MessagePack
envelopes, FluxBIN rows, Streamable HTTP. No gateway, no JSON, no build step on
the page itself — `index.html` loads the packaged runtime with a plain
`<script type="module">` (SPEC-011 SDK-081).

## Run it

```bash
# 1. the server, with the demo module linked in
cargo build -p fluxum-server

# 2. the browser bundle the page imports
npm --prefix sdks/typescript install
npm --prefix sdks/typescript run build

# 3. serve the database and these files from one origin
FLUXUM_PROFILE=development \
FLUXUM_SERVER_STATIC_DIR="$PWD/demo" \
  ./target/debug/fluxum-server
```

Then open <http://127.0.0.1:15800/>.

## Why the server serves the page

`/rpc` sends no CORS headers, so a page hosted anywhere else would have every
request blocked before the SDK saw it. `server.static_dir` exists for exactly
this and is **off by default** — a production server configured without it has
no file surface at all.

## What to look at

- **ChatMessage** is public: every row reaches every subscriber. `stream 10/s`
  drives inserts so the table updates continuously; new rows flash.
- **Task** carries `#[visibility(owner_only(owner))]`. Change the identity in
  the top bar and switch user: the table changes because the server *never
  sent* the other rows, not because the page filtered them.
- **OnlineUser** is maintained by `on_connect` / `on_disconnect` hooks.
- The dev `none` auth provider derives `Identity = SHA-256(token)`, so the name
  in the top bar *is* the user. Two names, two identities.

## Known issue: the push stream is slow to open in a browser

`GET /rpc` is a long-lived chunked response that stays silent until the first
commit. Against Chromium driven by Playwright, `fetch()` does not resolve with
the response headers for **~15 seconds**, so the page sits on "connecting"
before everything starts working normally.

The server is not the cause, and this is worth stating precisely because the
obvious suspects were checked and cleared:

- `curl` and Node receive the headers and the priming frame in single-digit
  milliseconds against the same server, verified repeatedly.
- `X-Content-Type-Options: nosniff` is sent, so it is not MIME sniffing.
- A priming keep-alive frame is written immediately, and padding it to 2 KB
  changed nothing — so it is not a byte threshold either.
- A 404 on the same endpoint (invalid session) comes back in 6 ms, so the
  browser reaches the server fine and it is not connection exhaustion.

The remaining suspect is the test harness's network interception buffering an
endless chunked response. It has not been reproduced in a browser outside
Playwright, which is the next thing to check before calling it anything else.

One real fix already landed from this hunt: the page closes its client on
`pagehide`. The push stream holds a connection for its lifetime, and a browser
allows ~6 per origin over HTTP/1.1 — a page that reloaded without closing
leaked one each time, and after six every request queued forever, which looks
exactly like a hung server.

## Known issue: presence disappears after a reload

`OnlineUser` is keyed by `identity`, so two live connections from the same
identity share one row — and the first `on_disconnect` deletes it for both.
Reload the page and the old session, expiring a minute later, erases the new
session's presence.

That is a modelling bug in the demo module rather than in the SDK: correct
presence is keyed by `ConnectionId`, or refcounted per identity. Left in place
because it is a good illustration of the class — the table reads as obviously
right until two connections collide on one primary key.
