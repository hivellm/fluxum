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

## Notes from making this work in a real browser

Two bugs only a browser could surface, both fixed:

**The push stream opened 15-20 seconds late.** A browser does not surface a
chunked response while the stream is quiet - `fetch()` stayed unresolved,
headers and all, until a chunk arrived after a multi-second gap, which for an
idle push stream means the first keep-alive tick. The timing proved it: with
the 20 s default cadence the response appeared at 20004 ms; dropping the
cadence to 3 s moved it to 3055 ms. `curl` and Node see the headers in
milliseconds, which is why every non-browser test passed.

Priming the stream with an immediate frame did not help, nor did padding that
frame to 2 KB - writes issued before the keep-alive loop simply do not count.
The fix was on the client: `openPushStream()` no longer awaits the response.
Connecting never depended on those headers having been parsed; the stream
exists to deliver frames later, and failures surface through `onClose`, which
is the only place a long-lived stream can report anything anyway.

**Presence vanished after a reload.** `OnlineUser` was keyed by identity, so
two connections from one identity shared a row and the first `on_disconnect`
deleted it for both. It is now an `ephemeral` table keyed by `ConnectionId`
with `#[owner]`, so the engine drops exactly that connection's row (DMX-011)
and the hand-written disconnect hook - a second implementation of the same
rule, free to drift - is gone.

Also fixed: the page closes its client on `pagehide`. The push stream holds a
connection for its lifetime and a browser allows ~6 per origin over HTTP/1.1,
so a page that reloaded without closing leaked one each time.
