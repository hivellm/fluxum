// Empty stand-in for `fs`/`net`/`tls` in the browser bundle.
//
// `@hivehub/thunder` ships one entry whose top level statically imports those
// three, for its Node client and server. A browser cannot resolve them, so
// bundling the SDK for the browser fails on Thunder's imports rather than on
// anything the SDK does — even though the only thing Fluxum takes from that
// package is `FrameReader`, which touches none of it.
//
// Aliasing them here is a workaround, not a fix. The fix is upstream: a
// `browser` export condition, or a wire-only subpath (`@hivehub/thunder/wire`)
// that carries the codec without the Node transports. Filed as
// hivellm/thunder#10. When that lands, delete this file and the alias in
// build.mjs.
//
// Every export here is unreachable in a browser: the code paths that would
// call them are the TCP client and server, and the browser build only ever
// takes the Streamable HTTP path.
export default {};
