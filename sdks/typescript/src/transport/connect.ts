// Transport selection (SPEC-011 SDK-082): one package, two environments, and
// the URI scheme decides the carrier.
//
//   fluxum://host:15801   → raw TCP        (Node.js only)
//   http(s)://host:15800  → Streamable HTTP (Node.js and browsers)

import { HttpTransport } from './http.ts';
import { TcpTransport, DEFAULT_TCP_PORT } from './tcp.ts';
import { TransportError } from './types.ts';
import type { Transport } from './types.ts';

export interface ConnectOptions {
  /** Frame cap (RPC-061). Defaults to Fluxum's 16 MB. */
  maxFrameBytes?: number;
  /** Injected in tests; HTTP only. */
  fetch?: typeof globalThis.fetch;
}

/** True when there is no Node process behind this runtime. */
function isBrowser(): boolean {
  // Checks for Node rather than for a browser: the set of non-browser hosts
  // (Deno, Bun, workers, edge runtimes) is open-ended, but "does this have
  // Node's TCP stack" is the question actually being asked.
  const proc = (globalThis as { process?: { versions?: { node?: string } } }).process;
  return proc?.versions?.node === undefined;
}

/**
 * Open a transport for `url`.
 *
 * `fluxum://` in a browser fails immediately with the `http(s)://` form to use
 * instead (SDK-082). The alternative — a promise that hangs or dies inside the
 * TCP stack — reports a missing module rather than the actual mistake, which
 * is having picked a scheme the environment cannot carry.
 */
export async function connect(url: string, options: ConnectOptions = {}): Promise<Transport> {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new TransportError(
      `not a valid URL: ${url}. Use fluxum://host:15801 (Node) or http(s)://host:15800.`,
    );
  }

  switch (parsed.protocol) {
    case 'fluxum:': {
      if (isBrowser()) {
        const host = parsed.hostname || 'host';
        throw new TransportError(
          `fluxum:// is a raw TCP scheme and browsers cannot open TCP sockets. ` +
            `Connect to the Streamable HTTP endpoint instead: http://${host}:15800`,
        );
      }
      const port = parsed.port === '' ? DEFAULT_TCP_PORT : Number(parsed.port);
      return TcpTransport.connect(parsed.hostname, port, options);
    }

    case 'http:':
    case 'https:':
      return new HttpTransport(parsed.origin + trimTrailingRpc(parsed.pathname), options);

    default:
      throw new TransportError(
        `unsupported scheme "${parsed.protocol}". Use fluxum:// (TCP, Node) or ` +
          `http(s):// (Streamable HTTP).`,
      );
  }
}

/**
 * Accept a base URL with or without the `/rpc` path.
 *
 * `HttpTransport` appends `/rpc` itself, so a user who passed the full
 * endpoint would otherwise reach `/rpc/rpc` — a 404 that reads as a dead
 * server rather than a doubled path.
 */
function trimTrailingRpc(pathname: string): string {
  const trimmed = pathname.replace(/\/+$/, '');
  return trimmed.endsWith('/rpc') ? trimmed.slice(0, -'/rpc'.length) : trimmed;
}
