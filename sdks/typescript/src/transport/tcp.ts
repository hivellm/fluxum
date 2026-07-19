// FluxRPC over raw TCP (SPEC-006 RPC-003), Node.js only.
//
// The message layer is identical to Streamable HTTP — same envelopes, same
// framing, same multiplexing. What TCP does not have is a session header:
// the connection *is* the session, so there is nothing to echo and no 404 to
// recover from. That asymmetry is the reason this file exists at all; the
// rest is `node:net` plumbing.
//
// `node:net` is imported lazily so that merely importing the SDK in a browser
// does not reach for a module that is not there. A bundler that statically
// resolves imports would otherwise fail the build for a path browsers never
// take.

import { FluxumFrameReader, DEFAULT_MAX_FRAME_BYTES } from '../protocol.ts';
import { TransportError } from './types.ts';
import type { CloseHandler, FrameHandler, Transport } from './types.ts';

/** Default FluxRPC TCP port (RPC-003). */
export const DEFAULT_TCP_PORT = 15801;

export interface TcpTransportOptions {
  /** Frame cap (RPC-061). Defaults to Fluxum's 16 MB. */
  maxFrameBytes?: number;
}

/** Minimal shape of the `node:net` socket this transport drives. */
interface NodeSocket {
  write(data: Uint8Array, cb: (err?: Error | null) => void): boolean;
  on(event: string, listener: (...args: never[]) => void): unknown;
  destroy(): void;
}

/** One Fluxum session over a TCP connection. */
export class TcpTransport implements Transport {
  readonly #socket: NodeSocket;
  readonly #reader: FluxumFrameReader;

  #frameHandler: FrameHandler | null = null;
  #closeHandler: CloseHandler | null = null;
  #closed = false;

  private constructor(socket: NodeSocket, maxFrameBytes: number) {
    this.#socket = socket;
    this.#reader = new FluxumFrameReader({ maxFrameBytes });

    socket.on('data', ((chunk: Uint8Array) => {
      // A frame-cap violation is fatal for the connection, not for one
      // message: past an oversized prefix the stream position is unknown, so
      // there is no safe way to resynchronize (RPC-061).
      try {
        this.#reader.push(chunk);
        for (;;) {
          const body = this.#reader.nextBody();
          if (body === null) break;
          this.#frameHandler?.(body);
        }
      } catch (err) {
        this.#socket.destroy();
        this.#finish(err instanceof Error ? err : new TransportError(String(err)));
      }
    }) as (...args: never[]) => void);

    socket.on('error', ((err: Error) => this.#finish(err)) as (...args: never[]) => void);
    socket.on('close', (() => this.#finish(null)) as (...args: never[]) => void);
  }

  /** Connect to `host:port` and resolve once the socket is up. */
  static async connect(
    host: string,
    port: number,
    options: TcpTransportOptions = {},
  ): Promise<TcpTransport> {
    const net = await importNet();
    const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;

    return new Promise<TcpTransport>((resolve, reject) => {
      const socket = net.createConnection({ host, port });
      // `error` before `connect` means the connection never came up: reject
      // rather than resolve a transport that is already dead. Handlers are
      // swapped for the instance's own in the constructor path below.
      const onConnectError = (err: Error): void => {
        socket.removeListener('connect', onConnect);
        reject(err);
      };
      const onConnect = (): void => {
        socket.removeListener('error', onConnectError);
        resolve(new TcpTransport(socket as unknown as NodeSocket, maxFrameBytes));
      };
      socket.once('error', onConnectError);
      socket.once('connect', onConnect);
    });
  }

  onFrame(handler: FrameHandler): void {
    this.#frameHandler = handler;
  }

  onClose(handler: CloseHandler): void {
    this.#closeHandler = handler;
  }

  send(frame: Uint8Array): Promise<void> {
    if (this.#closed) return Promise.reject(new TransportError('transport is closed'));
    return new Promise((resolve, reject) => {
      this.#socket.write(frame, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  close(): Promise<void> {
    if (!this.#closed) {
      this.#socket.destroy();
      this.#finish(null);
    }
    return Promise.resolve();
  }

  #finish(reason: Error | null): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#closeHandler?.(reason);
  }
}

interface NetModule {
  createConnection(options: { host: string; port: number }): {
    once(event: string, listener: (...args: never[]) => void): unknown;
    removeListener(event: string, listener: (...args: never[]) => void): unknown;
  };
}

async function importNet(): Promise<NetModule> {
  try {
    return (await import('node:net')) as unknown as NetModule;
  } catch {
    throw new TransportError(
      'TCP transport requires Node.js. In a browser, connect over Streamable HTTP ' +
        'with an http(s):// URL instead.',
    );
  }
}
