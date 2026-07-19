// TCP transport against a real socket (SPEC-006 RPC-003, SDK-082).
//
// A loopback server, not a mock: the whole point of this transport is the
// byte stream, and a mock socket would assert the plumbing while skipping the
// part that actually breaks — partial frames arriving across real TCP writes.
import assert from 'node:assert/strict';
import net from 'node:net';
import { test } from 'node:test';

import { decodeMessage, encodeMessage } from '../src/protocol.ts';
import { TcpTransport } from '../src/transport/tcp.ts';
import { connect } from '../src/transport/connect.ts';

/** A server that runs `onConnection` for each client, on an ephemeral port. */
async function serve(
  onConnection: (socket: net.Socket) => void,
): Promise<{ port: number; close: () => Promise<void> }> {
  const server = net.createServer(onConnection);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address !== null && typeof address === 'object');
  return {
    port: address.port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

test('a message round-trips over a real TCP connection', async () => {
  const server = await serve((socket) => {
    socket.on('data', () => socket.write(encodeMessage('AuthResult', [true, null])));
  });

  const transport = await TcpTransport.connect('127.0.0.1', server.port);
  const received = new Promise<string>((resolve) => {
    transport.onFrame((body) => resolve(decodeMessage(body).tag));
  });

  await transport.send(encodeMessage('Authenticate', [1, null, null, null, null]));
  assert.equal(await received, 'AuthResult');

  await transport.close();
  await server.close();
});

test('a frame split across TCP writes is reassembled', async () => {
  // The failure this guards is silent: a reader that assumed one write equals
  // one frame would decode garbage the first time the kernel split a packet.
  const frame = encodeMessage('TxUpdate', [42, null]);
  const server = await serve((socket) => {
    socket.on('data', () => {
      socket.write(frame.subarray(0, 3));
      setTimeout(() => socket.write(frame.subarray(3)), 10);
    });
  });

  const transport = await TcpTransport.connect('127.0.0.1', server.port);
  const received = new Promise<string>((resolve) => {
    transport.onFrame((body) => resolve(decodeMessage(body).tag));
  });

  await transport.send(encodeMessage('Ping', []));
  assert.equal(await received, 'TxUpdate');

  await transport.close();
  await server.close();
});

test('the close handler reports the peer hanging up', async () => {
  const server = await serve((socket) => socket.destroy());

  const transport = await TcpTransport.connect('127.0.0.1', server.port);
  await new Promise<void>((resolve) => transport.onClose(() => resolve()));

  await server.close();
});

test('connecting to a closed port rejects instead of resolving a dead transport', async () => {
  // The socket errors before it ever connects. Resolving here would hand back
  // a transport whose first send fails for reasons that read as unrelated.
  const server = await serve(() => {});
  const port = server.port;
  await server.close();

  await assert.rejects(TcpTransport.connect('127.0.0.1', port));
});

test('fluxum:// selects TCP under Node and connects', async () => {
  // SDK-082 from the other side: the scheme that fails fast in a browser is
  // the one that must work here.
  const server = await serve((socket) => {
    socket.on('data', () => socket.write(encodeMessage('AuthResult', [true, null])));
  });

  const transport = await connect(`fluxum://127.0.0.1:${server.port}`);
  const received = new Promise<string>((resolve) => {
    transport.onFrame((body) => resolve(decodeMessage(body).tag));
  });

  await transport.send(encodeMessage('Authenticate', [1, null, null, null, null]));
  assert.equal(await received, 'AuthResult');

  await transport.close();
  await server.close();
});

test('sending on a closed transport rejects', async () => {
  const server = await serve(() => {});
  const transport = await TcpTransport.connect('127.0.0.1', server.port);
  await transport.close();

  await assert.rejects(transport.send(encodeMessage('Ping', [])), /closed/);
  await server.close();
});
