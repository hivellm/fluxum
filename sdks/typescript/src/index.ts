// Public surface of `@hivehub/fluxum`.
//
// The build that turns this into shipped ESM/CJS plus `.d.ts`, under the
// SDK-083 size budget, lands with the packaging unit. What is exported here is
// the runtime as it stands: the wire layer and both transports.

export { connect } from './transport/connect.ts';
export type { ConnectOptions } from './transport/connect.ts';

export { HttpTransport, FLUXUM_CONTENT_TYPE, SESSION_HEADER } from './transport/http.ts';
export type { HttpTransportOptions } from './transport/http.ts';

export { TcpTransport, DEFAULT_TCP_PORT } from './transport/tcp.ts';
export type { TcpTransportOptions } from './transport/tcp.ts';

export { SessionExpiredError, TransportError } from './transport/types.ts';
export type { CloseHandler, FrameHandler, Transport } from './transport/types.ts';

export {
  DEFAULT_MAX_FRAME_BYTES,
  FRAME_HEADER_LEN,
  FluxumFrameReader,
  KEEPALIVE_FRAME,
  ProtocolError,
  decodeMessage,
  encodeFrame,
  encodeMessage,
  sliceRowList,
} from './protocol.ts';
export type { ServerMessage } from './protocol.ts';

export { FluxBinError, RowReader, decodeRow, toHex } from './fluxbin.ts';
export type { FluxType, FluxValue } from './fluxbin.ts';

export { FluxumClient, ReducerError, SchemaMismatchError, ServerError } from './client.ts';
export type { FluxumClientOptions, RowListener } from './client.ts';

export { RowCache, UnknownTableError } from './cache.ts';
export type { RowEvent, TableDiff, TableSchema, TableSnapshot } from './cache.ts';

export { BoundedQueue, QueueOverflowError } from './queue.ts';
export type { BoundedQueueOptions } from './queue.ts';

export { backoffDelay, reconnect, ReconnectFailedError } from './reconnect.ts';
export type { BackoffOptions, ReconnectHandlers, ReconnectOptions } from './reconnect.ts';
