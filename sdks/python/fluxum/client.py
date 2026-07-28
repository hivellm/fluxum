"""The asyncio-first Fluxum client (SPEC-011 SDK-060/SDK-061).

One `Connection` drives a session over FluxRPC/TCP: authenticate, subscribe
(each query's `InitialData` lands in a local row cache), call reducers, and
receive `TxUpdate` diffs on the same socket. On connection loss the client
reconnects, re-authenticates, resubscribes every active query and reconciles
its cache to the fresh snapshots (SDK-047) — the application keeps its handle
across the outage.

The cache is per-table, keyed by primary key, with per-query ownership so an
`unsubscribe` drops only the rows that query held and rows still covered by
another subscription survive (SDK-044).
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

from . import protocol
from .fluxbin import FluxValue

#: A table's cache hooks: its name and how to derive a primary key from a full
#: row's bytes and from a delete entry's bytes (the pk field(s) alone).
class TableSchema:
    __slots__ = ("name", "pk_of_row", "pk_of_delete")

    def __init__(
        self,
        name: str,
        pk_of_row: Callable[[bytes], str],
        pk_of_delete: Callable[[bytes], str],
    ) -> None:
        self.name = name
        self.pk_of_row = pk_of_row
        self.pk_of_delete = pk_of_delete


class FluxumError(Exception):
    """A server-reported failure.

    `code` is the stable SPEC-028 catalog code — the portable assertion.
    `catalog` is the canonical SCREAMING_SNAKE name for an `Error` frame
    (`None` for a reducer rejection, which carries a code but no catalog
    name). `app_code` is the reducer's optional application code.
    """

    def __init__(
        self,
        code: int,
        message: str,
        catalog: Optional[str] = None,
        app_code: Optional[str] = None,
    ) -> None:
        super().__init__(f"error {code}: {message}")
        self.code = code
        self.catalog = catalog
        self.app_code = app_code
        self.message = message


class Cache:
    """The client's row cache: per table, a pk → row-bytes map, materialized
    from the rows any active subscription currently holds."""

    def __init__(self, tables: Sequence[TableSchema]) -> None:
        self._schemas = {t.name: t for t in tables}
        self._rows: Dict[str, Dict[str, bytes]] = {t.name: {} for t in tables}
        # pk → the set of query_ids that hold it; a row is visible iff non-empty.
        self._owners: Dict[str, Dict[str, Set[int]]] = {t.name: {} for t in tables}

    def rows(self, table: str) -> List[bytes]:
        """Every currently-cached row of `table`, as raw FluxBIN bytes."""
        return list(self._rows.get(table, {}).values())

    def _insert(self, table: str, query_id: int, row: bytes) -> None:
        schema = self._schemas.get(table)
        if schema is None:
            return  # a table the runner did not register — ignore
        pk = schema.pk_of_row(row)
        self._rows[table][pk] = row
        self._owners[table].setdefault(pk, set()).add(query_id)

    def _delete(self, table: str, query_id: int, entry: bytes) -> None:
        schema = self._schemas.get(table)
        if schema is None:
            return
        pk = schema.pk_of_delete(entry)
        owners = self._owners[table].get(pk)
        if owners is None:
            return
        owners.discard(query_id)
        if not owners:
            self._owners[table].pop(pk, None)
            self._rows[table].pop(pk, None)

    def _drop_query(self, query_id: int) -> None:
        """Remove `query_id` from every row's owner set; a row with no owner
        left leaves the cache (SDK-044)."""
        for table, owners in self._owners.items():
            for pk in list(owners.keys()):
                owners[pk].discard(query_id)
                if not owners[pk]:
                    owners.pop(pk, None)
                    self._rows[table].pop(pk, None)

    def _clear(self) -> None:
        for table in self._rows:
            self._rows[table].clear()
            self._owners[table].clear()


class _Sub:
    __slots__ = ("sql", "query_id")

    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.query_id = 0


class Connection:
    """A live client session. Construct with [`Connection.connect`]."""

    def __init__(
        self,
        host: str,
        port: int,
        token: bytes,
        tables: Sequence[TableSchema],
        light_updates: bool = False,
    ) -> None:
        self._host = host
        self._port = port
        self._token = token
        # RPC-035: ask for TxUpdateLight broadcasts (provenance stripped,
        # row diffs + resume cursor kept). Re-applied on every reconnect.
        self._light_updates = light_updates
        self.cache = Cache(tables)
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._frames = protocol.FrameReader()
        self._next_id = 1
        self._pending: Dict[int, asyncio.Queue] = {}
        self._subs: List[_Sub] = []
        self.identity = "00" * 32
        self._closed = False
        self._reader_task: Optional[asyncio.Task] = None

    # --- lifecycle ----------------------------------------------------------

    @classmethod
    async def connect(
        cls,
        url: str,
        token: bytes = b"",
        tables: Sequence[TableSchema] = (),
        light_updates: bool = False,
    ) -> "Connection":
        """Open and authenticate a session. `url` is `fluxum://host:port` or a
        bare `host:port` (TCP). `light_updates=True` negotiates RPC-035
        TxUpdateLight broadcasts."""
        host, port = _parse_url(url)
        conn = cls(host, port, token, tables, light_updates=light_updates)
        await conn._establish()
        conn._reader_task = asyncio.ensure_future(conn._read_loop())
        return conn

    async def close(self) -> None:
        """Close the session; the reconnect loop stops."""
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
        self._shutdown_socket()

    def _shutdown_socket(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
        self._writer = None
        self._reader = None

    async def _establish(self) -> None:
        """Connect the socket, authenticate, and resubscribe the replay set."""
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        try:
            self._writer.get_extra_info("socket").setsockopt(
                __import__("socket").IPPROTO_TCP, __import__("socket").TCP_NODELAY, 1
            )
        except Exception:
            pass
        self._frames = protocol.FrameReader()

        # Authenticate inline (the reader task is not running yet).
        auth_id = self._alloc_id()
        # [id, token, compression, tx_updates, namespace]
        tx_updates = "light" if self._light_updates else None
        await self._send_raw("Authenticate", [auth_id, self._token, None, tx_updates, None])
        message = await self._read_message_inline()
        while message.tag != "AuthResult" or int(message.payload[0]) != auth_id:
            if message.tag == "Error" and _msg_id(message) == auth_id:
                raise _error_from(message)
            message = await self._read_message_inline()
        self.identity = _hex(message.payload[1])

        # Resubscribe the replay set against the fresh session (SDK-047).
        # This runs from the reconnect path, where the background reader is
        # NOT looping — so the InitialData is read INLINE off the socket, not
        # awaited through the reader's per-id queue (which nothing would fill).
        if self._subs:
            self.cache._clear()
            sqls = [s.sql for s in self._subs]
            self._subs = []
            await self._resubscribe_inline(sqls)

    async def _resubscribe_inline(self, queries: List[str]) -> None:
        subs = [_Sub(sql) for sql in queries]
        mid = self._alloc_id()
        await self._send_raw("Subscribe", [mid, queries])
        query_ids: List[int] = []
        while len(query_ids) < len(queries):
            message = await self._read_message_inline()
            if message.tag == "Error" and _msg_id(message) == mid:
                raise _error_from(message)
            if message.tag != "InitialData" or int(message.payload[0]) != mid:
                # A stray update can arrive mid-resubscribe; apply it and
                # keep waiting for our snapshots.
                if message.tag == "TxUpdate":
                    self._apply_tables(message.payload[5])
                elif message.tag == "TxUpdateLight":
                    self._apply_tables(message.payload[2])
                continue
            for entry in message.payload[2]:
                qid, inserts, deletes = protocol.table_update(entry)
                table = entry[1]
                query_ids.append(qid)
                for entry_bytes in deletes:
                    self.cache._delete(table, qid, entry_bytes)
                for row in inserts:
                    self.cache._insert(table, qid, row)
        for sub, qid in zip(subs, query_ids):
            sub.query_id = qid
        self._subs.extend(subs)

    # --- reading ------------------------------------------------------------

    async def _read_message_inline(self) -> protocol.ServerMessage:
        """Read one server message directly off the socket (handshake only)."""
        while True:
            body = self._frames.next_body()
            if body is not None:
                return protocol.decode_message(body)
            assert self._reader is not None
            chunk = await self._reader.read(65536)
            if not chunk:
                raise ConnectionError("connection closed during handshake")
            self._frames.push(chunk)

    async def _read_loop(self) -> None:
        """Background: decode frames, route replies, apply TxUpdates; reconnect
        with backoff on connection loss."""
        backoff = 0.2
        while not self._closed:
            try:
                assert self._reader is not None
                chunk = await self._reader.read(65536)
                if not chunk:
                    raise ConnectionError("connection closed")
                self._frames.push(chunk)
                while True:
                    body = self._frames.next_body()
                    if body is None:
                        break
                    self._route(protocol.decode_message(body))
                backoff = 0.2
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._closed:
                    return
                self._fail_pending()
                self._shutdown_socket()
                # Reconnect: re-establish (auth + resubscribe + reconcile).
                while not self._closed:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 5.0)
                    try:
                        await self._establish()
                        backoff = 0.2
                        break
                    except Exception:
                        continue

    def _route(self, message: protocol.ServerMessage) -> None:
        tag = message.tag
        if tag == "TxUpdate":
            # [tx_id, timestamp, reducer_name, caller, duration_us, tables, ...]
            self._apply_tables(message.payload[5])
            return
        if tag == "TxUpdateLight":
            # RPC-035: [tx_id, timestamp, tables, shard_id, tx_offset] —
            # same row diffs, provenance stripped; one apply path.
            self._apply_tables(message.payload[2])
            return
        mid = _msg_id(message)
        if mid is not None and mid in self._pending:
            self._pending[mid].put_nowait(message)
            return
        # An Error without a live waiter (or a stray frame) is dropped.

    def _fail_pending(self) -> None:
        """Wake every in-flight request with a disconnect so callers do not
        hang across a reconnect."""
        for queue in self._pending.values():
            queue.put_nowait(protocol.ServerMessage("__disconnected__", []))

    # --- requests -----------------------------------------------------------

    def _alloc_id(self) -> int:
        mid = self._next_id
        self._next_id += 1
        return mid

    async def _send_raw(self, tag: str, payload: List[Any]) -> None:
        assert self._writer is not None
        self._writer.write(protocol.encode_message(tag, payload))
        await self._writer.drain()

    async def _request(self, tag: str, payload_for) -> protocol.ServerMessage:
        """Send a request and await its single reply, correlated by id."""
        mid = self._alloc_id()
        queue: asyncio.Queue = asyncio.Queue()
        self._pending[mid] = queue
        try:
            await self._send_raw(tag, payload_for(mid))
            message = await asyncio.wait_for(queue.get(), timeout=10.0)
            if message.tag == "__disconnected__":
                raise ConnectionError("disconnected while awaiting a reply")
            return message
        finally:
            self._pending.pop(mid, None)

    # --- public operations --------------------------------------------------

    async def subscribe(self, queries: Sequence[str]) -> List[int]:
        """Register `queries`; await each `InitialData`, apply it to the cache,
        and return the server-assigned `query_id`s in query order (RPC-022)."""
        query_ids = await self._subscribe_inline(list(queries))
        return query_ids

    async def _subscribe_inline(self, queries: List[str]) -> List[int]:
        subs = [_Sub(sql) for sql in queries]
        mid = self._alloc_id()
        queue: asyncio.Queue = asyncio.Queue()
        self._pending[mid] = queue
        query_ids: List[int] = []
        try:
            await self._send_raw("Subscribe", [mid, queries])
            # One InitialData per query, each echoing this id (RPC-022/032).
            while len(query_ids) < len(queries):
                message = await asyncio.wait_for(queue.get(), timeout=10.0)
                if message.tag == "__disconnected__":
                    raise ConnectionError("disconnected during subscribe")
                if message.tag == "Error":
                    raise _error_from(message)
                if message.tag != "InitialData":
                    continue
                # InitialData: [id, schema_version, tables, ...]
                for entry in message.payload[2]:
                    qid, inserts, deletes = protocol.table_update(entry)
                    table = entry[1]
                    query_ids.append(qid)
                    for entry_bytes in deletes:
                        self.cache._delete(table, qid, entry_bytes)
                    for row in inserts:
                        self.cache._insert(table, qid, row)
        finally:
            self._pending.pop(mid, None)
        for sub, qid in zip(subs, query_ids):
            sub.query_id = qid
        self._subs.extend(subs)
        return query_ids

    async def unsubscribe(self, query_ids: Sequence[int]) -> None:
        """Drop the subscriptions whose `query_id`s are given (RPC-024). Rows
        those queries alone held leave the cache."""
        wanted = set(int(q) for q in query_ids)
        await self._send_raw("Unsubscribe", [self._alloc_id(), list(wanted)])
        for qid in wanted:
            self.cache._drop_query(qid)
        self._subs = [s for s in self._subs if s.query_id not in wanted]

    async def call_reducer(self, name: str, args: Sequence[Any]) -> None:
        """Call reducer `name` with `args`; return on commit, raise
        [`FluxumError`] on rejection (RPC-021/031)."""
        # [id, reducer, version, args, idempotency_key]
        message = await self._request(
            "ReducerCall", lambda mid: [mid, name, None, list(args), None]
        )
        if message.tag == "Error":
            raise _error_from(message)
        if message.tag == "ReducerResult":
            outcome = message.payload[1]
            # ["Ok", nil] or ["Err", [code, app_code, message]]
            if isinstance(outcome, list) and outcome and outcome[0] == "Err":
                err = outcome[1]
                raise FluxumError(int(err[0]), str(err[2]), app_code=_opt_str(err[1]))
            return
        raise FluxumError(0, f"unexpected reply to reducer call: {message.tag}")

    # --- TxUpdate -----------------------------------------------------------

    def _apply_tables(self, tables) -> None:
        """Apply one commit's per-table row diffs (TxUpdate or TxUpdateLight)."""
        for entry in tables:
            qid, inserts, deletes = protocol.table_update(entry)
            table = entry[1]
            # Deletes BEFORE inserts: an update arrives as delete-old + insert-
            # new of the same pk, and the new row must win (SPEC-005).
            for entry_bytes in deletes:
                self.cache._delete(table, qid, entry_bytes)
            for row in inserts:
                self.cache._insert(table, qid, row)


# --- helpers -----------------------------------------------------------------


def _parse_url(url: str) -> tuple:
    rest = url
    for scheme in ("fluxum://", "tcp://"):
        if rest.startswith(scheme):
            rest = rest[len(scheme) :]
            break
    if "://" in rest:
        raise ValueError(f"unsupported URL scheme: {url}")
    host, _, port = rest.rpartition(":")
    if not host or not port:
        raise ValueError(f"expected host:port, got {url!r}")
    return host, int(port)


def _hex(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return str(value)


def _opt_str(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _msg_id(message: protocol.ServerMessage) -> Optional[int]:
    if message.tag in ("AuthResult", "ReducerResult", "InitialData"):
        return int(message.payload[0])
    if message.tag == "Error":
        mid = message.payload[0]
        return None if mid is None else int(mid)
    return None


def _error_from(message: protocol.ServerMessage) -> FluxumError:
    # Error: [id, code, name, message, severity, retryable, retry_after_ms, ...]
    p = message.payload
    return FluxumError(int(p[1]), str(p[3]), catalog=str(p[2]))
