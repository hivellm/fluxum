"""The shared SDK conformance corpus, run by the Python client (TST-052).

Every SDK executes the SAME declarative corpus (`tests/conformance/` at the
repo root) against the same server build; identical observable results are
required from all runners. This module is the Python runner: an interpreter
of the corpus step vocabulary, release-blocking when red (SDK-064).
"""

from __future__ import annotations

import asyncio
import json

import pytest

from conftest import CORPUS_DIR, Server, server_available

from fluxum import Connection, FluxumError, TableSchema
from fluxum.fluxbin import RowReader

pytestmark = pytest.mark.skipif(
    not server_available(),
    reason="no fluxum-server binary — run: cargo build -p fluxum-server",
)

AWAIT_S = 5.0

# 64-bit widths surface as decimal strings (precision does not survive every
# language's number type); everything else is native (corpus README).
STRINGY_TYPES = frozenset({"U64", "I64", "Timestamp", "EntityId"})


def _scenarios():
    manifest = json.loads((CORPUS_DIR / "corpus.json").read_text())
    return manifest["scenarios"]


def _table_specs(manifest) -> dict:
    return manifest["tables"]


def _canonical(value, flux_type):
    if flux_type in STRINGY_TYPES:
        return str(value)
    return value


class Interpreter:
    """Runs one scenario's steps against spawned sessions (a port of the
    shared TypeScript interpreter — the corpus, not this file, is the truth)."""

    def __init__(self, manifest, server: Server) -> None:
        self._tables = manifest["tables"]
        self._server = server
        self._clients: dict = {}
        self._handles: dict = {}

    # cache-hook construction --------------------------------------------------

    def _table_schemas(self):
        schemas = []
        for name, spec in self._tables.items():
            columns = spec["columns"]
            types = [t for _, t in columns]
            pk_index = next(i for i, (col, _) in enumerate(columns) if col == spec["primary_key"])
            pk_type = types[pk_index]

            def pk_of_row(row, types=types, pk_index=pk_index):
                reader = RowReader(row)
                value = None
                for i in range(pk_index + 1):
                    value = reader.read(types[i])
                return str(value)

            def pk_of_delete(entry, pk_type=pk_type):
                return str(RowReader(entry).read(pk_type))

            schemas.append(TableSchema(name, pk_of_row, pk_of_delete))
        return schemas

    def _canonical_row(self, table: str, row: bytes) -> dict:
        columns = self._tables[table]["columns"]
        reader = RowReader(row)
        out = {}
        for name, flux_type in columns:
            out[name] = _canonical(reader.read(flux_type), flux_type)
        return out

    # value matching -----------------------------------------------------------

    def _resolve(self, expected):
        if isinstance(expected, str) and expected.startswith("$identity:"):
            return self._clients[expected[len("$identity:") :]].identity
        return expected

    def _matches(self, expected, actual) -> bool:
        if expected == "*":
            return True
        return self._resolve(expected) == actual

    def _row_matches(self, expected: dict, actual: dict) -> bool:
        return all(self._matches(v, actual.get(k)) for k, v in expected.items())

    def _client(self, name):
        client = self._clients.get(str(name))
        assert client is not None, f"step names client {name!r} before its connect step"
        return client

    # step execution -----------------------------------------------------------

    async def run_step(self, step: dict) -> None:
        (kind, body), = step.items()
        method = getattr(self, f"_step_{kind}", None)
        assert method is not None, f"unknown step {kind!r} — runner/corpus_version disagree"
        await method(body)

    async def _step_connect(self, body) -> None:
        name = str(body["client"])
        assert name not in self._clients, f"client {name!r} connected twice"
        token = body.get("token")
        token_bytes = b"" if token is None else str(token).encode("utf-8")
        self._clients[name] = await Connection.connect(
            self._server.tcp_url,
            token_bytes,
            self._table_schemas(),
            # RPC-035: the light-updates scenario negotiates TxUpdateLight.
            light_updates=bool(body.get("light_updates", False)),
        )

    async def _step_close(self, body) -> None:
        await self._client(body["client"]).close()

    async def _step_restart_server(self, body) -> None:
        self._server.restart()

    async def _step_subscribe(self, body) -> None:
        ids = await self._client(body["client"]).subscribe(list(body["queries"]))
        if isinstance(body.get("as"), str):
            self._handles[body["as"]] = ids

    async def _step_unsubscribe(self, body) -> None:
        label = str(body["handles"])
        ids = self._handles.get(label)
        assert ids is not None, f"unsubscribe names handle {label!r} before its subscribe"
        await self._client(body["client"]).unsubscribe(ids)

    async def _step_call(self, body) -> None:
        client = self._client(body["client"])
        expect = body.get("expect_error")
        coro = client.call_reducer(str(body["reducer"]), list(body["args"]))
        if expect is None:
            await coro
            return
        await self._expect_error(coro, expect)

    async def _step_subscribe_error(self, body) -> None:
        coro = self._client(body["client"]).subscribe(list(body["queries"]))
        await self._expect_error(coro, body["expect_error"])

    async def _step_call_until_error(self, body) -> None:
        client = self._client(body["client"])
        attempts = int(body["attempts"])
        expect = body["expect_error"]
        for _ in range(attempts):
            try:
                await client.call_reducer(str(body["reducer"]), list(body["args"]))
            except FluxumError as err:
                self._match_error(err, expect)
                return
        pytest.fail(f"all {attempts} calls succeeded; expected {expect}")

    async def _step_await_row(self, body) -> None:
        await self._await_count(body, want=1, at_least=True)

    async def _step_await_gone(self, body) -> None:
        await self._await_count(body, want=0, at_least=False)

    async def _step_await_count(self, body) -> None:
        await self._await_count(body, want=int(body["count"]), at_least=False)

    async def _step_expect_cache(self, body) -> None:
        client = self._client(body["client"])
        table = str(body["table"])
        expected = list(body["rows"])
        actual = [self._canonical_row(table, r) for r in client.cache.rows(table)]
        remaining = list(actual)
        for want in expected:
            match = next((i for i, row in enumerate(remaining) if self._row_matches(want, row)), None)
            assert match is not None, f"{table}: no cached row matches {want}; cache: {remaining}"
            remaining.pop(match)
        assert not remaining, f"{table}: unexpected extra rows: {remaining}"

    async def _step_expect_distinct_identities(self, body) -> None:
        names = list(body["clients"])
        identities = [self._client(n).identity for n in names]
        assert len(set(identities)) == len(names), f"identities collide: {identities}"

    # helpers ------------------------------------------------------------------

    async def _await_count(self, body, want: int, at_least: bool) -> None:
        client = self._client(body["client"])
        table = str(body["table"])
        where = body.get("where") or {}
        deadline = asyncio.get_event_loop().time() + AWAIT_S
        while True:
            matching = sum(
                1
                for r in client.cache.rows(table)
                if self._row_matches(where, self._canonical_row(table, r))
            )
            if (matching >= want) if at_least else (matching == want):
                return
            assert asyncio.get_event_loop().time() < deadline, (
                f"await {table} {where}: {matching} matching, wanted {want} after {AWAIT_S}s"
            )
            await asyncio.sleep(0.025)

    async def _expect_error(self, coro, expect) -> None:
        try:
            await coro
        except FluxumError as err:
            self._match_error(err, expect)
            return
        pytest.fail("operation succeeded; the scenario expected an error")

    def _match_error(self, err: FluxumError, expect: dict) -> None:
        if "contains" in expect:
            assert expect["contains"] in err.message, f'"{err.message}" lacks "{expect["contains"]}"'
        if "code" in expect:
            assert err.code == expect["code"], f"code {err.code} != {expect['code']} ({err.message})"
        if "catalog" in expect:
            assert err.catalog == expect["catalog"], f"catalog {err.catalog} != {expect['catalog']}"

    async def close(self) -> None:
        await asyncio.gather(
            *(c.close() for c in self._clients.values()), return_exceptions=True
        )


@pytest.mark.parametrize("scenario_name", _scenarios())
def test_scenario(scenario_name, corpus):
    scenario = json.loads((CORPUS_DIR / "scenarios" / f"{scenario_name}.json").read_text())
    server = Server(scenario_name)
    try:
        asyncio.run(_run(corpus, server, scenario))
    finally:
        server.stop()


async def _run(corpus, server, scenario) -> None:
    runner = Interpreter(corpus, server)
    try:
        for step in scenario["steps"]:
            await runner.run_step(step)
    finally:
        await runner.close()
