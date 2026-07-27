"""Shared fixtures: locate the repo, the server binary, and the corpus."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

# The package under test lives at sdks/python/fluxum.
SDK_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = SDK_ROOT.parents[1]
sys.path.insert(0, str(SDK_ROOT))

CORPUS_DIR = REPO_ROOT / "tests" / "conformance"
BINARY = REPO_ROOT / "target" / "debug" / (
    "fluxum-server.exe" if os.name == "nt" else "fluxum-server"
)


def server_available() -> bool:
    return BINARY.exists()


def free_port() -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def _wait_for_port(port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"server did not bind {port} in {timeout}s")


class Server:
    """A spawned fluxum-server with the demo module, on fresh ports + data dir.

    `restart()` kills and relaunches on the SAME ports and data directory —
    the crash-and-recover the corpus `restart_server` step exercises.
    """

    def __init__(self, label: str, timeout: float = 20.0) -> None:
        self.http_port = free_port()
        self.tcp_port = free_port()
        self._timeout = timeout
        self._dir = tempfile.mkdtemp(prefix=f"fluxum-py-{label}-")
        self._env = {
            **os.environ,
            "FLUXUM_PROFILE": "development",
            "FLUXUM_SERVER_HTTP_PORT": str(self.http_port),
            "FLUXUM_SERVER_TCP_PORT": str(self.tcp_port),
            "FLUXUM_STORAGE_DATA_DIR": self._dir,
            "FLUXUM_STORAGE_COMMIT_LOG_DIR": str(Path(self._dir) / "log"),
            "FLUXUM_STORAGE_PAGE_DIR": str(Path(self._dir) / "pages"),
        }
        self._proc = None
        self._launch()

    @property
    def tcp_url(self) -> str:
        return f"fluxum://127.0.0.1:{self.tcp_port}"

    def _launch(self) -> None:
        self._proc = subprocess.Popen(
            [str(BINARY)],
            env=self._env,
            # The scenario's fresh dir is also the CWD, so even config paths
            # that default CWD-relative (./data, ./data/pages) stay isolated
            # per scenario — one server's recovered state must never leak
            # into the next scenario's assertions.
            cwd=self._dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_port(self.tcp_port, self._timeout)

    def restart(self) -> None:
        self.stop()
        self._launch()

    def stop(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()


@pytest.fixture(scope="session")
def corpus():
    import json

    manifest = json.loads((CORPUS_DIR / "corpus.json").read_text())
    return manifest
