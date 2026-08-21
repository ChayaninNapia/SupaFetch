from __future__ import annotations

import logging
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import requests


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Aria2Service:
    host: str = "127.0.0.1"
    port: int = 6800
    secret: str = "supafetch-local"
    process: subprocess.Popen | None = None
    log_handle: object | None = None

    @property
    def rpc_url(self) -> str:
        return f"http://{self.host}:{self.port}/jsonrpc"

    def is_running(self) -> bool:
        try:
            response = requests.post(
                self.rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "id": "health",
                    "method": "aria2.getVersion",
                    "params": [f"token:{self.secret}"],
                },
                timeout=0.5,
            )
            return response.ok
        except requests.RequestException:
            return False

    def start(self) -> None:
        if self.is_running():
            logger.info("Using existing aria2 RPC at %s", self.rpc_url)
            return

        executable = shutil.which("aria2c")
        if not executable:
            raise RuntimeError(
                "aria2c was not found in PATH. Install aria2 and restart SupaFetch."
            )

        log_path = Path.cwd() / "supafetch-aria2.log"
        self.log_handle = log_path.open("a", encoding="utf-8")
        logger.info("Starting aria2c: %s", executable)
        logger.info("aria2 process log: %s", log_path)

        command = [
            executable,
            "--enable-rpc=true",
            f"--rpc-listen-port={self.port}",
            f"--rpc-secret={self.secret}",
            "--rpc-listen-all=false",
            "--continue=true",
            "--max-connection-per-server=16",
            "--split=2",
            "--min-split-size=1M",
            "--file-allocation=none",
            # HTTP/runtime tuning. Keep-alive is explicit so every managed aria2
            # process uses the same profile; the larger memory/socket buffers
            # reduce small write/receive overhead without changing file data.
            "--enable-http-keep-alive=true",
            "--disk-cache=64M",
            "--socket-recv-buffer-size=1M",
            "--connect-timeout=10",
            "--timeout=30",
            "--max-tries=5",
            "--retry-wait=1",
            "--summary-interval=0",
            "--console-log-level=info",
            "--log-level=debug",
            "--log=-",
        ]

        creationflags = 0
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            creationflags = subprocess.CREATE_NO_WINDOW

        self.process = subprocess.Popen(
            command,
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.is_running():
                logger.info("aria2 RPC ready at %s", self.rpc_url)
                return
            time.sleep(0.1)

        self.stop()
        raise RuntimeError("aria2 RPC did not start within 5 seconds.")

    def stop(self) -> None:
        if self.process and self.process.poll() is None:
            logger.info("Stopping aria2c")
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.process = None

        if self.log_handle:
            try:
                self.log_handle.close()
            except Exception:
                pass
            self.log_handle = None
