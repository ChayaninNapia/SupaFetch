from __future__ import annotations

import shutil
import subprocess
import time
from dataclasses import dataclass

import requests


@dataclass(slots=True)
class Aria2Service:
    host: str = "127.0.0.1"
    port: int = 6800
    secret: str = "supafetch-local"
    process: subprocess.Popen | None = None

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
            return

        executable = shutil.which("aria2c")
        if not executable:
            raise RuntimeError(
                "aria2c was not found in PATH. Install aria2 and restart SupaFetch."
            )

        command = [
            executable,
            "--enable-rpc=true",
            f"--rpc-listen-port={self.port}",
            f"--rpc-secret={self.secret}",
            "--rpc-listen-all=false",
            "--continue=true",
            "--max-connection-per-server=8",
            "--split=8",
            "--min-split-size=1M",
            "--file-allocation=none",
            "--summary-interval=0",
        ]

        creationflags = 0
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            creationflags = subprocess.CREATE_NO_WINDOW

        self.process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.is_running():
                return
            time.sleep(0.1)

        self.stop()
        raise RuntimeError("aria2 RPC did not start within 5 seconds.")

    def stop(self) -> None:
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.process = None
