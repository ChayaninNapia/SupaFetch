from __future__ import annotations

import logging
import os
import shutil
import socket
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
        return (
            f"http://{self.host}:{self.port}/jsonrpc"
        )

    def _rpc(
        self,
        method: str,
        *params,
        timeout: float = 0.7,
    ):
        response = requests.post(
            self.rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": "service",
                "method": method,
                "params": [
                    f"token:{self.secret}",
                    *params,
                ],
            },
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise RuntimeError(
                data["error"].get(
                    "message",
                    "aria2 RPC error",
                )
            )
        return data.get("result")

    def is_running(self) -> bool:
        try:
            return bool(
                self._rpc(
                    "aria2.getVersion",
                    timeout=0.5,
                )
            )
        except Exception:
            return False

    def _active_count(self) -> int:
        try:
            active = self._rpc(
                "aria2.tellActive",
                [],
                timeout=0.8,
            )
            return len(active or [])
        except Exception:
            return 0

    def _shutdown_existing(self) -> None:
        try:
            self._rpc(
                "aria2.shutdown",
                timeout=1.0,
            )
        except Exception:
            logger.debug(
                "Could not gracefully shut down existing aria2 RPC",
                exc_info=True,
            )

        deadline = time.monotonic() + 3.0
        while (
            time.monotonic() < deadline
            and self.is_running()
        ):
            time.sleep(0.1)

    def _port_is_free(self, port: int) -> bool:
        with socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        ) as sock:
            sock.settimeout(0.2)
            return (
                sock.connect_ex(
                    (self.host, port)
                )
                != 0
            )

    def _select_free_port(self) -> None:
        for candidate in range(
            self.port,
            self.port + 20,
        ):
            if self._port_is_free(candidate):
                self.port = candidate
                return
        raise RuntimeError(
            "Could not find a free local port for aria2 RPC."
        )

    def start(self) -> None:
        if self.is_running():
            active_count = self._active_count()
            if active_count <= 0:
                logger.info(
                    "Existing idle aria2 RPC detected at %s; "
                    "restarting with current performance profile",
                    self.rpc_url,
                )
                self._shutdown_existing()
            else:
                logger.warning(
                    "Existing aria2 RPC has %s active download(s); "
                    "starting this SupaFetch instance on another port",
                    active_count,
                )
                self.port += 1

        if not self._port_is_free(self.port):
            self._select_free_port()

        executable = shutil.which("aria2c")
        if not executable:
            raise RuntimeError(
                "aria2c was not found in PATH. "
                "Install aria2 and restart SupaFetch."
            )

        log_path = (
            Path.cwd() / "supafetch-aria2.log"
        )
        self.log_handle = log_path.open(
            "a",
            encoding="utf-8",
        )
        logger.info(
            "Starting aria2c: %s",
            executable,
        )
        logger.info(
            "aria2 process log: %s",
            log_path,
        )

        aria2_log_level = os.getenv(
            "SUPAFETCH_ARIA2_LOG_LEVEL",
            "notice",
        ).lower()
        if aria2_log_level not in {
            "debug",
            "info",
            "notice",
            "warn",
            "error",
        }:
            aria2_log_level = "notice"

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
            "--enable-http-keep-alive=true",
            "--disk-cache=64M",
            "--socket-recv-buffer-size=1M",
            "--connect-timeout=10",
            "--timeout=30",
            "--max-tries=5",
            "--retry-wait=1",
            "--summary-interval=0",
            "--console-log-level=warn",
            f"--log-level={aria2_log_level}",
            "--log=-",
        ]

        creationflags = 0
        if hasattr(
            subprocess,
            "CREATE_NO_WINDOW",
        ):
            creationflags = (
                subprocess.CREATE_NO_WINDOW
            )

        self.process = subprocess.Popen(
            command,
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.is_running():
                logger.info(
                    "aria2 RPC ready at %s",
                    self.rpc_url,
                )
                return
            if (
                self.process is not None
                and self.process.poll()
                is not None
            ):
                break
            time.sleep(0.1)

        self.stop()
        raise RuntimeError(
            "aria2 RPC did not start within 5 seconds."
        )

    def stop(self) -> None:
        if (
            self.process
            and self.process.poll() is None
        ):
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
