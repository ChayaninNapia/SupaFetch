from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

import requests


logger = logging.getLogger(__name__)


class Aria2Client:
    def __init__(self, rpc_url: str, secret: str) -> None:
        self.rpc_url = rpc_url
        self.secret = secret
        self._request_id = 0

    def _call(self, method: str, *params: Any) -> Any:
        self._request_id += 1
        request_id = str(self._request_id)
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": [f"token:{self.secret}", *params],
        }
        logger.debug("RPC -> %s id=%s", method, request_id)
        try:
            response = requests.post(self.rpc_url, json=payload, timeout=5)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException:
            logger.exception("RPC transport failure: %s id=%s", method, request_id)
            raise
        except ValueError:
            logger.exception("RPC returned invalid JSON: %s id=%s", method, request_id)
            raise

        if "error" in data:
            error = data["error"]
            logger.error("RPC error: %s id=%s error=%r", method, request_id, error)
            raise RuntimeError(error.get("message", "aria2 RPC error"))

        logger.debug("RPC <- %s id=%s ok", method, request_id)
        return data.get("result")

    def add_uri(self, url: str, directory: str | None = None) -> str:
        options: dict[str, str] = {}
        if directory:
            options["dir"] = directory
        gid = self._call("aria2.addUri", [url], options)
        logger.info("Download added gid=%s host=%s", gid, urlparse(url).hostname)
        return gid

    def pause(self, gid: str) -> str:
        logger.info("Pause requested gid=%s", gid)
        return self._call("aria2.pause", gid)

    def resume(self, gid: str) -> str:
        logger.info("Resume requested gid=%s", gid)
        return self._call("aria2.unpause", gid)

    def remove(self, gid: str) -> str:
        logger.info("Remove requested gid=%s", gid)
        try:
            return self._call("aria2.remove", gid)
        except RuntimeError:
            return self._call("aria2.removeDownloadResult", gid)

    def tell_status(self, gid: str) -> dict[str, Any]:
        keys = [
            "gid",
            "status",
            "totalLength",
            "completedLength",
            "downloadSpeed",
            "connections",
            "files",
            "errorCode",
            "errorMessage",
        ]
        return self._call("aria2.tellStatus", gid, keys)

    def tell_active(self) -> list[dict[str, Any]]:
        return self._call("aria2.tellActive")

    def tell_waiting(self, offset: int = 0, count: int = 100) -> list[dict[str, Any]]:
        return self._call("aria2.tellWaiting", offset, count)

    def tell_stopped(self, offset: int = 0, count: int = 100) -> list[dict[str, Any]]:
        return self._call("aria2.tellStopped", offset, count)
