from __future__ import annotations

from typing import Any

import requests


class Aria2Client:
    def __init__(self, rpc_url: str, secret: str) -> None:
        self.rpc_url = rpc_url
        self.secret = secret
        self._request_id = 0

    def _call(self, method: str, *params: Any) -> Any:
        self._request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": str(self._request_id),
            "method": method,
            "params": [f"token:{self.secret}", *params],
        }
        response = requests.post(self.rpc_url, json=payload, timeout=5)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise RuntimeError(data["error"].get("message", "aria2 RPC error"))
        return data.get("result")

    def add_uri(self, url: str, directory: str | None = None) -> str:
        options: dict[str, str] = {}
        if directory:
            options["dir"] = directory
        return self._call("aria2.addUri", [url], options)

    def pause(self, gid: str) -> str:
        return self._call("aria2.pause", gid)

    def resume(self, gid: str) -> str:
        return self._call("aria2.unpause", gid)

    def remove(self, gid: str) -> str:
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
            "files",
            "errorMessage",
        ]
        return self._call("aria2.tellStatus", gid, keys)

    def tell_active(self) -> list[dict[str, Any]]:
        return self._call("aria2.tellActive")

    def tell_waiting(self, offset: int = 0, count: int = 100) -> list[dict[str, Any]]:
        return self._call("aria2.tellWaiting", offset, count)

    def tell_stopped(self, offset: int = 0, count: int = 100) -> list[dict[str, Any]]:
        return self._call("aria2.tellStopped", offset, count)
