from __future__ import annotations

import logging
import threading
from typing import Any
from urllib.parse import urlparse

import requests


logger = logging.getLogger(__name__)


class Aria2RpcTimeout(RuntimeError):
    """Raised when the local aria2 RPC endpoint is temporarily unresponsive."""


class Aria2Client:
    STATUS_KEYS = [
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

    def __init__(
        self,
        rpc_url: str,
        secret: str,
    ) -> None:
        self.rpc_url = rpc_url
        self.secret = secret
        self._request_id = 0
        self._id_lock = threading.Lock()
        self._local = threading.local()
        self._sessions: list[requests.Session] = []
        self._sessions_lock = threading.Lock()

    def _next_request_id(self) -> str:
        with self._id_lock:
            self._request_id += 1
            return str(self._request_id)

    def _session(self) -> requests.Session:
        session = getattr(
            self._local,
            "session",
            None,
        )
        if session is None:
            session = requests.Session()
            self._local.session = session
            with self._sessions_lock:
                self._sessions.append(session)
        return session

    def _call(
        self,
        method: str,
        *params: Any,
        timeout: float
        | tuple[float, float] = 5.0,
    ) -> Any:
        request_id = self._next_request_id()
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": [
                f"token:{self.secret}",
                *params,
            ],
        }
        logger.debug(
            "RPC -> %s id=%s",
            method,
            request_id,
        )
        try:
            response = self._session().post(
                self.rpc_url,
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
        except requests.Timeout as exc:
            logger.warning(
                "RPC timeout: %s id=%s timeout=%s",
                method,
                request_id,
                timeout,
            )
            raise Aria2RpcTimeout(
                f"aria2 RPC timed out while calling {method}"
            ) from exc
        except requests.RequestException:
            logger.exception(
                "RPC transport failure: %s id=%s",
                method,
                request_id,
            )
            raise
        except ValueError:
            logger.exception(
                "RPC returned invalid JSON: %s id=%s",
                method,
                request_id,
            )
            raise

        if "error" in data:
            error = data["error"]
            logger.error(
                "RPC error: %s id=%s error=%r",
                method,
                request_id,
                error,
            )
            raise RuntimeError(
                error.get(
                    "message",
                    "aria2 RPC error",
                )
            )

        logger.debug(
            "RPC <- %s id=%s ok",
            method,
            request_id,
        )
        return data.get("result")

    def add_uri(
        self,
        url: str,
        directory: str | None = None,
        options: dict[str, str] | None = None,
    ) -> str:
        aria_options = dict(options or {})
        if directory:
            aria_options["dir"] = directory
        gid = self._call(
            "aria2.addUri",
            [url],
            aria_options,
        )
        logger.info(
            "Download added gid=%s host=%s options=%s",
            gid,
            urlparse(url).hostname,
            aria_options,
        )
        return gid

    def change_option(
        self,
        gid: str,
        options: dict[str, str],
    ) -> str:
        logger.info(
            "Changing download options gid=%s options=%s",
            gid,
            options,
        )
        return self._call(
            "aria2.changeOption",
            gid,
            options,
            timeout=(0.5, 6.0),
        )

    def pause(self, gid: str) -> str:
        logger.info("Pause requested gid=%s", gid)
        return self._call(
            "aria2.pause",
            gid,
        )

    def resume(self, gid: str) -> str:
        logger.info("Resume requested gid=%s", gid)
        return self._call(
            "aria2.unpause",
            gid,
        )

    def remove(self, gid: str) -> str:
        logger.info("Remove requested gid=%s", gid)
        try:
            return self._call(
                "aria2.remove",
                gid,
            )
        except RuntimeError:
            return self._call(
                "aria2.removeDownloadResult",
                gid,
            )

    def tell_status(
        self,
        gid: str,
    ) -> dict[str, Any]:
        return self._call(
            "aria2.tellStatus",
            gid,
            self.STATUS_KEYS,
            timeout=(0.5, 2.5),
        )

    def tell_active(
        self,
    ) -> list[dict[str, Any]]:
        return self._call(
            "aria2.tellActive",
            self.STATUS_KEYS,
        )

    def tell_waiting(
        self,
        offset: int = 0,
        count: int = 100,
    ) -> list[dict[str, Any]]:
        return self._call(
            "aria2.tellWaiting",
            offset,
            count,
            self.STATUS_KEYS,
        )

    def tell_stopped(
        self,
        offset: int = 0,
        count: int = 100,
    ) -> list[dict[str, Any]]:
        return self._call(
            "aria2.tellStopped",
            offset,
            count,
            self.STATUS_KEYS,
        )

    def close(self) -> None:
        with self._sessions_lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        for session in sessions:
            try:
                session.close()
            except Exception:
                logger.debug(
                    "Could not close RPC session",
                    exc_info=True,
                )
