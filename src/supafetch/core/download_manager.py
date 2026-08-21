from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

from supafetch.core.aria2_client import Aria2Client
from supafetch.models.download import Download


logger = logging.getLogger(__name__)


class DownloadManager:
    def __init__(self, client: Aria2Client) -> None:
        self.client = client
        self._gids: list[str] = []
        self._last_status: dict[str, str] = {}

    def add_download(self, url: str, directory: str | None = None) -> str:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Please enter a valid HTTP or HTTPS URL.")

        if directory:
            directory = str(Path(directory).expanduser().resolve())

        logger.info("Adding download host=%s directory=%s", parsed.hostname, directory or "default")
        gid = self.client.add_uri(url, directory)
        if gid not in self._gids:
            self._gids.append(gid)
        self._last_status[gid] = "added"
        logger.info("Tracking gid=%s", gid)
        return gid

    def pause(self, gid: str) -> None:
        self.client.pause(gid)

    def resume(self, gid: str) -> None:
        self.client.resume(gid)

    def remove(self, gid: str) -> None:
        self.client.remove(gid)
        if gid in self._gids:
            self._gids.remove(gid)
        self._last_status.pop(gid, None)

    def list_downloads(self) -> list[Download]:
        downloads: list[Download] = []
        for gid in list(self._gids):
            try:
                payload = self.client.tell_status(gid)
                download = Download.from_aria2(payload)
                downloads.append(download)

                previous = self._last_status.get(gid)
                if download.status != previous:
                    logger.info(
                        "Status gid=%s %s -> %s downloaded=%s/%s speed=%sB/s connections=%s errorCode=%s error=%r",
                        gid,
                        previous or "unknown",
                        download.status,
                        payload.get("completedLength", "0"),
                        payload.get("totalLength", "0"),
                        payload.get("downloadSpeed", "0"),
                        payload.get("connections", "?"),
                        payload.get("errorCode", ""),
                        payload.get("errorMessage", ""),
                    )
                    self._last_status[gid] = download.status

                if download.status == "error":
                    logger.error(
                        "Download failed gid=%s errorCode=%s error=%r",
                        gid,
                        payload.get("errorCode", ""),
                        payload.get("errorMessage", ""),
                    )
            except Exception:
                # A temporary RPC/network problem should not make the row disappear.
                logger.exception("Could not refresh gid=%s; keeping it tracked", gid)

        return downloads
