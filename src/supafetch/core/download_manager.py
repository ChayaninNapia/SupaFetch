from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

from supafetch.core.aria2_client import Aria2Client
from supafetch.core.performance_optimizer import PerformanceOptimizer
from supafetch.models.download import Download


logger = logging.getLogger(__name__)


class DownloadManager:
    def __init__(self, client: Aria2Client) -> None:
        self.client = client
        self.optimizer = PerformanceOptimizer()
        self._gids: list[str] = []
        self._last_status: dict[str, str] = {}
        self._hosts: dict[str, str] = {}

    def add_download(self, url: str, directory: str | None = None) -> str:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Please enter a valid HTTP or HTTPS URL.")

        if directory:
            directory = str(Path(directory).expanduser().resolve())

        host = (parsed.hostname or "unknown").lower()
        initial_connections = self.optimizer.initial_connections(host)
        options = {
            "split": str(initial_connections),
            "max-connection-per-server": str(initial_connections),
        }

        logger.info(
            "Adding download host=%s directory=%s adaptive_start_connections=%s",
            host,
            directory or "default",
            initial_connections,
        )
        gid = self.client.add_uri(url, directory, options)
        if gid not in self._gids:
            self._gids.append(gid)
        self._last_status[gid] = "added"
        self._hosts[gid] = host
        self.optimizer.register(gid, host, initial_connections)
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
        self._hosts.pop(gid, None)
        self.optimizer.remove(gid)

    def list_downloads(self) -> list[Download]:
        downloads: list[Download] = []
        for gid in list(self._gids):
            try:
                payload = self.client.tell_status(gid)
                download = Download.from_aria2(payload)

                average_speed, target_connections, mode = self.optimizer.observe(
                    gid=gid,
                    status=download.status,
                    speed_bps=download.speed_bps,
                    total_bytes=download.total_bytes,
                    completed_bytes=download.completed_bytes,
                )
                download.average_speed_bps = average_speed
                download.adaptive_mode = mode

                if target_connections is not None and download.status == "active":
                    try:
                        self.client.change_option(
                            gid,
                            {
                                "split": str(target_connections),
                                "max-connection-per-server": str(target_connections),
                            },
                        )
                        logger.info(
                            "Adaptive connection change applied gid=%s target=%s mode=%s",
                            gid,
                            target_connections,
                            mode,
                        )
                    except Exception:
                        logger.exception(
                            "Adaptive connection change failed gid=%s target=%s",
                            gid,
                            target_connections,
                        )
                        self.optimizer.change_failed(gid)
                        download.adaptive_mode = "change-failed"

                downloads.append(download)

                previous = self._last_status.get(gid)
                if download.status != previous:
                    logger.info(
                        "Status gid=%s %s -> %s downloaded=%s/%s speed=%sB/s avg=%sB/s connections=%s adaptive=%s errorCode=%s error=%r",
                        gid,
                        previous or "unknown",
                        download.status,
                        payload.get("completedLength", "0"),
                        payload.get("totalLength", "0"),
                        payload.get("downloadSpeed", "0"),
                        average_speed,
                        payload.get("connections", "?"),
                        download.adaptive_mode,
                        payload.get("errorCode", ""),
                        payload.get("errorMessage", ""),
                    )
                    self._last_status[gid] = download.status

                if download.status == "error":
                    logger.error(
                        "Download failed gid=%s host=%s errorCode=%s error=%r",
                        gid,
                        self._hosts.get(gid, "unknown"),
                        payload.get("errorCode", ""),
                        payload.get("errorMessage", ""),
                    )
            except Exception:
                # A temporary RPC/network problem should not make the row disappear.
                logger.exception("Could not refresh gid=%s; keeping it tracked", gid)

        return downloads
