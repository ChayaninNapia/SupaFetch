from __future__ import annotations

import logging
import threading
import time
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlparse

from supafetch.core.aria2_client import Aria2Client, Aria2RpcTimeout
from supafetch.core.performance_optimizer import PerformanceOptimizer
from supafetch.core.preflight_probe import RangePreflightProbe
from supafetch.models.download import Download


logger = logging.getLogger(__name__)


class DownloadManager:
    FALLBACK_RECONNECT_GRACE_SECONDS = 3.0

    def __init__(self, client: Aria2Client) -> None:
        self.client = client
        self.optimizer = PerformanceOptimizer()
        self.preflight = RangePreflightProbe()
        self._gids: list[str] = []
        self._last_status: dict[str, str] = {}
        self._hosts: dict[str, str] = {}
        self._last_downloads: dict[str, Download] = {}
        self._poll_suppressed_until: dict[str, float] = {}
        self._lock = threading.RLock()

    def add_download(self, url: str, directory: str | None = None) -> str:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Please enter a valid HTTP or HTTPS URL.")

        if directory:
            directory = str(Path(directory).expanduser().resolve())

        host = (parsed.hostname or "unknown").lower()

        preflight = self.preflight.run(url)
        if preflight.source == "probe-failed":
            selected_connections = self.optimizer.fallback_connections(host)
            strategy = "profile-fallback"
            confidence = self.optimizer.fallback_confidence(host)
            logger.warning(
                "Preflight unavailable host=%s; falling back to %s connection(s) confidence=%s",
                host,
                selected_connections,
                confidence,
            )
        else:
            selected_connections = preflight.best_connections
            strategy = preflight.source
            confidence = preflight.confidence
            self.optimizer.record_preflight(preflight)

        options = self._download_options(
            selected_connections,
            preflight.total_bytes,
        )
        logger.info(
            "Starting download host=%s strategy=%s connections=%s confidence=%s total=%s benchmark=%s",
            host,
            strategy,
            selected_connections,
            confidence,
            preflight.total_bytes,
            preflight.summary,
        )

        gid = self.client.add_uri(url, directory, options)
        with self._lock:
            if gid not in self._gids:
                self._gids.append(gid)
            self._last_status[gid] = "added"
            self._hosts[gid] = host
            self.optimizer.register(
                gid,
                host,
                selected_connections,
                strategy,
                confidence,
            )
        logger.info("Tracking gid=%s", gid)
        return gid

    def pause(self, gid: str) -> None:
        with self._lock:
            self.client.pause(gid)

    def resume(self, gid: str) -> None:
        with self._lock:
            self.client.resume(gid)

    def remove(self, gid: str) -> None:
        with self._lock:
            self.client.remove(gid)
            if gid in self._gids:
                self._gids.remove(gid)
            self._last_status.pop(gid, None)
            self._hosts.pop(gid, None)
            self._last_downloads.pop(gid, None)
            self._poll_suppressed_until.pop(gid, None)
            self.optimizer.remove(gid)

    def list_downloads(self) -> list[Download]:
        with self._lock:
            downloads: list[Download] = []
            for gid in list(self._gids):
                cached = self._last_downloads.get(gid)
                now = time.monotonic()
                suppress_until = self._poll_suppressed_until.get(gid, 0.0)

                if cached is not None and now < suppress_until:
                    downloads.append(
                        replace(
                            cached,
                            speed_bps=0,
                            adaptive_mode="reconnecting",
                        )
                    )
                    continue

                try:
                    payload = self.client.tell_status(gid)
                    download = Download.from_aria2(payload)

                    average_speed, target_connections, mode = self.optimizer.observe(
                        gid=gid,
                        status=download.status,
                        speed_bps=download.speed_bps,
                        completed_bytes=download.completed_bytes,
                    )
                    download.average_speed_bps = average_speed
                    download.adaptive_mode = mode

                    # Preflight V2 never increases concurrency mid-download.
                    # Only a sustained stall can trigger a conservative step-down.
                    if target_connections is not None and download.status == "active":
                        options = self._download_options(
                            target_connections,
                            download.total_bytes,
                        )
                        try:
                            self.client.change_option(gid, options)
                            self._poll_suppressed_until[gid] = (
                                time.monotonic() + self.FALLBACK_RECONNECT_GRACE_SECONDS
                            )
                            download.connections = target_connections
                            download.adaptive_mode = "reconnecting"
                            logger.info(
                                "Fallback connection change applied gid=%s target=%s grace=%.1fs",
                                gid,
                                target_connections,
                                self.FALLBACK_RECONNECT_GRACE_SECONDS,
                            )
                        except Aria2RpcTimeout:
                            self._poll_suppressed_until[gid] = (
                                time.monotonic() + self.FALLBACK_RECONNECT_GRACE_SECONDS
                            )
                            download.connections = target_connections
                            download.adaptive_mode = "reconnecting"
                            logger.warning(
                                "Fallback change response timed out gid=%s target=%s; treating as pending",
                                gid,
                                target_connections,
                            )
                        except Exception:
                            logger.exception(
                                "Fallback connection change failed gid=%s target=%s",
                                gid,
                                target_connections,
                            )
                            self.optimizer.change_failed(gid)
                            download.adaptive_mode = "fallback-failed"

                    self._last_downloads[gid] = download
                    downloads.append(download)

                    previous = self._last_status.get(gid)
                    if download.status != previous:
                        logger.info(
                            "Status gid=%s %s -> %s downloaded=%s/%s speed=%sB/s avg=%sB/s connections=%s optimizer=%s errorCode=%s error=%r",
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
                except Aria2RpcTimeout:
                    logger.warning(
                        "Status RPC timed out gid=%s; showing cached state and retrying later",
                        gid,
                    )
                    if cached is not None:
                        downloads.append(
                            replace(
                                cached,
                                speed_bps=0,
                                adaptive_mode="rpc-wait",
                            )
                        )
                except Exception:
                    logger.exception("Could not refresh gid=%s; keeping cached state", gid)
                    if cached is not None:
                        downloads.append(
                            replace(
                                cached,
                                speed_bps=0,
                                adaptive_mode="rpc-error",
                            )
                        )

            return downloads

    @staticmethod
    def _download_options(connections: int, total_bytes: int) -> dict[str, str]:
        if total_bytes >= 1024 * 1024 * 1024:
            min_split_size = "8M"
        elif total_bytes >= 256 * 1024 * 1024:
            min_split_size = "4M"
        else:
            min_split_size = "1M"

        return {
            "split": str(connections),
            "max-connection-per-server": str(connections),
            "min-split-size": min_split_size,
            "user-agent": RangePreflightProbe.USER_AGENT,
        }
