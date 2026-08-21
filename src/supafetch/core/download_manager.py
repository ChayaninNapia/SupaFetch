from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlparse

from supafetch.core.aria2_client import Aria2Client, Aria2RpcTimeout
from supafetch.core.performance_optimizer import PerformanceOptimizer
from supafetch.core.preflight_probe import PreflightResult, RangePreflightProbe
from supafetch.models.download import Download


logger = logging.getLogger(__name__)


class DownloadManager:
    CONNECTION_CHANGE_GRACE_SECONDS = 3.0
    RPC_TIMEOUT_BACKOFF_SECONDS = 3.0

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
        with self._lock:
            same_host_active = self._active_host_count(host)

        if same_host_active > 0:
            selected_connections = self.optimizer.fallback_connections(host)
            confidence = self.optimizer.fallback_confidence(host)
            expected_speed = self.optimizer.expected_speed(host)
            strategy = "shared-host-profile"
            total_bytes = 0
            benchmark_summary = (
                f"skipped: {same_host_active} active transfer(s) already use {host}"
            )
            logger.info(
                "Preflight skipped host=%s active_same_host=%s profile_configured=%s confidence=%s expected=%.2fMB/s",
                host,
                same_host_active,
                selected_connections,
                confidence,
                expected_speed / (1024 * 1024),
            )
        else:
            preflight = self.preflight.run(url)
            total_bytes = preflight.total_bytes
            benchmark_summary = preflight.summary
            if preflight.source == "probe-failed":
                selected_connections = self.optimizer.fallback_connections(host)
                strategy = "profile-fallback"
                confidence = self.optimizer.fallback_confidence(host)
                expected_speed = self.optimizer.expected_speed(host)
                logger.warning(
                    "Preflight unavailable host=%s; falling back to configured=%s confidence=%s",
                    host,
                    selected_connections,
                    confidence,
                )
            else:
                selected_connections = preflight.best_connections
                strategy = preflight.source
                confidence = preflight.confidence
                expected_speed = self._preflight_expected_speed(
                    preflight,
                    selected_connections,
                )
                self.optimizer.record_preflight(preflight)

        options = self._download_options(selected_connections, total_bytes)
        logger.info(
            "Starting download host=%s strategy=%s configured=%s confidence=%s expected=%.2fMB/s total=%s benchmark=%s",
            host,
            strategy,
            selected_connections,
            confidence,
            expected_speed / (1024 * 1024),
            total_bytes,
            benchmark_summary,
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
                expected_speed,
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
            active_by_host = Counter(
                self._hosts.get(gid, "unknown")
                for gid, item in self._last_downloads.items()
                if item.status == "active"
            )

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
                    host = self._hosts.get(gid, "unknown")

                    same_host_active = active_by_host.get(host, 0)
                    if download.status == "active" and (
                        cached is None or cached.status != "active"
                    ):
                        same_host_active += 1
                        active_by_host[host] = same_host_active
                    same_host_active = (
                        max(1, same_host_active)
                        if download.status == "active"
                        else 0
                    )

                    decision = self.optimizer.observe(
                        gid=gid,
                        status=download.status,
                        speed_bps=download.speed_bps,
                        completed_bytes=download.completed_bytes,
                        total_bytes=download.total_bytes,
                        observed_connections=download.connections,
                        same_host_active=same_host_active,
                    )
                    download.average_speed_bps = (
                        decision.stable_10_bps or download.speed_bps
                    )
                    download.stable_10_bps = decision.stable_10_bps
                    download.stable_30_bps = decision.stable_30_bps
                    download.peak_speed_bps = decision.peak_speed_bps
                    download.expected_speed_bps = decision.expected_speed_bps
                    download.configured_connections = decision.configured_connections
                    download.safe_connection_ceiling = decision.safe_connection_ceiling
                    download.rate_limit_risk = decision.rate_limit_risk
                    download.adaptive_mode = decision.mode

                    if (
                        decision.target_connections is not None
                        and download.status == "active"
                    ):
                        target = decision.target_connections
                        options = self._download_options(target, download.total_bytes)
                        try:
                            self.client.change_option(gid, options)
                            self._poll_suppressed_until[gid] = (
                                time.monotonic()
                                + self.CONNECTION_CHANGE_GRACE_SECONDS
                            )
                            # Do not overwrite download.connections here. That
                            # value is the actual count reported by aria2. The
                            # target is only the configured ceiling.
                            download.configured_connections = target
                            download.adaptive_mode = "reconnecting"
                            logger.info(
                                "Runtime connection ceiling change applied gid=%s configured_target=%s active_before=%s min_split=%s grace=%.1fs decision=%s",
                                gid,
                                target,
                                download.connections,
                                options["min-split-size"],
                                self.CONNECTION_CHANGE_GRACE_SECONDS,
                                decision.mode,
                            )
                        except Aria2RpcTimeout:
                            self._poll_suppressed_until[gid] = (
                                time.monotonic()
                                + self.CONNECTION_CHANGE_GRACE_SECONDS
                            )
                            download.configured_connections = target
                            download.adaptive_mode = "reconnecting"
                            logger.warning(
                                "Runtime connection ceiling response timed out gid=%s configured_target=%s; treating as pending",
                                gid,
                                target,
                            )
                        except Exception:
                            logger.exception(
                                "Runtime connection ceiling change failed gid=%s configured_target=%s",
                                gid,
                                target,
                            )
                            self.optimizer.change_failed(gid)
                            download.adaptive_mode = "change-failed"

                    self._last_downloads[gid] = download
                    downloads.append(download)

                    previous = self._last_status.get(gid)
                    if download.status != previous:
                        logger.info(
                            "Status gid=%s %s -> %s downloaded=%s/%s speed=%sB/s stable10=%sB/s stable30=%sB/s active_conn=%s configured_conn=%s safe_ceiling=%s rate_limit_risk=%s optimizer=%s errorCode=%s error=%r",
                            gid,
                            previous or "unknown",
                            download.status,
                            payload.get("completedLength", "0"),
                            payload.get("totalLength", "0"),
                            payload.get("downloadSpeed", "0"),
                            decision.stable_10_bps,
                            decision.stable_30_bps,
                            payload.get("connections", "?"),
                            download.configured_connections,
                            download.safe_connection_ceiling,
                            download.rate_limit_risk,
                            download.adaptive_mode,
                            payload.get("errorCode", ""),
                            payload.get("errorMessage", ""),
                        )
                        self._last_status[gid] = download.status

                    if download.status == "error":
                        logger.error(
                            "Download failed gid=%s host=%s errorCode=%s error=%r",
                            gid,
                            host,
                            payload.get("errorCode", ""),
                            payload.get("errorMessage", ""),
                        )
                except Aria2RpcTimeout:
                    self._poll_suppressed_until[gid] = (
                        time.monotonic() + self.RPC_TIMEOUT_BACKOFF_SECONDS
                    )
                    logger.warning(
                        "Status RPC timed out gid=%s; cached state kept, next poll delayed %.1fs",
                        gid,
                        self.RPC_TIMEOUT_BACKOFF_SECONDS,
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
                    logger.exception(
                        "Could not refresh gid=%s; keeping cached state",
                        gid,
                    )
                    if cached is not None:
                        downloads.append(
                            replace(
                                cached,
                                speed_bps=0,
                                adaptive_mode="rpc-error",
                            )
                        )

            return downloads

    def _active_host_count(self, host: str) -> int:
        return sum(
            1
            for gid, item in self._last_downloads.items()
            if self._hosts.get(gid) == host and item.status == "active"
        )

    @staticmethod
    def _preflight_expected_speed(
        result: PreflightResult,
        selected_connections: int,
    ) -> int:
        for measurement in result.measurements:
            if (
                measurement.connections == selected_connections
                and measurement.usable
            ):
                return measurement.effective_speed_bps
        return max(0, result.peak_speed_bps)

    @staticmethod
    def _download_options(
        connections: int,
        total_bytes: int,
    ) -> dict[str, str]:
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
