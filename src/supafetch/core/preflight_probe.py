from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from urllib.parse import urlparse

import requests


logger = logging.getLogger(__name__)
_CONTENT_RANGE_RE = re.compile(r"bytes\s+\d+-\d+/(\d+|\*)", re.IGNORECASE)


@dataclass(slots=True)
class ProbeMeasurement:
    connections: int
    speed_bps: int
    bytes_downloaded: int
    elapsed_seconds: float
    success: bool = True

    @property
    def efficiency_bps_per_connection(self) -> int:
        if self.connections <= 0:
            return 0
        return int(self.speed_bps / self.connections)


@dataclass(slots=True)
class PreflightResult:
    host: str
    range_supported: bool
    total_bytes: int
    best_connections: int
    peak_speed_bps: int = 0
    source: str = "probe"
    measurements: list[ProbeMeasurement] = field(default_factory=list)
    note: str = ""

    @property
    def summary(self) -> str:
        if not self.measurements:
            return f"{self.source}: {self.best_connections} connection(s)"
        points = ", ".join(
            f"{item.connections}c={item.speed_bps / (1024 * 1024):.2f}MB/s"
            for item in self.measurements
            if item.success
        )
        return f"{points} -> {self.best_connections}c"


class RangePreflightProbe:
    """Measure HTTP Range scalability before handing the URL to aria2.

    The probe intentionally downloads only small, non-overlapping byte ranges.
    It never logs the full URL so signed tokens do not leak into debug output.
    """

    CONNECTION_LEVELS = (1, 2, 4, 8, 16)
    MIN_IMPROVEMENT = 0.08
    NEAR_PEAK_RATIO = 0.95
    USER_AGENT = "SupaFetch/0.1 preflight"

    def run(self, url: str) -> PreflightResult:
        host = (urlparse(url).hostname or "unknown").lower()
        try:
            range_supported, total_bytes = self._inspect(url)
        except Exception as exc:
            logger.warning(
                "Preflight inspection failed host=%s error=%s",
                host,
                type(exc).__name__,
            )
            return PreflightResult(
                host=host,
                range_supported=False,
                total_bytes=0,
                best_connections=2,
                source="probe-failed",
                note=str(exc),
            )

        if not range_supported:
            logger.info("Preflight host=%s does not support byte ranges; using 1 connection", host)
            return PreflightResult(
                host=host,
                range_supported=False,
                total_bytes=total_bytes,
                best_connections=1,
                source="no-range",
            )

        levels = self._levels_for_size(total_bytes)
        chunk_size = self._chunk_size(total_bytes)
        measurements: list[ProbeMeasurement] = []
        plateau_count = 0
        previous_speed = 0

        logger.info(
            "Preflight benchmark host=%s total=%s levels=%s chunk=%s",
            host,
            total_bytes,
            levels,
            chunk_size,
        )

        for connections in levels:
            measurement = self._measure_level(
                url=url,
                total_bytes=total_bytes,
                connections=connections,
                chunk_size=chunk_size,
            )
            if not measurement.success:
                logger.warning(
                    "Preflight level failed host=%s connections=%s",
                    host,
                    connections,
                )
                break

            measurements.append(measurement)
            logger.info(
                "Preflight result host=%s connections=%s speed=%.2fMB/s elapsed=%.3fs efficiency=%.2fMB/s/conn",
                host,
                connections,
                measurement.speed_bps / (1024 * 1024),
                measurement.elapsed_seconds,
                measurement.efficiency_bps_per_connection / (1024 * 1024),
            )

            if previous_speed > 0:
                improvement = (measurement.speed_bps / previous_speed) - 1.0
                if improvement < self.MIN_IMPROVEMENT:
                    plateau_count += 1
                else:
                    plateau_count = 0
                logger.debug(
                    "Preflight scaling host=%s %sc improvement=%.1f%% plateau_count=%s",
                    host,
                    connections,
                    improvement * 100,
                    plateau_count,
                )
            previous_speed = measurement.speed_bps

            # Two consecutive weak scaling steps are enough evidence that the
            # server/path is saturated. Avoid wasting more bandwidth probing.
            if plateau_count >= 2:
                logger.info(
                    "Preflight saturation detected host=%s at <=%s connections",
                    host,
                    connections,
                )
                break

        successful = [item for item in measurements if item.success and item.speed_bps > 0]
        if not successful:
            return PreflightResult(
                host=host,
                range_supported=True,
                total_bytes=total_bytes,
                best_connections=2,
                source="probe-failed",
                measurements=measurements,
                note="No successful throughput samples",
            )

        peak_speed = max(item.speed_bps for item in successful)
        near_peak = [
            item
            for item in successful
            if item.speed_bps >= peak_speed * self.NEAR_PEAK_RATIO
        ]
        # Prefer the smallest concurrency that already reaches 95% of peak.
        # This avoids extra server load and connection overhead for tiny gains.
        best = min(near_peak, key=lambda item: item.connections)
        result = PreflightResult(
            host=host,
            range_supported=True,
            total_bytes=total_bytes,
            best_connections=best.connections,
            peak_speed_bps=peak_speed,
            source="probe",
            measurements=measurements,
        )
        logger.info("Preflight selected host=%s %s", host, result.summary)
        return result

    def _inspect(self, url: str) -> tuple[bool, int]:
        headers = {
            "Range": "bytes=0-0",
            "Accept-Encoding": "identity",
            "User-Agent": self.USER_AGENT,
        }
        with requests.get(
            url,
            headers=headers,
            stream=True,
            allow_redirects=True,
            timeout=(2.0, 4.0),
        ) as response:
            if response.status_code == 206:
                content_range = response.headers.get("Content-Range", "")
                match = _CONTENT_RANGE_RE.search(content_range)
                total = 0
                if match and match.group(1) != "*":
                    total = int(match.group(1))
                return True, total

            if response.status_code == 200:
                total = int(response.headers.get("Content-Length", "0") or 0)
                return False, total

            response.raise_for_status()
            return False, 0

    def _measure_level(
        self,
        url: str,
        total_bytes: int,
        connections: int,
        chunk_size: int,
    ) -> ProbeMeasurement:
        ranges = self._make_ranges(total_bytes, connections, chunk_size)
        if not ranges:
            return ProbeMeasurement(connections, 0, 0, 0.0, success=False)

        started = time.perf_counter()
        downloaded = 0
        success = True

        with ThreadPoolExecutor(max_workers=connections, thread_name_prefix="preflight") as pool:
            futures = [pool.submit(self._fetch_range, url, start, end) for start, end in ranges]
            for future in as_completed(futures):
                try:
                    downloaded += future.result()
                except Exception as exc:
                    success = False
                    logger.debug(
                        "Range probe worker failed connections=%s error=%s",
                        connections,
                        type(exc).__name__,
                    )

        elapsed = max(0.001, time.perf_counter() - started)
        speed_bps = int(downloaded / elapsed) if success and downloaded > 0 else 0
        return ProbeMeasurement(
            connections=connections,
            speed_bps=speed_bps,
            bytes_downloaded=downloaded,
            elapsed_seconds=elapsed,
            success=success and downloaded > 0,
        )

    def _fetch_range(self, url: str, start: int, end: int) -> int:
        expected = end - start + 1
        headers = {
            "Range": f"bytes={start}-{end}",
            "Accept-Encoding": "identity",
            "User-Agent": self.USER_AGENT,
        }
        with requests.get(
            url,
            headers=headers,
            stream=True,
            allow_redirects=True,
            timeout=(2.0, 5.0),
        ) as response:
            if response.status_code != 206:
                raise RuntimeError(f"Range request returned HTTP {response.status_code}")

            read = 0
            for block in response.iter_content(chunk_size=64 * 1024):
                if not block:
                    continue
                read += len(block)
                if read >= expected:
                    break
            if read < expected:
                raise RuntimeError(f"Short range response: {read}/{expected} bytes")
            return expected

    @classmethod
    def _levels_for_size(cls, total_bytes: int) -> tuple[int, ...]:
        mib = 1024 * 1024
        if 0 < total_bytes < 32 * mib:
            return (1,)
        if 0 < total_bytes < 128 * mib:
            return (1, 2)
        if 0 < total_bytes < 512 * mib:
            return (1, 2, 4)
        if 0 < total_bytes < 1024 * mib:
            return (1, 2, 4, 8)
        return cls.CONNECTION_LEVELS

    @staticmethod
    def _chunk_size(total_bytes: int) -> int:
        mib = 1024 * 1024
        if total_bytes >= 1024 * mib:
            return 1 * mib
        if total_bytes >= 256 * mib:
            return 768 * 1024
        return 512 * 1024

    @staticmethod
    def _make_ranges(
        total_bytes: int,
        connections: int,
        chunk_size: int,
    ) -> list[tuple[int, int]]:
        if total_bytes <= 0:
            return []
        chunk_size = min(chunk_size, max(1, total_bytes // connections))
        max_start = max(0, total_bytes - chunk_size)
        if connections == 1:
            starts = [0]
        else:
            starts = [
                int((max_start * index) / (connections - 1))
                for index in range(connections)
            ]
        return [
            (start, min(total_bytes - 1, start + chunk_size - 1))
            for start in starts
        ]
