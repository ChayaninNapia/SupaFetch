from __future__ import annotations

import logging
import re
import statistics
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from urllib.parse import urlparse

import requests


logger = logging.getLogger(__name__)
_CONTENT_RANGE_RE = re.compile(r"bytes\s+\d+-\d+/(\d+|\*)", re.IGNORECASE)


@dataclass(slots=True)
class ProbeWorkerResult:
    success: bool
    measured_bytes: int = 0
    elapsed_seconds: float = 0.0
    retries: int = 0
    error: str = ""

    @property
    def speed_bps(self) -> int:
        if not self.success or self.elapsed_seconds <= 0:
            return 0
        return int(self.measured_bytes / self.elapsed_seconds)


@dataclass(slots=True)
class ProbeMeasurement:
    connections: int
    speed_bps: int
    bytes_downloaded: int
    elapsed_seconds: float
    successful_workers: int
    total_workers: int
    retries: int = 0
    errors: dict[str, int] = field(default_factory=dict)

    @property
    def success_ratio(self) -> float:
        if self.total_workers <= 0:
            return 0.0
        return self.successful_workers / self.total_workers

    @property
    def effective_speed_bps(self) -> int:
        return int(self.speed_bps * self.success_ratio)

    @property
    def efficiency_bps_per_connection(self) -> int:
        if self.connections <= 0:
            return 0
        return int(self.speed_bps / self.connections)

    @property
    def usable(self) -> bool:
        return (
            self.speed_bps > 0
            and self.success_ratio >= RangePreflightProbe.MIN_SUCCESS_RATIO
        )


@dataclass(slots=True)
class PreflightResult:
    host: str
    range_supported: bool
    total_bytes: int
    best_connections: int
    peak_speed_bps: int = 0
    source: str = "probe-v2"
    confidence: str = "low"
    measurements: list[ProbeMeasurement] = field(default_factory=list)
    note: str = ""

    @property
    def summary(self) -> str:
        if not self.measurements:
            return (
                f"{self.source}: {self.best_connections} connection(s) "
                f"[{self.confidence}]"
            )
        points = ", ".join(
            (
                f"{item.connections}c={item.speed_bps / (1024 * 1024):.2f}MB/s"
                f"@{item.success_ratio * 100:.0f}%"
            )
            for item in self.measurements
            if item.speed_bps > 0
        )
        return f"{points} -> {self.best_connections}c [{self.confidence}]"


class RangePreflightProbe:
    """Estimate steady HTTP Range scalability before aria2 starts."""

    CONNECTION_LEVELS = (1, 2, 4, 8, 16)
    MIN_IMPROVEMENT = 0.08
    NEAR_PEAK_RATIO = 0.95
    MIN_SUCCESS_RATIO = 0.75
    HIGH_SUCCESS_RATIO = 0.95
    WARMUP_BYTES = 256 * 1024
    MEASURE_SECONDS = 2.0
    WARMUP_SYNC_TIMEOUT = 4.0
    MAX_PROBE_SECONDS = 36.0
    MAX_ATTEMPTS_PER_WORKER = 2
    LARGE_FILE_BYTES = 1024 * 1024 * 1024
    LARGE_FILE_MIN_PROBE_CONNECTIONS = 8
    USER_AGENT = "SupaFetch/0.1"

    def run(self, url: str) -> PreflightResult:
        host = (urlparse(url).hostname or "unknown").lower()
        probe_started = time.monotonic()
        try:
            range_supported, total_bytes = self._inspect(url)
        except Exception as exc:
            logger.warning(
                "Preflight inspection failed host=%s error=%s",
                host,
                self._safe_error(exc),
            )
            return PreflightResult(
                host=host,
                range_supported=False,
                total_bytes=0,
                best_connections=2,
                source="probe-failed",
                confidence="low",
                note=self._safe_error(exc),
            )

        if not range_supported:
            logger.info(
                "Preflight host=%s does not support byte ranges; using 1 connection",
                host,
            )
            return PreflightResult(
                host=host,
                range_supported=False,
                total_bytes=total_bytes,
                best_connections=1,
                source="no-range",
                confidence="high",
            )

        levels = self._levels_for_size(total_bytes)
        minimum_probe_connections = (
            self.LARGE_FILE_MIN_PROBE_CONNECTIONS
            if total_bytes >= self.LARGE_FILE_BYTES
            else 1
        )
        measurements: list[ProbeMeasurement] = []
        plateau_count = 0
        previous_effective_speed = 0

        logger.info(
            "Preflight V2 benchmark host=%s total=%s levels=%s warmup=%s measure_window=%.1fs budget=%.0fs min_probe=%sc",
            host,
            total_bytes,
            levels,
            self.WARMUP_BYTES,
            self.MEASURE_SECONDS,
            self.MAX_PROBE_SECONDS,
            minimum_probe_connections,
        )

        for connections in levels:
            elapsed_budget = time.monotonic() - probe_started
            if (
                measurements
                and elapsed_budget >= self.MAX_PROBE_SECONDS
                and connections > minimum_probe_connections
            ):
                logger.info(
                    "Preflight time budget reached host=%s after %s level(s)",
                    host,
                    len(measurements),
                )
                break

            measurement = self._measure_level(
                url=url,
                total_bytes=total_bytes,
                connections=connections,
                measure_bytes=self._measure_bytes_per_worker(connections),
            )
            measurements.append(measurement)

            error_text = ""
            if measurement.errors:
                error_text = " errors=" + ", ".join(
                    f"{name} x{count}"
                    for name, count in sorted(measurement.errors.items())
                )

            logger.info(
                "Preflight V2 result host=%s connections=%s speed=%.2fMB/s effective=%.2fMB/s success=%s/%s(%.0f%%) retries=%s elapsed=%.3fs efficiency=%.2fMB/s/conn%s",
                host,
                connections,
                measurement.speed_bps / (1024 * 1024),
                measurement.effective_speed_bps / (1024 * 1024),
                measurement.successful_workers,
                measurement.total_workers,
                measurement.success_ratio * 100,
                measurement.retries,
                measurement.elapsed_seconds,
                measurement.efficiency_bps_per_connection / (1024 * 1024),
                error_text,
            )

            if measurement.successful_workers == 0:
                logger.warning(
                    "Preflight level unusable host=%s connections=%s: all workers failed",
                    host,
                    connections,
                )
                break

            if measurement.success_ratio < self.MIN_SUCCESS_RATIO:
                logger.warning(
                    "Preflight level unstable host=%s connections=%s success_ratio=%.0f%%; not probing higher concurrency",
                    host,
                    connections,
                    measurement.success_ratio * 100,
                )
                break

            if previous_effective_speed > 0:
                improvement = (
                    measurement.effective_speed_bps / max(1, previous_effective_speed)
                ) - 1.0
                if improvement < self.MIN_IMPROVEMENT:
                    plateau_count += 1
                else:
                    plateau_count = 0
                logger.debug(
                    "Preflight V2 scaling host=%s %sc improvement=%.1f%% plateau_count=%s reliability=%.0f%%",
                    host,
                    connections,
                    improvement * 100,
                    plateau_count,
                    measurement.success_ratio * 100,
                )
            previous_effective_speed = measurement.effective_speed_bps

            if plateau_count >= 2 and connections >= minimum_probe_connections:
                logger.info(
                    "Preflight saturation detected host=%s at <=%s connections",
                    host,
                    connections,
                )
                break

        usable = [item for item in measurements if item.usable]
        if not usable:
            return PreflightResult(
                host=host,
                range_supported=True,
                total_bytes=total_bytes,
                best_connections=2,
                source="probe-failed",
                confidence="low",
                measurements=measurements,
                note="No reliable steady-state throughput samples",
            )

        peak_effective = max(item.effective_speed_bps for item in usable)
        near_peak = [
            item
            for item in usable
            if item.effective_speed_bps >= peak_effective * self.NEAR_PEAK_RATIO
        ]
        best = min(near_peak, key=lambda item: item.connections)
        confidence = self._confidence(best, usable)
        result = PreflightResult(
            host=host,
            range_supported=True,
            total_bytes=total_bytes,
            best_connections=best.connections,
            peak_speed_bps=max(item.speed_bps for item in usable),
            source="probe-v2",
            confidence=confidence,
            measurements=measurements,
        )
        logger.info("Preflight V2 selected host=%s %s", host, result.summary)
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
        measure_bytes: int,
    ) -> ProbeMeasurement:
        span_size = self.WARMUP_BYTES + measure_bytes
        ranges = self._make_ranges(total_bytes, connections, span_size)
        if not ranges:
            return ProbeMeasurement(
                connections=connections,
                speed_bps=0,
                bytes_downloaded=0,
                elapsed_seconds=0.0,
                successful_workers=0,
                total_workers=connections,
                errors={"invalid-range": connections},
            )

        release_measurement = threading.Event()
        ready_lock = threading.Lock()
        ready_count = [0]
        results: list[ProbeWorkerResult] = []
        level_started = time.perf_counter()

        def mark_ready() -> None:
            with ready_lock:
                ready_count[0] += 1
                if ready_count[0] >= connections:
                    release_measurement.set()

        with ThreadPoolExecutor(
            max_workers=connections,
            thread_name_prefix="preflight-v2",
        ) as pool:
            futures = [
                pool.submit(
                    self._probe_worker,
                    url,
                    start,
                    end,
                    measure_bytes,
                    release_measurement,
                    mark_ready,
                )
                for start, end in ranges
            ]

            release_measurement.wait(timeout=self.WARMUP_SYNC_TIMEOUT)
            release_measurement.set()

            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append(
                        ProbeWorkerResult(
                            success=False,
                            error=self._safe_error(exc),
                        )
                    )

        successful = [
            item for item in results if item.success and item.speed_bps > 0
        ]
        failed = [item for item in results if not item.success]
        errors = Counter(item.error or "unknown-error" for item in failed)
        retries = sum(item.retries for item in results)

        if successful:
            median_worker_speed = statistics.median(
                item.speed_bps for item in successful
            )
            speed_bps = int(median_worker_speed * len(successful))
            measured_bytes = sum(item.measured_bytes for item in successful)
            elapsed = max(item.elapsed_seconds for item in successful)
        else:
            speed_bps = 0
            measured_bytes = 0
            elapsed = max(0.001, time.perf_counter() - level_started)

        return ProbeMeasurement(
            connections=connections,
            speed_bps=speed_bps,
            bytes_downloaded=measured_bytes,
            elapsed_seconds=elapsed,
            successful_workers=len(successful),
            total_workers=connections,
            retries=retries,
            errors=dict(errors),
        )

    def _probe_worker(
        self,
        url: str,
        start: int,
        end: int,
        measure_bytes: int,
        release_measurement: threading.Event,
        mark_ready,
    ) -> ProbeWorkerResult:
        last_error = "unknown-error"
        for attempt in range(1, self.MAX_ATTEMPTS_PER_WORKER + 1):
            try:
                return self._probe_worker_once(
                    url=url,
                    start=start,
                    end=end,
                    measure_bytes=measure_bytes,
                    release_measurement=release_measurement,
                    mark_ready=mark_ready,
                    retries=attempt - 1,
                )
            except Exception as exc:
                last_error = self._safe_error(exc)
                logger.debug(
                    "Range probe worker attempt failed attempt=%s/%s error=%s",
                    attempt,
                    self.MAX_ATTEMPTS_PER_WORKER,
                    last_error,
                )
                if attempt < self.MAX_ATTEMPTS_PER_WORKER:
                    time.sleep(0.2 * attempt)
        return ProbeWorkerResult(
            success=False,
            retries=self.MAX_ATTEMPTS_PER_WORKER - 1,
            error=last_error,
        )

    def _probe_worker_once(
        self,
        url: str,
        start: int,
        end: int,
        measure_bytes: int,
        release_measurement: threading.Event,
        mark_ready,
        retries: int,
    ) -> ProbeWorkerResult:
        headers = {
            "Range": f"bytes={start}-{end}",
            "Accept-Encoding": "identity",
            "User-Agent": self.USER_AGENT,
        }

        with requests.Session() as session:
            with session.get(
                url,
                headers=headers,
                stream=True,
                allow_redirects=True,
                timeout=(2.0, 6.0),
            ) as response:
                if response.status_code != 206:
                    retry_after = response.headers.get("Retry-After")
                    suffix = f" retry-after={retry_after}" if retry_after else ""
                    raise RuntimeError(f"HTTP {response.status_code}{suffix}")

                warmup_remaining = self.WARMUP_BYTES
                measured = 0
                measuring = False
                measurement_started = 0.0
                ready_announced = False

                for block in response.iter_content(chunk_size=64 * 1024):
                    if not block:
                        continue

                    if warmup_remaining > 0:
                        consumed = min(len(block), warmup_remaining)
                        warmup_remaining -= consumed
                        block = block[consumed:]
                        if warmup_remaining == 0 and not ready_announced:
                            mark_ready()
                            ready_announced = True
                            release_measurement.wait(
                                timeout=self.WARMUP_SYNC_TIMEOUT + 1.0
                            )
                            measuring = True
                            measurement_started = time.perf_counter()

                    if measuring and block:
                        measured += len(block)

                    if measuring:
                        elapsed = time.perf_counter() - measurement_started
                        if (
                            measured >= measure_bytes
                            or elapsed >= self.MEASURE_SECONDS
                        ):
                            break

                if warmup_remaining > 0:
                    raise RuntimeError(
                        "short warm-up response "
                        f"({self.WARMUP_BYTES - warmup_remaining}/"
                        f"{self.WARMUP_BYTES} bytes)"
                    )
                if measured <= 0:
                    raise RuntimeError("no steady-state bytes measured")

                elapsed = max(
                    0.001,
                    time.perf_counter() - measurement_started,
                )
                return ProbeWorkerResult(
                    success=True,
                    measured_bytes=measured,
                    elapsed_seconds=elapsed,
                    retries=retries,
                )

    @classmethod
    def _confidence(
        cls,
        best: ProbeMeasurement,
        usable: list[ProbeMeasurement],
    ) -> str:
        if (
            best.success_ratio >= cls.HIGH_SUCCESS_RATIO
            and len(usable) >= 3
        ):
            return "high"
        if (
            best.success_ratio >= cls.MIN_SUCCESS_RATIO
            and len(usable) >= 2
        ):
            return "medium"
        return "low"

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
    def _measure_bytes_per_worker(connections: int) -> int:
        mib = 1024 * 1024
        if connections <= 1:
            return 4 * mib
        if connections == 2:
            return 3 * mib
        if connections == 4:
            return 2 * mib
        if connections == 8:
            return 1536 * 1024
        return 1 * mib

    @staticmethod
    def _make_ranges(
        total_bytes: int,
        connections: int,
        span_size: int,
    ) -> list[tuple[int, int]]:
        if total_bytes <= 0 or connections <= 0:
            return []
        span_size = min(span_size, max(1, total_bytes // connections))
        max_start = max(0, total_bytes - span_size)
        if connections == 1:
            starts = [0]
        else:
            starts = [
                int((max_start * index) / (connections - 1))
                for index in range(connections)
            ]
        return [
            (start, min(total_bytes - 1, start + span_size - 1))
            for start in starts
        ]

    @staticmethod
    def _safe_error(exc: Exception) -> str:
        if isinstance(exc, requests.exceptions.ConnectTimeout):
            return "connect-timeout"
        if isinstance(exc, requests.exceptions.ReadTimeout):
            return "read-timeout"
        if isinstance(exc, requests.exceptions.SSLError):
            return "tls-error"
        if isinstance(exc, requests.exceptions.ConnectionError):
            text = str(exc).lower()
            if "max retries exceeded" in text:
                return "connection-error/max-retries"
            if "connection reset" in text:
                return "connection-reset"
            return "connection-error"
        if isinstance(exc, requests.exceptions.HTTPError):
            response = exc.response
            if response is not None:
                return f"HTTP {response.status_code}"
            return "http-error"

        text = str(exc).strip().replace("\n", " ")
        if isinstance(exc, RuntimeError):
            if text.startswith("HTTP "):
                return text[:80]
            if text.startswith("short warm-up response"):
                return text[:120]
            if text == "no steady-state bytes measured":
                return text
        return type(exc).__name__
