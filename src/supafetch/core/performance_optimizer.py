from __future__ import annotations

import json
import logging
import statistics
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path

from supafetch.core.preflight_probe import PreflightResult


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HostProfile:
    # best_connections is the configured/requested aria2 ceiling that produced
    # the best real-transfer result. It is intentionally separate from the
    # number of connections aria2 actually kept active.
    best_connections: int = 2
    best_speed_bps: int = 0
    best_observed_connections: int = 0
    max_observed_connections: int = 0
    safe_connection_ceiling: int = 16
    rate_limit_risk: bool = False
    range_supported: bool = True
    last_probe_epoch: float = 0.0
    confidence: str = "low"


class HostProfileStore:
    def __init__(self) -> None:
        self.path = Path.home() / ".supafetch" / "performance_profiles.json"
        self._profiles: dict[str, HostProfile] = {}
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except Exception:
            logger.exception("Could not load host performance profiles")
            return

        for host, values in raw.items():
            try:
                safe_ceiling = max(
                    1,
                    min(16, int(values.get("safe_connection_ceiling", 16))),
                )
                best_connections = max(
                    1,
                    min(safe_ceiling, int(values.get("best_connections", 2))),
                )
                self._profiles[host] = HostProfile(
                    best_connections=best_connections,
                    best_speed_bps=max(0, int(values.get("best_speed_bps", 0))),
                    best_observed_connections=max(
                        0,
                        min(16, int(values.get("best_observed_connections", 0))),
                    ),
                    max_observed_connections=max(
                        0,
                        min(16, int(values.get("max_observed_connections", 0))),
                    ),
                    safe_connection_ceiling=safe_ceiling,
                    rate_limit_risk=bool(values.get("rate_limit_risk", False)),
                    range_supported=bool(values.get("range_supported", True)),
                    last_probe_epoch=float(values.get("last_probe_epoch", 0.0)),
                    confidence=str(values.get("confidence", "low")),
                )
            except Exception:
                logger.warning("Ignoring invalid host profile for %s", host)

    def get(self, host: str) -> HostProfile:
        return self._profiles.get(host, HostProfile())

    def record_preflight(self, result: PreflightResult) -> None:
        if not result.host:
            return

        current = self._profiles.get(result.host, HostProfile())
        safe_ceiling = max(1, min(16, result.safe_connection_ceiling))
        selected = max(1, min(safe_ceiling, result.best_connections))

        current.best_connections = selected
        current.best_speed_bps = max(0, result.peak_speed_bps)
        current.safe_connection_ceiling = safe_ceiling
        current.rate_limit_risk = result.rate_limit_detected
        current.range_supported = result.range_supported
        current.last_probe_epoch = time.time()
        current.confidence = result.confidence
        self._profiles[result.host] = current
        self._save()

        logger.info(
            "Saved preflight profile host=%s configured_best=%s peak=%sB/s safe_ceiling=%s rate_limit_risk=%s confidence=%s",
            result.host,
            current.best_connections,
            current.best_speed_bps,
            current.safe_connection_ceiling,
            current.rate_limit_risk,
            current.confidence,
        )

    def record_runtime_result(
        self,
        host: str,
        configured_connections: int,
        observed_typical: int,
        observed_peak: int,
        speed_bps: int,
    ) -> None:
        if not host or speed_bps <= 0:
            return

        current = self._profiles.get(host, HostProfile())
        configured = max(
            1,
            min(current.safe_connection_ceiling, configured_connections),
        )
        observed_typical = max(0, min(16, observed_typical))
        observed_peak = max(observed_typical, min(16, observed_peak))

        current.best_connections = configured
        current.best_speed_bps = speed_bps
        current.best_observed_connections = observed_typical
        current.max_observed_connections = max(
            current.max_observed_connections,
            observed_peak,
        )
        current.range_supported = True
        current.confidence = "real-transfer"
        self._profiles[host] = current
        self._save()

        logger.info(
            "Learned runtime profile host=%s configured=%s observed_typical=%s observed_peak=%s stable_speed=%sB/s safe_ceiling=%s rate_limit_risk=%s",
            host,
            configured,
            observed_typical,
            observed_peak,
            speed_bps,
            current.safe_connection_ceiling,
            current.rate_limit_risk,
        )

    def lower_after_error(self, host: str, current_connections: int) -> None:
        if not host or current_connections <= 1:
            return
        current = self._profiles.get(host, HostProfile())
        reduced = PerformanceOptimizer.previous_level(current_connections)
        current.best_connections = min(reduced, current.safe_connection_ceiling)
        current.confidence = "degraded"
        self._profiles[host] = current
        self._save()
        logger.info(
            "Reduced learned host configured connections after error host=%s -> %s",
            host,
            current.best_connections,
        )

    def fallback_connections(self, host: str) -> int:
        profile = self.get(host)
        if not profile.range_supported:
            return 1
        return max(
            1,
            min(profile.safe_connection_ceiling, profile.best_connections or 2),
        )

    def fallback_confidence(self, host: str) -> str:
        return self.get(host).confidence

    def expected_speed(self, host: str) -> int:
        return max(0, self.get(host).best_speed_bps)

    def safe_connection_ceiling(self, host: str) -> int:
        return max(1, min(16, self.get(host).safe_connection_ceiling))

    def rate_limit_risk(self, host: str) -> bool:
        return self.get(host).rate_limit_risk

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                host: {
                    "best_connections": profile.best_connections,
                    "best_speed_bps": profile.best_speed_bps,
                    "best_observed_connections": profile.best_observed_connections,
                    "max_observed_connections": profile.max_observed_connections,
                    "safe_connection_ceiling": profile.safe_connection_ceiling,
                    "rate_limit_risk": profile.rate_limit_risk,
                    "range_supported": profile.range_supported,
                    "last_probe_epoch": profile.last_probe_epoch,
                    "confidence": profile.confidence,
                }
                for host, profile in self._profiles.items()
            }
            self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            logger.exception("Could not save host performance profiles")


@dataclass(slots=True)
class PerformanceDecision:
    stable_10_bps: int
    stable_30_bps: int
    peak_speed_bps: int
    expected_speed_bps: int
    target_connections: int | None
    mode: str
    configured_connections: int = 0
    observed_connections: int = 0
    safe_connection_ceiling: int = 16
    rate_limit_risk: bool = False


@dataclass(slots=True)
class TransferState:
    host: str
    connections: int
    mode: str
    expected_speed_bps: int
    safe_connection_ceiling: int = 16
    rate_limit_risk: bool = False
    started_at: float = field(default_factory=time.monotonic)
    last_change_at: float = field(default_factory=time.monotonic)
    history: deque[tuple[float, int]] = field(
        default_factory=lambda: deque(maxlen=180)
    )
    peak_speed_bps: int = 0
    phase: str = "monitoring"
    previous_connections: int = 1
    baseline_speed_bps: int = 0
    boost_attempts: int = 0
    boost_locked: bool = False
    last_progress_at: float = field(default_factory=time.monotonic)
    last_completed_bytes: int = 0
    observed_connections: int = 0
    observed_history: deque[int] = field(default_factory=lambda: deque(maxlen=60))
    observed_peak_connections: int = 0
    last_telemetry_log_at: float = 0.0

    @property
    def observed_typical_connections(self) -> int:
        values = [value for value in self.observed_history if value > 0]
        if not values:
            return self.observed_connections
        # Median is deliberately conservative when aria2 oscillates between,
        # for example, 3 and 4 active connections.
        return int(round(statistics.median(values)))


class PerformanceOptimizer:
    """Phase 3 runtime validation with observed-connection awareness."""

    CONNECTION_LEVELS = (1, 2, 4, 8, 16)
    UNDERPERFORM_RATIO = 0.70
    BOOST_MIN_IMPROVEMENT = 0.15
    INITIAL_MONITOR_SECONDS = 20.0
    BOOST_TEST_SECONDS = 15.0
    BOOST_SETTLE_SECONDS = 20.0
    STALL_SECONDS = 14.0
    MIN_REMAINING_FOR_BOOST = 128 * 1024 * 1024
    MIN_REMAINING_FOR_SECOND_BOOST = 256 * 1024 * 1024
    TELEMETRY_LOG_SECONDS = 10.0

    def __init__(self) -> None:
        self.profiles = HostProfileStore()
        self.states: dict[str, TransferState] = {}

    def record_preflight(self, result: PreflightResult) -> None:
        self.profiles.record_preflight(result)

    def fallback_connections(self, host: str) -> int:
        return self.profiles.fallback_connections(host)

    def fallback_confidence(self, host: str) -> str:
        return self.profiles.fallback_confidence(host)

    def expected_speed(self, host: str) -> int:
        return self.profiles.expected_speed(host)

    def register(
        self,
        gid: str,
        host: str,
        connections: int,
        source: str,
        confidence: str = "",
        expected_speed_bps: int = 0,
    ) -> None:
        safe_ceiling = self.profiles.safe_connection_ceiling(host)
        normalized = min(self.normalize_level(connections), safe_ceiling)
        rate_limit_risk = self.profiles.rate_limit_risk(host)
        suffix = f" ({confidence.title()})" if confidence else ""
        mode = f"{source}:{normalized}c{suffix}"
        if rate_limit_risk:
            mode += f" cap{safe_ceiling}"

        now = time.monotonic()
        self.states[gid] = TransferState(
            host=host,
            connections=normalized,
            previous_connections=normalized,
            mode=mode,
            expected_speed_bps=max(0, expected_speed_bps),
            safe_connection_ceiling=safe_ceiling,
            rate_limit_risk=rate_limit_risk,
            started_at=now,
            last_change_at=now,
            last_progress_at=now,
        )
        logger.info(
            "Performance tracker registered gid=%s host=%s configured=%s safe_ceiling=%s rate_limit_risk=%s source=%s confidence=%s expected=%.2fMB/s",
            gid,
            host,
            normalized,
            safe_ceiling,
            rate_limit_risk,
            source,
            confidence or "n/a",
            expected_speed_bps / (1024 * 1024),
        )

    def remove(self, gid: str) -> None:
        self.states.pop(gid, None)

    def observe(
        self,
        gid: str,
        status: str,
        speed_bps: int,
        completed_bytes: int,
        total_bytes: int,
        observed_connections: int,
        same_host_active: int = 1,
    ) -> PerformanceDecision:
        state = self.states.get(gid)
        if not state:
            speed = max(0, speed_bps)
            return PerformanceDecision(
                speed,
                0,
                speed,
                0,
                None,
                "off",
                observed_connections,
                observed_connections,
            )

        now = time.monotonic()
        completed = max(0, completed_bytes)
        self._observe_connection_count(gid, state, observed_connections)

        if completed > state.last_completed_bytes:
            state.last_progress_at = now
            state.last_completed_bytes = completed

        if not state.history or completed >= state.history[-1][1]:
            state.history.append((now, completed))
        else:
            state.history.clear()
            state.history.append((now, completed))

        stable_10 = self._stable_speed(state.history, 10.0)
        stable_30 = self._stable_speed(state.history, 30.0)
        stable_reference = stable_30 or stable_10
        state.peak_speed_bps = max(state.peak_speed_bps, stable_10, stable_30)

        self._log_telemetry(
            gid,
            state,
            now,
            speed_bps=max(0, speed_bps),
            stable_10=stable_10,
            stable_30=stable_30,
            same_host_active=same_host_active,
        )

        if status == "complete":
            learned = stable_reference or state.peak_speed_bps
            if learned > 0 and same_host_active <= 1:
                self._record_runtime_state(state, learned)
            return self._decision(state, stable_10, stable_30, None, "complete")

        if status == "error":
            self.profiles.lower_after_error(state.host, state.connections)
            return self._decision(state, stable_10, stable_30, None, "error")

        if status != "active":
            return self._decision(state, stable_10, stable_30, None, state.mode)

        remaining = max(0, total_bytes - completed) if total_bytes > 0 else 0

        if same_host_active > 1:
            if state.phase == "testing" and state.connections != state.previous_connections:
                target = state.previous_connections
                logger.info(
                    "Runtime boost aborted gid=%s because %s transfers share host=%s; rollback configured %s -> %s",
                    gid,
                    same_host_active,
                    state.host,
                    state.connections,
                    target,
                )
                state.connections = target
                state.phase = "locked"
                state.boost_locked = True
                state.mode = f"shared-host:{target}c"
                self._reset_measurement_windows(state, now)
                return self._decision(state, 0, 0, target, state.mode)
            state.mode = f"shared-host:{state.connections}c"
            return self._decision(state, stable_10, stable_30, None, state.mode)

        if (
            completed > 0
            and now - state.last_progress_at >= self.STALL_SECONDS
            and state.connections > 1
            and now - state.last_change_at >= self.STALL_SECONDS
        ):
            target = self.previous_level(state.connections)
            logger.warning(
                "Runtime stall fallback gid=%s host=%s configured %s -> %s no_progress=%.1fs",
                gid,
                state.host,
                state.connections,
                target,
                now - state.last_progress_at,
            )
            state.previous_connections = state.connections
            state.connections = target
            state.phase = "locked"
            state.boost_locked = True
            state.mode = f"stall-fallback:{target}c"
            self._reset_measurement_windows(state, now)
            return self._decision(state, 0, 0, target, state.mode)

        if state.boost_locked:
            return self._decision(state, stable_10, stable_30, None, state.mode)

        if state.phase == "testing":
            if now - state.last_change_at < self.BOOST_TEST_SECONDS or stable_10 <= 0:
                return self._decision(state, stable_10, stable_30, None, state.mode)

            candidate = stable_10
            baseline = max(1, state.baseline_speed_bps)
            improvement = candidate / baseline - 1.0
            observed_typical = state.observed_typical_connections
            observed_peak = state.observed_peak_connections

            if improvement >= self.BOOST_MIN_IMPROVEMENT:
                logger.info(
                    "Runtime boost accepted gid=%s host=%s configured=%s observed_typical=%s observed_peak=%s baseline=%.2fMB/s candidate=%.2fMB/s improvement=%.1f%%",
                    gid,
                    state.host,
                    state.connections,
                    observed_typical,
                    observed_peak,
                    baseline / (1024 * 1024),
                    candidate / (1024 * 1024),
                    improvement * 100,
                )
                self._record_runtime_state(state, candidate)
                state.phase = "settling"
                state.mode = f"boosted:cfg{state.connections}/obs{observed_typical}"
                state.last_change_at = now
                state.history.clear()
                return self._decision(state, 0, 0, None, state.mode)

            target = state.previous_connections
            logger.info(
                "Runtime boost rollback gid=%s host=%s configured %s -> %s observed_typical=%s observed_peak=%s baseline=%.2fMB/s candidate=%.2fMB/s improvement=%.1f%%",
                gid,
                state.host,
                state.connections,
                target,
                observed_typical,
                observed_peak,
                baseline / (1024 * 1024),
                candidate / (1024 * 1024),
                improvement * 100,
            )
            state.connections = target
            state.phase = "locked"
            state.boost_locked = True
            state.mode = f"rollback:{target}c"
            self._reset_measurement_windows(state, now)
            return self._decision(state, 0, 0, target, state.mode)

        if (
            state.phase == "settling"
            and now - state.last_change_at >= self.BOOST_SETTLE_SECONDS
        ):
            if state.connections >= state.safe_connection_ceiling:
                state.phase = "locked"
                state.boost_locked = True
                label = "rate-limit-cap" if state.rate_limit_risk else "maxed"
                state.mode = f"{label}:cfg{state.connections}/obs{state.observed_typical_connections}"
                logger.info(
                    "Runtime boost ceiling reached gid=%s host=%s configured=%s observed_typical=%s safe_ceiling=%s rate_limit_risk=%s",
                    gid,
                    state.host,
                    state.connections,
                    state.observed_typical_connections,
                    state.safe_connection_ceiling,
                    state.rate_limit_risk,
                )
                return self._decision(state, stable_10, stable_30, None, state.mode)

            if (
                stable_10 > 0
                and (remaining == 0 or remaining >= self.MIN_REMAINING_FOR_SECOND_BOOST)
            ):
                return self._start_boost(gid, state, stable_10, now)

        elapsed = now - state.started_at
        if (
            state.phase == "monitoring"
            and elapsed >= self.INITIAL_MONITOR_SECONDS
            and stable_10 > 0
            and state.connections < state.safe_connection_ceiling
            and (remaining == 0 or remaining >= self.MIN_REMAINING_FOR_BOOST)
            and state.expected_speed_bps > 0
            and stable_10 < state.expected_speed_bps * self.UNDERPERFORM_RATIO
        ):
            logger.info(
                "Runtime underperformance gid=%s host=%s stable10=%.2fMB/s expected=%.2fMB/s ratio=%.0f%% configured=%s observed=%s safe_ceiling=%s",
                gid,
                state.host,
                stable_10 / (1024 * 1024),
                state.expected_speed_bps / (1024 * 1024),
                stable_10 / max(1, state.expected_speed_bps) * 100,
                state.connections,
                state.observed_connections,
                state.safe_connection_ceiling,
            )
            return self._start_boost(gid, state, stable_10, now)

        return self._decision(state, stable_10, stable_30, None, state.mode)

    def change_failed(self, gid: str) -> None:
        state = self.states.get(gid)
        if not state:
            return
        if state.phase == "testing":
            state.connections = state.previous_connections
        state.phase = "locked"
        state.boost_locked = True
        state.mode = "change-failed"
        self._reset_measurement_windows(state, time.monotonic())
        logger.warning("Runtime connection change failed gid=%s; locking optimizer", gid)

    def _start_boost(
        self,
        gid: str,
        state: TransferState,
        baseline_speed_bps: int,
        now: float,
    ) -> PerformanceDecision:
        target = self.next_level(state.connections, state.safe_connection_ceiling)
        if target == state.connections:
            state.phase = "locked"
            state.boost_locked = True
            label = "rate-limit-cap" if state.rate_limit_risk else "maxed"
            state.mode = f"{label}:{state.connections}c"
            return self._decision(state, 0, 0, None, state.mode)

        previous = state.connections
        state.previous_connections = previous
        state.connections = target
        state.baseline_speed_bps = max(1, baseline_speed_bps)
        state.phase = "testing"
        state.boost_attempts += 1
        state.mode = f"boost-test:{previous}->{target}c"
        self._reset_measurement_windows(state, now)
        logger.info(
            "Runtime boost test gid=%s host=%s configured %s -> %s baseline=%.2fMB/s attempt=%s safe_ceiling=%s rate_limit_risk=%s",
            gid,
            state.host,
            previous,
            target,
            baseline_speed_bps / (1024 * 1024),
            state.boost_attempts,
            state.safe_connection_ceiling,
            state.rate_limit_risk,
        )
        return self._decision(state, 0, 0, target, state.mode)

    def _record_runtime_state(self, state: TransferState, speed_bps: int) -> None:
        self.profiles.record_runtime_result(
            host=state.host,
            configured_connections=state.connections,
            observed_typical=state.observed_typical_connections,
            observed_peak=state.observed_peak_connections,
            speed_bps=speed_bps,
        )

    @staticmethod
    def _stable_speed(history: deque[tuple[float, int]], window: float) -> int:
        if len(history) < 2:
            return 0
        now, current_bytes = history[-1]
        target_time = now - window
        candidate: tuple[float, int] | None = None
        for sample in history:
            if sample[0] <= target_time:
                candidate = sample
            else:
                break

        if candidate is None:
            first = history[0]
            elapsed = now - first[0]
            if elapsed < window * 0.8:
                return 0
            candidate = first

        elapsed = now - candidate[0]
        delta = current_bytes - candidate[1]
        if elapsed <= 0 or delta < 0:
            return 0
        return int(delta / elapsed)

    def _observe_connection_count(
        self,
        gid: str,
        state: TransferState,
        observed_connections: int,
    ) -> None:
        if observed_connections <= 0:
            return
        if state.observed_connections and observed_connections != state.observed_connections:
            logger.info(
                "aria2 active connection count changed gid=%s host=%s %s -> %s configured=%s",
                gid,
                state.host,
                state.observed_connections,
                observed_connections,
                state.connections,
            )
        state.observed_connections = observed_connections
        state.observed_history.append(observed_connections)
        state.observed_peak_connections = max(
            state.observed_peak_connections,
            observed_connections,
        )

    def _log_telemetry(
        self,
        gid: str,
        state: TransferState,
        now: float,
        speed_bps: int,
        stable_10: int,
        stable_30: int,
        same_host_active: int,
    ) -> None:
        if now - state.last_telemetry_log_at < self.TELEMETRY_LOG_SECONDS:
            return
        state.last_telemetry_log_at = now
        logger.info(
            "Throughput gid=%s host=%s instant=%.2fMB/s stable10=%.2fMB/s stable30=%.2fMB/s peak=%.2fMB/s expected=%.2fMB/s active_conn=%s typical_conn=%s configured_conn=%s safe_ceiling=%s rate_limit_risk=%s same_host_active=%s mode=%s",
            gid,
            state.host,
            speed_bps / (1024 * 1024),
            stable_10 / (1024 * 1024),
            stable_30 / (1024 * 1024),
            state.peak_speed_bps / (1024 * 1024),
            state.expected_speed_bps / (1024 * 1024),
            state.observed_connections or 0,
            state.observed_typical_connections,
            state.connections,
            state.safe_connection_ceiling,
            state.rate_limit_risk,
            same_host_active,
            state.mode,
        )

    @staticmethod
    def _reset_measurement_windows(state: TransferState, now: float) -> None:
        state.history.clear()
        state.observed_history.clear()
        state.observed_peak_connections = 0
        state.last_change_at = now

    @staticmethod
    def _decision(
        state: TransferState,
        stable_10: int,
        stable_30: int,
        target: int | None,
        mode: str,
    ) -> PerformanceDecision:
        return PerformanceDecision(
            stable_10_bps=stable_10,
            stable_30_bps=stable_30,
            peak_speed_bps=state.peak_speed_bps,
            expected_speed_bps=state.expected_speed_bps,
            target_connections=target,
            mode=mode,
            configured_connections=state.connections,
            observed_connections=state.observed_connections,
            safe_connection_ceiling=state.safe_connection_ceiling,
            rate_limit_risk=state.rate_limit_risk,
        )

    @classmethod
    def normalize_level(cls, value: int) -> int:
        return min(cls.CONNECTION_LEVELS, key=lambda item: abs(item - value))

    @classmethod
    def next_level(cls, current: int, ceiling: int = 16) -> int:
        for level in cls.CONNECTION_LEVELS:
            if level > current and level <= ceiling:
                return level
        return current

    @classmethod
    def previous_level(cls, current: int) -> int:
        lower = [level for level in cls.CONNECTION_LEVELS if level < current]
        return lower[-1] if lower else 1
