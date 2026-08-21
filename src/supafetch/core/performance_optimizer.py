from __future__ import annotations

import json
import logging
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from supafetch.core.preflight_probe import PreflightResult


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HostProfile:
    probe_best_connections: int = 2
    probe_speed_bps: int = 0
    runtime_best_connections: int = 0
    runtime_best_speed_bps: int = 0
    runtime_best_observed_connections: int = 0
    runtime_peak_observed_connections: int = 0
    safe_connection_ceiling: int = 16
    rate_limit_risk: bool = False
    range_supported: bool = True
    last_probe_epoch: float = 0.0
    last_runtime_epoch: float = 0.0
    confidence: str = "low"
    config_stats: dict[str, dict[str, int | float]] = field(
        default_factory=dict
    )


class HostProfileStore:
    """Persist probe evidence separately from real-transfer evidence."""

    def __init__(self) -> None:
        self.path = (
            Path.home()
            / ".supafetch"
            / "performance_profiles.json"
        )
        self._profiles: dict[str, HostProfile] = {}
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(
                self.path.read_text(encoding="utf-8")
            )
        except FileNotFoundError:
            return
        except Exception:
            logger.exception(
                "Could not load host performance profiles"
            )
            return

        for host, values in raw.items():
            try:
                safe_ceiling = self._clamp_connections(
                    int(
                        values.get(
                            "safe_connection_ceiling",
                            16,
                        )
                    )
                )

                legacy_best = self._clamp_connections(
                    int(values.get("best_connections", 2))
                )
                legacy_speed = max(
                    0,
                    int(values.get("best_speed_bps", 0)),
                )
                legacy_confidence = str(
                    values.get("confidence", "low")
                )
                legacy_is_runtime = (
                    legacy_confidence == "real-transfer"
                )

                probe_best = self._clamp_connections(
                    int(
                        values.get(
                            "probe_best_connections",
                            (
                                legacy_best
                                if not legacy_is_runtime
                                else 2
                            ),
                        )
                    )
                )
                probe_speed = max(
                    0,
                    int(
                        values.get(
                            "probe_speed_bps",
                            (
                                legacy_speed
                                if not legacy_is_runtime
                                else 0
                            ),
                        )
                    ),
                )
                runtime_best = int(
                    values.get(
                        "runtime_best_connections",
                        (
                            legacy_best
                            if legacy_is_runtime
                            else 0
                        ),
                    )
                )
                runtime_best = (
                    self._clamp_connections(runtime_best)
                    if runtime_best > 0
                    else 0
                )
                runtime_speed = max(
                    0,
                    int(
                        values.get(
                            "runtime_best_speed_bps",
                            (
                                legacy_speed
                                if legacy_is_runtime
                                else 0
                            ),
                        )
                    ),
                )

                config_stats_raw = values.get(
                    "config_stats",
                    {},
                )
                config_stats: dict[
                    str,
                    dict[str, int | float],
                ] = {}
                if isinstance(config_stats_raw, dict):
                    for key, stat in config_stats_raw.items():
                        if not isinstance(stat, dict):
                            continue
                        config_stats[str(key)] = {
                            "best_speed_bps": max(
                                0,
                                int(
                                    stat.get(
                                        "best_speed_bps",
                                        0,
                                    )
                                ),
                            ),
                            "last_speed_bps": max(
                                0,
                                int(
                                    stat.get(
                                        "last_speed_bps",
                                        0,
                                    )
                                ),
                            ),
                            "samples": max(
                                0,
                                int(stat.get("samples", 0)),
                            ),
                            "observed_typical": max(
                                0,
                                min(
                                    16,
                                    int(
                                        stat.get(
                                            "observed_typical",
                                            0,
                                        )
                                    ),
                                ),
                            ),
                            "observed_peak": max(
                                0,
                                min(
                                    16,
                                    int(
                                        stat.get(
                                            "observed_peak",
                                            0,
                                        )
                                    ),
                                ),
                            ),
                            "updated_epoch": float(
                                stat.get(
                                    "updated_epoch",
                                    0.0,
                                )
                            ),
                        }

                profile = HostProfile(
                    probe_best_connections=min(
                        probe_best,
                        safe_ceiling,
                    ),
                    probe_speed_bps=probe_speed,
                    runtime_best_connections=(
                        min(runtime_best, safe_ceiling)
                        if runtime_best
                        else 0
                    ),
                    runtime_best_speed_bps=runtime_speed,
                    runtime_best_observed_connections=max(
                        0,
                        min(
                            16,
                            int(
                                values.get(
                                    "runtime_best_observed_connections",
                                    values.get(
                                        "best_observed_connections",
                                        0,
                                    ),
                                )
                            ),
                        ),
                    ),
                    runtime_peak_observed_connections=max(
                        0,
                        min(
                            16,
                            int(
                                values.get(
                                    "runtime_peak_observed_connections",
                                    values.get(
                                        "max_observed_connections",
                                        0,
                                    ),
                                )
                            ),
                        ),
                    ),
                    safe_connection_ceiling=safe_ceiling,
                    rate_limit_risk=bool(
                        values.get(
                            "rate_limit_risk",
                            False,
                        )
                    ),
                    range_supported=bool(
                        values.get(
                            "range_supported",
                            True,
                        )
                    ),
                    last_probe_epoch=float(
                        values.get(
                            "last_probe_epoch",
                            0.0,
                        )
                    ),
                    last_runtime_epoch=float(
                        values.get(
                            "last_runtime_epoch",
                            0.0,
                        )
                    ),
                    confidence=legacy_confidence,
                    config_stats=config_stats,
                )
                self._profiles[host] = profile
                self._recompute_runtime_best(profile)
            except Exception:
                logger.warning(
                    "Ignoring invalid host profile for %s",
                    host,
                )

    def get(self, host: str) -> HostProfile:
        return self._profiles.get(host, HostProfile())

    def record_preflight(
        self,
        result: PreflightResult,
    ) -> None:
        if not result.host:
            return

        current = self._profiles.get(
            result.host,
            HostProfile(),
        )
        safe_ceiling = self._clamp_connections(
            result.safe_connection_ceiling
        )

        current.probe_best_connections = min(
            self._clamp_connections(
                result.best_connections
            ),
            safe_ceiling,
        )
        current.probe_speed_bps = max(
            0,
            result.selected_speed_bps,
        )
        current.safe_connection_ceiling = safe_ceiling
        current.rate_limit_risk = (
            result.rate_limit_detected
        )
        current.range_supported = result.range_supported
        current.last_probe_epoch = time.time()
        if current.runtime_best_speed_bps <= 0:
            current.confidence = result.confidence

        if (
            current.runtime_best_connections
            > safe_ceiling
        ):
            current.runtime_best_connections = 0
            current.runtime_best_speed_bps = 0
            current.runtime_best_observed_connections = 0

        self._profiles[result.host] = current
        self._recompute_runtime_best(current)
        self._save()

        logger.info(
            "Saved preflight profile host=%s probe_best=%s "
            "probe_speed=%.2fMB/s runtime_best=%s "
            "runtime_speed=%.2fMB/s safe_ceiling=%s "
            "rate_limit_risk=%s",
            result.host,
            current.probe_best_connections,
            current.probe_speed_bps / (1024 * 1024),
            current.runtime_best_connections or 0,
            current.runtime_best_speed_bps
            / (1024 * 1024),
            current.safe_connection_ceiling,
            current.rate_limit_risk,
        )

    def record_runtime_sample(
        self,
        host: str,
        configured_connections: int,
        observed_typical: int,
        observed_peak: int,
        speed_bps: int,
    ) -> None:
        if not host or speed_bps <= 0:
            return

        current = self._profiles.get(
            host,
            HostProfile(),
        )
        configured = min(
            self._clamp_connections(
                configured_connections
            ),
            current.safe_connection_ceiling,
        )
        key = str(configured)
        previous = current.config_stats.get(key, {})
        previous_best = max(
            0,
            int(previous.get("best_speed_bps", 0)),
        )
        samples = max(
            0,
            int(previous.get("samples", 0)),
        ) + 1

        current.config_stats[key] = {
            "best_speed_bps": max(
                previous_best,
                speed_bps,
            ),
            "last_speed_bps": speed_bps,
            "samples": samples,
            "observed_typical": max(
                0,
                min(16, observed_typical),
            ),
            "observed_peak": max(
                max(
                    0,
                    int(
                        previous.get(
                            "observed_peak",
                            0,
                        )
                    ),
                ),
                max(0, min(16, observed_peak)),
            ),
            "updated_epoch": time.time(),
        }
        current.last_runtime_epoch = time.time()
        current.range_supported = True
        current.confidence = "real-transfer"
        self._profiles[host] = current
        self._recompute_runtime_best(current)
        self._save()

        logger.info(
            "Learned runtime sample host=%s configured=%s "
            "observed_typical=%s observed_peak=%s "
            "stable_speed=%.2fMB/s runtime_best=%s "
            "runtime_best_speed=%.2fMB/s",
            host,
            configured,
            observed_typical,
            observed_peak,
            speed_bps / (1024 * 1024),
            current.runtime_best_connections,
            current.runtime_best_speed_bps
            / (1024 * 1024),
        )

    def lower_after_error(
        self,
        host: str,
        current_connections: int,
    ) -> None:
        if not host or current_connections <= 1:
            return
        current = self._profiles.get(
            host,
            HostProfile(),
        )
        reduced = PerformanceOptimizer.previous_level(
            current_connections
        )
        current.safe_connection_ceiling = min(
            current.safe_connection_ceiling,
            max(1, reduced),
        )
        current.rate_limit_risk = True
        current.confidence = "degraded"
        self._profiles[host] = current
        self._recompute_runtime_best(current)
        self._save()
        logger.info(
            "Reduced host safe ceiling after transfer error "
            "host=%s -> %s",
            host,
            current.safe_connection_ceiling,
        )

    def fallback_connections(self, host: str) -> int:
        profile = self.get(host)
        if not profile.range_supported:
            return 1
        if profile.runtime_best_connections > 0:
            return min(
                profile.runtime_best_connections,
                profile.safe_connection_ceiling,
            )
        return min(
            profile.probe_best_connections or 2,
            profile.safe_connection_ceiling,
        )

    def fallback_confidence(self, host: str) -> str:
        profile = self.get(host)
        if profile.runtime_best_speed_bps > 0:
            return "real-transfer"
        return profile.confidence

    def expected_speed(
        self,
        host: str,
        connections: int | None = None,
    ) -> int:
        profile = self.get(host)
        if connections is not None:
            stat = profile.config_stats.get(
                str(connections)
            )
            if stat:
                best = max(
                    0,
                    int(
                        stat.get(
                            "best_speed_bps",
                            0,
                        )
                    ),
                )
                if best > 0:
                    return best
            if (
                connections
                == profile.probe_best_connections
                and profile.probe_speed_bps > 0
            ):
                return profile.probe_speed_bps

        if profile.runtime_best_speed_bps > 0:
            return profile.runtime_best_speed_bps
        return max(0, profile.probe_speed_bps)

    def safe_connection_ceiling(
        self,
        host: str,
    ) -> int:
        return self._clamp_connections(
            self.get(host).safe_connection_ceiling
        )

    def rate_limit_risk(self, host: str) -> bool:
        return self.get(host).rate_limit_risk

    def runtime_profile_fresh(
        self,
        host: str,
        max_age_seconds: float = 1800.0,
    ) -> bool:
        profile = self.get(host)
        if (
            profile.runtime_best_connections <= 0
            or profile.runtime_best_speed_bps <= 0
            or profile.last_runtime_epoch <= 0
        ):
            return False
        return (
            time.time() - profile.last_runtime_epoch
            <= max_age_seconds
        )

    def _recompute_runtime_best(
        self,
        profile: HostProfile,
    ) -> None:
        profile.runtime_best_connections = 0
        profile.runtime_best_speed_bps = 0
        profile.runtime_best_observed_connections = 0
        candidates: list[
            tuple[int, int, int, int]
        ] = []
        for key, stat in profile.config_stats.items():
            try:
                connections = int(key)
            except (TypeError, ValueError):
                continue
            if (
                connections < 1
                or connections
                > profile.safe_connection_ceiling
            ):
                continue
            speed = max(
                0,
                int(stat.get("best_speed_bps", 0)),
            )
            if speed <= 0:
                continue
            candidates.append(
                (
                    connections,
                    speed,
                    max(
                        0,
                        int(
                            stat.get(
                                "observed_typical",
                                0,
                            )
                        ),
                    ),
                    max(
                        0,
                        int(
                            stat.get(
                                "observed_peak",
                                0,
                            )
                        ),
                    ),
                )
            )

        if not candidates:
            return

        best_speed = max(
            item[1] for item in candidates
        )
        near_best = [
            item
            for item in candidates
            if item[1] >= best_speed * 0.98
        ]
        winner = min(
            near_best,
            key=lambda item: item[0],
        )
        profile.runtime_best_connections = winner[0]
        profile.runtime_best_speed_bps = winner[1]
        profile.runtime_best_observed_connections = (
            winner[2]
        )
        profile.runtime_peak_observed_connections = max(
            profile.runtime_peak_observed_connections,
            winner[3],
        )

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            payload = {
                host: {
                    "probe_best_connections": (
                        profile.probe_best_connections
                    ),
                    "probe_speed_bps": (
                        profile.probe_speed_bps
                    ),
                    "runtime_best_connections": (
                        profile.runtime_best_connections
                    ),
                    "runtime_best_speed_bps": (
                        profile.runtime_best_speed_bps
                    ),
                    "runtime_best_observed_connections": (
                        profile.runtime_best_observed_connections
                    ),
                    "runtime_peak_observed_connections": (
                        profile.runtime_peak_observed_connections
                    ),
                    "safe_connection_ceiling": (
                        profile.safe_connection_ceiling
                    ),
                    "rate_limit_risk": (
                        profile.rate_limit_risk
                    ),
                    "range_supported": (
                        profile.range_supported
                    ),
                    "last_probe_epoch": (
                        profile.last_probe_epoch
                    ),
                    "last_runtime_epoch": (
                        profile.last_runtime_epoch
                    ),
                    "confidence": profile.confidence,
                    "config_stats": profile.config_stats,
                }
                for host, profile in self._profiles.items()
            }
            self.path.write_text(
                json.dumps(payload, indent=2),
                encoding="utf-8",
            )
        except Exception:
            logger.exception(
                "Could not save host performance profiles"
            )

    @staticmethod
    def _clamp_connections(value: int) -> int:
        return max(1, min(16, int(value)))


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
        default_factory=lambda: deque(maxlen=240)
    )
    peak_speed_bps: int = 0
    phase: str = "monitoring"
    last_progress_at: float = field(
        default_factory=time.monotonic
    )
    last_completed_bytes: int = 0
    observed_connections: int = 0
    observed_history: deque[int] = field(
        default_factory=lambda: deque(maxlen=90)
    )
    observed_peak_connections: int = 0
    last_telemetry_log_at: float = 0.0
    session_results: dict[int, int] = field(
        default_factory=dict
    )
    test_origin_connections: int = 0

    @property
    def observed_typical_connections(self) -> int:
        values = [
            value
            for value in self.observed_history
            if value > 0
        ]
        if not values:
            return self.observed_connections
        return int(round(statistics.median(values)))


class PerformanceOptimizer:
    """Runtime exhaustive-search optimizer for aria2 concurrency.

    For large single-host transfers the optimizer measures the current
    configuration, then tests every higher aria2 level through the host's safe
    ceiling. A slower intermediate level does not terminate the search.
    """

    CONNECTION_LEVELS = (1, 2, 4, 8, 16)
    INITIAL_MONITOR_SECONDS = 20.0
    TEST_SECONDS = 15.0
    STALL_SECONDS = 14.0
    TELEMETRY_LOG_SECONDS = 10.0
    MIN_REMAINING_FOR_EXPLORATION = (
        256 * 1024 * 1024
    )
    MIN_REMAINING_FOR_NEXT_TEST = (
        128 * 1024 * 1024
    )

    def __init__(self) -> None:
        self.profiles = HostProfileStore()
        self.states: dict[str, TransferState] = {}

    def record_preflight(
        self,
        result: PreflightResult,
    ) -> None:
        self.profiles.record_preflight(result)

    def fallback_connections(self, host: str) -> int:
        return self.profiles.fallback_connections(host)

    def fallback_confidence(self, host: str) -> str:
        return self.profiles.fallback_confidence(host)

    def expected_speed(
        self,
        host: str,
        connections: int | None = None,
    ) -> int:
        return self.profiles.expected_speed(
            host,
            connections,
        )

    def safe_connection_ceiling(
        self,
        host: str,
    ) -> int:
        return self.profiles.safe_connection_ceiling(
            host
        )

    def runtime_profile_fresh(
        self,
        host: str,
        max_age_seconds: float = 1800.0,
    ) -> bool:
        return self.profiles.runtime_profile_fresh(
            host,
            max_age_seconds,
        )

    def configured_connections(
        self,
        gid: str,
    ) -> int:
        state = self.states.get(gid)
        return state.connections if state else 0

    def host_budget_ceiling(
        self,
        host: str,
        active_transfers: int,
    ) -> int:
        return self._budget_ceiling(
            self.profiles.safe_connection_ceiling(host),
            active_transfers,
        )

    def register(
        self,
        gid: str,
        host: str,
        connections: int,
        source: str,
        confidence: str = "",
        expected_speed_bps: int = 0,
    ) -> None:
        safe_ceiling = (
            self.profiles.safe_connection_ceiling(
                host
            )
        )
        normalized = min(
            self.normalize_level(connections),
            safe_ceiling,
        )
        rate_limit_risk = (
            self.profiles.rate_limit_risk(host)
        )
        suffix = (
            f" ({confidence.title()})"
            if confidence
            else ""
        )
        mode = f"{source}:{normalized}c{suffix}"
        if rate_limit_risk:
            mode += f" cap{safe_ceiling}"

        now = time.monotonic()
        self.states[gid] = TransferState(
            host=host,
            connections=normalized,
            mode=mode,
            expected_speed_bps=max(
                0,
                expected_speed_bps,
            ),
            safe_connection_ceiling=safe_ceiling,
            rate_limit_risk=rate_limit_risk,
            started_at=now,
            last_change_at=now,
            last_progress_at=now,
            test_origin_connections=normalized,
        )
        logger.info(
            "Performance V4 tracker registered gid=%s "
            "host=%s configured=%s safe_ceiling=%s "
            "rate_limit_risk=%s source=%s "
            "confidence=%s expected=%.2fMB/s",
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
        self._observe_connection_count(
            gid,
            state,
            observed_connections,
        )

        if completed > state.last_completed_bytes:
            state.last_progress_at = now
            state.last_completed_bytes = completed

        if (
            not state.history
            or completed >= state.history[-1][1]
        ):
            state.history.append((now, completed))
        else:
            self._reset_measurement_windows(
                state,
                now,
            )
            state.history.append((now, completed))

        stable_10 = self._stable_speed(
            state.history,
            10.0,
        )
        stable_30 = self._stable_speed(
            state.history,
            30.0,
        )
        state.peak_speed_bps = max(
            state.peak_speed_bps,
            stable_10,
            stable_30,
        )

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
            if (
                not state.session_results
                and same_host_active <= 1
            ):
                learned = (
                    stable_30
                    or stable_10
                    or state.peak_speed_bps
                )
                if learned > 0:
                    self._record_runtime_state(
                        state,
                        learned,
                    )
            return self._decision(
                state,
                stable_10,
                stable_30,
                None,
                "complete",
            )

        if status == "error":
            self.profiles.lower_after_error(
                state.host,
                state.connections,
            )
            return self._decision(
                state,
                stable_10,
                stable_30,
                None,
                "error",
            )

        if status != "active":
            return self._decision(
                state,
                stable_10,
                stable_30,
                None,
                state.mode,
            )

        remaining = (
            max(0, total_bytes - completed)
            if total_bytes > 0
            else 0
        )

        budget_ceiling = self._budget_ceiling(
            state.safe_connection_ceiling,
            same_host_active,
        )
        if same_host_active > 1:
            if state.connections > budget_ceiling:
                old = state.connections
                state.connections = budget_ceiling
                state.phase = "shared-host"
                state.mode = (
                    f"shared-budget:{old}->{budget_ceiling}c"
                )
                self._reset_measurement_windows(
                    state,
                    now,
                )
                logger.info(
                    "Host budget rebalance gid=%s host=%s "
                    "active_transfers=%s configured %s -> %s "
                    "host_ceiling=%s",
                    gid,
                    state.host,
                    same_host_active,
                    old,
                    budget_ceiling,
                    state.safe_connection_ceiling,
                )
                return self._decision(
                    state,
                    0,
                    0,
                    budget_ceiling,
                    state.mode,
                )

            state.phase = "shared-host"
            state.mode = (
                f"shared-host:{state.connections}c/"
                f"budget{budget_ceiling}"
            )
            return self._decision(
                state,
                stable_10,
                stable_30,
                None,
                state.mode,
            )

        if state.phase == "shared-host":
            state.phase = "monitoring"
            state.started_at = now
            state.mode = (
                f"solo-monitor:{state.connections}c"
            )
            self._reset_measurement_windows(
                state,
                now,
            )
            return self._decision(
                state,
                0,
                0,
                None,
                state.mode,
            )

        if (
            completed > 0
            and now - state.last_progress_at
            >= self.STALL_SECONDS
            and state.connections > 1
            and now - state.last_change_at
            >= self.STALL_SECONDS
        ):
            target = self.previous_level(
                state.connections
            )
            logger.warning(
                "Runtime stall fallback gid=%s host=%s "
                "configured %s -> %s no_progress=%.1fs",
                gid,
                state.host,
                state.connections,
                target,
                now - state.last_progress_at,
            )
            state.connections = target
            state.phase = "locked"
            state.mode = f"stall-fallback:{target}c"
            self._reset_measurement_windows(
                state,
                now,
            )
            return self._decision(
                state,
                0,
                0,
                target,
                state.mode,
            )

        if state.phase == "locked":
            return self._decision(
                state,
                stable_10,
                stable_30,
                None,
                state.mode,
            )

        if state.phase == "monitoring":
            if (
                now - state.started_at
                < self.INITIAL_MONITOR_SECONDS
                or stable_10 <= 0
            ):
                return self._decision(
                    state,
                    stable_10,
                    stable_30,
                    None,
                    state.mode,
                )

            sample = stable_30 or stable_10
            self._capture_current_result(
                state,
                sample,
            )

            if (
                remaining
                and remaining
                < self.MIN_REMAINING_FOR_EXPLORATION
            ):
                return self._lock_best(
                    gid,
                    state,
                    now,
                    stable_10,
                    stable_30,
                    "short-remaining",
                )

            target = self._next_untested_level(
                state,
            )
            if target is None:
                return self._lock_best(
                    gid,
                    state,
                    now,
                    stable_10,
                    stable_30,
                    "already-explored",
                )
            return self._start_test(
                gid,
                state,
                target,
                now,
            )

        if state.phase == "testing":
            if (
                now - state.last_change_at
                < self.TEST_SECONDS
                or stable_10 <= 0
            ):
                return self._decision(
                    state,
                    stable_10,
                    stable_30,
                    None,
                    state.mode,
                )

            sample = stable_30 or stable_10
            self._capture_current_result(
                state,
                sample,
            )

            next_target = self._next_untested_level(
                state
            )
            if (
                next_target is not None
                and (
                    remaining == 0
                    or remaining
                    >= self.MIN_REMAINING_FOR_NEXT_TEST
                )
            ):
                return self._start_test(
                    gid,
                    state,
                    next_target,
                    now,
                )

            return self._lock_best(
                gid,
                state,
                now,
                stable_10,
                stable_30,
                "sweep-complete",
            )

        return self._decision(
            state,
            stable_10,
            stable_30,
            None,
            state.mode,
        )

    def change_failed(self, gid: str) -> None:
        state = self.states.get(gid)
        if not state:
            return
        state.phase = "locked"
        winner = self._session_winner(state)
        if winner is not None:
            state.connections = winner
            state.mode = f"change-failed:winner{winner}c"
        else:
            state.mode = "change-failed"
        self._reset_measurement_windows(
            state,
            time.monotonic(),
        )
        logger.warning(
            "Runtime connection change failed gid=%s; "
            "locking optimizer at configured=%s",
            gid,
            state.connections,
        )

    def _capture_current_result(
        self,
        state: TransferState,
        speed_bps: int,
    ) -> None:
        if speed_bps <= 0:
            return
        previous = state.session_results.get(
            state.connections,
            0,
        )
        state.session_results[state.connections] = max(
            previous,
            speed_bps,
        )
        self._record_runtime_state(
            state,
            speed_bps,
        )

    def _record_runtime_state(
        self,
        state: TransferState,
        speed_bps: int,
    ) -> None:
        self.profiles.record_runtime_sample(
            host=state.host,
            configured_connections=state.connections,
            observed_typical=(
                state.observed_typical_connections
            ),
            observed_peak=(
                state.observed_peak_connections
            ),
            speed_bps=speed_bps,
        )

    def _start_test(
        self,
        gid: str,
        state: TransferState,
        target: int,
        now: float,
    ) -> PerformanceDecision:
        previous = state.connections
        state.connections = target
        state.phase = "testing"
        state.mode = f"sweep:{previous}->{target}c"
        self._reset_measurement_windows(
            state,
            now,
        )
        logger.info(
            "Runtime V4 sweep test gid=%s host=%s "
            "configured %s -> %s tested=%s "
            "safe_ceiling=%s rate_limit_risk=%s",
            gid,
            state.host,
            previous,
            target,
            sorted(state.session_results),
            state.safe_connection_ceiling,
            state.rate_limit_risk,
        )
        return self._decision(
            state,
            0,
            0,
            target,
            state.mode,
        )

    def _lock_best(
        self,
        gid: str,
        state: TransferState,
        now: float,
        stable_10: int,
        stable_30: int,
        reason: str,
    ) -> PerformanceDecision:
        winner = self._session_winner(state)
        if winner is None:
            state.phase = "locked"
            state.mode = (
                f"locked:{state.connections}c"
            )
            return self._decision(
                state,
                stable_10,
                stable_30,
                None,
                state.mode,
            )

        current = state.connections
        winner_speed = state.session_results[winner]
        current_speed = state.session_results.get(
            current,
            0,
        )
        state.phase = "locked"
        state.connections = winner
        state.mode = (
            f"winner:{winner}c "
            f"{winner_speed / (1024 * 1024):.2f}MB/s"
        )
        logger.info(
            "Runtime V4 sweep winner gid=%s host=%s "
            "winner=%sc speed=%.2fMB/s current=%sc "
            "current_speed=%.2fMB/s reason=%s results=%s",
            gid,
            state.host,
            winner,
            winner_speed / (1024 * 1024),
            current,
            current_speed / (1024 * 1024),
            reason,
            {
                key: round(
                    value / (1024 * 1024),
                    2,
                )
                for key, value in sorted(
                    state.session_results.items()
                )
            },
        )
        if winner != current:
            self._reset_measurement_windows(
                state,
                now,
            )
            return self._decision(
                state,
                0,
                0,
                winner,
                state.mode,
            )

        return self._decision(
            state,
            stable_10,
            stable_30,
            None,
            state.mode,
        )

    def _next_untested_level(
        self,
        state: TransferState,
    ) -> int | None:
        for level in self.CONNECTION_LEVELS:
            if (
                level > state.connections
                and level
                <= state.safe_connection_ceiling
                and level
                not in state.session_results
            ):
                return level
        return None

    @staticmethod
    def _session_winner(
        state: TransferState,
    ) -> int | None:
        if not state.session_results:
            return None
        best_speed = max(
            state.session_results.values()
        )
        near_best = [
            connections
            for connections, speed
            in state.session_results.items()
            if speed >= best_speed * 0.98
        ]
        return min(near_best)

    @classmethod
    def _budget_ceiling(
        cls,
        host_ceiling: int,
        active_transfers: int,
    ) -> int:
        if active_transfers <= 1:
            return cls.normalize_floor_level(
                host_ceiling
            )
        raw = max(
            1,
            host_ceiling // active_transfers,
        )
        return cls.normalize_floor_level(raw)

    @staticmethod
    def _stable_speed(
        history: deque[tuple[float, int]],
        window: float,
    ) -> int:
        if len(history) < 2:
            return 0
        now, current_bytes = history[-1]
        target_time = now - window
        candidate: tuple[
            float,
            int,
        ] | None = None
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
        if (
            state.observed_connections
            and observed_connections
            != state.observed_connections
        ):
            logger.info(
                "aria2 active connection count changed "
                "gid=%s host=%s %s -> %s configured=%s",
                gid,
                state.host,
                state.observed_connections,
                observed_connections,
                state.connections,
            )
        state.observed_connections = (
            observed_connections
        )
        state.observed_history.append(
            observed_connections
        )
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
        if (
            now - state.last_telemetry_log_at
            < self.TELEMETRY_LOG_SECONDS
        ):
            return
        state.last_telemetry_log_at = now
        logger.info(
            "Throughput gid=%s host=%s instant=%.2fMB/s "
            "stable10=%.2fMB/s stable30=%.2fMB/s "
            "peak=%.2fMB/s expected=%.2fMB/s "
            "active_conn=%s typical_conn=%s "
            "configured_conn=%s safe_ceiling=%s "
            "rate_limit_risk=%s same_host_active=%s "
            "phase=%s mode=%s",
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
            state.phase,
            state.mode,
        )

    @staticmethod
    def _reset_measurement_windows(
        state: TransferState,
        now: float,
    ) -> None:
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
            observed_connections=(
                state.observed_connections
            ),
            safe_connection_ceiling=(
                state.safe_connection_ceiling
            ),
            rate_limit_risk=(
                state.rate_limit_risk
            ),
        )

    @classmethod
    def normalize_level(
        cls,
        value: int,
    ) -> int:
        return min(
            cls.CONNECTION_LEVELS,
            key=lambda item: abs(item - value),
        )

    @classmethod
    def normalize_floor_level(
        cls,
        value: int,
    ) -> int:
        eligible = [
            level
            for level in cls.CONNECTION_LEVELS
            if level <= max(1, value)
        ]
        return eligible[-1] if eligible else 1

    @classmethod
    def next_level(
        cls,
        current: int,
        ceiling: int = 16,
    ) -> int:
        for level in cls.CONNECTION_LEVELS:
            if level > current and level <= ceiling:
                return level
        return current

    @classmethod
    def previous_level(
        cls,
        current: int,
    ) -> int:
        lower = [
            level
            for level in cls.CONNECTION_LEVELS
            if level < current
        ]
        return lower[-1] if lower else 1
