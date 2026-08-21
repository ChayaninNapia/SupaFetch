from __future__ import annotations

import json
import logging
import math
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
    config_stats: dict[str, dict[str, int | float]] = field(default_factory=dict)


class HostProfileStore:
    """Persistent online-learning evidence for each host/configuration."""

    EWMA_ALPHA = 0.35

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

        if not isinstance(raw, dict):
            return

        for host, values in raw.items():
            if not isinstance(values, dict):
                continue
            try:
                safe = self._clamp(values.get("safe_connection_ceiling", 16))
                legacy_best = self._clamp(values.get("best_connections", 2))
                legacy_speed = max(0, int(values.get("best_speed_bps", 0)))
                confidence = str(values.get("confidence", "low"))
                legacy_runtime = confidence == "real-transfer"

                probe_best = self._clamp(
                    values.get(
                        "probe_best_connections",
                        2 if legacy_runtime else legacy_best,
                    )
                )
                probe_speed = max(
                    0,
                    int(
                        values.get(
                            "probe_speed_bps",
                            0 if legacy_runtime else legacy_speed,
                        )
                    ),
                )
                runtime_best = int(
                    values.get(
                        "runtime_best_connections",
                        legacy_best if legacy_runtime else 0,
                    )
                )
                runtime_best = self._clamp(runtime_best) if runtime_best > 0 else 0
                runtime_speed = max(
                    0,
                    int(
                        values.get(
                            "runtime_best_speed_bps",
                            legacy_speed if legacy_runtime else 0,
                        )
                    ),
                )

                stats: dict[str, dict[str, int | float]] = {}
                raw_stats = values.get("config_stats", {})
                if isinstance(raw_stats, dict):
                    for key, item in raw_stats.items():
                        if not isinstance(item, dict):
                            continue
                        best = max(0, int(item.get("best_speed_bps", 0)))
                        last = max(0, int(item.get("last_speed_bps", 0)))
                        ewma = max(
                            0,
                            int(item.get("ewma_speed_bps", last or best)),
                        )
                        samples = max(0, int(item.get("samples", 0)))
                        # V4 could persist an impossible burst as "best".
                        # EWMA/last are the authoritative prior in V5.
                        if last > 0 and best > last * 6:
                            best = max(last, ewma)
                        stats[str(key)] = {
                            "best_speed_bps": best,
                            "last_speed_bps": last,
                            "ewma_speed_bps": ewma,
                            "samples": samples,
                            "observed_typical": max(
                                0, min(16, int(item.get("observed_typical", 0)))
                            ),
                            "observed_peak": max(
                                0, min(16, int(item.get("observed_peak", 0)))
                            ),
                            "updated_epoch": float(item.get("updated_epoch", 0.0)),
                        }

                if runtime_best > 0 and runtime_speed > 0 and str(runtime_best) not in stats:
                    stats[str(runtime_best)] = {
                        "best_speed_bps": runtime_speed,
                        "last_speed_bps": runtime_speed,
                        "ewma_speed_bps": runtime_speed,
                        "samples": 1,
                        "observed_typical": max(
                            0,
                            min(
                                16,
                                int(
                                    values.get(
                                        "runtime_best_observed_connections",
                                        values.get("best_observed_connections", 0),
                                    )
                                ),
                            ),
                        ),
                        "observed_peak": max(
                            0,
                            min(
                                16,
                                int(
                                    values.get(
                                        "runtime_peak_observed_connections",
                                        values.get("max_observed_connections", 0),
                                    )
                                ),
                            ),
                        ),
                        "updated_epoch": float(values.get("last_runtime_epoch", 0.0)),
                    }

                profile = HostProfile(
                    probe_best_connections=min(probe_best, safe),
                    probe_speed_bps=probe_speed,
                    runtime_best_connections=min(runtime_best, safe) if runtime_best else 0,
                    runtime_best_speed_bps=runtime_speed,
                    runtime_best_observed_connections=max(
                        0,
                        min(
                            16,
                            int(
                                values.get(
                                    "runtime_best_observed_connections",
                                    values.get("best_observed_connections", 0),
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
                                    values.get("max_observed_connections", 0),
                                )
                            ),
                        ),
                    ),
                    safe_connection_ceiling=safe,
                    rate_limit_risk=bool(values.get("rate_limit_risk", False)),
                    range_supported=bool(values.get("range_supported", True)),
                    last_probe_epoch=float(values.get("last_probe_epoch", 0.0)),
                    last_runtime_epoch=float(values.get("last_runtime_epoch", 0.0)),
                    confidence=confidence,
                    config_stats=stats,
                )
                self._profiles[str(host)] = profile
                self._recompute_runtime_best(profile)
            except Exception:
                logger.warning("Ignoring invalid host profile for %s", host)

    def get(self, host: str) -> HostProfile:
        return self._profiles.get(host, HostProfile())

    def record_preflight(self, result: PreflightResult) -> None:
        if not result.host:
            return
        current = self._profiles.get(result.host, HostProfile())
        safe = self._clamp(result.safe_connection_ceiling)
        current.probe_best_connections = min(
            self._clamp(result.best_connections), safe
        )
        current.probe_speed_bps = max(0, int(result.selected_speed_bps))
        current.safe_connection_ceiling = safe
        current.rate_limit_risk = bool(result.rate_limit_detected)
        current.range_supported = bool(result.range_supported)
        current.last_probe_epoch = time.time()
        if current.runtime_best_speed_bps <= 0:
            current.confidence = result.confidence
        self._profiles[result.host] = current
        self._recompute_runtime_best(current)
        self._save()

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
        current = self._profiles.get(host, HostProfile())
        configured = min(
            self._clamp(configured_connections),
            current.safe_connection_ceiling,
        )
        key = str(configured)
        previous = current.config_stats.get(key, {})
        prev_best = max(0, int(previous.get("best_speed_bps", 0)))
        prev_ewma = max(
            0,
            int(
                previous.get(
                    "ewma_speed_bps",
                    previous.get("last_speed_bps", 0),
                )
            ),
        )
        samples = max(0, int(previous.get("samples", 0))) + 1
        ewma = (
            speed_bps
            if prev_ewma <= 0
            else int(
                prev_ewma * (1.0 - self.EWMA_ALPHA)
                + speed_bps * self.EWMA_ALPHA
            )
        )
        current.config_stats[key] = {
            "best_speed_bps": max(prev_best, speed_bps),
            "last_speed_bps": speed_bps,
            "ewma_speed_bps": ewma,
            "samples": samples,
            "observed_typical": max(0, min(16, observed_typical)),
            "observed_peak": max(
                max(0, int(previous.get("observed_peak", 0))),
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
            "AI learned host=%s config=%sc sample=%.2fMB/s ewma=%.2fMB/s samples=%s best=%sc %.2fMB/s",
            host,
            configured,
            speed_bps / (1024 * 1024),
            ewma / (1024 * 1024),
            samples,
            current.runtime_best_connections or configured,
            current.runtime_best_speed_bps / (1024 * 1024),
        )

    def lower_after_error(self, host: str, current_connections: int) -> None:
        if not host or current_connections <= 1:
            return
        current = self._profiles.get(host, HostProfile())
        reduced = PerformanceOptimizer.previous_level(current_connections)
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
            "Reduced host safe ceiling after rate limit host=%s -> %s",
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
        return "real-transfer" if profile.runtime_best_speed_bps > 0 else profile.confidence

    def expected_speed(self, host: str, connections: int | None = None) -> int:
        profile = self.get(host)
        if connections is not None:
            stat = profile.config_stats.get(str(connections))
            if stat:
                value = int(
                    stat.get(
                        "ewma_speed_bps",
                        stat.get("last_speed_bps", 0),
                    )
                )
                if value > 0:
                    return value
            if (
                connections == profile.probe_best_connections
                and profile.probe_speed_bps > 0
            ):
                return profile.probe_speed_bps
        if profile.runtime_best_speed_bps > 0:
            return profile.runtime_best_speed_bps
        return max(0, profile.probe_speed_bps)

    def config_prior(self, host: str, connections: int) -> tuple[int, int, float]:
        profile = self.get(host)
        stat = profile.config_stats.get(str(connections), {})
        estimate = max(
            0,
            int(
                stat.get(
                    "ewma_speed_bps",
                    stat.get("last_speed_bps", 0),
                )
            ),
        )
        samples = max(0, int(stat.get("samples", 0)))
        updated = float(stat.get("updated_epoch", 0.0))
        if estimate <= 0 and connections == profile.probe_best_connections:
            estimate = max(0, profile.probe_speed_bps)
        return estimate, samples, updated

    def safe_connection_ceiling(self, host: str) -> int:
        return self._clamp(self.get(host).safe_connection_ceiling)

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
        return time.time() - profile.last_runtime_epoch <= max_age_seconds

    def _recompute_runtime_best(self, profile: HostProfile) -> None:
        candidates: list[tuple[int, int, int, int]] = []
        for key, stat in profile.config_stats.items():
            try:
                connections = int(key)
            except (TypeError, ValueError):
                continue
            if connections < 1 or connections > profile.safe_connection_ceiling:
                continue
            estimate = max(
                0,
                int(
                    stat.get(
                        "ewma_speed_bps",
                        stat.get("last_speed_bps", 0),
                    )
                ),
            )
            if estimate <= 0:
                continue
            candidates.append(
                (
                    connections,
                    estimate,
                    max(0, int(stat.get("observed_typical", 0))),
                    max(0, int(stat.get("observed_peak", 0))),
                )
            )

        if not candidates:
            profile.runtime_best_connections = 0
            profile.runtime_best_speed_bps = 0
            profile.runtime_best_observed_connections = 0
            return

        best_speed = max(item[1] for item in candidates)
        near_best = [item for item in candidates if item[1] >= best_speed * 0.98]
        winner = min(near_best, key=lambda item: item[0])
        profile.runtime_best_connections = winner[0]
        profile.runtime_best_speed_bps = winner[1]
        profile.runtime_best_observed_connections = winner[2]
        profile.runtime_peak_observed_connections = max(
            profile.runtime_peak_observed_connections,
            winner[3],
        )

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                host: {
                    "probe_best_connections": profile.probe_best_connections,
                    "probe_speed_bps": profile.probe_speed_bps,
                    "runtime_best_connections": profile.runtime_best_connections,
                    "runtime_best_speed_bps": profile.runtime_best_speed_bps,
                    "runtime_best_observed_connections": (
                        profile.runtime_best_observed_connections
                    ),
                    "runtime_peak_observed_connections": (
                        profile.runtime_peak_observed_connections
                    ),
                    "safe_connection_ceiling": profile.safe_connection_ceiling,
                    "rate_limit_risk": profile.rate_limit_risk,
                    "range_supported": profile.range_supported,
                    "last_probe_epoch": profile.last_probe_epoch,
                    "last_runtime_epoch": profile.last_runtime_epoch,
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
            logger.exception("Could not save host performance profiles")

    @staticmethod
    def _clamp(value: int | float | str) -> int:
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
    last_progress_at: float = field(default_factory=time.monotonic)
    last_completed_bytes: int = 0
    history: deque[tuple[float, int]] = field(
        default_factory=lambda: deque(maxlen=300)
    )
    peak_speed_bps: int = 0
    observed_connections: int = 0
    observed_history: deque[int] = field(
        default_factory=lambda: deque(maxlen=120)
    )
    observed_peak_connections: int = 0
    last_telemetry_log_at: float = 0.0

    phase: str = "monitoring"
    winner_connections: int = 0
    winner_reference_bps: int = 0
    degraded_since: float = 0.0
    last_explore_at: float = 0.0
    next_reexplore_at: float = 0.0
    last_learning_at: float = 0.0

    session_ewma: dict[int, int] = field(default_factory=dict)
    session_samples: dict[int, int] = field(default_factory=dict)
    session_last_test_at: dict[int, float] = field(default_factory=dict)

    round_kind: str = ""
    round_origin: int = 0
    round_baseline_bps: int = 0
    round_results: dict[int, int] = field(default_factory=dict)
    round_tested: set[int] = field(default_factory=set)
    round_test_limit: int = 0

    @property
    def observed_typical_connections(self) -> int:
        values = [value for value in self.observed_history if value > 0]
        if not values:
            return self.observed_connections
        return int(round(statistics.median(values)))


class PerformanceOptimizer:
    """Adaptive Intelligence V5.

    Uses online learning and an optimistic multi-armed-bandit policy over
    aria2 connection levels. It initially explores promising levels, exploits
    the best measured level, monitors for regime changes, and re-explores
    selectively when throughput degrades or the environment has been stable
    long enough to justify a re-check.
    """

    CONNECTION_LEVELS = (1, 2, 4, 8, 16)

    INITIAL_MONITOR_SECONDS = 18.0
    TEST_SECONDS = 15.0
    STALL_SECONDS = 14.0
    TELEMETRY_LOG_SECONDS = 10.0
    LEARN_INTERVAL_SECONDS = 45.0

    DEGRADE_RATIO = 0.72
    DEGRADE_HOLD_SECONDS = 25.0
    REEXPLORE_COOLDOWN_SECONDS = 75.0
    PERIODIC_RECHECK_SECONDS = 150.0
    RECHECK_TEST_LIMIT = 2
    SWITCH_MIN_IMPROVEMENT = 0.06

    MIN_REMAINING_FOR_INITIAL = 256 * 1024 * 1024
    MIN_REMAINING_FOR_RECHECK = 192 * 1024 * 1024
    MIN_REMAINING_FOR_NEXT_TEST = 128 * 1024 * 1024

    UCB_BONUS = 0.28
    UNSEEN_BONUS = 0.22
    STALE_PRIOR_SECONDS = 600.0

    def __init__(self) -> None:
        self.profiles = HostProfileStore()
        self.states: dict[str, TransferState] = {}

    def record_preflight(self, result: PreflightResult) -> None:
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
        return self.profiles.expected_speed(host, connections)

    def safe_connection_ceiling(self, host: str) -> int:
        return self.profiles.safe_connection_ceiling(host)

    def runtime_profile_fresh(
        self,
        host: str,
        max_age_seconds: float = 1800.0,
    ) -> bool:
        return self.profiles.runtime_profile_fresh(host, max_age_seconds)

    def configured_connections(self, gid: str) -> int:
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
        safe = self.profiles.safe_connection_ceiling(host)
        normalized = min(self.normalize_level(connections), safe)
        rate_risk = self.profiles.rate_limit_risk(host)
        now = time.monotonic()
        mode = f"ai-warmup:{normalized}c"
        if rate_risk:
            mode += f"/cap{safe}"

        self.states[gid] = TransferState(
            host=host,
            connections=normalized,
            mode=mode,
            expected_speed_bps=max(0, expected_speed_bps),
            safe_connection_ceiling=safe,
            rate_limit_risk=rate_risk,
            started_at=now,
            last_change_at=now,
            last_progress_at=now,
            next_reexplore_at=now + self.REEXPLORE_COOLDOWN_SECONDS,
            winner_connections=normalized,
        )
        logger.info(
            "Adaptive Intelligence V5 registered gid=%s host=%s config=%sc safe=%sc risk=%s source=%s confidence=%s expected=%.2fMB/s",
            gid,
            host,
            normalized,
            safe,
            rate_risk,
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
            self._reset_measurement_windows(state, now)
            state.history.append((now, completed))

        stable_10 = self._stable_speed(state.history, 10.0)
        stable_30 = self._stable_speed(state.history, 30.0)
        state.peak_speed_bps = max(
            state.peak_speed_bps,
            stable_10,
            stable_30,
        )

        self._log_telemetry(
            gid,
            state,
            now,
            max(0, speed_bps),
            stable_10,
            stable_30,
            same_host_active,
        )

        if status == "complete":
            learned = stable_30 or stable_10 or state.peak_speed_bps
            if learned > 0 and same_host_active <= 1:
                self._learn_sample(state, state.connections, learned, now)
            return self._decision(
                state, stable_10, stable_30, None, "complete"
            )

        if status == "error":
            return self._decision(
                state, stable_10, stable_30, None, "error"
            )

        if status != "active":
            return self._decision(
                state, stable_10, stable_30, None, state.mode
            )

        remaining = (
            max(0, total_bytes - completed)
            if total_bytes > 0
            else 0
        )

        budget = self._budget_ceiling(
            state.safe_connection_ceiling,
            same_host_active,
        )
        if same_host_active > 1:
            state.degraded_since = 0.0
            if state.connections > budget:
                old = state.connections
                state.connections = budget
                state.phase = "shared-host"
                state.mode = f"ai-shared:{old}->{budget}c"
                self._reset_measurement_windows(state, now)
                return self._decision(
                    state, 0, 0, budget, state.mode
                )
            state.phase = "shared-host"
            state.mode = f"ai-shared:{state.connections}c/budget{budget}"
            return self._decision(
                state, stable_10, stable_30, None, state.mode
            )

        if state.phase == "shared-host":
            state.phase = "monitoring"
            state.started_at = now
            state.mode = f"ai-solo-warmup:{state.connections}c"
            state.round_results.clear()
            state.round_tested.clear()
            self._reset_measurement_windows(state, now)
            return self._decision(state, 0, 0, None, state.mode)

        if (
            completed > 0
            and now - state.last_progress_at >= self.STALL_SECONDS
            and state.connections > 1
            and now - state.last_change_at >= self.STALL_SECONDS
        ):
            target = self.previous_level(state.connections)
            state.connections = target
            state.phase = "exploiting"
            state.mode = f"ai-stall-recover:{target}c"
            state.next_reexplore_at = now + self.REEXPLORE_COOLDOWN_SECONDS
            state.degraded_since = 0.0
            self._reset_measurement_windows(state, now)
            logger.warning(
                "AI stall recovery gid=%s host=%s -> %sc",
                gid,
                state.host,
                target,
            )
            return self._decision(state, 0, 0, target, state.mode)

        if state.phase == "monitoring":
            if (
                now - state.started_at < self.INITIAL_MONITOR_SECONDS
                or stable_10 <= 0
            ):
                return self._decision(
                    state, stable_10, stable_30, None, state.mode
                )
            sample = stable_30 or stable_10
            self._learn_sample(state, state.connections, sample, now)
            state.winner_connections = state.connections
            state.winner_reference_bps = sample
            state.expected_speed_bps = sample

            if remaining and remaining < self.MIN_REMAINING_FOR_INITIAL:
                return self._enter_exploit(
                    gid,
                    state,
                    now,
                    stable_10,
                    stable_30,
                    "short-file",
                )
            return self._begin_round(
                gid,
                state,
                now,
                baseline_bps=sample,
                kind="initial",
                remaining=remaining,
            )

        if state.phase == "testing":
            if (
                now - state.last_change_at < self.TEST_SECONDS
                or stable_10 <= 0
            ):
                return self._decision(
                    state, stable_10, stable_30, None, state.mode
                )

            sample = stable_30 or stable_10
            self._learn_sample(state, state.connections, sample, now)
            state.round_results[state.connections] = sample
            state.round_tested.add(state.connections)

            allowed_tests = state.round_test_limit
            tested_candidates = max(
                0,
                len(state.round_tested) - 1,
            )
            can_continue = (
                tested_candidates < allowed_tests
                and (
                    remaining == 0
                    or remaining >= self.MIN_REMAINING_FOR_NEXT_TEST
                )
            )

            if can_continue:
                candidate = self._choose_candidate(
                    state,
                    now,
                    exclude=state.round_tested,
                    kind=state.round_kind,
                )
                if candidate is not None:
                    return self._start_test(
                        gid, state, candidate, now
                    )

            return self._finish_round(
                gid,
                state,
                now,
                stable_10,
                stable_30,
            )

        if state.phase in {"exploiting", "locked"}:
            # "locked" is retained for compatibility with V4.1 rate-limit
            # terminal handling. A live transfer in normal conditions uses
            # exploiting and can re-explore.
            if state.phase == "locked":
                return self._decision(
                    state, stable_10, stable_30, None, state.mode
                )

            sample = stable_30 or stable_10
            if (
                sample > 0
                and now - state.last_learning_at >= self.LEARN_INTERVAL_SECONDS
            ):
                self._learn_sample(
                    state, state.connections, sample, now
                )

            self._update_winner_reference(state, stable_30 or stable_10)
            trigger = self._reexplore_trigger(
                state,
                now,
                stable_10,
                stable_30,
                remaining,
            )
            if trigger:
                baseline = stable_30 or stable_10
                if baseline > 0:
                    logger.info(
                        "AI re-explore trigger gid=%s host=%s reason=%s current=%sc stable30=%.2fMB/s reference=%.2fMB/s",
                        gid,
                        state.host,
                        trigger,
                        state.connections,
                        stable_30 / (1024 * 1024),
                        state.winner_reference_bps / (1024 * 1024),
                    )
                    return self._begin_round(
                        gid,
                        state,
                        now,
                        baseline_bps=baseline,
                        kind="recheck",
                        remaining=remaining,
                    )

            state.mode = (
                f"ai-exploit:{state.connections}c "
                f"ref{state.winner_reference_bps / (1024 * 1024):.2f}"
            )
            return self._decision(
                state, stable_10, stable_30, None, state.mode
            )

        return self._decision(
            state, stable_10, stable_30, None, state.mode
        )

    def change_failed(self, gid: str) -> None:
        state = self.states.get(gid)
        if not state:
            return
        now = time.monotonic()
        winner = self._best_known_connection(state)
        if winner:
            state.connections = winner
        state.phase = "exploiting"
        state.mode = f"ai-change-failed:{state.connections}c"
        state.next_reexplore_at = now + self.REEXPLORE_COOLDOWN_SECONDS
        state.degraded_since = 0.0
        self._reset_measurement_windows(state, now)
        logger.warning(
            "AI connection change failed gid=%s; cooldown at %sc",
            gid,
            state.connections,
        )

    def _begin_round(
        self,
        gid: str,
        state: TransferState,
        now: float,
        baseline_bps: int,
        kind: str,
        remaining: int,
    ) -> PerformanceDecision:
        state.round_kind = kind
        state.round_origin = state.connections
        state.round_baseline_bps = baseline_bps
        state.round_results = {state.connections: baseline_bps}
        state.round_tested = {state.connections}
        state.last_explore_at = now
        state.degraded_since = 0.0

        if kind == "initial":
            candidates = self._eligible_initial_candidates(state)
            state.round_test_limit = len(candidates)
        else:
            state.round_test_limit = min(
                self.RECHECK_TEST_LIMIT,
                len(self._eligible_levels(state)) - 1,
            )

        if state.round_test_limit <= 0:
            return self._enter_exploit(
                gid,
                state,
                now,
                0,
                0,
                f"{kind}-no-candidate",
            )

        candidate = self._choose_candidate(
            state,
            now,
            exclude=state.round_tested,
            kind=kind,
        )
        if candidate is None:
            return self._enter_exploit(
                gid,
                state,
                now,
                0,
                0,
                f"{kind}-no-candidate",
            )

        logger.info(
            "AI %s round start gid=%s host=%s origin=%sc baseline=%.2fMB/s candidate=%sc remaining=%.1fMB",
            kind,
            gid,
            state.host,
            state.round_origin,
            baseline_bps / (1024 * 1024),
            candidate,
            remaining / (1024 * 1024),
        )
        return self._start_test(gid, state, candidate, now)

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
        state.mode = f"ai-explore:{previous}->{target}c"
        self._reset_measurement_windows(state, now)
        logger.info(
            "AI explore gid=%s host=%s %sc -> %sc score=%.3f tested=%s",
            gid,
            state.host,
            previous,
            target,
            self._candidate_score(state, target, now),
            sorted(state.round_tested),
        )
        return self._decision(state, 0, 0, target, state.mode)

    def _finish_round(
        self,
        gid: str,
        state: TransferState,
        now: float,
        stable_10: int,
        stable_30: int,
    ) -> PerformanceDecision:
        if not state.round_results:
            return self._enter_exploit(
                gid, state, now, stable_10, stable_30, "empty-round"
            )

        best_speed = max(state.round_results.values())
        near_best = [
            (connections, speed)
            for connections, speed in state.round_results.items()
            if speed >= best_speed * 0.98
        ]
        raw_winner, winner_speed = min(
            near_best,
            key=lambda item: item[0],
        )

        origin_speed = max(
            1,
            state.round_results.get(
                state.round_origin,
                state.round_baseline_bps,
            ),
        )
        if (
            raw_winner != state.round_origin
            and winner_speed
            < origin_speed * (1.0 + self.SWITCH_MIN_IMPROVEMENT)
        ):
            winner = state.round_origin
            winner_speed = origin_speed
        else:
            winner = raw_winner

        current = state.connections
        state.winner_connections = winner
        state.winner_reference_bps = winner_speed
        state.expected_speed_bps = winner_speed
        state.phase = "exploiting"
        state.next_reexplore_at = now + self.REEXPLORE_COOLDOWN_SECONDS
        state.degraded_since = 0.0
        state.mode = (
            f"ai-winner:{winner}c "
            f"{winner_speed / (1024 * 1024):.2f}MB/s"
        )

        logger.info(
            "AI round winner gid=%s host=%s kind=%s winner=%sc %.2fMB/s origin=%sc %.2fMB/s results=%s",
            gid,
            state.host,
            state.round_kind,
            winner,
            winner_speed / (1024 * 1024),
            state.round_origin,
            origin_speed / (1024 * 1024),
            {
                key: round(value / (1024 * 1024), 2)
                for key, value in sorted(state.round_results.items())
            },
        )

        if winner != current:
            state.connections = winner
            self._reset_measurement_windows(state, now)
            return self._decision(
                state, 0, 0, winner, state.mode
            )

        return self._decision(
            state, stable_10, stable_30, None, state.mode
        )

    def _enter_exploit(
        self,
        gid: str,
        state: TransferState,
        now: float,
        stable_10: int,
        stable_30: int,
        reason: str,
    ) -> PerformanceDecision:
        state.phase = "exploiting"
        state.winner_connections = state.connections
        sample = stable_30 or stable_10 or state.expected_speed_bps
        if sample > 0:
            state.winner_reference_bps = sample
            state.expected_speed_bps = sample
        state.next_reexplore_at = now + self.REEXPLORE_COOLDOWN_SECONDS
        state.mode = f"ai-exploit:{state.connections}c/{reason}"
        logger.info(
            "AI exploit gid=%s host=%s config=%sc reason=%s reference=%.2fMB/s",
            gid,
            state.host,
            state.connections,
            reason,
            state.winner_reference_bps / (1024 * 1024),
        )
        return self._decision(
            state, stable_10, stable_30, None, state.mode
        )

    def _reexplore_trigger(
        self,
        state: TransferState,
        now: float,
        stable_10: int,
        stable_30: int,
        remaining: int,
    ) -> str:
        if (
            remaining
            and remaining < self.MIN_REMAINING_FOR_RECHECK
        ):
            state.degraded_since = 0.0
            return ""

        if now < state.next_reexplore_at:
            return ""

        reference = max(
            0,
            state.winner_reference_bps,
            state.expected_speed_bps,
        )
        current = stable_30 or stable_10

        if reference > 0 and current > 0:
            if current < reference * self.DEGRADE_RATIO:
                if state.degraded_since <= 0:
                    state.degraded_since = now
                elif now - state.degraded_since >= self.DEGRADE_HOLD_SECONDS:
                    return "degraded"
            else:
                state.degraded_since = 0.0

        if (
            state.last_explore_at > 0
            and now - state.last_explore_at >= self.PERIODIC_RECHECK_SECONDS
        ):
            return "periodic"

        return ""

    def _update_winner_reference(
        self,
        state: TransferState,
        current_speed: int,
    ) -> None:
        if current_speed <= 0:
            return
        if state.winner_reference_bps <= 0:
            state.winner_reference_bps = current_speed
            state.expected_speed_bps = current_speed
            return

        # Adapt up quickly to newly available capacity, down only slowly.
        if current_speed > state.winner_reference_bps:
            alpha = 0.05
        else:
            # observe() normally runs every second; decay slowly enough that a
            # sustained drop can still trigger regime-change detection.
            alpha = 0.001
        state.winner_reference_bps = int(
            state.winner_reference_bps * (1.0 - alpha)
            + current_speed * alpha
        )
        state.expected_speed_bps = state.winner_reference_bps

    def _learn_sample(
        self,
        state: TransferState,
        connections: int,
        speed_bps: int,
        now: float,
    ) -> None:
        if speed_bps <= 0:
            return
        previous = state.session_ewma.get(connections, 0)
        state.session_ewma[connections] = (
            speed_bps
            if previous <= 0
            else int(previous * 0.65 + speed_bps * 0.35)
        )
        state.session_samples[connections] = (
            state.session_samples.get(connections, 0) + 1
        )
        state.session_last_test_at[connections] = now
        state.last_learning_at = now
        self.profiles.record_runtime_sample(
            host=state.host,
            configured_connections=connections,
            observed_typical=state.observed_typical_connections,
            observed_peak=state.observed_peak_connections,
            speed_bps=speed_bps,
        )

    def _choose_candidate(
        self,
        state: TransferState,
        now: float,
        exclude: set[int],
        kind: str,
    ) -> int | None:
        if kind == "initial":
            eligible = self._eligible_initial_candidates(state)
        else:
            eligible = [
                level
                for level in self._eligible_levels(state)
                if level != state.connections
            ]
        candidates = [level for level in eligible if level not in exclude]
        if not candidates:
            return None

        scored = [
            (self._candidate_score(state, level, now), level)
            for level in candidates
        ]
        scored.sort(key=lambda item: (item[0], -item[1]), reverse=True)
        return scored[0][1]

    def _candidate_score(
        self,
        state: TransferState,
        connections: int,
        now: float,
    ) -> float:
        current_ref = max(
            state.winner_reference_bps,
            state.round_baseline_bps,
            state.expected_speed_bps,
            1,
        )
        session = state.session_ewma.get(connections, 0)
        session_samples = state.session_samples.get(connections, 0)
        prior, prior_samples, updated_epoch = self.profiles.config_prior(
            state.host, connections
        )

        if session > 0:
            estimate = session
            samples = max(1, session_samples)
        elif prior > 0:
            estimate = prior
            samples = max(1, prior_samples)
        else:
            estimate = int(current_ref * 0.85)
            samples = 0

        normalized = estimate / current_ref
        uncertainty = self.UCB_BONUS / math.sqrt(samples + 1.0)
        if samples == 0:
            uncertainty += self.UNSEEN_BONUS

        if updated_epoch > 0:
            age = max(0.0, time.time() - updated_epoch)
            uncertainty += min(0.12, age / self.STALE_PRIOR_SECONDS * 0.04)

        distance = abs(
            math.log2(max(1, connections))
            - math.log2(max(1, state.connections))
        )
        reconnect_penalty = distance * 0.025

        return normalized + uncertainty - reconnect_penalty

    def _eligible_initial_candidates(
        self,
        state: TransferState,
    ) -> list[int]:
        levels = self._eligible_levels(state)
        origin = state.round_origin or state.connections
        current_estimate = max(
            state.round_baseline_bps,
            state.winner_reference_bps,
            state.expected_speed_bps,
            1,
        )
        result: list[int] = []
        for level in levels:
            if level == origin:
                continue
            if level > origin:
                result.append(level)
                continue
            prior, _, _ = self.profiles.config_prior(state.host, level)
            if (
                state.rate_limit_risk
                or (prior > 0 and prior >= current_estimate * 0.85)
            ):
                result.append(level)
        return result

    def _eligible_levels(self, state: TransferState) -> list[int]:
        return [
            level
            for level in self.CONNECTION_LEVELS
            if level <= state.safe_connection_ceiling
        ]

    def _best_known_connection(
        self,
        state: TransferState,
    ) -> int | None:
        candidates: dict[int, int] = dict(state.session_ewma)
        profile = self.profiles.get(state.host)
        for key, stat in profile.config_stats.items():
            try:
                connection = int(key)
            except (TypeError, ValueError):
                continue
            if connection > state.safe_connection_ceiling:
                continue
            estimate = max(
                0,
                int(
                    stat.get(
                        "ewma_speed_bps",
                        stat.get("last_speed_bps", 0),
                    )
                ),
            )
            candidates[connection] = max(
                candidates.get(connection, 0),
                estimate,
            )
        if not candidates:
            return state.connections
        best = max(candidates.values())
        near = [
            connection
            for connection, speed in candidates.items()
            if speed >= best * 0.98
        ]
        return min(near) if near else state.connections

    @classmethod
    def _budget_ceiling(
        cls,
        host_ceiling: int,
        active_transfers: int,
    ) -> int:
        if active_transfers <= 1:
            return cls.normalize_floor_level(host_ceiling)
        raw = max(1, host_ceiling // active_transfers)
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
        if (
            state.observed_connections
            and observed_connections != state.observed_connections
        ):
            logger.info(
                "aria2 active connections gid=%s host=%s %s->%s configured=%s",
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
            "AI telemetry gid=%s host=%s instant=%.2fMB/s stable10=%.2fMB/s stable30=%.2fMB/s ref=%.2fMB/s active=%s typical=%s config=%s safe=%s phase=%s next_recheck=%.0fs mode=%s",
            gid,
            state.host,
            speed_bps / (1024 * 1024),
            stable_10 / (1024 * 1024),
            stable_30 / (1024 * 1024),
            state.winner_reference_bps / (1024 * 1024),
            state.observed_connections or 0,
            state.observed_typical_connections,
            state.connections,
            state.safe_connection_ceiling,
            state.phase,
            max(0.0, state.next_reexplore_at - now),
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
            observed_connections=state.observed_connections,
            safe_connection_ceiling=state.safe_connection_ceiling,
            rate_limit_risk=state.rate_limit_risk,
        )

    @classmethod
    def normalize_level(cls, value: int) -> int:
        return min(
            cls.CONNECTION_LEVELS,
            key=lambda item: abs(item - value),
        )

    @classmethod
    def normalize_floor_level(cls, value: int) -> int:
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
    def previous_level(cls, current: int) -> int:
        lower = [
            level
            for level in cls.CONNECTION_LEVELS
            if level < current
        ]
        return lower[-1] if lower else 1
