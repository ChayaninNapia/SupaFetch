from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HostProfile:
    best_connections: int = 2
    best_speed_bps: int = 0


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
                self._profiles[host] = HostProfile(
                    best_connections=max(2, min(16, int(values.get("best_connections", 2)))),
                    best_speed_bps=max(0, int(values.get("best_speed_bps", 0))),
                )
            except Exception:
                logger.warning("Ignoring invalid host profile for %s", host)

    def get(self, host: str) -> HostProfile:
        return self._profiles.get(host, HostProfile())

    def update(self, host: str, connections: int, speed_bps: int) -> None:
        if not host or speed_bps <= 0:
            return
        current = self._profiles.get(host)
        if current and current.best_speed_bps >= speed_bps:
            return
        self._profiles[host] = HostProfile(connections, speed_bps)
        self._save()
        logger.info(
            "Learned host profile host=%s best_connections=%s best_speed=%sB/s",
            host,
            connections,
            speed_bps,
        )

    def lower_after_error(self, host: str, current_connections: int) -> None:
        if not host or current_connections <= 2:
            return
        reduced = max(2, current_connections // 2)
        current = self._profiles.get(host)
        speed = current.best_speed_bps if current else 0
        self._profiles[host] = HostProfile(reduced, speed)
        self._save()
        logger.info("Reduced learned host connections after error host=%s -> %s", host, reduced)

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                host: {
                    "best_connections": profile.best_connections,
                    "best_speed_bps": profile.best_speed_bps,
                }
                for host, profile in self._profiles.items()
            }
            self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            logger.exception("Could not save host performance profiles")


@dataclass(slots=True)
class AdaptiveState:
    host: str
    connections: int
    samples: deque[int] = field(default_factory=lambda: deque(maxlen=8))
    phase: str = "warming"
    baseline_speed_bps: int = 0
    previous_connections: int = 2
    best_connections: int = 2
    best_speed_bps: int = 0
    last_decision_at: float = field(default_factory=time.monotonic)
    zero_samples: int = 0

    @property
    def rolling_average_bps(self) -> int:
        if not self.samples:
            return 0
        return int(sum(self.samples) / len(self.samples))


class PerformanceOptimizer:
    CONNECTION_LEVELS = (2, 4, 8, 16)
    WARMUP_SECONDS = 5.0
    EVALUATION_SECONDS = 6.0
    MIN_IMPROVEMENT = 0.10
    DEGRADE_THRESHOLD = 0.92

    def __init__(self) -> None:
        self.profiles = HostProfileStore()
        self.states: dict[str, AdaptiveState] = {}

    def initial_connections(self, host: str) -> int:
        profile = self.profiles.get(host)
        if profile.best_speed_bps > 0:
            return profile.best_connections
        return 2

    def register(self, gid: str, host: str, initial_connections: int) -> None:
        initial = self._normalize_level(initial_connections)
        self.states[gid] = AdaptiveState(
            host=host,
            connections=initial,
            previous_connections=initial,
            best_connections=initial,
        )
        logger.info("Adaptive optimizer registered gid=%s host=%s start_connections=%s", gid, host, initial)

    def remove(self, gid: str) -> None:
        self.states.pop(gid, None)

    def observe(
        self,
        gid: str,
        status: str,
        speed_bps: int,
        total_bytes: int,
        completed_bytes: int,
    ) -> tuple[int, int | None, str]:
        state = self.states.get(gid)
        if not state:
            return speed_bps, None, "off"

        speed = max(0, speed_bps)
        state.samples.append(speed)
        state.zero_samples = state.zero_samples + 1 if speed == 0 else 0
        average = state.rolling_average_bps

        if status == "complete":
            if average > state.best_speed_bps:
                state.best_speed_bps = average
                state.best_connections = state.connections
            self.profiles.update(state.host, state.best_connections, state.best_speed_bps)
            return average, None, "learned"

        if status == "error":
            self.profiles.lower_after_error(state.host, state.connections)
            return average, None, "error"

        if status != "active":
            return average, None, state.phase

        # Tiny files finish too quickly to benefit from probing.
        remaining = max(0, total_bytes - completed_bytes)
        if total_bytes and total_bytes < 32 * 1024 * 1024:
            return average, None, "small-file"
        if remaining and remaining < 16 * 1024 * 1024:
            return average, None, "finishing"

        now = time.monotonic()
        elapsed = now - state.last_decision_at

        # If a previously healthy transfer stalls at higher concurrency, back off.
        if state.zero_samples >= 4 and state.connections > 2:
            target = self._previous_level(state.connections)
            self._prepare_change(state, target, "fallback")
            logger.warning("Adaptive stall fallback gid=%s %s -> %s", gid, state.connections, target)
            return average, target, "fallback"

        if len(state.samples) < 5 or elapsed < self.WARMUP_SECONDS:
            return average, None, state.phase

        if state.phase in {"warming", "stable", "settling"}:
            state.baseline_speed_bps = max(1, average)
            if average > state.best_speed_bps:
                state.best_speed_bps = average
                state.best_connections = state.connections

            ceiling = self._connection_ceiling(total_bytes)
            target = self._next_level(state.connections, ceiling)
            if target == state.connections:
                state.phase = "stable"
                self.profiles.update(state.host, state.best_connections, state.best_speed_bps)
                return average, None, "stable"

            state.previous_connections = state.connections
            state.connections = target
            state.samples.clear()
            state.phase = "probing"
            state.last_decision_at = now
            logger.info(
                "Adaptive probe gid=%s host=%s %s -> %s baseline=%sB/s",
                gid,
                state.host,
                state.previous_connections,
                target,
                state.baseline_speed_bps,
            )
            return average, target, "probing"

        if state.phase == "probing" and elapsed >= self.EVALUATION_SECONDS and len(state.samples) >= 5:
            candidate_speed = max(1, average)
            ratio = candidate_speed / max(1, state.baseline_speed_bps)
            if ratio >= 1.0 + self.MIN_IMPROVEMENT:
                state.best_speed_bps = max(state.best_speed_bps, candidate_speed)
                state.best_connections = state.connections
                state.phase = "stable"
                state.last_decision_at = now
                state.samples.clear()
                logger.info(
                    "Adaptive accepted gid=%s connections=%s speed=%sB/s improvement=%.1f%%",
                    gid,
                    state.connections,
                    candidate_speed,
                    (ratio - 1.0) * 100,
                )
                self.profiles.update(state.host, state.best_connections, state.best_speed_bps)
                return candidate_speed, None, "accepted"

            rollback = state.previous_connections
            logger.info(
                "Adaptive rollback gid=%s %s -> %s candidate=%sB/s baseline=%sB/s improvement=%.1f%%",
                gid,
                state.connections,
                rollback,
                candidate_speed,
                state.baseline_speed_bps,
                (ratio - 1.0) * 100,
            )
            state.connections = rollback
            state.phase = "settling"
            state.last_decision_at = now
            state.samples.clear()
            self.profiles.update(state.host, state.best_connections, state.best_speed_bps)
            return candidate_speed, rollback, "rollback"

        return average, None, state.phase

    def change_failed(self, gid: str) -> None:
        state = self.states.get(gid)
        if not state:
            return
        logger.warning("Adaptive option change failed gid=%s; disabling further probing for this transfer", gid)
        state.connections = state.previous_connections
        state.phase = "stable"
        state.last_decision_at = time.monotonic()
        state.samples.clear()

    @classmethod
    def _connection_ceiling(cls, total_bytes: int) -> int:
        if total_bytes <= 0:
            return 8
        if total_bytes < 100 * 1024 * 1024:
            return 2
        if total_bytes < 1024 * 1024 * 1024:
            return 8
        return 16

    @classmethod
    def _normalize_level(cls, value: int) -> int:
        return min(cls.CONNECTION_LEVELS, key=lambda item: abs(item - value))

    @classmethod
    def _next_level(cls, current: int, ceiling: int) -> int:
        allowed = [level for level in cls.CONNECTION_LEVELS if level <= ceiling]
        for level in allowed:
            if level > current:
                return level
        return current

    @classmethod
    def _previous_level(cls, current: int) -> int:
        lower = [level for level in cls.CONNECTION_LEVELS if level < current]
        return lower[-1] if lower else 2

    @staticmethod
    def _prepare_change(state: AdaptiveState, target: int, phase: str) -> None:
        state.previous_connections = state.connections
        state.connections = target
        state.phase = phase
        state.samples.clear()
        state.last_decision_at = time.monotonic()
