from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from supafetch.core.preflight_probe import PreflightResult


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HostProfile:
    best_connections: int = 2
    best_speed_bps: int = 0
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
                self._profiles[host] = HostProfile(
                    best_connections=max(1, min(16, int(values.get("best_connections", 2)))),
                    best_speed_bps=max(0, int(values.get("best_speed_bps", 0))),
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
        self._profiles[result.host] = HostProfile(
            best_connections=result.best_connections,
            best_speed_bps=result.peak_speed_bps,
            range_supported=result.range_supported,
            last_probe_epoch=time.time(),
            confidence=result.confidence,
        )
        self._save()
        logger.info(
            "Saved preflight profile host=%s best_connections=%s peak=%sB/s range=%s confidence=%s",
            result.host,
            result.best_connections,
            result.peak_speed_bps,
            result.range_supported,
            result.confidence,
        )

    def update_actual_speed(self, host: str, connections: int, speed_bps: int) -> None:
        if not host or speed_bps <= 0:
            return
        current = self._profiles.get(host)
        if current is None:
            self._profiles[host] = HostProfile(
                best_connections=connections,
                best_speed_bps=speed_bps,
                range_supported=True,
                last_probe_epoch=0.0,
                confidence="real-transfer",
            )
            self._save()
            return

        if speed_bps > current.best_speed_bps:
            current.best_speed_bps = speed_bps
            current.best_connections = connections
            self._save()
            logger.info(
                "Updated host peak from real transfer host=%s connections=%s speed=%sB/s",
                host,
                connections,
                speed_bps,
            )

    def lower_after_error(self, host: str, current_connections: int) -> None:
        if not host or current_connections <= 1:
            return
        current = self._profiles.get(host, HostProfile())
        reduced = PerformanceOptimizer.previous_level(current_connections)
        current.best_connections = reduced
        current.confidence = "degraded"
        self._profiles[host] = current
        self._save()
        logger.info("Reduced learned host connections after error host=%s -> %s", host, reduced)

    def fallback_connections(self, host: str) -> int:
        profile = self.get(host)
        if not profile.range_supported:
            return 1
        return max(1, min(16, profile.best_connections or 2))

    def fallback_confidence(self, host: str) -> str:
        return self.get(host).confidence

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                host: {
                    "best_connections": profile.best_connections,
                    "best_speed_bps": profile.best_speed_bps,
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
class TransferState:
    host: str
    connections: int
    mode: str
    samples: deque[int] = field(default_factory=lambda: deque(maxlen=12))
    zero_samples: int = 0
    fallback_applied: bool = False
    best_observed_speed_bps: int = 0

    @property
    def rolling_average_bps(self) -> int:
        nonzero = [sample for sample in self.samples if sample > 0]
        if not nonzero:
            return 0
        return int(sum(nonzero) / len(nonzero))


class PerformanceOptimizer:
    """Track transfers after the preflight benchmark.

    Phase 2 avoids increasing concurrency during an active transfer. The only
    mid-download connection change is a conservative fallback after a sustained
    stall.
    """

    CONNECTION_LEVELS = (1, 2, 4, 8, 16)
    STALL_SAMPLES_BEFORE_FALLBACK = 6

    def __init__(self) -> None:
        self.profiles = HostProfileStore()
        self.states: dict[str, TransferState] = {}

    def record_preflight(self, result: PreflightResult) -> None:
        self.profiles.record_preflight(result)

    def fallback_connections(self, host: str) -> int:
        return self.profiles.fallback_connections(host)

    def fallback_confidence(self, host: str) -> str:
        return self.profiles.fallback_confidence(host)

    def register(
        self,
        gid: str,
        host: str,
        connections: int,
        source: str,
        confidence: str = "",
    ) -> None:
        normalized = self.normalize_level(connections)
        suffix = f" ({confidence.title()})" if confidence else ""
        mode = f"{source}:{normalized}c{suffix}"
        self.states[gid] = TransferState(
            host=host,
            connections=normalized,
            mode=mode,
        )
        logger.info(
            "Performance tracker registered gid=%s host=%s connections=%s source=%s confidence=%s",
            gid,
            host,
            normalized,
            source,
            confidence or "n/a",
        )

    def remove(self, gid: str) -> None:
        self.states.pop(gid, None)

    def observe(
        self,
        gid: str,
        status: str,
        speed_bps: int,
        completed_bytes: int,
    ) -> tuple[int, int | None, str]:
        state = self.states.get(gid)
        if not state:
            return max(0, speed_bps), None, "off"

        speed = max(0, speed_bps)
        state.samples.append(speed)
        if speed > 0:
            state.zero_samples = 0
            state.best_observed_speed_bps = max(state.best_observed_speed_bps, speed)
        elif status == "active" and completed_bytes > 0:
            state.zero_samples += 1

        average = state.rolling_average_bps

        if status == "complete":
            actual_peak = max(state.best_observed_speed_bps, average)
            self.profiles.update_actual_speed(
                state.host,
                state.connections,
                actual_peak,
            )
            return average, None, "complete"

        if status == "error":
            self.profiles.lower_after_error(state.host, state.connections)
            return average, None, "error"

        if status != "active":
            return average, None, state.mode

        if (
            not state.fallback_applied
            and state.zero_samples >= self.STALL_SAMPLES_BEFORE_FALLBACK
            and state.connections > 1
        ):
            target = self.previous_level(state.connections)
            logger.warning(
                "Transfer stall fallback gid=%s host=%s %s -> %s connections",
                gid,
                state.host,
                state.connections,
                target,
            )
            state.connections = target
            state.mode = f"fallback:{target}c"
            state.fallback_applied = True
            state.samples.clear()
            state.zero_samples = 0
            return average, target, state.mode

        return average, None, state.mode

    def change_failed(self, gid: str) -> None:
        state = self.states.get(gid)
        if not state:
            return
        state.mode = "fallback-failed"
        state.fallback_applied = True
        logger.warning("Fallback option change failed gid=%s", gid)

    @classmethod
    def normalize_level(cls, value: int) -> int:
        return min(cls.CONNECTION_LEVELS, key=lambda item: abs(item - value))

    @classmethod
    def previous_level(cls, current: int) -> int:
        lower = [level for level in cls.CONNECTION_LEVELS if level < current]
        return lower[-1] if lower else 1
