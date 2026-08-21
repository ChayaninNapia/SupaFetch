from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class Download:
    gid: str
    name: str
    status: str
    total_bytes: int
    completed_bytes: int
    speed_bps: int
    error_message: str = ""

    @property
    def progress(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return min(100.0, (self.completed_bytes / self.total_bytes) * 100)

    @classmethod
    def from_aria2(cls, payload: dict[str, Any]) -> "Download":
        files = payload.get("files") or []
        path = files[0].get("path", "") if files else ""
        name = Path(path).name if path else payload.get("gid", "Unknown")
        return cls(
            gid=payload.get("gid", ""),
            name=name,
            status=payload.get("status", "unknown"),
            total_bytes=int(payload.get("totalLength", 0)),
            completed_bytes=int(payload.get("completedLength", 0)),
            speed_bps=int(payload.get("downloadSpeed", 0)),
            error_message=payload.get("errorMessage", ""),
        )
