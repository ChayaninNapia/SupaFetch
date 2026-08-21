from __future__ import annotations

from supafetch.core.adaptive_optimizer import PerformanceOptimizer
from supafetch.core.download_manager import DownloadManager as BaseDownloadManager


class DownloadManager(BaseDownloadManager):
    """V5 manager using the online-learning optimizer.

    All mature V4.1 download/RPC/error handling stays in the base manager;
    only the performance controller is replaced. This keeps V5 easy to
    rollback and prevents the adaptive experiment from duplicating download
    lifecycle code.
    """

    def __init__(self, client) -> None:
        super().__init__(client)
        self.optimizer = PerformanceOptimizer()
