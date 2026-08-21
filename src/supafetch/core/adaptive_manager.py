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

        # V4/V4.1 profiles remain useful as low-weight priors, but the first
        # transfer after each V5 app start must collect fresh preflight
        # evidence. This prevents a historical bad/outlier profile from
        # bypassing the corrected V4.1 probe. Once V5 learns a real transfer
        # in this process, last_runtime_epoch is refreshed and cache reuse
        # works normally for following downloads.
        for profile in self.optimizer.profiles._profiles.values():
            profile.last_runtime_epoch = 0.0
