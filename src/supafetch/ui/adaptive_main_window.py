from __future__ import annotations

from supafetch.ui.main_window import MainWindow as BaseMainWindow


class MainWindow(BaseMainWindow):
    """V5 window marker so the active runtime is obvious at a glance."""

    def __init__(self, manager) -> None:
        super().__init__(manager)
        self.setWindowTitle("SupaFetch V5 - Adaptive Intelligence")
