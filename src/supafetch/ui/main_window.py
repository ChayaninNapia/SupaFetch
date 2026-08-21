from __future__ import annotations

import logging

from PySide6.QtCore import QObject, QThread, QTimer, Signal, Slot
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from supafetch.core.download_manager import DownloadManager
from supafetch.models.download import Download
from supafetch.utils.formatters import format_bytes, format_duration, format_speed


logger = logging.getLogger(__name__)


class BackgroundWorker(QObject):
    refreshed = Signal(object)
    refresh_failed = Signal(str)
    added = Signal(str)
    add_failed = Signal(str)

    def __init__(self, manager: DownloadManager) -> None:
        super().__init__()
        self.manager = manager

    @Slot()
    def refresh(self) -> None:
        try:
            self.refreshed.emit(self.manager.list_downloads())
        except Exception as exc:
            logger.exception("Background download refresh failed")
            self.refresh_failed.emit(str(exc))

    @Slot(str, str)
    def add_download(self, url: str, directory: str) -> None:
        try:
            gid = self.manager.add_download(url, directory or None)
            self.added.emit(gid)
        except Exception as exc:
            logger.exception("Background add download failed")
            self.add_failed.emit(str(exc))


class MainWindow(QMainWindow):
    refresh_requested = Signal()
    add_requested = Signal(str, str)

    def __init__(self, manager: DownloadManager) -> None:
        super().__init__()
        self.manager = manager
        self.setWindowTitle("SupaFetch")
        self.resize(1180, 540)

        self.table = QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels(
            [
                "File",
                "Progress",
                "Downloaded",
                "Speed",
                "Avg Speed",
                "Conn",
                "ETA",
                "Optimizer",
                "Status",
                "GID",
            ]
        )
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(9, QHeaderView.ResizeToContents)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)

        self.add_button = QPushButton("Add URL")
        pause_button = QPushButton("Pause")
        resume_button = QPushButton("Resume")
        remove_button = QPushButton("Remove")

        self.add_button.clicked.connect(self.add_download)
        pause_button.clicked.connect(self.pause_selected)
        resume_button.clicked.connect(self.resume_selected)
        remove_button.clicked.connect(self.remove_selected)

        controls = QHBoxLayout()
        controls.addWidget(self.add_button)
        controls.addWidget(pause_button)
        controls.addWidget(resume_button)
        controls.addWidget(remove_button)
        controls.addStretch()

        layout = QVBoxLayout()
        layout.addLayout(controls)
        layout.addWidget(self.table)

        central = QWidget()
        central.setLayout(layout)
        self.setCentralWidget(central)

        self._refresh_in_flight = False
        self._add_in_flight = False
        self._worker_thread = QThread(self)
        self._worker = BackgroundWorker(self.manager)
        self._worker.moveToThread(self._worker_thread)
        self.refresh_requested.connect(self._worker.refresh)
        self.add_requested.connect(self._worker.add_download)
        self._worker.refreshed.connect(self._on_refresh_ready)
        self._worker.refresh_failed.connect(self._on_refresh_failed)
        self._worker.added.connect(self._on_add_ready)
        self._worker.add_failed.connect(self._on_add_failed)
        self._worker_thread.start()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.request_refresh)
        self.timer.start(1000)
        QTimer.singleShot(0, self.request_refresh)

    def add_download(self) -> None:
        if self._add_in_flight:
            return

        url, accepted = QInputDialog.getText(self, "Add download", "Download URL:")
        if not accepted or not url.strip():
            return

        directory = QFileDialog.getExistingDirectory(self, "Choose download folder")
        self._add_in_flight = True
        self.add_button.setEnabled(False)
        self.add_button.setText("Probing...")
        self.statusBar().showMessage("Benchmarking the server before download...")
        self.add_requested.emit(url.strip(), directory or "")

    def selected_gid(self) -> str | None:
        row = self.table.currentRow()
        if row < 0:
            return None
        item = self.table.item(row, 9)
        return item.text() if item else None

    def pause_selected(self) -> None:
        gid = self.selected_gid()
        if gid:
            self._run_action(lambda: self.manager.pause(gid))

    def resume_selected(self) -> None:
        gid = self.selected_gid()
        if gid:
            self._run_action(lambda: self.manager.resume(gid))

    def remove_selected(self) -> None:
        gid = self.selected_gid()
        if gid:
            self._run_action(lambda: self.manager.remove(gid))

    def _run_action(self, action) -> None:
        try:
            action()
            self.request_refresh()
        except Exception as exc:
            QMessageBox.warning(self, "SupaFetch", str(exc))

    @Slot()
    def request_refresh(self) -> None:
        # The add operation and preflight probe run on the same worker. Do not
        # queue refresh calls behind it every second.
        if (
            self._refresh_in_flight
            or self._add_in_flight
            or not self._worker_thread.isRunning()
        ):
            return
        self._refresh_in_flight = True
        self.refresh_requested.emit()

    @Slot(object)
    def _on_refresh_ready(self, downloads: object) -> None:
        self._refresh_in_flight = False
        items = downloads if isinstance(downloads, list) else []
        self.table.setRowCount(len(items))
        for row, download in enumerate(items):
            if isinstance(download, Download):
                self._render_download(row, download)

    @Slot(str)
    def _on_refresh_failed(self, message: str) -> None:
        self._refresh_in_flight = False
        logger.warning("Refresh worker reported an error: %s", message)

    @Slot(str)
    def _on_add_ready(self, gid: str) -> None:
        self._add_in_flight = False
        self.add_button.setEnabled(True)
        self.add_button.setText("Add URL")
        self.statusBar().showMessage(f"Download started: {gid}", 5000)
        self.request_refresh()

    @Slot(str)
    def _on_add_failed(self, message: str) -> None:
        self._add_in_flight = False
        self.add_button.setEnabled(True)
        self.add_button.setText("Add URL")
        self.statusBar().clearMessage()
        QMessageBox.critical(self, "Could not add download", message)

    def _render_download(self, row: int, download: Download) -> None:
        progress = QProgressBar()
        progress.setRange(0, 1000)
        progress.setValue(round(download.progress * 10))
        progress.setFormat(f"{download.progress:.1f}%")
        progress.setTextVisible(True)
        progress.setStyleSheet(
            "QProgressBar {"
            "  border: 1px solid #b8b8b8;"
            "  border-radius: 4px;"
            "  text-align: center;"
            "  background: #f2f2f2;"
            "}"
            "QProgressBar::chunk {"
            "  background-color: #2eaf55;"
            "  border-radius: 3px;"
            "}"
        )
        self.table.setCellWidget(row, 1, progress)

        eta = self._format_eta(download)
        values = {
            0: download.name,
            2: f"{format_bytes(download.completed_bytes)} / {format_bytes(download.total_bytes)}",
            3: format_speed(download.speed_bps),
            4: format_speed(download.average_speed_bps),
            5: str(download.connections),
            6: eta,
            7: download.adaptive_mode,
            8: download.status,
            9: download.gid,
        }
        for column, value in values.items():
            self.table.setItem(row, column, QTableWidgetItem(value))

    @staticmethod
    def _format_eta(download: Download) -> str:
        if download.status == "complete":
            return "Done"
        if download.status == "paused":
            return "Paused"

        eta_speed = download.average_speed_bps or download.speed_bps
        if eta_speed <= 0 or download.total_bytes <= 0:
            return "--"

        remaining_bytes = max(0, download.total_bytes - download.completed_bytes)
        return format_duration(remaining_bytes / eta_speed)

    def closeEvent(self, event: QCloseEvent) -> None:
        self.timer.stop()
        self._worker_thread.quit()
        if not self._worker_thread.wait(15000):
            logger.warning("Background worker did not stop before shutdown timeout")
        super().closeEvent(event)
