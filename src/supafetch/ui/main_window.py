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
from supafetch.utils.formatters import (
    format_bytes,
    format_duration,
    format_speed,
)


logger = logging.getLogger(__name__)


class StatusWorker(QObject):
    refreshed = Signal(object)
    failed = Signal(str)

    def __init__(self, manager: DownloadManager) -> None:
        super().__init__()
        self.manager = manager

    @Slot()
    def refresh(self) -> None:
        try:
            self.refreshed.emit(self.manager.list_downloads())
        except Exception as exc:
            logger.exception("Background status refresh failed")
            self.failed.emit(str(exc))


class AddWorker(QObject):
    completed = Signal(str)
    failed = Signal(str)

    def __init__(self, manager: DownloadManager) -> None:
        super().__init__()
        self.manager = manager

    @Slot(str, str)
    def add_download(self, url: str, directory: str) -> None:
        try:
            gid = self.manager.add_download(url, directory or None)
            self.completed.emit(gid)
        except Exception as exc:
            logger.exception("Background add download failed")
            self.failed.emit(str(exc))


class ControlWorker(QObject):
    completed = Signal(str, str)
    failed = Signal(str, str)

    def __init__(self, manager: DownloadManager) -> None:
        super().__init__()
        self.manager = manager

    @Slot(str)
    def pause(self, gid: str) -> None:
        self._run("pause", gid, self.manager.pause)

    @Slot(str)
    def resume(self, gid: str) -> None:
        self._run("resume", gid, self.manager.resume)

    @Slot(str)
    def remove(self, gid: str) -> None:
        self._run("remove", gid, self.manager.remove)

    def _run(self, name: str, gid: str, action) -> None:
        try:
            action(gid)
            self.completed.emit(name, gid)
        except Exception as exc:
            logger.exception("Background %s failed gid=%s", name, gid)
            self.failed.emit(name, str(exc))


class MainWindow(QMainWindow):
    refresh_requested = Signal()
    add_requested = Signal(str, str)
    pause_requested = Signal(str)
    resume_requested = Signal(str)
    remove_requested = Signal(str)

    def __init__(self, manager: DownloadManager) -> None:
        super().__init__()
        self.manager = manager
        self.setWindowTitle("SupaFetch")
        self.resize(1500, 560)

        self.table = QTableWidget(0, 13)
        self.table.setHorizontalHeaderLabels(
            [
                "File",
                "Progress",
                "Downloaded",
                "Speed",
                "Stable 10s",
                "Stable 30s",
                "Peak",
                "Expected",
                "Active / Config",
                "ETA",
                "Optimizer",
                "Status",
                "GID",
            ]
        )
        self.table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.Stretch
        )
        self.table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeToContents
        )
        self.table.horizontalHeader().setSectionResizeMode(
            12, QHeaderView.ResizeToContents
        )
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

        self._status_thread = QThread(self)
        self._status_worker = StatusWorker(self.manager)
        self._status_worker.moveToThread(self._status_thread)
        self.refresh_requested.connect(self._status_worker.refresh)
        self._status_worker.refreshed.connect(self._on_refresh_ready)
        self._status_worker.failed.connect(self._on_refresh_failed)
        self._status_thread.start()

        self._add_thread = QThread(self)
        self._add_worker = AddWorker(self.manager)
        self._add_worker.moveToThread(self._add_thread)
        self.add_requested.connect(self._add_worker.add_download)
        self._add_worker.completed.connect(self._on_add_completed)
        self._add_worker.failed.connect(self._on_add_failed)
        self._add_thread.start()

        self._control_thread = QThread(self)
        self._control_worker = ControlWorker(self.manager)
        self._control_worker.moveToThread(self._control_thread)
        self.pause_requested.connect(self._control_worker.pause)
        self.resume_requested.connect(self._control_worker.resume)
        self.remove_requested.connect(self._control_worker.remove)
        self._control_worker.completed.connect(self._on_control_completed)
        self._control_worker.failed.connect(self._on_control_failed)
        self._control_thread.start()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.request_refresh)
        self.timer.start(1000)
        QTimer.singleShot(0, self.request_refresh)

    def add_download(self) -> None:
        if self._add_in_flight:
            return

        url, accepted = QInputDialog.getText(
            self, "Add download", "Download URL:"
        )
        if not accepted or not url.strip():
            return

        directory = QFileDialog.getExistingDirectory(
            self, "Choose download folder"
        )
        self._add_in_flight = True
        self.add_button.setEnabled(False)
        self.add_button.setText("Preparing...")
        self.statusBar().showMessage(
            "Selecting the fastest download strategy..."
        )
        self.add_requested.emit(url.strip(), directory or "")

    def selected_gid(self) -> str | None:
        row = self.table.currentRow()
        if row < 0:
            return None
        item = self.table.item(row, 12)
        return item.text() if item else None

    def pause_selected(self) -> None:
        gid = self.selected_gid()
        if gid:
            self.pause_requested.emit(gid)

    def resume_selected(self) -> None:
        gid = self.selected_gid()
        if gid:
            self.resume_requested.emit(gid)

    def remove_selected(self) -> None:
        gid = self.selected_gid()
        if gid:
            self.remove_requested.emit(gid)

    @Slot()
    def request_refresh(self) -> None:
        if self._refresh_in_flight or not self._status_thread.isRunning():
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
    def _on_add_completed(self, gid: str) -> None:
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

    @Slot(str, str)
    def _on_control_completed(self, action: str, gid: str) -> None:
        self.statusBar().showMessage(f"{action.title()} completed", 2500)
        self.request_refresh()

    @Slot(str, str)
    def _on_control_failed(self, action: str, message: str) -> None:
        QMessageBox.warning(self, f"Could not {action}", message)

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
        configured = download.configured_connections or download.connections
        connection_text = f"{download.connections} / {configured}"
        if download.rate_limit_risk:
            connection_text += f" (cap {download.safe_connection_ceiling})"

        values = {
            0: download.name,
            2: (
                f"{format_bytes(download.completed_bytes)} "
                f"/ {format_bytes(download.total_bytes)}"
            ),
            3: format_speed(download.speed_bps),
            4: format_speed(download.stable_10_bps),
            5: format_speed(download.stable_30_bps),
            6: format_speed(download.peak_speed_bps),
            7: format_speed(download.expected_speed_bps),
            8: connection_text,
            9: eta,
            10: download.adaptive_mode,
            11: download.status,
            12: download.gid,
        }
        for column, value in values.items():
            self.table.setItem(row, column, QTableWidgetItem(value))

    @staticmethod
    def _format_eta(download: Download) -> str:
        if download.status == "complete":
            return "Done"
        if download.status == "paused":
            return "Paused"

        eta_speed = (
            download.stable_30_bps
            or download.stable_10_bps
            or download.speed_bps
        )
        if eta_speed <= 0 or download.total_bytes <= 0:
            return "--"

        remaining_bytes = max(
            0, download.total_bytes - download.completed_bytes
        )
        return format_duration(remaining_bytes / eta_speed)

    def closeEvent(self, event: QCloseEvent) -> None:
        self.timer.stop()

        for name, thread, timeout in (
            ("Status", self._status_thread, 10000),
            ("Add", self._add_thread, 15000),
            ("Control", self._control_thread, 10000),
        ):
            thread.quit()
            if not thread.wait(timeout):
                logger.warning("%s worker did not stop before timeout", name)

        super().closeEvent(event)
