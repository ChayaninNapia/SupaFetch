from __future__ import annotations

from PySide6.QtCore import QTimer
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


class MainWindow(QMainWindow):
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
                "Adaptive",
                "Status",
                "GID",
            ]
        )
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(9, QHeaderView.ResizeToContents)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)

        add_button = QPushButton("Add URL")
        pause_button = QPushButton("Pause")
        resume_button = QPushButton("Resume")
        remove_button = QPushButton("Remove")

        add_button.clicked.connect(self.add_download)
        pause_button.clicked.connect(self.pause_selected)
        resume_button.clicked.connect(self.resume_selected)
        remove_button.clicked.connect(self.remove_selected)

        controls = QHBoxLayout()
        controls.addWidget(add_button)
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

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.refresh)
        self.timer.start(750)

    def add_download(self) -> None:
        url, accepted = QInputDialog.getText(self, "Add download", "Download URL:")
        if not accepted or not url.strip():
            return

        directory = QFileDialog.getExistingDirectory(self, "Choose download folder")
        if not directory:
            directory = None

        try:
            self.manager.add_download(url.strip(), directory)
            self.refresh()
        except Exception as exc:
            QMessageBox.critical(self, "Could not add download", str(exc))

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
            self.refresh()
        except Exception as exc:
            QMessageBox.warning(self, "SupaFetch", str(exc))

    def refresh(self) -> None:
        downloads = self.manager.list_downloads()
        self.table.setRowCount(len(downloads))
        for row, download in enumerate(downloads):
            self._render_download(row, download)

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
