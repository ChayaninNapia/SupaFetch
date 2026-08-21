from __future__ import annotations

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from supafetch.core.download_manager import DownloadManager
from supafetch.models.download import Download
from supafetch.utils.formatters import format_bytes, format_speed


class MainWindow(QMainWindow):
    def __init__(self, manager: DownloadManager) -> None:
        super().__init__()
        self.manager = manager
        self.setWindowTitle("SupaFetch")
        self.resize(900, 520)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["File", "Progress", "Downloaded", "Speed", "Status", "GID"]
        )
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
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
        item = self.table.item(row, 5)
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
        values = [
            download.name,
            f"{download.progress:.1f}%",
            f"{format_bytes(download.completed_bytes)} / {format_bytes(download.total_bytes)}",
            format_speed(download.speed_bps),
            download.status,
            download.gid,
        ]
        for column, value in enumerate(values):
            self.table.setItem(row, column, QTableWidgetItem(value))
