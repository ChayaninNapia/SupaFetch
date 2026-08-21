from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication, QMessageBox

from supafetch.core.aria2_client import Aria2Client
from supafetch.core.aria2_service import Aria2Service
from supafetch.core.download_manager import DownloadManager
from supafetch.ui.main_window import MainWindow


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("SupaFetch")

    service = Aria2Service()
    try:
        service.start()
    except Exception as exc:
        QMessageBox.critical(None, "SupaFetch", str(exc))
        return 1

    client = Aria2Client(service.rpc_url, service.secret)
    manager = DownloadManager(client)
    window = MainWindow(manager)
    window.show()

    exit_code = app.exec()
    service.stop()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
