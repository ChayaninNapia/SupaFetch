from __future__ import annotations

import logging
import os
import sys

from PySide6.QtWidgets import QApplication, QMessageBox

from supafetch.core.aria2_client import Aria2Client
from supafetch.core.aria2_service import Aria2Service
from supafetch.core.download_manager import DownloadManager
from supafetch.ui.main_window import MainWindow


def configure_logging() -> None:
    requested = os.getenv(
        "SUPAFETCH_LOG_LEVEL",
        "INFO",
    ).upper()
    level = getattr(
        logging,
        requested,
        logging.INFO,
    )
    logging.basicConfig(
        level=level,
        format=(
            "%(asctime)s | %(levelname)-8s | "
            "%(name)s | %(message)s"
        ),
        datefmt="%H:%M:%S",
    )
    logging.getLogger("urllib3").setLevel(
        logging.WARNING
    )


def main() -> int:
    configure_logging()
    logger = logging.getLogger("supafetch")
    logger.info("Starting SupaFetch")

    app = QApplication(sys.argv)
    app.setApplicationName("SupaFetch")

    service = Aria2Service()
    try:
        service.start()
    except Exception as exc:
        logger.exception(
            "Failed to start aria2 service"
        )
        QMessageBox.critical(
            None,
            "SupaFetch",
            str(exc),
        )
        return 1

    client = Aria2Client(
        service.rpc_url,
        service.secret,
    )
    manager = DownloadManager(client)
    window = MainWindow(manager)
    window.show()

    exit_code = app.exec()
    logger.info("Stopping SupaFetch")
    client.close()
    service.stop()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
