from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from supafetch.core.aria2_client import Aria2Client
from supafetch.models.download import Download


class DownloadManager:
    def __init__(self, client: Aria2Client) -> None:
        self.client = client
        self._gids: list[str] = []

    def add_download(self, url: str, directory: str | None = None) -> str:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Please enter a valid HTTP or HTTPS URL.")

        if directory:
            directory = str(Path(directory).expanduser().resolve())

        gid = self.client.add_uri(url, directory)
        if gid not in self._gids:
            self._gids.append(gid)
        return gid

    def pause(self, gid: str) -> None:
        self.client.pause(gid)

    def resume(self, gid: str) -> None:
        self.client.resume(gid)

    def remove(self, gid: str) -> None:
        self.client.remove(gid)
        if gid in self._gids:
            self._gids.remove(gid)

    def list_downloads(self) -> list[Download]:
        downloads: list[Download] = []
        missing: list[str] = []
        for gid in list(self._gids):
            try:
                downloads.append(Download.from_aria2(self.client.tell_status(gid)))
            except Exception:
                missing.append(gid)

        for gid in missing:
            if gid in self._gids:
                self._gids.remove(gid)
        return downloads
