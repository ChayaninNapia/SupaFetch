# SupaFetch

SupaFetch is a desktop download manager built with Python, PySide6, and aria2.

## Architecture

- **PySide6**: desktop user interface
- **Python**: application logic, state, and orchestration
- **aria2 JSON-RPC**: download engine control
- **aria2c**: HTTP/HTTPS download engine with resume and multi-connection support

## Current MVP

- Add a direct HTTP/HTTPS download URL
- Start downloads through aria2
- Display file name, progress, speed, and status
- Pause and resume downloads
- Remove downloads from the current aria2 session

## Requirements

- Python 3.11+
- aria2 (`aria2c`) installed and available in `PATH`

## Setup

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -e .
```

Run SupaFetch:

```bash
supafetch
```

or:

```bash
python -m supafetch.main
```

SupaFetch starts a local aria2 RPC process automatically on `127.0.0.1:6800`.

## Branch workflow

- `main` — stable branch
- `dev` — active development branch

Feature work should normally branch from `dev` and merge back into `dev` before a release is promoted to `main`.

## Project structure

```text
SupaFetch/
├── src/
│   └── supafetch/
│       ├── main.py
│       ├── core/
│       │   ├── aria2_client.py
│       │   ├── aria2_service.py
│       │   └── download_manager.py
│       ├── models/
│       │   └── download.py
│       ├── ui/
│       │   └── main_window.py
│       └── utils/
│           └── formatters.py
├── pyproject.toml
├── .gitignore
└── README.md
```
