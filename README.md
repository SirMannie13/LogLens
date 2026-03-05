# LogLens — Desktop Log Analyzer (PySide6 + SQLite)

A lightweight desktop log analyzer that parses log files into a local SQLite database for fast filtering, searching, and exporting.

![LogLens Screenshot](assets/screenshot.png)

## Features
- Load one or more `.log/.txt/.json/.ndjson` files
- Parses common timestamp + level formats (and JSON logs)
- Filter by log level and search across message/source/raw/file
- Sortable table view
- Export filtered results to CSV
- **Double-click any row** to view the full raw log line (with copy)

## Tech Stack
- Python
- PySide6 (Qt UI)
- SQLite (local storage)

## Run Locally
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
# source .venv/bin/activate

pip install -r requirements.txt
python main.py