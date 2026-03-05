import csv
import os
import sqlite3
import sys
from pathlib import Path
from typing import List

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Qt, Signal, Slot, QTimer
from PySide6.QtGui import QAction
from PySide6.QtSql import QSqlDatabase, QSqlTableModel
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QComboBox,
    QStatusBar,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from log_parser import parse_line


APP_NAME = "LogLens"
LEVELS = ["All", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "(none)"]


def app_data_dir() -> Path:
    base = Path.home() / f".{APP_NAME.lower()}"
    base.mkdir(parents=True, exist_ok=True)
    return base


def db_path() -> Path:
    return app_data_dir() / "loglens.db"


def init_sqlite(db_file: Path) -> None:
    with sqlite3.connect(db_file) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entries (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT,
              level TEXT,
              source TEXT,
              message TEXT,
              raw TEXT,
              file TEXT,
              line_no INTEGER
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_level ON entries(level)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_ts ON entries(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_file ON entries(file)")
        conn.commit()


def clear_sqlite(db_file: Path) -> None:
    with sqlite3.connect(db_file) as conn:
        conn.execute("DELETE FROM entries")
        conn.execute("VACUUM")
        conn.commit()


def escape_like(s: str) -> str:
    # Escape quotes for SQL string literal; LIKE wildcard escaping is out-of-scope for MVP
    return s.replace("'", "''")


class WorkerSignals(QObject):
    progress = Signal(int)          # processed lines
    finished = Signal(int, int)     # total_lines, inserted
    error = Signal(str)


class ParseWorker(QRunnable):
    def __init__(self, db_file: Path, files: List[str]):
        super().__init__()
        self.db_file = db_file
        self.files = files
        self.signals = WorkerSignals()

    @Slot()
    def run(self):
        try:
            init_sqlite(self.db_file)

            total_lines = 0
            inserted = 0

            with sqlite3.connect(self.db_file) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA cache_size=20000")

                cur = conn.cursor()
                cur.execute("BEGIN")

                batch = 0
                for fp in self.files:
                    # store just filename in db for nicer UI
                    fname = os.path.basename(fp)

                    with open(fp, "r", encoding="utf-8", errors="replace") as f:
                        for line_no, line in enumerate(f, start=1):
                            total_lines += 1
                            pl = parse_line(line)

                            cur.execute(
                                """
                                INSERT INTO entries (ts, level, source, message, raw, file, line_no)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """,
                                (pl.ts, pl.level, pl.source, pl.message, pl.raw, fname, line_no),
                            )
                            inserted += 1
                            batch += 1

                            if batch >= 1500:
                                conn.commit()
                                cur.execute("BEGIN")
                                batch = 0
                                self.signals.progress.emit(total_lines)

                conn.commit()

            self.signals.finished.emit(total_lines, inserted)

        except Exception as e:
            self.signals.error.emit(str(e))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} — Log Analyzer")
        self.resize(1100, 700)

        self.db_file = db_path()
        init_sqlite(self.db_file)

        self.thread_pool = QThreadPool.globalInstance()

        # --- UI ---
        self.level_combo = QComboBox()
        self.level_combo.addItems(LEVELS)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search message/source/raw…")

        self.open_btn = QPushButton("Open Logs…")
        self.clear_btn = QPushButton("Clear")
        self.export_btn = QPushButton("Export CSV…")

        self.count_label = QLabel("0 rows")
        self.count_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        # table
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSortingEnabled(True)

        # status
        self.status = QStatusBar()
        self.setStatusBar(self.status)

        # layout
        top = QHBoxLayout()
        top.addWidget(QLabel("Level:"))
        top.addWidget(self.level_combo)
        top.addSpacing(10)
        top.addWidget(QLabel("Search:"))
        top.addWidget(self.search_edit, 1)
        top.addSpacing(10)
        top.addWidget(self.open_btn)
        top.addWidget(self.export_btn)
        top.addWidget(self.clear_btn)
        top.addSpacing(10)
        top.addWidget(self.count_label)

        root = QVBoxLayout()
        root.addLayout(top)
        root.addWidget(self.table)

        container = QWidget()
        container.setLayout(root)
        self.setCentralWidget(container)

        # menu (optional but nice)
        file_menu = self.menuBar().addMenu("&File")
        act_open = QAction("Open Logs…", self)
        act_export = QAction("Export CSV…", self)
        act_quit = QAction("Quit", self)
        file_menu.addAction(act_open)
        file_menu.addAction(act_export)
        file_menu.addSeparator()
        file_menu.addAction(act_quit)
        act_open.triggered.connect(self.pick_files)
        act_export.triggered.connect(self.export_csv)
        act_quit.triggered.connect(self.close)

        # db model
        self.qdb = self._open_qt_db()
        self.model = QSqlTableModel(self, self.qdb)
        self.model.setTable("entries")
        self.model.setEditStrategy(QSqlTableModel.OnManualSubmit)
        self.model.select()

        # nicer headers
        self.model.setHeaderData(0, Qt.Horizontal, "ID")
        self.model.setHeaderData(1, Qt.Horizontal, "Timestamp")
        self.model.setHeaderData(2, Qt.Horizontal, "Level")
        self.model.setHeaderData(3, Qt.Horizontal, "Source")
        self.model.setHeaderData(4, Qt.Horizontal, "Message")
        self.model.setHeaderData(5, Qt.Horizontal, "Raw")
        self.model.setHeaderData(6, Qt.Horizontal, "File")
        self.model.setHeaderData(7, Qt.Horizontal, "Line")

        self.table.setModel(self.model)
        self.table.setColumnHidden(0, True)  # hide ID
        self.table.setColumnHidden(5, True)  # hide raw by default (unhide if you want)

        self.table.resizeColumnsToContents()
        self.table.horizontalHeader().setStretchLastSection(True)

        # debounce filter updates
        self._filter_timer = QTimer(self)
        self._filter_timer.setInterval(150)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.timeout.connect(self.apply_filter)

        # wiring
        self.open_btn.clicked.connect(self.pick_files)
        self.clear_btn.clicked.connect(self.clear_db)
        self.export_btn.clicked.connect(self.export_csv)
        self.level_combo.currentIndexChanged.connect(lambda: self._filter_timer.start())
        self.search_edit.textChanged.connect(lambda: self._filter_timer.start())

        self.refresh_counts()

    def _open_qt_db(self) -> QSqlDatabase:
        # Give the connection a stable name; if re-opening, close first.
        conn_name = "loglens_conn"
        if QSqlDatabase.contains(conn_name):
            db = QSqlDatabase.database(conn_name)
            if db.isOpen():
                db.close()
            QSqlDatabase.removeDatabase(conn_name)

        db = QSqlDatabase.addDatabase("QSQLITE", conn_name)
        db.setDatabaseName(str(self.db_file))
        if not db.open():
            raise RuntimeError(f"Failed to open DB: {db.lastError().text()}")
        return db

    def current_filter_sql(self) -> str:
        parts = []

        level = self.level_combo.currentText()
        if level != "All":
            if level == "(none)":
                parts.append("(level IS NULL OR level = '')")
            else:
                parts.append(f"(level = '{escape_like(level)}')")

        q = self.search_edit.text().strip()
        if q:
            qq = escape_like(q)
            parts.append(
                "("
                f"message LIKE '%{qq}%' OR "
                f"source LIKE '%{qq}%' OR "
                f"raw LIKE '%{qq}%' OR "
                f"file LIKE '%{qq}%'"
                ")"
            )

        return " AND ".join(parts)

    @Slot()
    def apply_filter(self):
        flt = self.current_filter_sql()
        self.model.setFilter(flt)
        self.model.select()
        self.refresh_counts()

    def refresh_counts(self):
        # Row count from model; good enough for MVP
        rows = self.model.rowCount()
        self.count_label.setText(f"{rows:,} rows")

    @Slot()
    def pick_files(self):
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Select log file(s)",
            str(Path.home()),
            "Log files (*.log *.txt *.out *.json *.ndjson);;All files (*.*)",
        )
        if not files:
            return
        self.parse_files(files)

    def parse_files(self, files: List[str]):
        self.status.showMessage("Parsing logs…")
        self.open_btn.setEnabled(False)
        self.export_btn.setEnabled(False)
        self.clear_btn.setEnabled(False)

        # append into existing DB (MVP). If you want “new session” each time, call clear_db() first.
        worker = ParseWorker(self.db_file, files)
        worker.signals.progress.connect(self.on_progress)
        worker.signals.finished.connect(self.on_finished)
        worker.signals.error.connect(self.on_error)
        self.thread_pool.start(worker)

    @Slot(int)
    def on_progress(self, lines):
        self.status.showMessage(f"Parsing… {lines:,} lines processed")

    @Slot(int, int)
    def on_finished(self, total_lines, inserted):
        self.status.showMessage(f"Done. {inserted:,} lines inserted ({total_lines:,} processed).")
        self.open_btn.setEnabled(True)
        self.export_btn.setEnabled(True)
        self.clear_btn.setEnabled(True)

        # Refresh Qt model
        self.model.select()
        self.refresh_counts()

    @Slot(str)
    def on_error(self, msg):
        self.open_btn.setEnabled(True)
        self.export_btn.setEnabled(True)
        self.clear_btn.setEnabled(True)
        self.status.clearMessage()
        QMessageBox.critical(self, "Parse Error", msg)

    @Slot()
    def clear_db(self):
        if QMessageBox.question(self, "Clear", "Delete all loaded log entries?") != QMessageBox.Yes:
            return
        clear_sqlite(self.db_file)
        self.model.select()
        self.refresh_counts()
        self.status.showMessage("Cleared.", 2500)

    @Slot()
    def export_csv(self):
        out_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export filtered results to CSV",
            str(Path.home() / "log_export.csv"),
            "CSV (*.csv)",
        )
        if not out_path:
            return

        where = self.current_filter_sql()
        sql = """
          SELECT ts, level, source, message, file, line_no
          FROM entries
        """
        if where:
            sql += " WHERE " + where
        sql += " ORDER BY id ASC"

        try:
            with sqlite3.connect(self.db_file) as conn, open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["timestamp", "level", "source", "message", "file", "line_no"])
                for row in conn.execute(sql):
                    w.writerow(row)
            self.status.showMessage(f"Exported to {out_path}", 4000)
        except Exception as e:
            QMessageBox.critical(self, "Export Error", str(e))


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()