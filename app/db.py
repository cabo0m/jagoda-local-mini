import sqlite3
from pathlib import Path

from app import db_migrations

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "jagoda_memory.db"


def get_connection():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    db_migrations.apply_all_migrations(conn)
    conn.commit()
    return conn


def init_db():
    conn = get_connection()
    conn.commit()
    conn.close()
