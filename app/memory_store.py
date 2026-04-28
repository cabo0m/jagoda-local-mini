from __future__ import annotations

import json
import mimetypes
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app import db_migrations, memory_config as config


def _resolve_path(value: str | os.PathLike[str] | Path) -> Path:
    return Path(value).resolve(strict=False)


def get_allowed_roots() -> list[Path]:
    raw_roots = getattr(config, "ALLOWED_ROOTS", None)

    if raw_roots is None:
        root = getattr(config, "ROOT", None)
        if root is None:
            raise AttributeError("Konfiguracja musi zawierać ROOT albo ALLOWED_ROOTS")
        raw_roots = [root]
    elif isinstance(raw_roots, (str, os.PathLike)):
        raw_roots = [raw_roots]

    roots: list[Path] = []
    seen: set[str] = set()

    for raw_root in raw_roots:
        if raw_root is None:
            continue

        resolved = _resolve_path(raw_root)
        key = os.path.normcase(str(resolved))

        if key not in seen:
            seen.add(key)
            roots.append(resolved)

    if not roots:
        raise ValueError("Brak dozwolonych katalogów w konfiguracji")

    return roots


def _is_within(base: Path, target: Path) -> bool:
    try:
        common = os.path.commonpath([str(base), str(target)])
    except ValueError:
        return False

    return os.path.normcase(common) == os.path.normcase(str(base))


def safe_path(user_path: str | None) -> Path:
    raw = (user_path or ".").strip()
    candidate = Path(raw)
    root = _resolve_path(config.ROOT)

    target = _resolve_path(candidate) if candidate.is_absolute() else _resolve_path(root / candidate)

    allowed_roots = get_allowed_roots()
    for allowed_root in allowed_roots:
        if _is_within(allowed_root, target):
            return target

    allowed = ", ".join(str(root) for root in allowed_roots)
    raise ValueError(f"Ścieżka jest poza dozwolonymi katalogami: {allowed}")


def rel_path(path: Path) -> str:
    target = _resolve_path(path)
    root = _resolve_path(config.ROOT)

    if target == root:
        return "."

    if _is_within(root, target):
        return str(target.relative_to(root))

    for allowed_root in get_allowed_roots():
        if _is_within(allowed_root, target):
            return str(target)

    allowed = ", ".join(str(root) for root in get_allowed_roots())
    raise ValueError(f"Ścieżka jest poza dozwolonymi katalogami: {allowed}")


def guess_mime(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def normalize_score(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_db_connection() -> sqlite3.Connection:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    db_migrations.apply_all_migrations(conn)
    conn.commit()
    return conn


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in existing:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def ensure_memory_schema(conn: sqlite3.Connection) -> None:
    db_migrations.apply_all_migrations(conn)
    conn.commit()


def parse_params_json(params_json: str) -> Any:
    raw = (params_json or "").strip()
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"params_json nie jest poprawnym JSON-em: {exc}") from exc
    if not isinstance(value, (list, dict)):
        raise ValueError("params_json musi być listą albo obiektem JSON")
    return value


def is_read_only_sql(query: str) -> bool:
    return query.lstrip().lower().startswith(("select", "with", "pragma", "explain"))


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def require_memory_row(conn: sqlite3.Connection, memory_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
    if not row:
        raise FileNotFoundError(f"Nie znaleziono wspomnienia o id={memory_id}")
    return row


def require_sleep_run_row(conn: sqlite3.Connection, run_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM sleep_runs WHERE id = ?", (run_id,)).fetchone()
    if not row:
        raise FileNotFoundError(f"Nie znaleziono sleep_run o id={run_id}")
    return row


def create_sleep_run(
    conn: sqlite3.Connection,
    mode: str,
    freedom_level: int,
    notes: str | None = None,
    rollback_of_run_id: int | None = None,
    workspace_id: int | None = None,
    project_key: str | None = None,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO sleep_runs (started_at, status, mode, freedom_level, notes, rollback_of_run_id, workspace_id, project_key)
        VALUES (?, 'started', ?, ?, ?, ?, ?, ?)
        """,
        (utc_now_iso(), mode, int(freedom_level), notes, rollback_of_run_id, workspace_id, project_key),
    )
    conn.commit()
    return int(cursor.lastrowid)


def add_sleep_action(conn: sqlite3.Connection, run_id: int, action_type: str, memory_id: int | None, old_value: Any, new_value: Any, reason: str) -> None:
    conn.execute(
        """
        INSERT INTO sleep_run_actions (run_id, memory_id, action_type, old_value, new_value, reason, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            memory_id,
            action_type,
            None if old_value is None else json.dumps(old_value, ensure_ascii=False),
            None if new_value is None else json.dumps(new_value, ensure_ascii=False),
            reason,
            utc_now_iso(),
        ),
    )


def decode_action_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return value
    return value


def finalize_sleep_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    scanned_count: int,
    changed_count: int,
    archived_count: int,
    downgraded_count: int,
    duplicate_count: int,
    conflict_count: int = 0,
    created_summary_count: int = 0,
) -> None:
    conn.execute(
        """
        UPDATE sleep_runs
        SET
            finished_at = ?,
            status = ?,
            scanned_count = ?,
            changed_count = ?,
            archived_count = ?,
            downgraded_count = ?,
            duplicate_count = ?,
            conflict_count = ?,
            created_summary_count = ?
        WHERE id = ?
        """,
        (
            utc_now_iso(),
            status,
            scanned_count,
            changed_count,
            archived_count,
            downgraded_count,
            duplicate_count,
            conflict_count,
            created_summary_count,
            run_id,
        ),
    )
    conn.commit()
