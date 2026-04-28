from __future__ import annotations

import html
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _safe_count(conn: sqlite3.Connection, table_name: str, where_sql: str = "", params: tuple[object, ...] = ()) -> int | None:
    if not _table_exists(conn, table_name):
        return None
    sql = f"SELECT COUNT(*) FROM {table_name}"
    if where_sql:
        sql += " WHERE " + where_sql
    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row is not None else 0


def _latest_backup_snapshot(app_dir: Path) -> dict[str, object]:
    base = app_dir / "backups" / "vps_runtime"
    if not base.exists():
        return {
            "exists": False,
            "base_dir": str(base),
            "latest_dir": None,
            "latest_archive": None,
        }
    dirs = sorted(
        [item for item in base.glob("vps_runtime_*") if item.is_dir()],
        key=lambda item: item.name,
        reverse=True,
    )
    archives = sorted(
        [item for item in base.glob("vps_runtime_*.tar.gz") if item.is_file()],
        key=lambda item: item.name,
        reverse=True,
    )
    latest_dir = dirs[0] if dirs else None
    latest_archive = archives[0] if archives else None
    return {
        "exists": True,
        "base_dir": str(base),
        "latest_dir": str(latest_dir) if latest_dir else None,
        "latest_archive": str(latest_archive) if latest_archive else None,
        "latest_archive_size": latest_archive.stat().st_size if latest_archive else None,
        "latest_manifest_exists": bool(latest_dir and (latest_dir / "MANIFEST.txt").exists()),
    }


def build_public_health_payload(
    *,
    app_dir: Path,
    db_path: Path,
    public_base_url: str,
    started_at: datetime,
    security_audit_log_path: Path,
    invite_codes_configured: bool,
    allow_uninvited_oauth: bool,
    oauth_ram_cache_count: int,
) -> dict[str, object]:
    uptime_seconds = round(time.time() - started_at.timestamp(), 3)
    db_exists = db_path.exists()
    audit_exists = security_audit_log_path.exists()

    checks: dict[str, object] = {
        "app_dir_exists": app_dir.exists(),
        "db_exists": db_exists,
        "audit_log_configured": bool(str(security_audit_log_path)),
        "public_base_url_configured": bool(public_base_url),
        "uninvited_oauth_disabled": not allow_uninvited_oauth,
    }
    db: dict[str, object] = {
        "path": str(db_path),
        "exists": db_exists,
        "size": db_path.stat().st_size if db_exists else None,
    }
    invites: dict[str, object] = {
        "db_enabled": False,
        "total": None,
        "active_or_used": None,
        "revoked": None,
        "expired": None,
        "legacy_env_configured": invite_codes_configured,
    }
    oauth_tokens: dict[str, object] = {
        "table_exists": False,
        "stored_total": None,
        "stored_active": None,
        "stored_expired": None,
        "ram_cache_count": oauth_ram_cache_count,
    }

    if db_exists:
        conn = sqlite3.connect(str(db_path))
        try:
            now_epoch = int(time.time())
            db["tables"] = {
                "memories": _table_exists(conn, "memories"),
                "mpbm_invites": _table_exists(conn, "mpbm_invites"),
                "mpbm_oauth_access_tokens": _table_exists(conn, "mpbm_oauth_access_tokens"),
            }
            db["memory_count"] = _safe_count(conn, "memories")
            if _table_exists(conn, "mpbm_invites"):
                invites["db_enabled"] = True
                invites["total"] = _safe_count(conn, "mpbm_invites")
                invites["revoked"] = _safe_count(conn, "mpbm_invites", "revoked_at IS NOT NULL")
                invites["expired"] = _safe_count(conn, "mpbm_invites", "expires_at IS NOT NULL AND expires_at <= ?", (now_epoch,))
                invites["active_or_used"] = _safe_count(
                    conn,
                    "mpbm_invites",
                    "revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ?)",
                    (now_epoch,),
                )
            if _table_exists(conn, "mpbm_oauth_access_tokens"):
                oauth_tokens["table_exists"] = True
                oauth_tokens["stored_total"] = _safe_count(conn, "mpbm_oauth_access_tokens")
                oauth_tokens["stored_active"] = _safe_count(
                    conn,
                    "mpbm_oauth_access_tokens",
                    "revoked_at IS NULL AND expires_at > ?",
                    (now_epoch,),
                )
                oauth_tokens["stored_expired"] = _safe_count(conn, "mpbm_oauth_access_tokens", "expires_at <= ?", (now_epoch,))
        finally:
            conn.close()

    checks["invite_db_enabled"] = bool(invites["db_enabled"])
    checks["oauth_token_store_ready"] = bool(oauth_tokens["table_exists"])
    checks["audit_log_exists"] = audit_exists
    backup = _latest_backup_snapshot(app_dir)
    checks["latest_vps_backup_exists"] = bool(backup.get("latest_archive"))

    status = "ok" if all(bool(value) for value in checks.values()) else "degraded"
    return {
        "status": status,
        "service": "MPbM public health",
        "checked_at": _utc_now(),
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
        "uptime_seconds": uptime_seconds,
        "public_base_url": public_base_url,
        "mcp_path": "/mcp/",
        "checks": checks,
        "db": db,
        "invites": invites,
        "oauth_tokens": oauth_tokens,
        "audit_log": {
            "path": str(security_audit_log_path),
            "exists": audit_exists,
            "size": security_audit_log_path.stat().st_size if audit_exists else None,
        },
        "backup": backup,
        "notes": [
            "This endpoint is redacted: it never returns bearer tokens, raw invite codes, auth codes, or env contents.",
            "Use /health for minimal liveness and /mpbm-health for operator-readable public health.",
        ],
    }


def render_public_health_html(payload: dict[str, object]) -> str:
    status = html.escape(str(payload.get("status", "unknown")))
    checks = payload.get("checks", {}) if isinstance(payload.get("checks"), dict) else {}
    rows = []
    for key, value in sorted(checks.items()):
        mark = "OK" if value else "WARN"
        rows.append(f"<tr><td>{html.escape(str(key))}</td><td>{html.escape(mark)}</td></tr>")
    body = html.escape(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return f"""
<!doctype html>
<html lang="pl">
  <head>
    <meta charset="utf-8">
    <title>MPbM public health</title>
    <style>
      body {{ font-family: system-ui, sans-serif; max-width: 960px; margin: 2rem auto; line-height: 1.45; padding: 0 1rem; }}
      table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
      td, th {{ border: 1px solid #ddd; padding: .45rem .6rem; }}
      pre {{ background: #f6f6f6; padding: 1rem; overflow: auto; }}
      .status {{ font-size: 1.4rem; font-weight: 700; }}
    </style>
  </head>
  <body>
    <h1>MPbM public health</h1>
    <p class="status">Status: {status}</p>
    <table><thead><tr><th>check</th><th>result</th></tr></thead><tbody>{''.join(rows)}</tbody></table>
    <h2>Redacted JSON</h2>
    <pre>{body}</pre>
  </body>
</html>
"""
