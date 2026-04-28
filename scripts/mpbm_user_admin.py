from __future__ import annotations

"""Small operator CLI for MPbM public users and workspace memberships.

This script manages identities only. It never creates or prints bearer tokens.
For external users, create an invite code with scripts/mpbm_invites.py and let
OAuth/PKCE issue the access token through the normal public flow.
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def default_db_path() -> Path:
    if os.environ.get("DB_PATH"):
        return Path(os.environ["DB_PATH"]).resolve()
    return Path(__file__).resolve().parents[1] / "data" / "jagoda_memory.db"


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(Path(db_path).resolve())
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def ensure_workspace(conn: sqlite3.Connection, workspace_key: str, name: str | None = None) -> sqlite3.Row:
    row = conn.execute(
        "SELECT id, workspace_key, name, status FROM workspaces WHERE workspace_key = ? LIMIT 1",
        (workspace_key,),
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO workspaces (workspace_key, name, status) VALUES (?, ?, 'active')",
            (workspace_key, name or workspace_key),
        )
    else:
        conn.execute(
            "UPDATE workspaces SET status = 'active', name = COALESCE(?, name) WHERE id = ?",
            (name, int(row["id"])),
        )
    updated = conn.execute(
        "SELECT id, workspace_key, name, status FROM workspaces WHERE workspace_key = ? LIMIT 1",
        (workspace_key,),
    ).fetchone()
    if updated is None:
        raise RuntimeError(f"workspace provisioning failed: {workspace_key}")
    return updated


def ensure_user(conn: sqlite3.Connection, user_key: str, display_name: str | None = None) -> sqlite3.Row:
    row = conn.execute(
        "SELECT id, external_user_key, display_name, status, last_seen_at FROM users WHERE external_user_key = ? LIMIT 1",
        (user_key,),
    ).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO users (external_user_key, display_name, status, last_seen_at)
            VALUES (?, ?, 'active', ?)
            """,
            (user_key, display_name or user_key, utc_now()),
        )
    else:
        conn.execute(
            """
            UPDATE users
            SET status = 'active', display_name = COALESCE(?, display_name), last_seen_at = ?
            WHERE id = ?
            """,
            (display_name, utc_now(), int(row["id"])),
        )
    updated = conn.execute(
        "SELECT id, external_user_key, display_name, status, last_seen_at FROM users WHERE external_user_key = ? LIMIT 1",
        (user_key,),
    ).fetchone()
    if updated is None:
        raise RuntimeError(f"user provisioning failed: {user_key}")
    return updated


def ensure_membership(
    conn: sqlite3.Connection,
    *,
    workspace_id: int,
    user_id: int,
    role_code: str = "member",
) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT id, workspace_id, user_id, role_code, status
        FROM workspace_memberships
        WHERE workspace_id = ? AND user_id = ? AND role_code = ?
        LIMIT 1
        """,
        (workspace_id, user_id, role_code),
    ).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO workspace_memberships (workspace_id, user_id, role_code, status)
            VALUES (?, ?, ?, 'active')
            """,
            (workspace_id, user_id, role_code),
        )
    else:
        conn.execute(
            "UPDATE workspace_memberships SET status = 'active' WHERE id = ?",
            (int(row["id"]),),
        )
    updated = conn.execute(
        """
        SELECT id, workspace_id, user_id, role_code, status
        FROM workspace_memberships
        WHERE workspace_id = ? AND user_id = ? AND role_code = ?
        LIMIT 1
        """,
        (workspace_id, user_id, role_code),
    ).fetchone()
    if updated is None:
        raise RuntimeError("membership provisioning failed")
    return updated


def show_user(conn: sqlite3.Connection, user_key: str) -> dict[str, Any] | None:
    user = conn.execute(
        "SELECT id, external_user_key, display_name, status, last_seen_at FROM users WHERE external_user_key = ? LIMIT 1",
        (user_key,),
    ).fetchone()
    if user is None:
        return None
    memberships = conn.execute(
        """
        SELECT wm.id, w.workspace_key, wm.role_code, wm.status
        FROM workspace_memberships wm
        JOIN workspaces w ON w.id = wm.workspace_id
        WHERE wm.user_id = ?
        ORDER BY w.workspace_key, wm.role_code
        """,
        (int(user["id"]),),
    ).fetchall()
    return {"user": row_to_dict(user), "memberships": [row_to_dict(row) for row in memberships]}


def list_users(conn: sqlite3.Connection, limit: int, active_only: bool) -> list[dict[str, Any]]:
    where = "WHERE status = 'active'" if active_only else ""
    rows = conn.execute(
        f"""
        SELECT id, external_user_key, display_name, status, last_seen_at
        FROM users
        {where}
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [row_to_dict(row) for row in rows]


def deactivate_user(conn: sqlite3.Connection, user_key: str) -> bool:
    cursor = conn.execute(
        "UPDATE users SET status = 'inactive' WHERE external_user_key = ?",
        (user_key,),
    )
    conn.execute(
        """
        UPDATE workspace_memberships
        SET status = 'inactive'
        WHERE user_id IN (SELECT id FROM users WHERE external_user_key = ?)
        """,
        (user_key,),
    )
    return int(cursor.rowcount or 0) > 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage MPbM public users and workspace memberships.")
    parser.add_argument("--db-path", default=str(default_db_path()))
    sub = parser.add_subparsers(dest="command", required=True)

    ensure = sub.add_parser("ensure", aliases=["add"], help="Create or reactivate a user and membership.")
    ensure.add_argument("--user-key", required=True)
    ensure.add_argument("--display-name", default=None)
    ensure.add_argument("--workspace-key", default="default")
    ensure.add_argument("--workspace-name", default=None)
    ensure.add_argument("--role-code", default="member")

    show = sub.add_parser("show", help="Show one user and memberships.")
    show.add_argument("--user-key", required=True)

    list_cmd = sub.add_parser("list", help="List users.")
    list_cmd.add_argument("--limit", type=int, default=50)
    list_cmd.add_argument("--active-only", action="store_true")

    deactivate = sub.add_parser("deactivate", help="Deactivate one user and memberships.")
    deactivate.add_argument("--user-key", required=True)

    args = parser.parse_args(argv)
    with connect(args.db_path) as conn:
        if args.command in {"ensure", "add"}:
            workspace = ensure_workspace(conn, args.workspace_key, args.workspace_name)
            user = ensure_user(conn, args.user_key, args.display_name)
            membership = ensure_membership(
                conn,
                workspace_id=int(workspace["id"]),
                user_id=int(user["id"]),
                role_code=args.role_code,
            )
            conn.commit()
            print_json(
                {
                    "status": "ok",
                    "user": row_to_dict(user),
                    "workspace": row_to_dict(workspace),
                    "membership": row_to_dict(membership),
                }
            )
            return 0

        if args.command == "show":
            item = show_user(conn, args.user_key)
            print_json({"item": item})
            return 0 if item else 1

        if args.command == "list":
            print_json({"items": list_users(conn, args.limit, args.active_only)})
            return 0

        if args.command == "deactivate":
            ok = deactivate_user(conn, args.user_key)
            conn.commit()
            print_json({"status": "deactivated" if ok else "not_found", "user_key": args.user_key})
            return 0 if ok else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
