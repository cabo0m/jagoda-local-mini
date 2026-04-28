from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_INVITE_TTL_DAYS = 14
DEFAULT_SCOPES = "mcp:tools memories:read memories:write"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def now_epoch() -> int:
    return int(time.time())


def parse_utc_to_epoch(value: str | None) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    if raw.isdigit():
        return int(raw)
    normalized = raw.replace("Z", "+00:00")
    return int(datetime.fromisoformat(normalized).timestamp())


def epoch_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def invite_code_hash(code: str) -> str:
    normalized = str(code or "").strip()
    return hashlib.sha256(("mpbm_invite_v1:" + normalized).encode("utf-8")).hexdigest().upper()


def generate_invite_code(prefix: str = "MPBM") -> str:
    safe_prefix = "".join(ch for ch in prefix.upper() if ch.isalnum() or ch in {"_", "-"}) or "MPBM"
    return f"{safe_prefix}_{secrets.token_urlsafe(24)}"


@dataclass(frozen=True)
class InviteValidation:
    status: str
    record: dict[str, Any] | None = None


class InviteStore:
    """SQLite-backed MPbM invite code store.

    Raw invite codes are never stored. The database keeps only a SHA-256 hash
    of the code with a purpose prefix. The raw code is shown once by the admin
    creation command and then becomes unrecoverable.
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).resolve()

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mpbm_invites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code_hash TEXT NOT NULL UNIQUE,
                    user_key TEXT NOT NULL,
                    workspace_key TEXT NOT NULL DEFAULT 'default',
                    scopes TEXT NOT NULL DEFAULT 'mcp:tools memories:read memories:write',
                    expires_at INTEGER,
                    used_at TEXT,
                    use_count INTEGER NOT NULL DEFAULT 0,
                    revoked_at TEXT,
                    created_by TEXT,
                    created_at TEXT NOT NULL,
                    note TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mpbm_invites_user_key ON mpbm_invites(user_key)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mpbm_invites_expires_at ON mpbm_invites(expires_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_mpbm_invites_revoked_at ON mpbm_invites(revoked_at)")

    def create_invite(
        self,
        *,
        user_key: str,
        workspace_key: str = "default",
        scopes: str = DEFAULT_SCOPES,
        ttl_days: int = DEFAULT_INVITE_TTL_DAYS,
        expires_at: int | None = None,
        created_by: str | None = None,
        note: str | None = None,
        raw_code: str | None = None,
        prefix: str = "MPBM",
    ) -> dict[str, Any]:
        self.ensure_schema()
        code = raw_code or generate_invite_code(prefix)
        if expires_at is None and ttl_days > 0:
            expires_at = int((datetime.now(timezone.utc) + timedelta(days=ttl_days)).timestamp())
        record = {
            "code": code,
            "code_hash": invite_code_hash(code),
            "user_key": user_key,
            "workspace_key": workspace_key,
            "scopes": scopes,
            "expires_at": expires_at,
            "expires_at_iso": epoch_to_iso(expires_at),
            "created_by": created_by,
            "created_at": utc_now(),
            "note": note,
        }
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO mpbm_invites
                    (code_hash, user_key, workspace_key, scopes, expires_at, created_by, created_at, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["code_hash"],
                    user_key,
                    workspace_key,
                    scopes,
                    expires_at,
                    created_by,
                    record["created_at"],
                    note,
                ),
            )
            record["id"] = int(cursor.lastrowid)
        return record

    def list_invites(self, *, include_revoked: bool = True, limit: int = 100) -> list[dict[str, Any]]:
        self.ensure_schema()
        where = "" if include_revoked else "WHERE revoked_at IS NULL"
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, code_hash, user_key, workspace_key, scopes, expires_at,
                       used_at, use_count, revoked_at, created_by, created_at, note
                FROM mpbm_invites
                {where}
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_public_dict(row) for row in rows]

    def get_invite(self, invite_id: int) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, code_hash, user_key, workspace_key, scopes, expires_at,
                       used_at, use_count, revoked_at, created_by, created_at, note
                FROM mpbm_invites
                WHERE id = ?
                LIMIT 1
                """,
                (invite_id,),
            ).fetchone()
        return self._row_to_public_dict(row) if row else None

    def has_any_invites(self) -> bool:
        self.ensure_schema()
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM mpbm_invites LIMIT 1").fetchone()
        return row is not None

    def validate_code(self, code: str) -> InviteValidation:
        self.ensure_schema()
        code_hash = invite_code_hash(code)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, code_hash, user_key, workspace_key, scopes, expires_at,
                       used_at, use_count, revoked_at, created_by, created_at, note
                FROM mpbm_invites
                WHERE code_hash = ?
                LIMIT 1
                """,
                (code_hash,),
            ).fetchone()
            if row is None:
                return InviteValidation("missing", None)
            record = self._row_to_public_dict(row)
            if row["revoked_at"]:
                return InviteValidation("revoked", record)
            expires_at = row["expires_at"]
            if expires_at is not None and int(expires_at) <= now_epoch():
                return InviteValidation("expired", record)
            conn.execute(
                """
                UPDATE mpbm_invites
                SET used_at = COALESCE(used_at, ?), use_count = use_count + 1
                WHERE id = ?
                """,
                (utc_now(), int(row["id"])),
            )
            updated = conn.execute(
                """
                SELECT id, code_hash, user_key, workspace_key, scopes, expires_at,
                       used_at, use_count, revoked_at, created_by, created_at, note
                FROM mpbm_invites
                WHERE id = ?
                """,
                (int(row["id"]),),
            ).fetchone()
        return InviteValidation("ok", self._row_to_public_dict(updated))

    def revoke_invite(self, invite_id: int) -> bool:
        self.ensure_schema()
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE mpbm_invites SET revoked_at = COALESCE(revoked_at, ?) WHERE id = ?",
                (utc_now(), invite_id),
            )
            return int(cursor.rowcount or 0) > 0

    def renew_invite(self, invite_id: int, *, ttl_days: int | None = None, expires_at: int | None = None) -> bool:
        self.ensure_schema()
        if expires_at is None:
            ttl = DEFAULT_INVITE_TTL_DAYS if ttl_days is None else ttl_days
            expires_at = int((datetime.now(timezone.utc) + timedelta(days=ttl)).timestamp()) if ttl > 0 else None
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE mpbm_invites SET expires_at = ?, revoked_at = NULL WHERE id = ?",
                (expires_at, invite_id),
            )
            return int(cursor.rowcount or 0) > 0

    def delete_invite(self, invite_id: int) -> bool:
        self.ensure_schema()
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM mpbm_invites WHERE id = ?", (invite_id,))
            return int(cursor.rowcount or 0) > 0

    def _row_to_public_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        expires_at = row["expires_at"]
        now = now_epoch()
        if row["revoked_at"]:
            status = "revoked"
        elif expires_at is not None and int(expires_at) <= now:
            status = "expired"
        elif int(row["use_count"] or 0) > 0:
            status = "used"
        else:
            status = "active"
        return {
            "id": int(row["id"]),
            "code_hash_prefix": str(row["code_hash"])[:16],
            "code_hash": str(row["code_hash"]),
            "user_key": row["user_key"],
            "workspace_key": row["workspace_key"],
            "scopes": row["scopes"],
            "expires_at": expires_at,
            "expires_at_iso": epoch_to_iso(expires_at),
            "used_at": row["used_at"],
            "use_count": int(row["use_count"] or 0),
            "revoked_at": row["revoked_at"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "note": row["note"],
            "status": status,
        }


def default_db_path() -> Path:
    env_db = sys.modules.get("os")
    # Keep this function simple and import-light for CLI use.
    import os

    if os.environ.get("DB_PATH"):
        return Path(os.environ["DB_PATH"]).resolve()
    return Path(__file__).resolve().parent / "data" / "jagoda_memory.db"


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage MPbM invite codes. Raw codes are shown only once on create.")
    parser.add_argument("--db-path", default=str(default_db_path()))
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", aliases=["add"], help="Create an invite and print the raw code once.")
    create.add_argument("--user-key", required=True)
    create.add_argument("--workspace-key", default="default")
    create.add_argument("--scopes", default=DEFAULT_SCOPES)
    create.add_argument("--ttl-days", type=int, default=DEFAULT_INVITE_TTL_DAYS)
    create.add_argument("--expires-at", default=None, help="ISO UTC datetime or epoch seconds. Overrides --ttl-days.")
    create.add_argument("--created-by", default=None)
    create.add_argument("--note", default=None)
    create.add_argument("--prefix", default="MPBM")

    list_cmd = sub.add_parser("list", help="List invite metadata without raw codes.")
    list_cmd.add_argument("--limit", type=int, default=100)
    list_cmd.add_argument("--active-only", action="store_true")

    show = sub.add_parser("show", help="Show one invite by id without raw code.")
    show.add_argument("id", type=int)

    revoke = sub.add_parser("revoke", help="Revoke one invite by id.")
    revoke.add_argument("id", type=int)

    renew = sub.add_parser("renew", help="Renew one invite by id and clear revoked_at.")
    renew.add_argument("id", type=int)
    renew.add_argument("--ttl-days", type=int, default=DEFAULT_INVITE_TTL_DAYS)
    renew.add_argument("--expires-at", default=None)

    delete = sub.add_parser("delete", help="Delete one invite by id. Use mainly for operator mistakes.")
    delete.add_argument("id", type=int)

    args = parser.parse_args(argv)
    store = InviteStore(args.db_path)

    if args.command in {"create", "add"}:
        expires_at = parse_utc_to_epoch(args.expires_at)
        record = store.create_invite(
            user_key=args.user_key,
            workspace_key=args.workspace_key,
            scopes=args.scopes,
            ttl_days=args.ttl_days,
            expires_at=expires_at,
            created_by=args.created_by,
            note=args.note,
            prefix=args.prefix,
        )
        _print_json(
            {
                "status": "created",
                "id": record["id"],
                "invite_code_SHOW_ONCE": record["code"],
                "user_key": record["user_key"],
                "workspace_key": record["workspace_key"],
                "scopes": record["scopes"],
                "expires_at": record["expires_at_iso"],
                "code_hash_prefix": record["code_hash"][:16],
                "warning": "Store this code now. It is not saved in raw form and cannot be recovered.",
            }
        )
        return 0

    if args.command == "list":
        _print_json({"items": store.list_invites(include_revoked=not args.active_only, limit=args.limit)})
        return 0

    if args.command == "show":
        item = store.get_invite(args.id)
        _print_json({"item": item})
        return 0 if item else 1

    if args.command == "revoke":
        ok = store.revoke_invite(args.id)
        _print_json({"status": "revoked" if ok else "not_found", "id": args.id})
        return 0 if ok else 1

    if args.command == "renew":
        expires_at = parse_utc_to_epoch(args.expires_at)
        ok = store.renew_invite(args.id, ttl_days=args.ttl_days, expires_at=expires_at)
        item = store.get_invite(args.id) if ok else None
        _print_json({"status": "renewed" if ok else "not_found", "id": args.id, "item": item})
        return 0 if ok else 1

    if args.command == "delete":
        ok = store.delete_invite(args.id)
        _print_json({"status": "deleted" if ok else "not_found", "id": args.id})
        return 0 if ok else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
