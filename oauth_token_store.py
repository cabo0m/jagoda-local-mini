from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _now_epoch() -> int:
    return int(time.time())


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class OAuthTokenStore:
    """SQLite-backed OAuth access token store.

    Stores only a SHA-256 hash of the bearer token, never the raw token.
    This lets OAuth sessions survive a jagoda-mcp process restart without
    leaking bearer secrets into the database.
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
                CREATE TABLE IF NOT EXISTS mpbm_oauth_access_tokens (
                    token_hash TEXT PRIMARY KEY,
                    claims_json TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT,
                    revoked_at TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_mpbm_oauth_access_tokens_expires_at "
                "ON mpbm_oauth_access_tokens(expires_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_mpbm_oauth_access_tokens_revoked_at "
                "ON mpbm_oauth_access_tokens(revoked_at)"
            )

    def store(self, token: str, claims: dict[str, Any]) -> None:
        self.ensure_schema()
        expires_at = int(claims.get("expires_at") or 0)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO mpbm_oauth_access_tokens
                    (token_hash, claims_json, expires_at, created_at, last_seen_at, revoked_at)
                VALUES (?, ?, ?, ?, NULL, NULL)
                """,
                (
                    token_hash(token),
                    json.dumps(claims, ensure_ascii=False, sort_keys=True),
                    expires_at,
                    _utc_now(),
                ),
            )

    def load(self, token: str) -> tuple[str, dict[str, Any] | None]:
        """Return (status, claims).

        status is one of: ok, missing, expired, revoked.
        """
        self.ensure_schema()
        hashed = token_hash(token)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT claims_json, expires_at, revoked_at
                FROM mpbm_oauth_access_tokens
                WHERE token_hash = ?
                LIMIT 1
                """,
                (hashed,),
            ).fetchone()
            if row is None:
                return "missing", None
            if row["revoked_at"]:
                return "revoked", None
            expires_at = int(row["expires_at"] or 0)
            if expires_at <= _now_epoch():
                conn.execute("DELETE FROM mpbm_oauth_access_tokens WHERE token_hash = ?", (hashed,))
                return "expired", None
            conn.execute(
                "UPDATE mpbm_oauth_access_tokens SET last_seen_at = ? WHERE token_hash = ?",
                (_utc_now(), hashed),
            )
            return "ok", dict(json.loads(str(row["claims_json"])))

    def delete(self, token: str) -> None:
        self.ensure_schema()
        with self._connect() as conn:
            conn.execute("DELETE FROM mpbm_oauth_access_tokens WHERE token_hash = ?", (token_hash(token),))

    def revoke(self, token: str) -> None:
        self.ensure_schema()
        with self._connect() as conn:
            conn.execute(
                "UPDATE mpbm_oauth_access_tokens SET revoked_at = ? WHERE token_hash = ?",
                (_utc_now(), token_hash(token)),
            )

    def purge_expired(self) -> int:
        self.ensure_schema()
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM mpbm_oauth_access_tokens WHERE expires_at <= ?",
                (_now_epoch(),),
            )
            return int(cursor.rowcount or 0)
