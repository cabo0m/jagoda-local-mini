from __future__ import annotations

import hashlib
import sqlite3
import time

from invite_store import InviteStore, invite_code_hash


def test_invite_store_create_validates_without_raw_code_persistence(tmp_path) -> None:
    db_path = tmp_path / "invites.db"
    store = InviteStore(db_path)

    record = store.create_invite(user_key="basia", raw_code="SECRET-CODE-1", ttl_days=1)
    assert record["code"] == "SECRET-CODE-1"

    raw_db = db_path.read_bytes().decode("latin-1", errors="ignore")
    assert "SECRET-CODE-1" not in raw_db
    assert invite_code_hash("SECRET-CODE-1") in raw_db

    validation = store.validate_code("SECRET-CODE-1")
    assert validation.status == "ok"
    assert validation.record is not None
    assert validation.record["user_key"] == "basia"
    assert validation.record["use_count"] == 1


def test_invite_store_rejects_missing_revoked_and_expired(tmp_path) -> None:
    store = InviteStore(tmp_path / "invites.db")

    revoked = store.create_invite(user_key="basia", raw_code="REVOKE-ME", ttl_days=1)
    assert store.revoke_invite(revoked["id"]) is True
    assert store.validate_code("REVOKE-ME").status == "revoked"

    expired = store.create_invite(user_key="adam", raw_code="OLD-CODE", expires_at=int(time.time()) - 1)
    assert expired["id"] > 0
    assert store.validate_code("OLD-CODE").status == "expired"

    assert store.validate_code("NO-SUCH-CODE").status == "missing"


def test_invite_store_renew_reactivates_revoked_invite(tmp_path) -> None:
    store = InviteStore(tmp_path / "invites.db")

    record = store.create_invite(user_key="basia", raw_code="RENEW-ME", ttl_days=1)
    store.revoke_invite(record["id"])
    assert store.validate_code("RENEW-ME").status == "revoked"

    assert store.renew_invite(record["id"], ttl_days=7) is True
    validation = store.validate_code("RENEW-ME")
    assert validation.status == "ok"
    assert validation.record is not None
    assert validation.record["revoked_at"] is None


def test_invite_store_delete_removes_invite(tmp_path) -> None:
    store = InviteStore(tmp_path / "invites.db")
    record = store.create_invite(user_key="basia", raw_code="DELETE-ME", ttl_days=1)

    assert store.delete_invite(record["id"]) is True
    assert store.validate_code("DELETE-ME").status == "missing"


def test_invite_hash_has_purpose_prefix() -> None:
    code = "SAME-CODE"
    assert invite_code_hash(code) != hashlib.sha256(code.encode("utf-8")).hexdigest().upper()
