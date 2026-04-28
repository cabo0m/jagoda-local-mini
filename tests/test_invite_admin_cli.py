from __future__ import annotations

import json

from invite_store import main


def _read_json(capsys):
    captured = capsys.readouterr()
    assert captured.err == ""
    return json.loads(captured.out)


def test_mpbm_invites_cli_add_list_revoke_renew_without_raw_code_leak(tmp_path, capsys) -> None:
    db_path = tmp_path / "invites.db"

    assert main([
        "--db-path",
        str(db_path),
        "add",
        "--user-key",
        "basia",
        "--workspace-key",
        "default",
        "--created-by",
        "pytest",
        "--note",
        "cli smoke",
    ]) == 0
    created = _read_json(capsys)
    raw_code = created["invite_code_SHOW_ONCE"]
    invite_id = int(created["id"])
    assert raw_code.startswith("MPBM_")
    assert created["user_key"] == "basia"
    assert created["code_hash_prefix"]

    assert main(["--db-path", str(db_path), "list"]) == 0
    listed = _read_json(capsys)
    list_text = json.dumps(listed, ensure_ascii=False)
    assert raw_code not in list_text
    assert listed["items"][0]["id"] == invite_id
    assert listed["items"][0]["status"] == "active"
    assert listed["items"][0]["user_key"] == "basia"

    assert main(["--db-path", str(db_path), "revoke", str(invite_id)]) == 0
    revoked = _read_json(capsys)
    assert revoked == {"id": invite_id, "status": "revoked"}

    assert main(["--db-path", str(db_path), "show", str(invite_id)]) == 0
    shown_revoked = _read_json(capsys)
    assert shown_revoked["item"]["status"] == "revoked"

    assert main(["--db-path", str(db_path), "renew", str(invite_id), "--ttl-days", "3"]) == 0
    renewed = _read_json(capsys)
    assert renewed["status"] == "renewed"
    assert renewed["item"]["status"] == "active"

    assert main(["--db-path", str(db_path), "delete", str(invite_id)]) == 0
    deleted = _read_json(capsys)
    assert deleted == {"id": invite_id, "status": "deleted"}


def test_mpbm_invites_cli_rejects_unknown_show_id(tmp_path, capsys) -> None:
    db_path = tmp_path / "invites.db"

    assert main(["--db-path", str(db_path), "show", "999"]) == 1
    shown = _read_json(capsys)
    assert shown == {"item": None}



def test_mpbm_invites_wrapper_exists() -> None:
    from pathlib import Path

    assert Path("scripts/mpbm_invites.py").exists()
    assert Path("scripts/invite_admin.py").exists()
