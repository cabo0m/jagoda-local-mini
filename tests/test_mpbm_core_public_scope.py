from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any


def _seed_user(server: Any, user_key: str, workspace_key: str = "default") -> None:
    conn = server.get_db_connection()
    try:
        workspace = conn.execute(
            "SELECT id FROM workspaces WHERE workspace_key = ?",
            (workspace_key,),
        ).fetchone()
        assert workspace is not None
        conn.execute(
            "INSERT OR IGNORE INTO users (external_user_key, display_name, status) VALUES (?, ?, 'active')",
            (user_key, user_key),
        )
        user = conn.execute(
            "SELECT id FROM users WHERE external_user_key = ?",
            (user_key,),
        ).fetchone()
        assert user is not None
        conn.execute(
            """
            INSERT OR IGNORE INTO workspace_memberships (workspace_id, user_id, role_code, status)
            VALUES (?, ?, 'member', 'active')
            """,
            (workspace["id"], user["id"]),
        )
        conn.commit()
    finally:
        conn.close()


def _complete_onboarding(module: Any) -> None:
    result = module.save_initialization_profile(
        userDisplayName="Public User",
        aiDisplayName="AI",
        responseStyle="konkretnie",
        memoryExclusions="",
    )
    assert result["status"] == "onboarding_completed"
    assert result["onboarding_required"] is False


def test_public_create_memory_uses_configured_actor(server: Any, monkeypatch) -> None:
    server.apply_schema_migrations()
    _seed_user(server, "public-user")
    import server_mpbm_core

    monkeypatch.setenv("MPBM_PUBLIC_USER_KEY", "public-user")
    monkeypatch.setenv("MPBM_PUBLIC_WORKSPACE_KEY", "default")
    module = importlib.reload(server_mpbm_core)
    module.core.get_db_connection = server.get_db_connection
    module.core._insert_memory = server._insert_memory
    _complete_onboarding(module)

    result = module.create_memory(
        content="Prywatny fakt aktora publicznego",
        memory_type="personal_note",
        summary_short="public actor private memory",
    )

    memory = result["memory"]
    assert result["status"] == "created"
    assert memory["visibility_scope"] == "private"
    assert memory["owner_user_id"] is not None

    visible = module.list_memories(limit=10)
    assert memory["id"] in {item["id"] for item in visible["items"]}


def test_public_get_memory_rejects_other_users_private_memory(server: Any, monkeypatch) -> None:
    server.apply_schema_migrations()
    _seed_user(server, "public-user")
    _seed_user(server, "other-user")
    import server_mpbm_core

    monkeypatch.setenv("MPBM_PUBLIC_USER_KEY", "public-user")
    monkeypatch.setenv("MPBM_PUBLIC_WORKSPACE_KEY", "default")
    module = importlib.reload(server_mpbm_core)
    module.core.get_db_connection = server.get_db_connection
    module.core._insert_memory = server._insert_memory
    _complete_onboarding(module)

    own = module.create_memory(
        content="Własne wspomnienie",
        memory_type="personal_note",
        summary_short="own",
    )["memory"]
    other = server.create_private_memory(
        content="Cudze wspomnienie",
        memory_type="personal_note",
        owner_user_key="other-user",
        summary_short="other",
    )["memory"]

    assert module.get_memory(int(own["id"]))["memory"]["id"] == own["id"]
    try:
        module.get_memory(int(other["id"]))
    except ValueError as exc:
        assert "not visible" in str(exc)
    else:
        raise AssertionError("Expected private memory access to be rejected")


def test_public_core_does_not_expose_administrative_or_jagoda_bootstrap_tools() -> None:
    source = Path("server_mpbm_core.py").read_text(encoding="utf-8")

    assert "def undo_run" not in source
    assert "def run_sandman_v1" not in source
    assert "def link_memories" not in source
    assert "def restore_jagoda_core" not in source
    assert "user_key:" not in source
    assert "owner_user_key:" not in source


def test_public_restore_core_requires_read_scope(monkeypatch) -> None:
    import server_mpbm_core

    monkeypatch.setenv("MPBM_PUBLIC_SCOPES", "memories:write")
    module = importlib.reload(server_mpbm_core)

    try:
        module.restore_core(project_key="morenatech", limit=6)
    except PermissionError as exc:
        assert "memories:read" in str(exc)
    else:
        raise AssertionError("Expected restore_core to require memories:read scope")


def test_public_restore_core_returns_safe_bootstrap_and_provisions_actor(server: Any, monkeypatch) -> None:
    server.apply_schema_migrations()
    import server_mpbm_core

    monkeypatch.setenv("MPBM_PUBLIC_USER_KEY", "public-bootstrap-user")
    monkeypatch.setenv("MPBM_PUBLIC_WORKSPACE_KEY", "default")
    monkeypatch.setenv("MPBM_PUBLIC_SCOPES", "memories:read memories:write")
    module = importlib.reload(server_mpbm_core)
    module.core.get_db_connection = server.get_db_connection
    module.core._insert_memory = server._insert_memory
    _complete_onboarding(module)

    result = module.restore_core(project_key="morenatech", limit=6)

    assert result["status"] == "ok"
    assert result["bootstrap_tool"] == "restore_core"
    assert result["restored_subject"] == "authenticated_user"
    assert result["actor"]["user_key"] == "public-bootstrap-user"
    assert "bootstrap_protocol" in result
    assert "workshop_index" in result
    assert "recommended_next_call" in result
    assert "identity" not in result
    assert "user_anchor" not in result
    assert "core_memories" not in result
    assert "project_anchors" not in result
    assert "recent_context" not in result
    assert result["privacy"]["assistant_identity_included"] is False
    assert result["privacy"]["michal_anchor_included"] is False
    assert result["privacy"]["admin_workshops_included"] is False
    assert result["privacy"]["full_memory_content_included"] is False
    workshops = {item["area"]: item for item in result["workshop_index"]}
    assert workshops["bootstrap_user_context"]["first_call"] is True
    assert workshops["bootstrap_user_context"]["tools"] == ["restore_core"]
    assert "admin_dangerous" not in workshops

    conn = server.get_db_connection()
    try:
        user = conn.execute(
            "SELECT id FROM users WHERE external_user_key = ?",
            ("public-bootstrap-user",),
        ).fetchone()
        assert user is not None
        membership = conn.execute(
            """
            SELECT wm.id
            FROM workspace_memberships wm
            JOIN workspaces w ON w.id = wm.workspace_id
            WHERE wm.user_id = ? AND w.workspace_key = 'default' AND wm.status = 'active'
            """,
            (int(user["id"]),),
        ).fetchone()
        assert membership is not None
    finally:
        conn.close()


def test_public_restore_core_returns_metadata_without_content(server: Any, monkeypatch) -> None:
    server.apply_schema_migrations()
    _seed_user(server, "public-user")
    import server_mpbm_core

    monkeypatch.setenv("MPBM_PUBLIC_USER_KEY", "public-user")
    monkeypatch.setenv("MPBM_PUBLIC_WORKSPACE_KEY", "default")
    monkeypatch.setenv("MPBM_PUBLIC_SCOPES", "memories:read memories:write")
    module = importlib.reload(server_mpbm_core)
    module.core.get_db_connection = server.get_db_connection
    module.core._insert_memory = server._insert_memory
    _complete_onboarding(module)

    created = module.create_memory(
        content="Sekret użytkownika, który nie może wypłynąć w bootstrapie",
        memory_type="personal_note",
        summary_short="safe bootstrap metadata",
        project_key="morenatech",
    )["memory"]

    result = module.restore_core(project_key="morenatech", limit=6)
    ids = {item["id"] for item in result["user_context"]}
    assert created["id"] in ids
    matching = [item for item in result["user_context"] if item["id"] == created["id"]][0]
    assert matching["summary_short"] == "safe bootstrap metadata"
    assert "content" not in matching
    assert "owner_user_id" not in matching


def test_public_onboarding_gate_blocks_memory_tools_for_new_actor(server: Any, monkeypatch) -> None:
    server.apply_schema_migrations()
    import server_mpbm_core

    monkeypatch.setenv("MPBM_PUBLIC_USER_KEY", "fresh-onboarding-user")
    monkeypatch.setenv("MPBM_PUBLIC_WORKSPACE_KEY", "default")
    monkeypatch.setenv("MPBM_PUBLIC_SCOPES", "memories:read memories:write")
    module = importlib.reload(server_mpbm_core)
    module.core.get_db_connection = server.get_db_connection
    module.core._insert_memory = server._insert_memory

    status = module.get_onboarding_status()
    assert status["status"] == "ok"
    assert status["onboarding"]["onboarding_required"] is True
    assert status["onboarding"]["memory_tools_unlocked"] is False

    blocked = module.list_memories(limit=5)
    assert blocked["status"] == "onboarding_required"
    assert blocked["attempted_tool"] == "list_memories"
    assert blocked["required_tool"] == "save_initialization_profile"
    assert {q["key"] for q in blocked["questions"]} == {
        "userDisplayName",
        "aiDisplayName",
        "responseStyle",
        "memoryExclusions",
    }


def test_public_save_initialization_profile_unlocks_memory_tools(server: Any, monkeypatch) -> None:
    server.apply_schema_migrations()
    import server_mpbm_core

    monkeypatch.setenv("MPBM_PUBLIC_USER_KEY", "fresh-completed-user")
    monkeypatch.setenv("MPBM_PUBLIC_WORKSPACE_KEY", "default")
    monkeypatch.setenv("MPBM_PUBLIC_SCOPES", "memories:read memories:write")
    module = importlib.reload(server_mpbm_core)
    module.core.get_db_connection = server.get_db_connection
    module.core._insert_memory = server._insert_memory

    saved = module.save_initialization_profile(
        userDisplayName="  Ala  ",
        aiDisplayName=" Asystentka ",
        responseStyle=" krótko ",
        memoryExclusions=" finanse ",
    )
    assert saved["status"] == "onboarding_completed"
    assert saved["profile"] == {
        "userDisplayName": "Ala",
        "aiDisplayName": "Asystentka",
        "responseStyle": "krótko",
        "memoryExclusions": "finanse",
    }

    listed = module.list_memories(limit=10)
    assert listed.get("status") != "onboarding_required"
    ids = {item["id"] for item in listed["items"]}
    assert saved["profile_memory_id"] in ids


def test_public_skip_initialization_unlocks_memory_tools(server: Any, monkeypatch) -> None:
    server.apply_schema_migrations()
    import server_mpbm_core

    monkeypatch.setenv("MPBM_PUBLIC_USER_KEY", "fresh-skipped-user")
    monkeypatch.setenv("MPBM_PUBLIC_WORKSPACE_KEY", "default")
    monkeypatch.setenv("MPBM_PUBLIC_SCOPES", "memories:read memories:write")
    module = importlib.reload(server_mpbm_core)
    module.core.get_db_connection = server.get_db_connection

    skipped = module.skip_initialization()
    assert skipped["status"] == "onboarding_skipped"

    listed = module.list_memories(limit=5)
    assert listed.get("status") != "onboarding_required"

