from __future__ import annotations

"""Lightweight MCP entrypoint for MPbM Core.

Public security model:
- callers cannot pass user_key / owner_user_key / workspace_key as tool arguments;
- the HTTP OAuth layer injects trusted x-mpbm-user-key and x-mpbm-workspace-key headers;
- the HTTP OAuth layer injects trusted x-mpbm-scopes for per-tool scope checks;
- direct non-HTTP calls fall back to MPBM_PUBLIC_USER_KEY / MPBM_PUBLIC_WORKSPACE_KEY
  and MPBM_PUBLIC_SCOPES;
- unknown token users are auto-provisioned before any memory operation, so they do
  not fall back to system:legacy;
- public bootstrap is restore_core, not restore_jagoda_core. restore_core returns
  only authenticated-user, scope-aware metadata and never returns Jagoda identity,
  Michal anchors, admin workshops, or full memory content.
"""

import json
import os
from typing import Any

from fastmcp import FastMCP

try:
    from fastmcp.server.dependencies import get_http_headers
except Exception:  # pragma: no cover - keeps local stubs usable in tests
    get_http_headers = None  # type: ignore[assignment]

import server_core as core

mcp = FastMCP("MPbM Core")

DEFAULT_PUBLIC_USER_KEY = os.environ.get("MPBM_PUBLIC_USER_KEY", "system:legacy")
DEFAULT_PUBLIC_WORKSPACE_KEY = os.environ.get("MPBM_PUBLIC_WORKSPACE_KEY", "default")
DEFAULT_PUBLIC_SCOPES = os.environ.get("MPBM_PUBLIC_SCOPES", "mcp:tools memories:read memories:write")

ONBOARDING_STATUS_PENDING = "pending"
ONBOARDING_STATUS_COMPLETED = "completed"
ONBOARDING_STATUS_SKIPPED = "skipped"
ONBOARDING_ALLOWED_TOOLS = (
    "whoami",
    "get_onboarding_status",
    "save_initialization_profile",
    "skip_initialization",
)


def _header_value(name: str) -> str | None:
    if get_http_headers is None:
        return None
    try:
        headers = get_http_headers(include_all=True)
    except RuntimeError:
        return None
    for key, value in headers.items():
        if key.lower() == name.lower() and str(value).strip():
            return str(value).strip()
    return None


def _scope_set(raw_scope: object) -> set[str]:
    if raw_scope is None:
        return set()
    return {item.strip() for item in str(raw_scope).split() if item.strip()}


def _actor_user_key() -> str:
    return _header_value("x-mpbm-user-key") or DEFAULT_PUBLIC_USER_KEY


def _actor_workspace_key() -> str:
    return _header_value("x-mpbm-workspace-key") or DEFAULT_PUBLIC_WORKSPACE_KEY


def _actor_scopes() -> set[str]:
    return _scope_set(_header_value("x-mpbm-scopes") or DEFAULT_PUBLIC_SCOPES)


def _require_scope(required_scope: str) -> None:
    scopes = _actor_scopes()
    if required_scope not in scopes:
        raise PermissionError(f"insufficient_scope: required {required_scope}")


def _require_read_scope() -> None:
    _require_scope("memories:read")


def _require_write_scope() -> None:
    _require_scope("memories:write")


def _clean_optional(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _onboarding_questions() -> list[dict[str, str]]:
    return [
        {"key": "userDisplayName", "question": "Jak mam się do Ciebie zwracać?", "placeholder": "podaj imię"},
        {"key": "aiDisplayName", "question": "Jak chcesz zwracać się do AI?", "placeholder": "np. AI, Asystentka, nadaj mi imię"},
        {"key": "responseStyle", "question": "Jaki styl odpowiedzi preferujesz?", "placeholder": "np. konkretnie, rozmownie, bez lania wody"},
        {"key": "memoryExclusions", "question": "Czego nie zapamiętywać?", "placeholder": "np. spraw prywatnych, zdrowia, finansów"},
    ]


def _ensure_onboarding_table(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mpbm_user_onboarding (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            workspace_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            user_display_name TEXT,
            ai_display_name TEXT,
            response_style TEXT,
            memory_exclusions TEXT,
            profile_memory_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            skipped_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(workspace_id) REFERENCES workspaces(id),
            FOREIGN KEY(profile_memory_id) REFERENCES memories(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mpbm_user_onboarding_status ON mpbm_user_onboarding(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mpbm_user_onboarding_workspace ON mpbm_user_onboarding(workspace_id)")


def _ensure_onboarding_record(conn: Any, user_id: int, workspace_id: int, actor_key: str) -> None:
    _ensure_onboarding_table(conn)
    existing = conn.execute("SELECT id FROM mpbm_user_onboarding WHERE user_id = ? LIMIT 1", (user_id,)).fetchone()
    if existing is not None:
        return
    now = core.utc_now_iso()
    if actor_key == "system:legacy":
        status = ONBOARDING_STATUS_SKIPPED
        skipped_at = now
    else:
        status = ONBOARDING_STATUS_PENDING
        skipped_at = None
    conn.execute(
        """
        INSERT INTO mpbm_user_onboarding
            (user_id, workspace_id, status, created_at, updated_at, skipped_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (user_id, workspace_id, status, now, now, skipped_at),
    )


def _actor_identity() -> tuple[str, str, int, int]:
    actor_key, workspace_key = _actor_keys()
    conn = core.get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT u.id AS actor_id, w.id AS workspace_id
            FROM users u
            JOIN workspace_memberships wm ON wm.user_id = u.id AND wm.status = 'active'
            JOIN workspaces w ON w.id = wm.workspace_id AND w.status = 'active'
            WHERE u.external_user_key = ? AND w.workspace_key = ?
            LIMIT 1
            """,
            (actor_key, workspace_key),
        ).fetchone()
        if row is None:
            raise ValueError(f"actor context not found for {actor_key}")
        return actor_key, workspace_key, int(row["actor_id"]), int(row["workspace_id"])
    finally:
        conn.close()


def _load_onboarding_state(actor_id: int) -> dict[str, Any]:
    conn = core.get_db_connection()
    try:
        _ensure_onboarding_table(conn)
        row = conn.execute(
            """
            SELECT status, user_display_name, ai_display_name, response_style,
                   memory_exclusions, profile_memory_id, created_at, updated_at,
                   completed_at, skipped_at
            FROM mpbm_user_onboarding
            WHERE user_id = ?
            LIMIT 1
            """,
            (actor_id,),
        ).fetchone()
        if row is None:
            return {
                "status": ONBOARDING_STATUS_PENDING,
                "onboarding_required": True,
                "onboarding_completed": False,
                "onboarding_skipped": False,
                "memory_tools_unlocked": False,
                "profile": None,
                "questions": _onboarding_questions(),
            }
        status = str(row["status"] or ONBOARDING_STATUS_PENDING)
        profile = None
        if status == ONBOARDING_STATUS_COMPLETED:
            profile = {
                "userDisplayName": row["user_display_name"],
                "aiDisplayName": row["ai_display_name"],
                "responseStyle": row["response_style"],
                "memoryExclusions": row["memory_exclusions"],
                "profileMemoryId": row["profile_memory_id"],
                "completedAt": row["completed_at"],
            }
        return {
            "status": status,
            "onboarding_required": status == ONBOARDING_STATUS_PENDING,
            "onboarding_completed": status == ONBOARDING_STATUS_COMPLETED,
            "onboarding_skipped": status == ONBOARDING_STATUS_SKIPPED,
            "memory_tools_unlocked": status in {ONBOARDING_STATUS_COMPLETED, ONBOARDING_STATUS_SKIPPED},
            "profile": profile,
            "questions": _onboarding_questions() if status == ONBOARDING_STATUS_PENDING else [],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "skipped_at": row["skipped_at"],
        }
    finally:
        conn.close()


def _onboarding_required_response(actor_key: str, workspace_key: str, state: dict[str, Any], attempted_tool: str | None = None) -> dict[str, Any]:
    return {
        "status": "onboarding_required",
        "onboarding_required": True,
        "onboarding_completed": False,
        "attempted_tool": attempted_tool,
        "message": "Zanim użyjesz pamięci, wykonaj krótką inicjalizację.",
        "required_tool": "save_initialization_profile",
        "allowed_tools": list(ONBOARDING_ALLOWED_TOOLS),
        "questions": state.get("questions") or _onboarding_questions(),
        "actor": {"user_key": actor_key, "workspace_key": workspace_key, "scopes": sorted(_actor_scopes())},
    }


def _onboarding_gate(tool_name: str) -> dict[str, Any] | None:
    actor_key, workspace_key, actor_id, _workspace_id = _actor_identity()
    state = _load_onboarding_state(actor_id)
    if state.get("onboarding_required"):
        return _onboarding_required_response(actor_key, workspace_key, state, attempted_tool=tool_name)
    return None


def _ensure_actor_identity() -> tuple[str, str]:
    """Ensures the authenticated actor exists before core resolves context.

    server_core.resolve_actor_context intentionally has a legacy fallback. The
    public MCP surface must not rely on that fallback, because a fresh OAuth user
    would otherwise be at risk of landing in the legacy bucket. This helper
    creates the user and workspace membership first.
    """
    user_key = _actor_user_key()
    workspace_key = _actor_workspace_key()
    conn = core.get_db_connection()
    try:
        workspace = conn.execute(
            "SELECT id FROM workspaces WHERE workspace_key = ? AND status = 'active' LIMIT 1",
            (workspace_key,),
        ).fetchone()
        if workspace is None:
            conn.execute(
                "INSERT INTO workspaces (workspace_key, name, status) VALUES (?, ?, 'active')",
                (workspace_key, workspace_key),
            )
            workspace = conn.execute(
                "SELECT id FROM workspaces WHERE workspace_key = ? LIMIT 1",
                (workspace_key,),
            ).fetchone()
        if workspace is None:
            raise ValueError(f"workspace provisioning failed for {workspace_key}")

        user = conn.execute(
            "SELECT id FROM users WHERE external_user_key = ? LIMIT 1",
            (user_key,),
        ).fetchone()
        if user is None:
            conn.execute(
                "INSERT INTO users (external_user_key, display_name, status, last_seen_at) VALUES (?, ?, 'active', ?)",
                (user_key, user_key, core.utc_now_iso()),
            )
            user = conn.execute(
                "SELECT id FROM users WHERE external_user_key = ? LIMIT 1",
                (user_key,),
            ).fetchone()
        else:
            conn.execute(
                "UPDATE users SET status = 'active', last_seen_at = ? WHERE id = ?",
                (core.utc_now_iso(), int(user["id"])),
            )
        if user is None:
            raise ValueError(f"user provisioning failed for {user_key}")

        membership = conn.execute(
            """
            SELECT id FROM workspace_memberships
            WHERE workspace_id = ? AND user_id = ? AND role_code = 'member'
            LIMIT 1
            """,
            (int(workspace["id"]), int(user["id"])),
        ).fetchone()
        if membership is None:
            conn.execute(
                """
                INSERT INTO workspace_memberships (workspace_id, user_id, role_code, status)
                VALUES (?, ?, 'member', 'active')
                """,
                (int(workspace["id"]), int(user["id"])),
            )
        else:
            conn.execute(
                "UPDATE workspace_memberships SET status = 'active' WHERE id = ?",
                (int(membership["id"]),),
            )
        _ensure_onboarding_record(
            conn,
            user_id=int(user["id"]),
            workspace_id=int(workspace["id"]),
            actor_key=user_key,
        )
        conn.commit()
    finally:
        conn.close()
    return user_key, workspace_key


def _actor_keys() -> tuple[str, str]:
    return _ensure_actor_identity()


def _public_bootstrap_protocol() -> dict[str, Any]:
    return {
        "stage_1": "restore_core resolves the authenticated public actor from trusted headers or public env fallback",
        "stage_2": "bootstrap returns only compact metadata for actor-visible memories",
        "stage_3": "full memory reads must go through ACL-aware find_memories/get_memory",
        "stage_4": "writes create private memories owned by the authenticated actor unless another scoped tool is explicitly added",
        "rule": "actor first, scope second, memory content last",
    }


def _public_workshop_index() -> list[dict[str, Any]]:
    return [
        {
            "area": "bootstrap_user_context",
            "purpose": "safe public bootstrap for the authenticated user's visible context",
            "tools": ["restore_core"],
            "audience": "public_clients",
            "risk": "low",
            "first_call": True,
        },
        {
            "area": "memory_basics",
            "purpose": "search, list, read and recall memories visible to the authenticated actor",
            "tools": ["find_memories", "list_memories", "get_memory", "get_memory_links", "recall_memory"],
            "audience": "public_clients",
            "risk": "low",
        },
        {
            "area": "private_writes",
            "purpose": "create private memories owned by the authenticated actor",
            "tools": ["create_memory"],
            "audience": "public_clients",
            "risk": "low_write",
        },
    ]


def _public_recommended_next_call() -> dict[str, str]:
    return {
        "after_bootstrap": "Use find_memories or list_memories for task-specific context.",
        "when_user_asks_about_memory": "find_memories",
        "when_user_asks_to_read_specific_memory": "get_memory",
        "when_user_asks_to_save_context": "create_memory",
        "when_user_asks_about_links": "get_memory_links",
    }


def _compact_public_memory(item: dict[str, Any]) -> dict[str, Any]:
    """Return safe bootstrap metadata without full memory content."""
    allowed_keys = (
        "id",
        "summary_short",
        "memory_type",
        "tags",
        "importance_score",
        "confidence_score",
        "project_key",
        "conversation_key",
        "visibility_scope",
        "created_at",
        "last_accessed_at",
    )
    return {key: item.get(key) for key in allowed_keys if key in item}


def _require_visible_memory(memory_id: int) -> dict[str, Any]:
    _require_read_scope()
    user_key, workspace_key = _actor_keys()
    result = core.list_memories_for_user(
        user_key=user_key,
        workspace_key=workspace_key,
        limit=500,
    )
    for item in result.get("items", []):
        if int(item["id"]) == int(memory_id):
            return item
    raise ValueError("memory is not visible for the authenticated actor")


@mcp.tool
def whoami() -> dict[str, Any]:
    """Returns authenticated public actor and onboarding status."""
    actor_key, workspace_key, actor_id, _workspace_id = _actor_identity()
    return {
        "status": "ok",
        "actor": {"user_key": actor_key, "workspace_key": workspace_key, "scopes": sorted(_actor_scopes())},
        "onboarding": _load_onboarding_state(actor_id),
    }


@mcp.tool
def get_onboarding_status() -> dict[str, Any]:
    """Returns current onboarding state and required initialization questions."""
    actor_key, workspace_key, actor_id, _workspace_id = _actor_identity()
    return {
        "status": "ok",
        "actor": {"user_key": actor_key, "workspace_key": workspace_key, "scopes": sorted(_actor_scopes())},
        "onboarding": _load_onboarding_state(actor_id),
    }


@mcp.tool
def save_initialization_profile(
    userDisplayName: str | None = None,
    aiDisplayName: str | None = None,
    responseStyle: str | None = None,
    memoryExclusions: str | None = None,
) -> dict[str, Any]:
    """Completes MPbM initialization and stores a private user profile memory."""
    _require_write_scope()
    actor_key, workspace_key, actor_id, workspace_id = _actor_identity()
    profile = {
        "userDisplayName": _clean_optional(userDisplayName),
        "aiDisplayName": _clean_optional(aiDisplayName),
        "responseStyle": _clean_optional(responseStyle),
        "memoryExclusions": _clean_optional(memoryExclusions),
    }
    profile_memory = core.create_private_memory(
        content=json.dumps(
            {"kind": "mpbm_initialization_profile", "profile": profile, "notes": "User completed MPbM initialization."},
            ensure_ascii=False,
            sort_keys=True,
        ),
        memory_type="initialization_profile",
        owner_user_key=actor_key,
        summary_short="Profil inicjalizacji użytkownika MPbM",
        source="mpbm_onboarding",
        importance_score=0.5,
        confidence_score=1.0,
        tags="mpbm,onboarding,initialization,user-profile",
        project_key="mpbm",
        conversation_key="mpbm-onboarding",
        workspace_key=workspace_key,
    )["memory"]

    profile_memory_id = int(profile_memory["id"])
    now = core.utc_now_iso()
    conn = core.get_db_connection()
    try:
        _ensure_onboarding_table(conn)
        conn.execute(
            """
            UPDATE memories
            SET scope_code = ?, last_accessed_at = ?
            WHERE id = ?
            """,
            ("user", now, profile_memory_id),
        )
        conn.execute(
            """
            INSERT INTO mpbm_user_onboarding
                (user_id, workspace_id, status, user_display_name, ai_display_name,
                 response_style, memory_exclusions, profile_memory_id,
                 created_at, updated_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                status = excluded.status,
                user_display_name = excluded.user_display_name,
                ai_display_name = excluded.ai_display_name,
                response_style = excluded.response_style,
                memory_exclusions = excluded.memory_exclusions,
                profile_memory_id = excluded.profile_memory_id,
                updated_at = excluded.updated_at,
                completed_at = excluded.completed_at,
                skipped_at = NULL
            """,
            (
                actor_id,
                workspace_id,
                ONBOARDING_STATUS_COMPLETED,
                profile["userDisplayName"],
                profile["aiDisplayName"],
                profile["responseStyle"],
                profile["memoryExclusions"],
                profile_memory_id,
                now,
                now,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "onboarding_completed",
        "onboarding_required": False,
        "onboarding_completed": True,
        "actor": {"user_key": actor_key, "workspace_key": workspace_key},
        "profile": profile,
        "profile_memory_id": profile_memory_id,
    }


@mcp.tool
def skip_initialization() -> dict[str, Any]:
    """Marks MPbM initialization as skipped and unlocks normal memory tools."""
    _require_write_scope()
    actor_key, workspace_key, actor_id, workspace_id = _actor_identity()
    now = core.utc_now_iso()
    conn = core.get_db_connection()
    try:
        _ensure_onboarding_table(conn)
        conn.execute(
            """
            INSERT INTO mpbm_user_onboarding
                (user_id, workspace_id, status, created_at, updated_at, skipped_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                status = excluded.status,
                updated_at = excluded.updated_at,
                skipped_at = excluded.skipped_at,
                completed_at = NULL
            """,
            (actor_id, workspace_id, ONBOARDING_STATUS_SKIPPED, now, now, now),
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "onboarding_skipped",
        "onboarding_required": False,
        "onboarding_completed": False,
        "onboarding_skipped": True,
        "actor": {"user_key": actor_key, "workspace_key": workspace_key},
    }


@mcp.tool
def restore_core(
    project_key: str | None = None,
    limit: int = 12,
    include_recent: bool = True,
) -> dict[str, Any]:
    """Restore only authenticated user's safe, scope-aware public context.

    This public bootstrap intentionally excludes Jagoda identity, Michal anchors,
    admin workshops and full memory content. Use find_memories/get_memory for
    task-specific, ACL-aware full reads after bootstrap.
    """
    _require_read_scope()
    gate = _onboarding_gate("restore_core")
    if gate is not None:
        return gate
    user_key, workspace_key = _actor_keys()
    safe_limit = max(1, min(int(limit or 12), 50))
    privacy = {
        "assistant_identity_included": False,
        "michal_anchor_included": False,
        "admin_workshops_included": False,
        "shared_private_context_included": False,
        "full_memory_content_included": False,
        "scope_enforced": True,
    }
    raw_result: dict[str, Any] = {"items": [], "filters": {}}
    if include_recent:
        raw_result = core.list_memories(
            limit=safe_limit,
            sort_by="recent",
            project_key=project_key,
            user_key=user_key,
            workspace_key=workspace_key,
        )
    items = [_compact_public_memory(item) for item in raw_result.get("items", [])]
    return {
        "status": "ok",
        "bootstrap_tool": "restore_core",
        "restored_subject": "authenticated_user",
        "actor": {
            "user_key": user_key,
            "workspace_key": workspace_key,
            "project_key": project_key,
            "scopes": sorted(_actor_scopes()),
        },
        "bootstrap_protocol": _public_bootstrap_protocol(),
        "workshop_index": _public_workshop_index(),
        "recommended_next_call": _public_recommended_next_call(),
        "privacy": privacy,
        "filters": raw_result.get("filters", {}),
        "user_context": items,
        "context_count": len(items),
        "warnings": [
            "restore_core returns metadata only, never full memory content.",
            "restore_jagoda_core is intentionally not part of the public MPbM surface.",
        ],
    }


@mcp.tool
def create_memory(
    content: str,
    memory_type: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
) -> dict[str, Any]:
    """Creates a private memory owned by the authenticated public actor."""
    _require_write_scope()
    gate = _onboarding_gate("create_memory")
    if gate is not None:
        return gate
    user_key, workspace_key = _actor_keys()
    return core.create_private_memory(
        content=content,
        memory_type=memory_type,
        owner_user_key=user_key,
        summary_short=summary_short,
        source=source,
        importance_score=importance_score,
        confidence_score=confidence_score,
        tags=tags,
        project_key=project_key,
        conversation_key=conversation_key,
        workspace_key=workspace_key,
    )


@mcp.tool
def find_memories(
    text_query: str,
    limit: int = 20,
    memory_type: str | None = None,
    tag: str | None = None,
    min_importance: float = 0.0,
    sort_by: str = "active",
    project_key: str | None = None,
    conversation_key: str | None = None,
    parent_memory_id: int | None = None,
) -> dict[str, Any]:
    """Searches only memories visible to the authenticated public actor."""
    _require_read_scope()
    gate = _onboarding_gate("find_memories")
    if gate is not None:
        return gate
    user_key, workspace_key = _actor_keys()
    return core.find_memories(
        text_query=text_query,
        limit=limit,
        memory_type=memory_type,
        tag=tag,
        min_importance=min_importance,
        sort_by=sort_by,
        project_key=project_key,
        conversation_key=conversation_key,
        parent_memory_id=parent_memory_id,
        user_key=user_key,
        workspace_key=workspace_key,
    )


@mcp.tool
def list_memories(
    limit: int = 20,
    memory_type: str | None = None,
    tag: str | None = None,
    min_importance: float = 0.0,
    sort_by: str = "active",
    text_query: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    parent_memory_id: int | None = None,
) -> dict[str, Any]:
    """Lists only memories visible to the authenticated public actor."""
    _require_read_scope()
    gate = _onboarding_gate("list_memories")
    if gate is not None:
        return gate
    user_key, workspace_key = _actor_keys()
    return core.list_memories(
        limit=limit,
        memory_type=memory_type,
        tag=tag,
        min_importance=min_importance,
        sort_by=sort_by,
        text_query=text_query,
        project_key=project_key,
        conversation_key=conversation_key,
        parent_memory_id=parent_memory_id,
        user_key=user_key,
        workspace_key=workspace_key,
    )


@mcp.tool
def get_memory(memory_id: int) -> dict[str, Any]:
    gate = _onboarding_gate("get_memory")
    if gate is not None:
        return gate
    _require_visible_memory(memory_id)
    return core.get_memory(memory_id=memory_id)


@mcp.tool
def get_memory_links(memory_id: int) -> dict[str, Any]:
    gate = _onboarding_gate("get_memory_links")
    if gate is not None:
        return gate
    _require_visible_memory(memory_id)
    return core.get_memory_links(memory_id=memory_id)


@mcp.tool
def recall_memory(
    memory_id: int,
    strength: float = 0.1,
    recall_type: str = "manual",
) -> dict[str, Any]:
    _require_read_scope()
    _require_write_scope()
    gate = _onboarding_gate("recall_memory")
    if gate is not None:
        return gate
    _require_visible_memory(memory_id)
    return core.recall_memory(
        memory_id=memory_id,
        strength=strength,
        recall_type=recall_type,
    )


if __name__ == "__main__":
    mcp.run(transport="http", host="127.0.0.1", port=8015, path="/mcp/")
