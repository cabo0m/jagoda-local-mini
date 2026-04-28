from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any

SYSTEM_LEGACY_USER_KEY = "system:legacy"
DEFAULT_WORKSPACE_KEY = "default"

# Prawidłowe wartości visibility_scope
VISIBILITY_SCOPES = ("private", "workspace", "project", "shared_explicit", "system", "global_readonly", "inherited")


@dataclass
class ActorContext:
    """Kontekst aktora dla operacji pamięciowych."""

    user_id: int
    user_key: str
    workspace_id: int
    workspace_key: str
    role_codes: list[str] = field(default_factory=list)
    session_id: str | None = None
    actor_type: str = "user"   # "user" | "system" | "agent"
    project_key: str | None = None
    conversation_key: str | None = None

    @property
    def is_system(self) -> bool:
        return self.actor_type == "system"

    @property
    def is_agent(self) -> bool:
        return self.actor_type == "agent"


def resolve_actor_context(
    conn: sqlite3.Connection,
    *,
    user_key: str | None = None,
    workspace_key: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    actor_type: str = "user",
    session_id: str | None = None,
) -> ActorContext:
    """
    Wylicza ActorContext z bazy. Fallback do system:legacy + default workspace.

    Używaj tego helpera wszędzie, gdzie potrzebujesz kontekstu aktora.
    Nie konstruuj ActorContext ręcznie — może brakować rekordów w DB.
    """
    resolved_user_key = user_key or SYSTEM_LEGACY_USER_KEY
    resolved_workspace_key = workspace_key or DEFAULT_WORKSPACE_KEY

    user_row = conn.execute(
        "SELECT id, external_user_key FROM users WHERE external_user_key = ? AND status = 'active'",
        (resolved_user_key,),
    ).fetchone()

    if user_row is None and resolved_user_key != SYSTEM_LEGACY_USER_KEY:
        # Fallback do system:legacy
        user_row = conn.execute(
            "SELECT id, external_user_key FROM users WHERE external_user_key = ?",
            (SYSTEM_LEGACY_USER_KEY,),
        ).fetchone()

    if user_row is None:
        raise ValueError(
            f"User '{resolved_user_key}' not found and system:legacy fallback also missing. "
            "Run migration 0010_multiuser_identity_foundation first."
        )

    workspace_row = conn.execute(
        "SELECT id, workspace_key FROM workspaces WHERE workspace_key = ? AND status = 'active'",
        (resolved_workspace_key,),
    ).fetchone()

    if workspace_row is None and resolved_workspace_key != DEFAULT_WORKSPACE_KEY:
        workspace_row = conn.execute(
            "SELECT id, workspace_key FROM workspaces WHERE workspace_key = ?",
            (DEFAULT_WORKSPACE_KEY,),
        ).fetchone()

    if workspace_row is None:
        raise ValueError(
            f"Workspace '{resolved_workspace_key}' not found and default workspace also missing. "
            "Run migration 0010_multiuser_identity_foundation first."
        )

    membership_rows = conn.execute(
        "SELECT role_code FROM workspace_memberships WHERE workspace_id = ? AND user_id = ? AND status = 'active'",
        (workspace_row["id"], user_row["id"]),
    ).fetchall()
    role_codes = [row["role_code"] for row in membership_rows]

    return ActorContext(
        user_id=int(user_row["id"]),
        user_key=str(user_row["external_user_key"]),
        workspace_id=int(workspace_row["id"]),
        workspace_key=str(workspace_row["workspace_key"]),
        role_codes=role_codes,
        session_id=session_id,
        actor_type=actor_type,
        project_key=project_key,
        conversation_key=conversation_key,
    )


def resolve_system_actor(conn: sqlite3.Connection) -> ActorContext:
    """Wylicza ActorContext dla aktora systemowego (maintenance, agent, itp.)."""
    return resolve_actor_context(
        conn,
        user_key=SYSTEM_LEGACY_USER_KEY,
        workspace_key=DEFAULT_WORKSPACE_KEY,
        actor_type="system",
    )


def build_memory_visibility_filter(actor: ActorContext) -> tuple[str, list[Any]]:
    """
    Buduje fragment SQL WHERE + listę params dla scope-aware retrieval.

    Reguły:
    - system actor: widzi wszystko w swoim workspace (plus rekordy bez workspace_id)
    - user actor: widzi własne private + workspace-scoped + project-scoped w swoim workspace

    Zwraca (sql_fragment, params) gotowe do wstrzyknięcia do WHERE clause.
    """
    if actor.is_system or actor.is_agent:
        sql = "(workspace_id = ? OR workspace_id IS NULL)"
        params: list[Any] = [actor.workspace_id]
        return sql, params

    # Użytkownik: private (własne) + workspace + project w jego workspace
    clauses: list[str] = []
    params = []

    # Własne prywatne
    clauses.append("(visibility_scope = 'private' AND owner_user_id = ?)")
    params.append(actor.user_id)

    # Workspace-level w jego workspace
    clauses.append("(visibility_scope = 'workspace' AND workspace_id = ?)")
    params.append(actor.workspace_id)

    # Projektowe w jego workspace
    if actor.project_key:
        clauses.append("(visibility_scope = 'project' AND workspace_id = ? AND project_key = ?)")
        params.extend([actor.workspace_id, actor.project_key])
    else:
        clauses.append("(visibility_scope = 'project' AND workspace_id = ?)")
        params.append(actor.workspace_id)

    # Rekordy legacy bez workspace_id — widoczne tylko użytkownikom default workspace
    # (po migracji 0010 wszystkie rekordy mają workspace_id, więc to jest wyłącznie
    # backward-compat fallback dla środowisk bez pełnej migracji)
    clauses.append("(workspace_id IS NULL AND visibility_scope IS NULL)")

    sql = "(" + " OR ".join(clauses) + ")"
    return sql, params


def infer_visibility_scope(
    *,
    memory_type: str | None,
    project_key: str | None,
    workspace_id: int | None,
    owner_user_id: int | None,
) -> str:
    """
    Wylicza domyślny visibility_scope na podstawie kontekstu zapisu.

    Polityka domyślna:
    - jest project_key i typ projektowy → 'project'
    - brak project_key, typ fact/summary → 'workspace'
    - typ osobisty → 'private'
    - fallback → 'private' (bezpieczniejsze niż za szeroki dostęp)
    """
    _PROJECT_TYPES = {
        "project", "project_note", "project_context", "project_direction",
        "project_design", "project_architecture", "project_milestone",
    }
    _WORKSPACE_TYPES = {"fact", "consolidated_summary"}
    _PRIVATE_TYPES = {
        "preference", "interaction_preference", "workflow_preference",
        "profile", "profile_note", "personal_note", "interest", "working",
    }

    mt = (memory_type or "").strip().lower()

    if project_key and mt in _PROJECT_TYPES:
        return "project"
    if mt in _WORKSPACE_TYPES and not project_key:
        return "workspace"
    if mt in _PRIVATE_TYPES:
        return "private"
    if project_key:
        return "project"

    return "private"
