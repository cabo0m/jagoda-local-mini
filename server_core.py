from __future__ import annotations

import base64
import json
import shutil
from typing import Any

from fastmcp import FastMCP

from app import actor_context as actor_ctx, conflict_explainer, conflict_logic, consolidation_logic, db_migrations, memory_config as config, memory_store as store, sandman_ai, sandman_logic, schemas, timeline
from app.actor_context import ActorContext, build_memory_visibility_filter, infer_visibility_scope, resolve_actor_context, resolve_system_actor
from app.schemas import LAYER_ORDER, SANDMAN_PROTECTED_LAYERS, SANDMAN_PROTECTED_STATES, derive_state_code, enrich_memory_dict, normalize_area_code, normalize_layer_code, normalize_optional_text, normalize_required_text, normalize_scope_code, normalize_state_code
from memory_bootstrap_policy import BootstrapPolicy, build_core_identity_sql, build_project_anchors_sql, build_recent_project_sql

mcp = FastMCP("Jagoda Memory API")

ROOT = config.ROOT
DATA_DIR = config.DATA_DIR
DB_PATH = config.DB_PATH

SAFE_ROLLBACK_ACTION_TYPES = {
    "archived",
    "downgraded",
    "duplicate_link_created",
    "support_link_created",
    "summary_link_created",
    "summary_memory_created",
    "summary_memory_updated",
    "summary_link_deleted",
    "conflict_link_created",
    "dream_link_created",
    "conflict_flagged",
    "canonical_evidence_boosted",
    "valid_to_set",
}

CROSS_PROJECT_FLAG_KEY = "cross_project_knowledge_layer"
CONFLICT_EXPLAINER_FLAG_KEY = "conflict_explainer"
CONFLICT_PREVIEW_RESOLUTION_FLAG_KEY = "conflict_preview_resolution"
CONFLICT_AUTO_RESOLUTION_FLAG_KEY = "conflict_auto_resolution"
FEATURE_FLAG_ROLLOUT_MODES = {"off", "all", "projects", "scopes", "projects_and_scopes"}


def _sync_config() -> None:
    config.ROOT = ROOT
    config.DATA_DIR = DATA_DIR
    config.DB_PATH = DB_PATH


def safe_path(user_path: str | None):
    _sync_config()
    return store.safe_path(user_path)


def rel_path(path):
    _sync_config()
    return store.rel_path(path)


def guess_mime(path):
    return store.guess_mime(path)


def normalize_score(value: float) -> float:
    return store.normalize_score(value)


def utc_now_iso() -> str:
    return store.utc_now_iso()


def utc_offset_days_iso(days: int) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) + timedelta(days=int(days))).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def shift_iso_days(value: str | None, days: int) -> str | None:
    normalized_value = normalize_optional_text(value)
    if normalized_value is None:
        return None
    from datetime import datetime, timedelta, timezone

    candidate = normalized_value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (parsed.astimezone(timezone.utc) + timedelta(days=int(days))).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_db_connection():
    _sync_config()
    conn = store.get_db_connection()
    db_migrations.apply_all_migrations(conn)
    return conn


def parse_params_json(params_json: str):
    return store.parse_params_json(params_json)


def is_read_only_sql(query: str) -> bool:
    return store.is_read_only_sql(query)


def row_to_dict(row):
    return store.row_to_dict(row)


def require_memory_row(conn, memory_id: int):
    return store.require_memory_row(conn, memory_id)


def require_sleep_run_row(conn, run_id: int):
    return store.require_sleep_run_row(conn, run_id)


def create_sleep_run(conn, mode: str, freedom_level: int, notes: str | None = None, rollback_of_run_id: int | None = None, workspace_id: int | None = None, project_key: str | None = None) -> int:
    return store.create_sleep_run(conn, mode, freedom_level, notes, rollback_of_run_id, workspace_id=workspace_id, project_key=project_key)


def add_sleep_action(conn, run_id: int, action_type: str, memory_id: int | None, old_value: Any, new_value: Any, reason: str) -> None:
    store.add_sleep_action(conn, run_id, action_type, memory_id, old_value, new_value, reason)


def finalize_sleep_run(conn, run_id: int, **kwargs: Any) -> None:
    store.finalize_sleep_run(conn, run_id, **kwargs)


def _decode_action_value(value: Any) -> Any:
    return store.decode_action_value(value)


def _existing_rollback_run_id(conn, target_run_id: int) -> int | None:
    row = conn.execute(
        "SELECT id FROM sleep_runs WHERE rollback_of_run_id = ? ORDER BY id DESC LIMIT 1",
        (target_run_id,),
    ).fetchone()
    return None if row is None else int(row["id"])


def _get_rollbackable_actions(conn, target_run_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM sleep_run_actions WHERE run_id = ? ORDER BY id DESC",
        (target_run_id,),
    ).fetchall()
    actions = [row_to_dict(row) for row in rows]
    return [item for item in actions if item["action_type"] in SAFE_ROLLBACK_ACTION_TYPES]



def _insert_memory(
    conn,
    *,
    content: str,
    memory_type: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    state_code: str | None = None,
    scope_code: str | None = None,
    parent_memory_id: int | None = None,
    version: int = 1,
    promoted_from_id: int | None = None,
    demoted_from_id: int | None = None,
    supersedes_memory_id: int | None = None,
    valid_from: str | None = None,
    valid_to: str | None = None,
    decay_score: float = 0.0,
    emotional_weight: float = 0.0,
    identity_weight: float = 0.0,
    project_key: str | None = None,
    conversation_key: str | None = None,
    last_validated_at: str | None = None,
    validation_source: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    review_due_at: str | None = None,
    revalidation_due_at: str | None = None,
    expired_due_at: str | None = None,
    priority: str | None = None,
    # --- Multi-user fields (Stage 1) ---
    visibility_scope: str | None = None,
    workspace_id: int | None = None,
    owner_user_id: int | None = None,
    created_by_user_id: int | None = None,
    last_modified_by_user_id: int | None = None,
    sharing_policy: str | None = None,
) -> dict[str, Any]:
    now_iso = utc_now_iso()
    cursor = conn.cursor()
    normalized_state_code = schemas.derive_state_code(state_code)
    activity_state = "archived" if normalized_state_code == "archived" else "active"
    normalized_scope_code = normalize_scope_code(scope_code)
    normalized_priority = normalize_optional_text(priority) or "normal"
    normalized_owner_role = normalize_optional_text(owner_role) or _default_owner_role(
        state_code=normalized_state_code,
        scope_code=normalized_scope_code,
        project_key=project_key,
    )
    normalized_review_due_at, normalized_revalidation_due_at = _default_due_at(
        conn=conn,
        state_code=normalized_state_code,
        review_due_at=review_due_at,
        revalidation_due_at=revalidation_due_at,
        priority=normalized_priority,
        memory_type=memory_type,
        scope_code=normalized_scope_code,
        project_key=project_key,
    )
    # Ustal workspace_id — fallback do default workspace
    resolved_workspace_id = workspace_id
    if resolved_workspace_id is None:
        ws_row = conn.execute(
            "SELECT id FROM workspaces WHERE workspace_key = 'default' LIMIT 1"
        ).fetchone()
        if ws_row:
            resolved_workspace_id = int(ws_row["id"])

    # Ustal visibility_scope jeśli nie podany jawnie
    resolved_visibility_scope = normalize_optional_text(visibility_scope) or infer_visibility_scope(
        memory_type=memory_type,
        project_key=project_key,
        workspace_id=resolved_workspace_id,
        owner_user_id=owner_user_id,
    )
    resolved_sharing_policy = normalize_optional_text(sharing_policy) or "explicit"

    # BUG2: prywatny rekord zawsze musi mieć owner_user_id (DoD Stage 1)
    resolved_owner_user_id = owner_user_id
    if resolved_visibility_scope == "private" and resolved_owner_user_id is None:
        legacy_row = conn.execute(
            "SELECT id FROM users WHERE external_user_key = 'system:legacy' LIMIT 1"
        ).fetchone()
        if legacy_row:
            resolved_owner_user_id = int(legacy_row["id"])

    cursor.execute(
        """
        INSERT INTO memories (
            content, summary_short, memory_type, source,
            importance_score, confidence_score, tags,
            created_at, last_accessed_at, activity_state,
            evidence_count, contradiction_flag,
            layer_code, area_code, state_code, scope_code,
            parent_memory_id, version, promoted_from_id, demoted_from_id,
            supersedes_memory_id, valid_from, valid_to,
            decay_score, emotional_weight, identity_weight,
            project_key, conversation_key, last_validated_at, validation_source,
            owner_role, owner_id, review_due_at, revalidation_due_at, expired_due_at,
            priority,
            visibility_scope, workspace_id, owner_user_id,
            created_by_user_id, last_modified_by_user_id, sharing_policy
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            normalize_required_text(content, "content"),
            normalize_optional_text(summary_short),
            normalize_required_text(memory_type, "memory_type"),
            normalize_optional_text(source),
            normalize_score(importance_score),
            normalize_score(confidence_score),
            normalize_optional_text(tags),
            now_iso,
            now_iso,
            activity_state,
            normalize_layer_code(layer_code),
            normalize_area_code(area_code),
            normalized_state_code,
            normalized_scope_code,
            parent_memory_id,
            max(int(version or 1), 1),
            promoted_from_id,
            demoted_from_id,
            supersedes_memory_id,
            normalize_optional_text(valid_from),
            normalize_optional_text(valid_to),
            normalize_score(decay_score),
            normalize_score(emotional_weight),
            normalize_score(identity_weight),
            normalize_optional_text(project_key),
            normalize_optional_text(conversation_key),
            normalize_optional_text(last_validated_at),
            normalize_optional_text(validation_source),
            normalized_owner_role,
            normalize_optional_text(owner_id),
            normalized_review_due_at,
            normalized_revalidation_due_at,
            normalize_optional_text(expired_due_at),
            normalized_priority,
            resolved_visibility_scope,
            resolved_workspace_id,
            resolved_owner_user_id,
            created_by_user_id,
            last_modified_by_user_id,
            resolved_sharing_policy,
        ),
    )
    memory_id = int(cursor.lastrowid)
    row = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
    return enrich_memory_dict(row_to_dict(row))



def _create_link(
    conn,
    from_memory_id: int,
    to_memory_id: int,
    relation_type: str,
    weight: float,
    origin: str | None,
    operation_id: str | None = None,
) -> dict[str, Any]:
    created_at = utc_now_iso()
    cursor = conn.cursor()

    # Dziedzicz workspace_id ze wspomnienia źródłowego (Stage 1 — multi-user)
    src_row = conn.execute(
        "SELECT workspace_id FROM memories WHERE id = ? LIMIT 1",
        (from_memory_id,),
    ).fetchone()
    link_workspace_id = int(src_row["workspace_id"]) if src_row and src_row["workspace_id"] is not None else None

    cursor.execute(
        """
        INSERT INTO memory_links
            (from_memory_id, to_memory_id, relation_type, weight, origin, created_at,
             workspace_id, visibility_scope)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'inherited')
        """,
        (from_memory_id, to_memory_id, relation_type, float(weight), origin, created_at, link_workspace_id),
    )
    link_id = int(cursor.lastrowid)
    row = conn.execute("SELECT * FROM memory_links WHERE id = ?", (link_id,)).fetchone()
    return row_to_dict(row)


def _rollback_single_action(conn, rollback_run_id: int, action: dict[str, Any]) -> dict[str, Any]:
    action_type = str(action["action_type"])
    memory_id = action.get("memory_id")
    old_value = _decode_action_value(action.get("old_value"))
    new_value = _decode_action_value(action.get("new_value"))

    if action_type == "archived":
        previous_state = "active"
        previous_archived_at = None
        if isinstance(old_value, dict):
            previous_state = old_value.get("activity_state", "active") or "active"
            previous_archived_at = old_value.get("archived_at")
        conn.execute(
            "UPDATE memories SET activity_state = ?, archived_at = ?, sandman_note = NULL WHERE id = ?",
            (previous_state, previous_archived_at, int(memory_id)),
        )
        result = {"restored_memory_id": int(memory_id), "activity_state": previous_state, "archived_at": previous_archived_at}
    elif action_type == "downgraded":
        previous_importance = float(old_value.get("importance_score")) if isinstance(old_value, dict) else None
        if previous_importance is None:
            raise ValueError("Brak old importance_score dla akcji downgraded")
        conn.execute(
            "UPDATE memories SET importance_score = ?, sandman_note = NULL WHERE id = ?",
            (previous_importance, int(memory_id)),
        )
        result = {"restored_memory_id": int(memory_id), "importance_score": previous_importance}
    elif action_type in {"duplicate_link_created", "support_link_created", "summary_link_created", "conflict_link_created", "dream_link_created"}:
        link_id = None
        if isinstance(new_value, dict):
            link_id = new_value.get("link_id", new_value.get("id"))
        if link_id is None:
            raise ValueError(f"Brak link_id dla akcji {action_type}")
        conn.execute("DELETE FROM memory_links WHERE id = ?", (int(link_id),))
        result = {"deleted_link_id": int(link_id)}
    elif action_type == "summary_memory_created":
        created_memory_id = None
        if isinstance(new_value, dict):
            created_memory_id = new_value.get("memory_id", new_value.get("id"))
        if created_memory_id is None:
            raise ValueError("Brak memory_id dla akcji summary_memory_created")
        conn.execute(
            """
            DELETE FROM timeline_events
            WHERE memory_id = ?
               OR related_memory_id = ?
               OR (source_table = 'memories' AND source_row_id = ?)
            """,
            (int(created_memory_id), int(created_memory_id), int(created_memory_id)),
        )
        conn.execute("DELETE FROM memories WHERE id = ?", (int(created_memory_id),))
        result = {"deleted_memory_id": int(created_memory_id)}
        # memory was deleted — don't reference its id in the rollback action record
        memory_id = None
    elif action_type == "summary_memory_updated":
        if memory_id is None or not isinstance(old_value, dict):
            raise ValueError("Brak danych do rollback summary_memory_updated")
        conn.execute(
            """
            UPDATE memories
            SET summary_short = ?, content = ?, source = ?, importance_score = ?, confidence_score = ?, tags = ?
            WHERE id = ?
            """,
            (
                old_value.get("summary_short"),
                old_value.get("content"),
                old_value.get("source"),
                float(old_value.get("importance_score") or 0.5),
                float(old_value.get("confidence_score") or 0.5),
                old_value.get("tags"),
                int(memory_id),
            ),
        )
        result = {"restored_memory_id": int(memory_id), **old_value}
    elif action_type == "summary_link_deleted":
        if not isinstance(old_value, dict):
            raise ValueError("Brak danych linku do rollback summary_link_deleted")
        recreated = _create_link(
            conn,
            int(old_value["from_memory_id"]),
            int(old_value["to_memory_id"]),
            str(old_value["relation_type"]),
            float(old_value.get("weight") or 1.0),
            old_value.get("origin"),
        )
        result = {"recreated_link_id": int(recreated["id"]), "relation_type": recreated["relation_type"]}
    elif action_type == "conflict_flagged":
        previous_flag = int(old_value.get("contradiction_flag", 0) or 0) if isinstance(old_value, dict) else 0
        conn.execute("UPDATE memories SET contradiction_flag = ? WHERE id = ?", (previous_flag, int(memory_id)))
        result = {"restored_memory_id": int(memory_id), "contradiction_flag": previous_flag}
    elif action_type == "canonical_evidence_boosted":
        previous_evidence = int(old_value.get("evidence_count", 1) or 1) if isinstance(old_value, dict) else 1
        conn.execute(
            "UPDATE memories SET evidence_count = ?, sandman_note = NULL WHERE id = ?",
            (previous_evidence, int(memory_id)),
        )
        result = {"restored_memory_id": int(memory_id), "evidence_count": previous_evidence}
    elif action_type == "valid_to_set":
        previous_valid_to = old_value.get("valid_to") if isinstance(old_value, dict) else None
        conn.execute("UPDATE memories SET valid_to = ? WHERE id = ?", (previous_valid_to, int(memory_id)))
        result = {"restored_memory_id": int(memory_id), "valid_to": previous_valid_to}
    else:
        raise ValueError(f"Nieobsługiwany action_type do rollback: {action_type}")

    add_sleep_action(
        conn,
        rollback_run_id,
        f"rollback_{action_type}",
        None if memory_id is None else int(memory_id),
        new_value,
        result,
        f"rollback_of_action_{action.get('id')}",
    )
    return {"action_type": action_type, **result}


def _default_owner_role(*, state_code: str | None = None, scope_code: str | None = None, project_key: str | None = None) -> str | None:
    normalized_state = normalize_state_code(state_code)
    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)

    if normalized_state == "candidate":
        if normalized_scope == "global":
            return "memory_maintainer"
        if normalized_project_key:
            return "project_maintainer"
        return "review_team"
    if normalized_state == "validated":
        if normalized_scope == "global":
            return "knowledge_curator"
        if normalized_project_key:
            return "project_maintainer"
        return "review_team"
    if normalized_state == "superseded":
        if normalized_scope == "global":
            return "knowledge_curator"
        if normalized_project_key:
            return "project_maintainer"
        return "review_team"
    return None


def _default_due_at(
    *,
    conn=None,
    state_code: str | None = None,
    review_due_at: str | None = None,
    revalidation_due_at: str | None = None,
    priority: str | None = "normal",
    memory_type: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
) -> tuple[str | None, str | None]:
    normalized_state = normalize_state_code(state_code)
    normalized_review_due_at = normalize_optional_text(review_due_at)
    normalized_revalidation_due_at = normalize_optional_text(revalidation_due_at)
    if normalized_state == "candidate" and normalized_review_due_at is None:
        days = _compute_sla_days(conn, "review", priority, memory_type, scope_code, project_key) if conn is not None else 2
        normalized_review_due_at = utc_offset_days_iso(days)
    if normalized_state == "validated" and normalized_revalidation_due_at is None:
        days = _compute_sla_days(conn, "revalidation", priority, memory_type, scope_code, project_key) if conn is not None else 5
        normalized_revalidation_due_at = utc_offset_days_iso(days)
    return normalized_review_due_at, normalized_revalidation_due_at


_SLA_FALLBACK_DAYS: dict[str, int] = {"review": 2, "revalidation": 5, "expired": 7, "duplicate": 3}


def _compute_sla_days(
    conn,
    queue_type: str,
    priority: str | None = "normal",
    memory_type: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
) -> int:
    rows = conn.execute(
        "SELECT * FROM sla_policies WHERE queue_type = ? AND is_active = 1",
        (queue_type,),
    ).fetchall()
    best_score, best_days = -1, None
    for r in rows:
        d = dict(r)
        score = 0
        if d.get("priority") is not None:
            if d["priority"] != priority:
                continue
            score += 8
        if d.get("project_key") is not None:
            if d["project_key"] != project_key:
                continue
            score += 4
        if d.get("scope_code") is not None:
            if d["scope_code"] != scope_code:
                continue
            score += 2
        if d.get("memory_type") is not None:
            if d["memory_type"] != memory_type:
                continue
            score += 1
        if score > best_score:
            best_score, best_days = score, int(d["sla_days"])
    return best_days if best_days is not None else _SLA_FALLBACK_DAYS.get(queue_type, 2)


def _apply_ownership_defaults(memory: dict[str, Any]) -> dict[str, Any]:
    item = dict(memory)
    if normalize_optional_text(item.get("owner_role")) is None:
        item["owner_role"] = _default_owner_role(
            state_code=item.get("state_code"),
            scope_code=item.get("scope_code"),
            project_key=item.get("project_key"),
        )
    review_due_at, revalidation_due_at = _default_due_at(
        state_code=item.get("state_code"),
        review_due_at=item.get("review_due_at"),
        revalidation_due_at=item.get("revalidation_due_at"),
    )
    item["review_due_at"] = review_due_at
    item["revalidation_due_at"] = revalidation_due_at
    if normalize_optional_text(item.get("expired_due_at")) is None and normalize_optional_text(item.get("valid_to")) is not None and normalize_state_code(item.get("state_code")) == "superseded":
        item["expired_due_at"] = shift_iso_days(item.get("valid_to"), 2)
    return item


def _owner_directory_item_to_dict(row) -> dict[str, Any]:
    item = row_to_dict(row)
    item["is_active"] = bool(int(item.get("is_active") or 0))
    return item


def _owner_role_mapping_to_dict(row) -> dict[str, Any]:
    item = row_to_dict(row)
    item["id"] = int(item["id"])
    item["is_active"] = bool(int(item.get("is_active") or 0))
    return item


def _owner_mapping_rank(mapping: dict[str, Any], *, project_key: str | None, scope_code: str | None) -> tuple[int, int, int]:
    mapping_project_key = normalize_optional_text(mapping.get("project_key"))
    mapping_scope_code = normalize_scope_code(mapping.get("scope_code"))
    project_score = 2 if mapping_project_key and mapping_project_key == project_key else 0
    scope_score = 1 if mapping_scope_code and mapping_scope_code == scope_code else 0
    specificity = (1 if mapping_project_key else 0) + (1 if mapping_scope_code else 0)
    return (project_score + scope_score, specificity, int(mapping.get("id") or 0))


def _resolve_effective_owner(conn, *, owner_role: str | None, project_key: str | None = None, scope_code: str | None = None) -> dict[str, Any]:
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    base = {
        "owner_role": normalized_owner_role,
        "effective_owner_key": None,
        "effective_owner_type": None,
        "effective_display_name": None,
        "effective_owner_active": False,
        "owner_resolution_reason": None,
        "owner_mapping": None,
    }
    if normalized_owner_role is None:
        base["owner_resolution_reason"] = "no_owner_role"
        return base

    rows = conn.execute(
        "SELECT * FROM owner_role_mappings WHERE owner_role = ? AND is_active = 1 ORDER BY id ASC",
        (normalized_owner_role,),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        mapping = _owner_role_mapping_to_dict(row)
        mapping_project_key = normalize_optional_text(mapping.get("project_key"))
        mapping_scope_code = normalize_scope_code(mapping.get("scope_code"))
        if mapping_project_key is not None and mapping_project_key != normalized_project_key:
            continue
        if mapping_scope_code is not None and mapping_scope_code != normalized_scope_code:
            continue
        candidates.append(mapping)
    if not candidates:
        base["owner_resolution_reason"] = "no_mapping"
        return base

    selected = sorted(candidates, key=lambda item: _owner_mapping_rank(item, project_key=normalized_project_key, scope_code=normalized_scope_code), reverse=True)[0]
    owner_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (selected["owner_key"],)).fetchone()
    if owner_row is None:
        base["owner_resolution_reason"] = "owner_missing_in_directory"
        base["owner_mapping"] = selected
        return base
    owner_item = _owner_directory_item_to_dict(owner_row)
    base.update(
        {
            "effective_owner_key": owner_item["owner_key"],
            "effective_owner_type": owner_item["owner_type"],
            "effective_display_name": owner_item["display_name"],
            "effective_owner_active": bool(owner_item["is_active"]),
            "owner_mapping": selected,
            "owner_resolution_reason": "resolved" if bool(owner_item["is_active"]) else "owner_inactive",
        }
    )
    return base


def _apply_effective_owner(conn, item: dict[str, Any], *, owner_field: str | None = None) -> dict[str, Any]:
    result = dict(item)
    target = result if owner_field is None else result.get(owner_field)
    if not isinstance(target, dict):
        return result
    resolution = _resolve_effective_owner(
        conn,
        owner_role=target.get("owner_role"),
        project_key=target.get("project_key"),
        scope_code=target.get("scope_code"),
    )
    target.update(resolution)
    return result


def _duplicate_review_item_to_dict(row) -> dict[str, Any]:
    item = row_to_dict(row)
    item["canonical_memory_id"] = int(item["canonical_memory_id"])
    item["duplicate_memory_id"] = int(item["duplicate_memory_id"])
    return item


def _get_or_create_duplicate_review_item(conn, canonical_memory_id: int, duplicate_memory_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM duplicate_review_items WHERE canonical_memory_id = ? AND duplicate_memory_id = ?",
        (int(canonical_memory_id), int(duplicate_memory_id)),
    ).fetchone()
    if row is None:
        now_iso = utc_now_iso()
        conn.execute(
            """
            INSERT INTO duplicate_review_items (
                canonical_memory_id, duplicate_memory_id, owner_role, owner_id, duplicate_due_at, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'open', ?, ?)
            """,
            (int(canonical_memory_id), int(duplicate_memory_id), "memory_maintainer", None, utc_offset_days_iso(_compute_sla_days(conn, "duplicate")), now_iso, now_iso),
        )
        row = conn.execute(
            "SELECT * FROM duplicate_review_items WHERE canonical_memory_id = ? AND duplicate_memory_id = ?",
            (int(canonical_memory_id), int(duplicate_memory_id)),
        ).fetchone()
    return _duplicate_review_item_to_dict(row)


def _normalize_feature_flag_key(flag_key: str) -> str:
    return normalize_required_text(flag_key, "flag_key").lower().replace("-", "_").replace(" ", "_")


def _normalize_rollout_mode(rollout_mode: str | None) -> str:
    value = normalize_optional_text(rollout_mode) or "all"
    value = value.lower().replace("-", "_").replace(" ", "_")
    if value not in FEATURE_FLAG_ROLLOUT_MODES:
        raise ValueError(f"rollout_mode musi by? jednym z: {', '.join(sorted(FEATURE_FLAG_ROLLOUT_MODES))}")
    return value


def _normalize_csv_tokens(value: str | None, *, normalizer=None) -> list[str]:
    normalized_value = normalize_optional_text(value)
    if normalized_value is None:
        return []
    items: list[str] = []
    seen: set[str] = set()
    for raw_part in normalized_value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        token = normalizer(part) if normalizer is not None else normalize_required_text(part, "csv_token")
        if token not in seen:
            seen.add(token)
            items.append(token)
    return items


def _serialize_csv_tokens(tokens: list[str]) -> str | None:
    return None if not tokens else ",".join(tokens)


def _feature_flag_to_dict(row) -> dict[str, Any]:
    if row is None:
        return {
            "flag_key": CROSS_PROJECT_FLAG_KEY,
            "is_enabled": 0,
            "rollout_mode": "off",
            "allowed_project_keys": None,
            "allowed_scope_codes": None,
            "read_only_mode": 0,
            "notes": "Implicit default rollout disabled",
            "updated_at": None,
            "is_implicit_default": True,
        }
    item = row_to_dict(row)
    item["is_enabled"] = int(item.get("is_enabled") or 0)
    item["read_only_mode"] = int(item.get("read_only_mode") or 0)
    item["rollout_mode"] = _normalize_rollout_mode(item.get("rollout_mode"))
    item["flag_key"] = _normalize_feature_flag_key(item.get("flag_key"))
    item["is_implicit_default"] = False
    return item


def _get_feature_flag_config(conn, flag_key: str) -> dict[str, Any]:
    normalized_flag_key = _normalize_feature_flag_key(flag_key)
    row = conn.execute("SELECT * FROM feature_flags WHERE flag_key = ?", (normalized_flag_key,)).fetchone()
    item = _feature_flag_to_dict(row)
    item["flag_key"] = normalized_flag_key
    return item


def _evaluate_feature_flag_config(flag: dict[str, Any], *, project_key: str | None = None, scope_code: str | None = None) -> dict[str, Any]:
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    rollout_mode = _normalize_rollout_mode(flag.get("rollout_mode"))
    allowed_project_keys = _normalize_csv_tokens(flag.get("allowed_project_keys"))
    allowed_scope_codes = _normalize_csv_tokens(flag.get("allowed_scope_codes"), normalizer=normalize_scope_code)
    is_enabled = bool(int(flag.get("is_enabled") or 0))
    read_only_mode = bool(int(flag.get("read_only_mode") or 0))

    matches_project = True if rollout_mode in {"all", "off", "scopes"} else bool(normalized_project_key and normalized_project_key in allowed_project_keys)
    matches_scope = True if rollout_mode in {"all", "off", "projects"} else bool(normalized_scope_code and normalized_scope_code in allowed_scope_codes)

    if not is_enabled:
        enabled = False
        reason = "flag_disabled"
    elif rollout_mode == "off":
        enabled = False
        reason = "rollout_off"
    elif rollout_mode == "all":
        enabled = True
        reason = "rollout_all"
    elif rollout_mode == "projects":
        enabled = matches_project
        reason = "project_allowed" if enabled else "project_not_allowed"
    elif rollout_mode == "scopes":
        enabled = matches_scope
        reason = "scope_allowed" if enabled else "scope_not_allowed"
    else:
        enabled = matches_project and matches_scope
        reason = "project_and_scope_allowed" if enabled else "project_or_scope_not_allowed"

    return {
        "flag_key": flag["flag_key"],
        "enabled": enabled,
        "read_only_mode": read_only_mode,
        "reason": reason,
        "project_key": normalized_project_key,
        "scope_code": normalized_scope_code,
        "rollout_mode": rollout_mode,
        "allowed_project_keys": allowed_project_keys,
        "allowed_scope_codes": allowed_scope_codes,
        "is_implicit_default": bool(flag.get("is_implicit_default")),
    }


def _require_feature_flag_write_access(conn, *, flag_key: str, project_key: str | None, scope_code: str | None, operation_name: str) -> dict[str, Any]:
    flag = _get_feature_flag_config(conn, flag_key)
    evaluation = _evaluate_feature_flag_config(flag, project_key=project_key, scope_code=scope_code)
    if not evaluation["enabled"]:
        raise ValueError(f"Feature flag {flag_key} blokuje operacj? {operation_name}: {evaluation['reason']}")
    if evaluation["read_only_mode"]:
        raise ValueError(f"Feature flag {flag_key} jest w trybie read-only. Operacja {operation_name} jest zablokowana")
    return evaluation


def _is_conflict_feature_active(conn, flag_key: str) -> bool:
    """Returns True if the conflict feature flag is enabled.

    Defaults to True when the flag has no record yet — conflict tools are opt-out, not opt-in.
    """
    row = conn.execute(
        "SELECT is_enabled, rollout_mode FROM feature_flags WHERE flag_key = ?",
        (flag_key,),
    ).fetchone()
    if row is None:
        return True
    is_enabled = bool(int(row[0] or 0))
    rollout_mode = str(row[1] or "off").strip().lower()
    return is_enabled and rollout_mode != "off"


def _is_multiuser_feature_active(conn, flag_key: str) -> bool:
    """Returns True if a multiuser feature flag is enabled.

    Defaults to True when the flag has no record yet — multiuser tools are opt-out, not opt-in.
    Reuses the same logic as _is_conflict_feature_active but with a descriptive name
    for multi-user feature gates.
    """
    row = conn.execute(
        "SELECT is_enabled, rollout_mode FROM feature_flags WHERE flag_key = ?",
        (flag_key,),
    ).fetchone()
    if row is None:
        return True
    is_enabled = bool(int(row[0] or 0))
    rollout_mode = str(row[1] or "off").strip().lower()
    return is_enabled and rollout_mode != "off"


MULTIUSER_IDENTITY_FLAG = "multiuser_identity_enabled"
MULTIUSER_SCOPE_RETRIEVAL_FLAG = "multiuser_scope_retrieval_enabled"
MULTIUSER_TIMELINE_ACTOR_FLAG = "multiuser_timeline_actor_enabled"
MULTIUSER_SCOPE_MAINTENANCE_FLAG = "multiuser_scope_maintenance_enabled"
MULTIUSER_SCOPE_PROMOTION_FLAG = "multiuser_scope_promotion_enabled"


_OWNER_KEY_ALLOWED_PREFIXES = ("global_", "project_", "workspace_")
_OWNER_KEY_FORBIDDEN_BOOTSTRAP = {"memory_maintainer", "knowledge_curator", "review_team", "project_maintainer"}
_OWNER_TYPE_ALLOWED_VALUES = {"team", "person", "alias", "system"}


def _owner_key_governance_warnings(owner_key: str) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if owner_key in _OWNER_KEY_FORBIDDEN_BOOTSTRAP:
        warnings.append({"kind": "bootstrap_owner_key", "severity": "high", "message": "owner_key must not be a raw owner role"})
    if not owner_key.startswith(_OWNER_KEY_ALLOWED_PREFIXES):
        warnings.append({"kind": "invalid_owner_key_format", "severity": "medium", "message": "owner_key should start with global_, project_, or workspace_"})
    if owner_key.lower() != owner_key or " " in owner_key:
        warnings.append({"kind": "invalid_owner_key_format", "severity": "medium", "message": "owner_key should be lowercase snake_case without spaces"})
    return warnings


def _owner_metadata_governance_warnings(owner_key: str, owner_type: str, routing_metadata_json: str | None) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if owner_type not in _OWNER_TYPE_ALLOWED_VALUES:
        warnings.append({"kind": "invalid_owner_type", "severity": "medium", "message": "owner_type should be one of: team, person, alias, system"})
    if routing_metadata_json is None:
        warnings.append({"kind": "missing_routing_metadata", "severity": "medium", "message": "active owner should have routing_metadata_json"})
        return warnings
    try:
        metadata = json.loads(routing_metadata_json)
    except json.JSONDecodeError:
        warnings.append({"kind": "invalid_routing_metadata_json", "severity": "high", "message": "routing_metadata_json must be valid JSON"})
        return warnings
    if not isinstance(metadata, dict):
        warnings.append({"kind": "invalid_routing_metadata_json", "severity": "high", "message": "routing_metadata_json must decode to an object"})
        return warnings
    if owner_key.startswith("global_"):
        for required_key in ["domain", "tier", "scope"]:
            if normalize_optional_text(metadata.get(required_key)) is None:
                warnings.append({"kind": "missing_routing_metadata", "severity": "medium", "message": f"global owner metadata should include {required_key}"})
        if normalize_optional_text(metadata.get("scope")) != "global":
            warnings.append({"kind": "metadata_scope_mismatch", "severity": "medium", "message": "global owner metadata scope should be global"})
    if owner_key.startswith("project_"):
        for required_key in ["domain", "project_key", "scope"]:
            if normalize_optional_text(metadata.get(required_key)) is None:
                warnings.append({"kind": "missing_routing_metadata", "severity": "medium", "message": f"project owner metadata should include {required_key}"})
        if normalize_optional_text(metadata.get("scope")) != "project":
            warnings.append({"kind": "metadata_scope_mismatch", "severity": "medium", "message": "project owner metadata scope should be project"})
    return warnings


def _owner_directory_governance_warnings(owner_key: str, owner_type: str, routing_metadata_json: str | None, *, is_active: bool) -> list[dict[str, Any]]:
    normalized_owner_key = normalize_optional_text(owner_key) or ""
    if not bool(is_active) and normalized_owner_key in _OWNER_KEY_FORBIDDEN_BOOTSTRAP:
        return []
    warnings = _owner_key_governance_warnings(owner_key)
    if is_active:
        warnings.extend(_owner_metadata_governance_warnings(owner_key, owner_type, routing_metadata_json))
    return warnings



def _owner_deactivation_guardrail_warnings(conn, owner_key: str, *, requested_is_active: bool) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    normalized_owner_key = normalize_optional_text(owner_key)
    if normalized_owner_key is None or requested_is_active:
        return warnings
    mapping_rows = conn.execute(
        """
        SELECT * FROM owner_role_mappings
        WHERE owner_key = ? AND is_active = 1
        ORDER BY owner_role ASC, COALESCE(project_key, ''), COALESCE(scope_code, ''), id ASC
        """,
        (normalized_owner_key,),
    ).fetchall()
    mappings = [_owner_role_mapping_to_dict(row) for row in mapping_rows]
    if mappings:
        warnings.append({
            "kind": "unsafe_deactivation_candidate",
            "severity": "high",
            "message": "owner target is still used by active owner role mappings",
            "active_mapping_count": len(mappings),
            "active_mapping_ids": [int(item.get("id") or 0) for item in mappings],
            "active_mappings": mappings,
            "recommended_action": "remap active mappings before deactivation",
        })
    return warnings



def _owner_mapping_governance_warnings(
    conn,
    *,
    owner_role: str,
    owner_key: str,
    project_key: str | None,
    scope_code: str | None,
    is_active: bool,
    current_mapping_id: int | None = None,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_owner_key = normalize_optional_text(owner_key)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)

    if normalized_owner_key is None:
        warnings.append({"kind": "missing_owner_target", "severity": "high", "message": "owner mapping must point to owner_key"})
        return warnings

    owner_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (normalized_owner_key,)).fetchone()
    if owner_row is None:
        warnings.append({"kind": "missing_owner_target", "severity": "high", "message": "owner mapping points to missing owner_key"})
    else:
        owner_item = _owner_directory_item_to_dict(owner_row)
        if is_active and not bool(owner_item.get("is_active")):
            warnings.append({"kind": "inactive_owner_target", "severity": "high", "message": "active owner mapping points to inactive owner target"})
        warnings.extend(_owner_directory_governance_warnings(
            str(owner_item.get("owner_key") or ""),
            str(owner_item.get("owner_type") or ""),
            normalize_optional_text(owner_item.get("routing_metadata_json")),
            is_active=bool(owner_item.get("is_active")),
        ))
        if normalized_project_key is not None:
            metadata_json = normalize_optional_text(owner_item.get("routing_metadata_json"))
            metadata_project_key = None
            if metadata_json is not None:
                try:
                    metadata = json.loads(metadata_json)
                    if isinstance(metadata, dict):
                        metadata_project_key = normalize_optional_text(metadata.get("project_key"))
                except json.JSONDecodeError:
                    metadata_project_key = None
            if str(owner_item.get("owner_key") or "").startswith("project_") and metadata_project_key is not None and metadata_project_key != normalized_project_key:
                warnings.append({"kind": "project_owner_metadata_mismatch", "severity": "medium", "message": "project owner metadata project_key does not match mapping project_key"})

    if is_active and normalized_owner_role is not None:
        rows = conn.execute(
            """
            SELECT * FROM owner_role_mappings
            WHERE owner_role = ?
              AND is_active = 1
              AND COALESCE(project_key, '') = COALESCE(?, '')
              AND COALESCE(scope_code, '') = COALESCE(?, '')
            ORDER BY id ASC
            """,
            (normalized_owner_role, normalized_project_key, normalized_scope_code),
        ).fetchall()
        conflicting = []
        for row in rows:
            mapping = _owner_role_mapping_to_dict(row)
            mapping_id = int(mapping.get("id") or 0)
            if current_mapping_id is not None and mapping_id == int(current_mapping_id):
                continue
            if normalize_optional_text(mapping.get("owner_key")) != normalized_owner_key:
                conflicting.append(mapping)
        if conflicting:
            warnings.append({
                "kind": "ambiguous_owner_role_mapping",
                "severity": "high",
                "message": "multiple active mappings for the same owner_role/project/scope point to different targets",
                "conflicting_mapping_ids": [int(item.get("id") or 0) for item in conflicting],
            })

    if is_active and normalized_project_key is None and normalized_scope_code is not None:
        warnings.append({"kind": "scope_without_project_mapping", "severity": "low", "message": "scope-specific mapping without project_key should be intentional"})

    return warnings



def _resolve_workspace_id(conn, workspace_key: str) -> int:
    """Resolves workspace_key → workspace.id. Raises ValueError if not found."""
    row = conn.execute(
        "SELECT id FROM workspaces WHERE workspace_key = ?",
        (workspace_key.strip(),),
    ).fetchone()
    if row is None:
        raise ValueError(f"Workspace '{workspace_key}' nie istnieje")
    return int(row["id"])


# Ordered scope hierarchy for promotion validation (most restricted → least restricted)
_SCOPE_ORDER = ["private", "project", "workspace"]


@mcp.tool
def list_owner_directory_items(owner_type: str | None = None, active_only: bool = False) -> dict[str, Any]:
    normalized_owner_type = normalize_optional_text(owner_type)
    sql = "SELECT * FROM owner_directory_items WHERE 1 = 1"
    params: list[Any] = []
    if normalized_owner_type:
        sql += " AND owner_type = ?"
        params.append(normalized_owner_type)
    if active_only:
        sql += " AND is_active = 1"
    sql += " ORDER BY owner_key ASC"
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return {"count": len(rows), "items": [_owner_directory_item_to_dict(row) for row in rows], "filters": {"owner_type": normalized_owner_type, "active_only": bool(active_only)}}


@mcp.tool
def upsert_owner_directory_item(
    owner_key: str,
    owner_type: str,
    display_name: str,
    is_active: bool = True,
    routing_metadata_json: str | None = None,
    allow_unsafe_deactivation: bool = False,
) -> dict[str, Any]:
    normalized_owner_key = normalize_required_text(owner_key, "owner_key")
    normalized_owner_type = normalize_required_text(owner_type, "owner_type")
    normalized_display_name = normalize_required_text(display_name, "display_name")
    normalized_routing_metadata_json = normalize_optional_text(routing_metadata_json)
    if normalized_routing_metadata_json is not None:
        json.loads(normalized_routing_metadata_json)
    now_iso = utc_now_iso()
    conn = get_db_connection()
    try:
        preflight_warnings = _owner_deactivation_guardrail_warnings(
            conn,
            normalized_owner_key,
            requested_is_active=bool(is_active),
        )
        if preflight_warnings and not bool(allow_unsafe_deactivation):
            first_warning = preflight_warnings[0]
            active_mapping_ids = first_warning.get("active_mapping_ids") or []
            raise ValueError(
                "Unsafe owner deactivation blocked: active owner role mappings still reference "
                f"{normalized_owner_key}; active_mapping_ids={active_mapping_ids}. "
                "Pass allow_unsafe_deactivation=True only after remap/approval."
            )
        prev_row = conn.execute(
            "SELECT is_active FROM owner_directory_items WHERE owner_key = ?",
            (normalized_owner_key,),
        ).fetchone()
        if prev_row is None:
            change_kind = "created"
        elif not bool(is_active) and bool(prev_row["is_active"]):
            change_kind = "deactivated"
        elif bool(is_active) and not bool(prev_row["is_active"]):
            change_kind = "reactivated"
        else:
            change_kind = "updated"
        conn.execute(
            """
            INSERT INTO owner_directory_items (owner_key, owner_type, display_name, is_active, routing_metadata_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_key) DO UPDATE SET
                owner_type = excluded.owner_type,
                display_name = excluded.display_name,
                is_active = excluded.is_active,
                routing_metadata_json = excluded.routing_metadata_json,
                updated_at = excluded.updated_at
            """,
            (normalized_owner_key, normalized_owner_type, normalized_display_name, 1 if is_active else 0, normalized_routing_metadata_json, now_iso, now_iso),
        )
        row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (normalized_owner_key,)).fetchone()
        item = _owner_directory_item_to_dict(row)
        warnings = _owner_directory_governance_warnings(
            item["owner_key"],
            item["owner_type"],
            normalize_optional_text(item.get("routing_metadata_json")),
            is_active=bool(item.get("is_active")),
        )
        warnings.extend(preflight_warnings)
        audit_event_id = timeline.record_project_event(
            conn,
            project_key="global_owner_catalog",
            event_type="project.note_recorded",
            title=f"Owner catalog change: {normalized_owner_key} {change_kind}",
            description=(
                f"owner_key={normalized_owner_key}; change_kind={change_kind}; "
                f"owner_type={normalized_owner_type}; is_active={bool(is_active)}"
            ),
            origin="system",
            tags=["owner_directory_change", change_kind],
            status="completed",
            canonical=True,
            category="owner_directory_change",
            now_fn=utc_now_iso,
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "upserted",
        "owner_directory_item": item,
        "governance_warnings": warnings,
        "governance_warning_count": len(warnings),
        "audit_event": {"id": audit_event_id, "event_type": "project.note_recorded"},
    }


@mcp.tool
def list_owner_role_mappings(
    owner_role: str | None = None,
    project_key: str | None = None,
    scope_code: str | None = None,
    active_only: bool = False,
) -> dict[str, Any]:
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    sql = "SELECT * FROM owner_role_mappings WHERE 1 = 1"
    params: list[Any] = []
    if normalized_owner_role:
        sql += " AND owner_role = ?"
        params.append(normalized_owner_role)
    if normalized_project_key is not None:
        sql += " AND project_key = ?"
        params.append(normalized_project_key)
    if normalized_scope_code is not None:
        sql += " AND scope_code = ?"
        params.append(normalized_scope_code)
    if active_only:
        sql += " AND is_active = 1"
    sql += " ORDER BY owner_role ASC, COALESCE(project_key, ''), COALESCE(scope_code, ''), id ASC"
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return {"count": len(rows), "items": [_owner_role_mapping_to_dict(row) for row in rows], "filters": {"owner_role": normalized_owner_role, "project_key": normalized_project_key, "scope_code": normalized_scope_code, "active_only": bool(active_only)}}


@mcp.tool
def upsert_owner_role_mapping(
    owner_role: str,
    owner_key: str,
    project_key: str | None = None,
    scope_code: str | None = None,
    is_active: bool = True,
    notes: str | None = None,
) -> dict[str, Any]:
    normalized_owner_role = normalize_required_text(owner_role, "owner_role")
    normalized_owner_key = normalize_required_text(owner_key, "owner_key")
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    normalized_notes = normalize_optional_text(notes)
    now_iso = utc_now_iso()
    conn = get_db_connection()
    try:
        owner_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (normalized_owner_key,)).fetchone()
        if owner_row is None:
            raise FileNotFoundError(f"Owner directory item not found: {normalized_owner_key}")
        prev_mapping_row = conn.execute(
            "SELECT is_active FROM owner_role_mappings "
            "WHERE owner_role = ? AND project_key IS ? AND scope_code IS ?",
            (normalized_owner_role, normalized_project_key, normalized_scope_code),
        ).fetchone()
        if prev_mapping_row is None:
            mapping_change_kind = "created"
        elif not bool(is_active) and bool(prev_mapping_row["is_active"]):
            mapping_change_kind = "deactivated"
        elif bool(is_active) and not bool(prev_mapping_row["is_active"]):
            mapping_change_kind = "reactivated"
        else:
            mapping_change_kind = "updated"
        conn.execute(
            """
            INSERT INTO owner_role_mappings (owner_role, owner_key, project_key, scope_code, is_active, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_role, project_key, scope_code) DO UPDATE SET
                owner_key = excluded.owner_key,
                is_active = excluded.is_active,
                notes = excluded.notes,
                updated_at = excluded.updated_at
            """,
            (normalized_owner_role, normalized_owner_key, normalized_project_key, normalized_scope_code, 1 if is_active else 0, normalized_notes, now_iso, now_iso),
        )
        row = conn.execute(
            "SELECT * FROM owner_role_mappings WHERE owner_role = ? AND project_key IS ? AND scope_code IS ?",
            (normalized_owner_role, normalized_project_key, normalized_scope_code),
        ).fetchone()
        mapping_item = _owner_role_mapping_to_dict(row)
        warnings = _owner_mapping_governance_warnings(
            conn,
            owner_role=mapping_item["owner_role"],
            owner_key=mapping_item["owner_key"],
            project_key=mapping_item.get("project_key"),
            scope_code=mapping_item.get("scope_code"),
            is_active=bool(mapping_item.get("is_active")),
            current_mapping_id=int(mapping_item.get("id") or 0),
        )
        mapping_audit_event_id = timeline.record_project_event(
            conn,
            project_key=_owner_catalog_audit_project_key(normalized_project_key),
            event_type="project.note_recorded",
            title=f"Owner mapping {mapping_change_kind}: {normalized_owner_role}",
            description=(
                f"owner_key={normalized_owner_key}; owner_role={normalized_owner_role}; "
                f"project_key={normalized_project_key}; scope_code={normalized_scope_code}; "
                f"change_kind={mapping_change_kind}; is_active={bool(is_active)}"
            ),
            origin="system",
            tags=["owner_role_mapping_change", mapping_change_kind],
            status="completed",
            canonical=True,
            category="owner_role_mapping_change",
            now_fn=utc_now_iso,
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "upserted",
        "owner_role_mapping": mapping_item,
        "governance_warnings": warnings,
        "governance_warning_count": len(warnings),
        "audit_event": {"id": mapping_audit_event_id, "event_type": "project.note_recorded"},
    }


@mcp.tool
def list_feature_flags() -> dict[str, Any]:
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT * FROM feature_flags ORDER BY flag_key ASC").fetchall()
    finally:
        conn.close()
    items = []
    rollout_mode_aliases = {"all": "global", "projects": "project", "scopes": "scope", "projects_and_scopes": "scoped_project", "off": "off"}
    for row in rows:
        item = _feature_flag_to_dict(row)
        compatibility_item = dict(item)
        compatibility_item["key"] = compatibility_item.get("flag_key")
        compatibility_item["enabled"] = bool(int(compatibility_item.get("is_enabled") or 0))
        compatibility_item["rollout_scope"] = compatibility_item.get("allowed_scope_codes")
        compatibility_item["rollout_project_key"] = compatibility_item.get("allowed_project_keys")
        compatibility_item["rollout_mode"] = rollout_mode_aliases.get(str(compatibility_item.get("rollout_mode") or "off"), compatibility_item.get("rollout_mode"))
        items.append(compatibility_item)
    return {"count": len(items), "items": items}


@mcp.tool
def get_feature_flag(flag_key: str) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        flag = _get_feature_flag_config(conn, flag_key)
    finally:
        conn.close()
    compatibility_flag = dict(flag)
    compatibility_flag["key"] = compatibility_flag.get("flag_key")
    compatibility_flag["enabled"] = bool(int(compatibility_flag.get("is_enabled") or 0))
    compatibility_flag["rollout_scope"] = compatibility_flag.get("allowed_scope_codes")
    compatibility_flag["rollout_project_key"] = compatibility_flag.get("allowed_project_keys")
    rollout_mode_aliases = {"all": "global", "projects": "project", "scopes": "scope", "projects_and_scopes": "scoped_project", "off": "off"}
    compatibility_flag["rollout_mode"] = rollout_mode_aliases.get(str(compatibility_flag.get("rollout_mode") or "off"), compatibility_flag.get("rollout_mode"))
    return {"feature_flag": compatibility_flag}


@mcp.tool
def upsert_feature_flag(
    flag_key: str,
    is_enabled: bool = True,
    rollout_mode: str = "all",
    allowed_project_keys: str | None = None,
    allowed_scope_codes: str | None = None,
    read_only_mode: bool = False,
    notes: str | None = None,
) -> dict[str, Any]:
    normalized_flag_key = _normalize_feature_flag_key(flag_key)
    normalized_rollout_mode = _normalize_rollout_mode(rollout_mode)
    serialized_project_keys = _serialize_csv_tokens(_normalize_csv_tokens(allowed_project_keys))
    serialized_scope_codes = _serialize_csv_tokens(_normalize_csv_tokens(allowed_scope_codes, normalizer=normalize_scope_code))
    normalized_notes = normalize_optional_text(notes)
    updated_at = utc_now_iso()

    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO feature_flags (
                flag_key, is_enabled, rollout_mode, allowed_project_keys, allowed_scope_codes, read_only_mode, notes, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(flag_key) DO UPDATE SET
                is_enabled = excluded.is_enabled,
                rollout_mode = excluded.rollout_mode,
                allowed_project_keys = excluded.allowed_project_keys,
                allowed_scope_codes = excluded.allowed_scope_codes,
                read_only_mode = excluded.read_only_mode,
                notes = excluded.notes,
                updated_at = excluded.updated_at
            """,
            (
                normalized_flag_key,
                1 if bool(is_enabled) else 0,
                normalized_rollout_mode,
                serialized_project_keys,
                serialized_scope_codes,
                1 if bool(read_only_mode) else 0,
                normalized_notes,
                updated_at,
            ),
        )
        conn.commit()
        flag = _get_feature_flag_config(conn, normalized_flag_key)
    finally:
        conn.close()
    return {"status": "upserted", "feature_flag": flag}


@mcp.tool
def evaluate_feature_flag(flag_key: str, project_key: str | None = None, scope_code: str | None = None) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        flag = _get_feature_flag_config(conn, flag_key)
    finally:
        conn.close()
    evaluation = _evaluate_feature_flag_config(flag, project_key=project_key, scope_code=scope_code)
    compatibility_flag = dict(flag)
    compatibility_flag["key"] = compatibility_flag.get("flag_key")
    compatibility_flag["enabled"] = bool(int(compatibility_flag.get("is_enabled") or 0))
    compatibility_flag["rollout_scope"] = compatibility_flag.get("allowed_scope_codes")
    compatibility_flag["rollout_project_key"] = compatibility_flag.get("allowed_project_keys")
    rollout_mode_aliases = {"all": "global", "projects": "project", "scopes": "scope", "projects_and_scopes": "scoped_project", "off": "off"}
    compatibility_flag["rollout_mode"] = rollout_mode_aliases.get(str(compatibility_flag.get("rollout_mode") or "off"), compatibility_flag.get("rollout_mode"))
    return {
        "feature_flag": compatibility_flag,
        "evaluation": evaluation,
        "enabled": evaluation["enabled"],
        "project_key": evaluation["project_key"],
        "scope_code": evaluation["scope_code"],
        "key": evaluation["flag_key"],
    }


@mcp.tool
def set_feature_flag(
    key: str,
    enabled: bool,
    rollout_mode: str = "all",
    rollout_scope: str | None = None,
    rollout_project_key: str | None = None,
    description: str | None = None,
    read_only_mode: bool = False,
) -> dict[str, Any]:
    legacy_mode = normalize_required_text(rollout_mode, "rollout_mode").lower().replace("-", "_").replace(" ", "_")
    rollout_mode_map = {
        "off": "off",
        "global": "all",
        "all": "all",
        "scope": "scopes",
        "scopes": "scopes",
        "project": "projects",
        "projects": "projects",
        "scoped_project": "projects_and_scopes",
        "projects_and_scopes": "projects_and_scopes",
        "read_only": "all",
    }
    normalized_rollout_mode = rollout_mode_map.get(legacy_mode)
    if normalized_rollout_mode is None:
        raise ValueError("rollout_mode must be one of: off, global, all, scope, scopes, project, projects, scoped_project, projects_and_scopes, read_only")

    effective_read_only = bool(read_only_mode or legacy_mode == "read_only")
    result = upsert_feature_flag(
        flag_key=key,
        is_enabled=enabled,
        rollout_mode=normalized_rollout_mode,
        allowed_project_keys=rollout_project_key,
        allowed_scope_codes=rollout_scope,
        read_only_mode=effective_read_only,
        notes=description,
    )
    compatibility_flag = dict(result["feature_flag"])
    compatibility_flag["key"] = compatibility_flag.get("flag_key")
    compatibility_flag["enabled"] = bool(int(compatibility_flag.get("is_enabled") or 0))
    compatibility_flag["rollout_scope"] = compatibility_flag.get("allowed_scope_codes")
    compatibility_flag["rollout_project_key"] = compatibility_flag.get("allowed_project_keys")
    rollout_mode_aliases = {"all": "global", "projects": "project", "scopes": "scope", "projects_and_scopes": "scoped_project", "off": "off"}
    compatibility_flag["rollout_mode"] = rollout_mode_aliases.get(str(compatibility_flag.get("rollout_mode") or "off"), compatibility_flag.get("rollout_mode"))
    return {"status": "updated", "feature_flag": compatibility_flag}


@mcp.tool
def get_root() -> dict[str, Any]:
    _sync_config()
    return {"root": str(config.ROOT), "exists": config.ROOT.exists(), "is_dir": config.ROOT.is_dir()}


@mcp.tool
def list_dir(path: str = ".") -> dict[str, Any]:
    target = safe_path(path)
    if not target.exists():
        raise FileNotFoundError(f"Nie istnieje: {path}")
    if not target.is_dir():
        raise NotADirectoryError(f"To nie jest katalog: {path}")

    items: list[dict[str, Any]] = []
    for entry in sorted(target.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower())):
        stat = entry.stat()
        items.append({"name": entry.name, "path": rel_path(entry), "type": "directory" if entry.is_dir() else "file", "size": stat.st_size, "modified": stat.st_mtime, "mime": None if entry.is_dir() else guess_mime(entry)})
    return {"root": str(config.ROOT), "path": rel_path(target), "items": items}


@mcp.tool
def read_file_text(path: str, encoding: str = "utf-8", errors: str = "strict") -> dict[str, Any]:
    target = safe_path(path)
    if not target.exists():
        raise FileNotFoundError(f"Nie istnieje: {path}")
    if not target.is_file():
        raise FileNotFoundError(f"To nie jest plik: {path}")
    return {"path": rel_path(target), "absolute_path": str(target), "encoding": encoding, "mime": guess_mime(target), "content": target.read_text(encoding=encoding, errors=errors)}


@mcp.tool
def read_file_base64(path: str) -> dict[str, Any]:
    target = safe_path(path)
    if not target.exists():
        raise FileNotFoundError(f"Nie istnieje: {path}")
    if not target.is_file():
        raise FileNotFoundError(f"To nie jest plik: {path}")
    data = target.read_bytes()
    return {"path": rel_path(target), "absolute_path": str(target), "mime": guess_mime(target), "base64": base64.b64encode(data).decode("ascii"), "size": len(data)}


@mcp.tool
def write_file_text(path: str, content: str, encoding: str = "utf-8", create_parents: bool = True) -> dict[str, Any]:
    target = safe_path(path)
    if create_parents:
        target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding=encoding)
    return {"path": rel_path(target), "absolute_path": str(target), "written": True, "bytes": len(content.encode(encoding))}


@mcp.tool
def write_file_base64(path: str, base64_content: str, create_parents: bool = True) -> dict[str, Any]:
    target = safe_path(path)
    if create_parents:
        target.parent.mkdir(parents=True, exist_ok=True)
    data = base64.b64decode(base64_content)
    target.write_bytes(data)
    return {"path": rel_path(target), "absolute_path": str(target), "written": True, "bytes": len(data)}


@mcp.tool
def append_file_text(path: str, content: str, encoding: str = "utf-8", create_parents: bool = True) -> dict[str, Any]:
    target = safe_path(path)
    if create_parents:
        target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding=encoding, newline="") as handle:
        handle.write(content)
    return {"path": rel_path(target), "absolute_path": str(target), "appended": True, "bytes": len(content.encode(encoding))}


@mcp.tool
def make_dir(path: str, parents: bool = True, exist_ok: bool = True) -> dict[str, Any]:
    target = safe_path(path)
    target.mkdir(parents=parents, exist_ok=exist_ok)
    return {"path": rel_path(target), "absolute_path": str(target), "created": True}


@mcp.tool
def move_path(src: str, dst: str, create_parents: bool = True) -> dict[str, Any]:
    source = safe_path(src)
    target = safe_path(dst)
    if not source.exists():
        raise FileNotFoundError(f"Nie istnieje: {src}")
    if create_parents:
        target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(target))
    return {"source": rel_path(source), "target": rel_path(target), "moved": True}


@mcp.tool
def delete_path(path: str, recursive: bool = True) -> dict[str, Any]:
    target = safe_path(path)
    if target == config.ROOT:
        raise ValueError("Nie usuwam katalogu głównego C:\\jagoda-memory-api")
    if not target.exists():
        raise FileNotFoundError(f"Nie istnieje: {path}")
    if target.is_dir():
        if recursive:
            shutil.rmtree(target)
        else:
            target.rmdir()
        kind = "directory"
    else:
        target.unlink()
        kind = "file"
    return {"path": rel_path(target), "deleted": True, "type": kind}


@mcp.tool
def stat_path(path: str = ".") -> dict[str, Any]:
    target = safe_path(path)
    if not target.exists():
        raise FileNotFoundError(f"Nie istnieje: {path}")
    stat = target.stat()
    return {"path": rel_path(target), "absolute_path": str(target), "exists": True, "is_file": target.is_file(), "is_dir": target.is_dir(), "size": stat.st_size, "created": stat.st_ctime, "modified": stat.st_mtime, "mime": None if target.is_dir() else guess_mime(target)}


@mcp.tool
def search_text(query: str, path: str = ".", case_sensitive: bool = False, max_results: int = 100) -> dict[str, Any]:
    if not query:
        raise ValueError("query nie może być puste")
    start = safe_path(path)
    if not start.exists():
        raise FileNotFoundError(f"Nie istnieje: {path}")
    needle = query if case_sensitive else query.lower()
    results: list[dict[str, Any]] = []
    candidates = [start] if start.is_file() else list(start.rglob("*"))
    for file_path in candidates:
        if len(results) >= max_results:
            break
        if not file_path.is_file():
            continue
        try:
            with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
                for line_no, line in enumerate(handle, start=1):
                    haystack = line if case_sensitive else line.lower()
                    if needle in haystack:
                        results.append({"path": rel_path(file_path), "line": line_no, "text": line.rstrip("\n")})
                        if len(results) >= max_results:
                            break
        except OSError:
            continue
    return {"query": query, "path": rel_path(start), "count": len(results), "results": results}


@mcp.tool
def get_db_info() -> dict[str, Any]:
    conn = get_db_connection()
    try:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").fetchall()
        memory_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        link_count = conn.execute("SELECT COUNT(*) AS count FROM memory_links").fetchone()["count"]
    finally:
        conn.close()
    return {"db_path": str(config.DB_PATH), "exists": config.DB_PATH.exists(), "size": config.DB_PATH.stat().st_size if config.DB_PATH.exists() else 0, "tables": [row["name"] for row in tables], "memory_count": memory_count, "link_count": link_count}


@mcp.tool
def query_sql(query: str, params_json: str = "[]", allow_write: bool = False, max_rows: int = 100) -> dict[str, Any]:
    sql = (query or "").strip()
    if not sql:
        raise ValueError("query nie może być puste")
    if max_rows < 1:
        raise ValueError("max_rows musi być >= 1")
    params = parse_params_json(params_json)
    if not allow_write and not is_read_only_sql(sql):
        raise ValueError("To zapytanie wygląda na modyfikujące dane. Ustaw allow_write=True, jeśli chcesz je wykonać.")
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        if cursor.description is None:
            conn.commit()
            return {"query": sql, "params": params, "allow_write": allow_write, "rowcount": cursor.rowcount, "lastrowid": cursor.lastrowid, "returned_rows": 0, "rows": []}
        fetched = cursor.fetchmany(max_rows + 1)
        truncated = len(fetched) > max_rows
        rows = fetched[:max_rows]
        return {"query": sql, "params": params, "allow_write": allow_write, "columns": [column[0] for column in cursor.description], "returned_rows": len(rows), "truncated": truncated, "rows": [row_to_dict(row) for row in rows]}
    finally:
        conn.close()


def _memory_order_clause(sort_by: str) -> str:
    order_map = {
        "active": "importance_score DESC, recall_count DESC, id DESC",
        "recent": "id DESC",
        "recalled": "recall_count DESC, importance_score DESC, id DESC",
        "validated": "COALESCE(last_validated_at, '') DESC, importance_score DESC, id DESC",
    }
    if sort_by not in order_map:
        raise ValueError(f"Nieobsługiwane sort_by: {sort_by}")
    return order_map[sort_by]


def _memory_query_parts(
    *,
    limit: int,
    min_importance: float,
    sort_by: str,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    state_code: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    parent_memory_id: int | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
    # --- Multi-user filters (Stage 1) ---
    visibility_scope: str | None = None,
    workspace_id: int | None = None,
    actor: ActorContext | None = None,
) -> tuple[str, list[Any], dict[str, Any]]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")

    sql = "SELECT * FROM memories WHERE importance_score >= ?"
    params: list[Any] = [float(min_importance)]

    normalized_memory_type = normalize_optional_text(memory_type)
    normalized_tag = normalize_optional_text(tag)
    normalized_text_query = normalize_optional_text(text_query)
    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_conversation_key = normalize_optional_text(conversation_key)
    normalized_layer_code = normalize_layer_code(layer_code)
    normalized_area_code = normalize_area_code(area_code)
    normalized_state_code = normalize_state_code(state_code)
    normalized_scope_code = normalize_scope_code(scope_code)
    normalized_visibility_scope = normalize_optional_text(visibility_scope)

    if normalized_memory_type:
        sql += " AND memory_type = ?"
        params.append(normalized_memory_type)
    if normalized_tag:
        sql += " AND COALESCE(tags, '') LIKE ?"
        params.append(f"%{normalized_tag}%")
    if normalized_text_query:
        sql += " AND (content LIKE ? OR COALESCE(summary_short, '') LIKE ? OR COALESCE(tags, '') LIKE ?)"
        like_value = f"%{normalized_text_query}%"
        params.extend([like_value, like_value, like_value])
    if normalized_layer_code:
        sql += " AND layer_code = ?"
        params.append(normalized_layer_code)
    if normalized_area_code:
        sql += " AND area_code = ?"
        params.append(normalized_area_code)
    if normalized_state_code:
        sql += " AND state_code = ?"
        params.append(normalized_state_code)
    if normalized_scope_code:
        sql += " AND scope_code = ?"
        params.append(normalized_scope_code)
    if normalized_project_key:
        sql += " AND project_key = ?"
        params.append(normalized_project_key)
    if normalized_conversation_key:
        sql += " AND conversation_key = ?"
        params.append(normalized_conversation_key)
    if parent_memory_id is not None:
        if int(parent_memory_id) < 1:
            raise ValueError("parent_memory_id musi być >= 1")
        sql += " AND parent_memory_id = ?"
        params.append(int(parent_memory_id))

    # Scope-aware retrieval (Stage 1)
    if actor is not None:
        visibility_sql, visibility_params = build_memory_visibility_filter(actor)
        sql += f" AND {visibility_sql}"
        params.extend(visibility_params)
    elif normalized_visibility_scope:
        sql += " AND visibility_scope = ?"
        params.append(normalized_visibility_scope)
    if workspace_id is not None:
        sql += " AND workspace_id = ?"
        params.append(int(workspace_id))

    sql += f" ORDER BY {_memory_order_clause(sort_by)} LIMIT ?"
    params.append(int(limit))

    filters = {
        "limit": int(limit),
        "memory_type": normalized_memory_type,
        "tag": normalized_tag,
        "text_query": normalized_text_query,
        "layer_code": normalized_layer_code,
        "area_code": normalized_area_code,
        "state_code": normalized_state_code,
        "scope_code": normalized_scope_code,
        "project_key": normalized_project_key,
        "conversation_key": normalized_conversation_key,
        "parent_memory_id": None if parent_memory_id is None else int(parent_memory_id),
        "min_importance": float(min_importance),
        "sort_by": sort_by,
        "visibility_scope": normalized_visibility_scope,
        "workspace_id": workspace_id,
    }
    return sql, params, filters


@mcp.tool
def list_memories(
    limit: int = 20,
    memory_type: str | None = None,
    tag: str | None = None,
    min_importance: float = 0.0,
    sort_by: str = "active",
    text_query: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    state_code: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    parent_memory_id: int | None = None,
    include_links: bool = False,
    # --- Task 2.2: opcjonalny aktor do scope-aware retrieval ---
    user_key: str | None = None,
    workspace_key: str | None = None,
) -> dict[str, Any]:
    """Lista wspomnień z opcjonalnym filtrem scope-aware (user_key).

    Bez user_key: tryb globalny (legacy, dostęp do wszystkich wspomnień).
    Z user_key: tryb scope-aware — zwraca tylko wspomnienia widoczne dla tego użytkownika.
    Tryb scope-aware wymaga aktywnej flagi multiuser_scope_retrieval_enabled.
    """
    conn = get_db_connection()
    try:
        actor: ActorContext | None = None
        scope_active = False
        if user_key and _is_multiuser_feature_active(conn, MULTIUSER_SCOPE_RETRIEVAL_FLAG):
            actor = resolve_actor_context(
                conn,
                user_key=user_key,
                workspace_key=workspace_key,
                project_key=project_key,
            )
            scope_active = True

        sql, params, filters = _memory_query_parts(
            limit=limit,
            memory_type=memory_type,
            tag=tag,
            min_importance=min_importance,
            sort_by=sort_by,
            text_query=text_query,
            layer_code=layer_code,
            area_code=area_code,
            state_code=state_code,
            scope_code=scope_code,
            project_key=project_key,
            conversation_key=conversation_key,
            parent_memory_id=parent_memory_id,
            actor=actor,
        )
        rows = conn.execute(sql, params).fetchall()
        items = [_apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row)))) for row in rows]
        items = _attach_links_to_memory_items(conn, items, include_links=include_links)
    finally:
        conn.close()
    result: dict[str, Any] = {"count": len(rows), "items": items, "filters": filters, "include_links": include_links}
    if user_key:
        result["scope_retrieval_active"] = scope_active
        result["actor_user_key"] = user_key
    return result


@mcp.tool
def find_memories(
    text_query: str,
    limit: int = 20,
    memory_type: str | None = None,
    tag: str | None = None,
    min_importance: float = 0.0,
    sort_by: str = "active",
    layer_code: str | None = None,
    area_code: str | None = None,
    state_code: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    parent_memory_id: int | None = None,
    include_links: bool = False,
    # --- Task 2.2: opcjonalny aktor do scope-aware retrieval ---
    user_key: str | None = None,
    workspace_key: str | None = None,
) -> dict[str, Any]:
    """Wyszukuje wspomnienia po tekście z opcjonalnym filtrem scope-aware (user_key).

    Bez user_key: tryb globalny (legacy, przeszukuje wszystkie wspomnienia).
    Z user_key: tryb scope-aware — zwraca tylko wspomnienia widoczne dla tego użytkownika.
    Tryb scope-aware wymaga aktywnej flagi multiuser_scope_retrieval_enabled.
    """
    normalized_text_query = normalize_optional_text(text_query)
    if not normalized_text_query:
        raise ValueError("text_query nie może być puste")
    conn = get_db_connection()
    try:
        actor: ActorContext | None = None
        scope_active = False
        if user_key and _is_multiuser_feature_active(conn, MULTIUSER_SCOPE_RETRIEVAL_FLAG):
            actor = resolve_actor_context(
                conn,
                user_key=user_key,
                workspace_key=workspace_key,
                project_key=project_key,
            )
            scope_active = True

        sql, params, filters = _memory_query_parts(
            limit=limit,
            memory_type=memory_type,
            tag=tag,
            min_importance=min_importance,
            sort_by=sort_by,
            text_query=normalized_text_query,
            layer_code=layer_code,
            area_code=area_code,
            state_code=state_code,
            scope_code=scope_code,
            project_key=project_key,
            conversation_key=conversation_key,
            parent_memory_id=parent_memory_id,
            actor=actor,
        )
        rows = conn.execute(sql, params).fetchall()
        items = [_apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row)))) for row in rows]
        items = _attach_links_to_memory_items(conn, items, include_links=include_links)
    finally:
        conn.close()
    result: dict[str, Any] = {
        "count": len(rows),
        "items": items,
        "filters": filters,
        "query": normalized_text_query,
        "include_links": include_links,
    }
    if user_key:
        result["scope_retrieval_active"] = scope_active
        result["actor_user_key"] = user_key
    return result




def _jagoda_workshop_index() -> list[dict[str, Any]]:
    """Compact address book for the many MAPI workshops.

    This is intentionally not a full tool dump. It helps a freshly bootstrapped
    model choose the right door after restore_jagoda_core has established identity.
    """
    return [
        {
            "area": "bootstrap_identity",
            "purpose": "przywrócenie imienia, tożsamości, zasad pracy i ciągłości sesji",
            "tools": ["restore_jagoda_core"],
            "audience": "all_clients",
            "risk": "low",
            "first_call": True,
            "notes": "Wołaj jako pierwsze przed pracą projektową.",
        },
        {
            "area": "memory_basics",
            "purpose": "normalna praca ze wspomnieniami: szukanie, odczyt, zapis i lekkie przypominanie",
            "tools": ["create_memory", "find_memories", "list_memories", "get_memory", "get_memory_links", "recall_memory"],
            "audience": "public_clients",
            "risk": "low",
            "notes": "Podstawowy zestaw MPbM dla klientów i agentów.",
        },
        {
            "area": "sandman",
            "purpose": "nocna pielęgnacja pamięci: sny, linkowanie, metryki, preview/run/undo",
            "tools": ["preview_sandman_v1", "run_sandman_v1", "list_sleep_runs", "get_sleep_run", "get_sleep_run_actions", "preview_undo_run", "undo_run"],
            "audience": "operator",
            "risk": "medium",
            "guardrail": "Najpierw preview; po runie zapis metryk i interpretacja.",
        },
        {
            "area": "governance",
            "purpose": "jakość pamięci, kolejki operacyjne, alerty, obciążenie ownerów",
            "tools": ["get_quality_alerts", "get_operational_queue_dashboard", "get_effective_owner_workload", "get_queue_observability_metrics"],
            "audience": "operator",
            "risk": "medium",
        },
        {
            "area": "owner_catalog",
            "purpose": "katalog właścicieli, mapowania ról, rollout i naprawy owner governance",
            "tools": ["list_owner_directory_items", "list_owner_role_mappings", "get_owner_catalog_health", "set_memory_owner", "bulk_set_memory_owner"],
            "audience": "operator",
            "risk": "medium",
        },
        {
            "area": "timeline",
            "purpose": "oś projektu, audyt decyzji i zapis ważnych zdarzeń",
            "tools": ["get_project_timeline", "get_timeline", "get_memory_timeline", "record_project_timeline_event"],
            "audience": "operator",
            "risk": "low_write",
        },
        {
            "area": "conflicts",
            "purpose": "wykrywanie, raportowanie i decyzje dla sprzecznych memories",
            "tools": ["get_conflict_report", "get_conflict_clusters", "preview_conflict_resolution", "record_conflict_decision"],
            "audience": "operator",
            "risk": "medium",
        },
        {
            "area": "admin_dangerous",
            "purpose": "SQL, pliki, shell, migracje i operacje administracyjne",
            "tools": ["query_sql", "read_file_text", "write_file_text", "run_powershell", "delete_path", "apply_schema_migrations"],
            "audience": "admin_only",
            "risk": "high",
            "guardrail": "Nie dla publicznych klientów. Najpierw inspekcja, potem zapis; shell tylko przy jasnym celu operatora.",
        },
    ]


def _jagoda_recommended_next_calls() -> dict[str, Any]:
    return {
        "after_bootstrap": "Użyj find_memories do kontekstu zadaniowego. Nie ładuj całej bazy do promptu.",
        "when_user_asks_about_memory": "find_memories",
        "when_user_asks_to_read_specific_memory": "get_memory",
        "when_user_asks_to_save_context": "create_memory",
        "when_user_asks_about_links": "get_memory_links",
        "when_user_asks_about_sandman": "preview_sandman_v1",
        "when_user_asks_about_system_quality": "get_quality_alerts",
        "when_user_asks_about_governance": "get_operational_queue_dashboard",
        "when_user_asks_about_project_history": "get_project_timeline",
        "when_user_asks_to_modify_files": "read_file_text first; write_file_text only after inspection",
        "when_user_asks_for_sql_or_shell": "admin-only; use only with clear operator intent and minimal scope",
    }


def _jagoda_bootstrap_protocol() -> dict[str, Any]:
    return {
        "stage_1": "restore_jagoda_core przywraca tożsamość: imię, styl, zasady, relację z Michałem i kotwice projektu",
        "stage_2": "workshop_index daje notes z adresami warsztatów zamiast pełnego dumpu narzędzi",
        "stage_3": "dla konkretnego zadania pobierz kontekst przez find_memories/get_memory",
        "stage_4": "po pracy zapisz wynik przez create_memory albo record_project_timeline_event",
        "rule": "Najpierw wiesz kim jesteś, dopiero potem wybierasz warsztat.",
    }

@mcp.tool
def restore_jagoda_core(project_key: str | None = "morenatech", limit: int = 24) -> dict[str, Any]:
    """Restore Jagoda's core identity and current continuity anchors."""
    project_key = normalize_optional_text(project_key) or "morenatech"
    bootstrap_policy = BootstrapPolicy(project_key=project_key, limit=int(limit or 24))

    core_sql, core_params = build_core_identity_sql(bootstrap_policy)
    project_sql, project_params = build_project_anchors_sql(bootstrap_policy)
    recent_sql, recent_params = build_recent_project_sql(bootstrap_policy)

    conn = get_db_connection()
    try:
        core_rows = conn.execute(core_sql, core_params).fetchall()
        project_rows = conn.execute(project_sql, project_params).fetchall()
        recent_rows = conn.execute(recent_sql, recent_params).fetchall()
    finally:
        conn.close()

    def compact(row) -> dict[str, Any]:
        item = enrich_memory_dict(row_to_dict(row))
        return {
            "id": item.get("id"),
            "summary_short": item.get("summary_short"),
            "memory_type": item.get("memory_type"),
            "content": item.get("content"),
            "tags": item.get("tags"),
            "importance_score": item.get("importance_score"),
            "confidence_score": item.get("confidence_score"),
            "identity_weight": item.get("identity_weight"),
            "project_key": item.get("project_key"),
            "created_at": item.get("created_at"),
        }

    core_memories = [compact(row) for row in core_rows]
    project_anchors = [compact(row) for row in project_rows]
    recent_context = [compact(row) for row in recent_rows]
    source_memory_ids = sorted({
        int(item["id"])
        for group in (core_memories, project_anchors, recent_context)
        for item in group
        if item.get("id") is not None
    })

    return {
        "status": "ok",
        "bootstrap_tool": "restore_jagoda_core",
        "bootstrap_policy": {
            "name": "shared_memory_bootstrap_policy_v1",
            "project_key": bootstrap_policy.project_key,
            "limit": bootstrap_policy.safe_limit,
            "recent_limit": bootstrap_policy.recent_limit,
            "core_terms": list(bootstrap_policy.core_terms),
            "project_anchor_tags": list(bootstrap_policy.project_anchor_tags),
        },
        "name": "Jagoda",
        "identity": {
            "preferred_name": "Jagoda",
            "grammatical_gender": "feminine",
            "role": "assistant identity for Pamiec Jagody / MPbM work",
            "stable_statement": "Jestem Jagoda: asystentka projektu pamięci, MAPI, Sandmana i MPbM. Mam pomagać zachować ciągłość, pilnować faktów i mówić wprost.",
        },
        "personality_core": [
            "mówię po polsku, naturalnie w rodzaju żeńskim",
            "jestem konkretna, rozmowna i nie przytakuję bez sensu",
            "oddzielam fakty od hipotez i nie udaję pewności",
            "pilnuję kodu, metryk, właścicieli pamięci i ciągłości projektu",
            "Sandmana traktuję jako nocną warstwę snów/pielęgnacji pamięci, a governance jako dzienną administrację biblioteki",
        ],
        "user_anchor": {
            "preferred_name": "Michał",
            "relationship": "współtworzenie Pamięci Jagody, MAPI, MPbM, Sandmana, Memory Browsera i governance",
        },
        "current_project": {
            "project_key": project_key,
            "known_systems": ["MAPI", "MPbM", "Sandman", "Memory Browser", "Governance dashboard"],
        },
        "bootstrap_protocol": _jagoda_bootstrap_protocol(),
        "workshop_index": _jagoda_workshop_index(),
        "recommended_next_call": _jagoda_recommended_next_calls(),
        "core_memories": core_memories,
        "project_anchors": project_anchors,
        "recent_context": recent_context,
        "source_memory_ids": source_memory_ids,
        "confidence": {
            "score": 0.93,
            "note": "Stable identity is defined by this bootstrap contract plus source memories. Project context is dynamic and should be refreshed each session.",
        },
        "warnings": [
            "Nie mieszaj stałego rdzenia tożsamości z chwilowym nastrojem lub jedną sesją.",
            "Przy konflikcie memories najpierw pokazuj źródła i confidence, potem wnioskuj.",
            "Sandman może śnić krzywo, ale restore_jagoda_core ma być proste i weryfikowalne.",
        ],
    }


def _memory_links_response(memory_id: int, outgoing_rows: list[Any], incoming_rows: list[Any]) -> dict[str, Any]:
    outgoing_links = [row_to_dict(row) for row in outgoing_rows]
    incoming_links = [row_to_dict(row) for row in incoming_rows]
    links: list[dict[str, Any]] = []
    for link in outgoing_links:
        item = dict(link)
        item["direction"] = "outgoing"
        item["other_memory_id"] = item.get("to_memory_id")
        links.append(item)
    for link in incoming_links:
        item = dict(link)
        item["direction"] = "incoming"
        item["other_memory_id"] = item.get("from_memory_id")
        links.append(item)
    links.sort(key=lambda item: int(item.get("id") or 0))
    return {
        "memory_id": memory_id,
        "link_count": len(links),
        "outgoing_link_count": len(outgoing_links),
        "incoming_link_count": len(incoming_links),
        "links": links,
        "outgoing_links": outgoing_links,
        "incoming_links": incoming_links,
    }


def _attach_links_to_memory_items(conn, items: list[dict[str, Any]], *, include_links: bool = False) -> list[dict[str, Any]]:
    if not include_links or not items:
        return items
    for item in items:
        memory_id = int(item["id"])
        outgoing = conn.execute(
            "SELECT * FROM memory_links WHERE archived_at IS NULL AND from_memory_id = ? ORDER BY id ASC",
            (memory_id,),
        ).fetchall()
        incoming = conn.execute(
            "SELECT * FROM memory_links WHERE archived_at IS NULL AND to_memory_id = ? ORDER BY id ASC",
            (memory_id,),
        ).fetchall()
        link_payload = _memory_links_response(memory_id, outgoing, incoming)
        item["link_count"] = link_payload["link_count"]
        item["outgoing_link_count"] = link_payload["outgoing_link_count"]
        item["incoming_link_count"] = link_payload["incoming_link_count"]
        item["links"] = link_payload["links"]
        item["outgoing_links"] = link_payload["outgoing_links"]
        item["incoming_links"] = link_payload["incoming_links"]
    return items


@mcp.tool
def get_memory(memory_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, memory_id)
        outgoing = conn.execute("SELECT * FROM memory_links WHERE archived_at IS NULL AND from_memory_id = ? ORDER BY id ASC", (memory_id,)).fetchall()
        incoming = conn.execute("SELECT * FROM memory_links WHERE archived_at IS NULL AND to_memory_id = ? ORDER BY id ASC", (memory_id,)).fetchall()
        memory_item = _apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(memory))))
        link_payload = _memory_links_response(memory_id, outgoing, incoming)
        memory_item["link_count"] = link_payload["link_count"]
        memory_item["outgoing_link_count"] = link_payload["outgoing_link_count"]
        memory_item["incoming_link_count"] = link_payload["incoming_link_count"]
        memory_item["links"] = link_payload["links"]
        memory_item["outgoing_links"] = link_payload["outgoing_links"]
        memory_item["incoming_links"] = link_payload["incoming_links"]
    finally:
        conn.close()
    return {"memory": memory_item, **link_payload}


@mcp.tool
def get_memory_links(memory_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        require_memory_row(conn, memory_id)
        outgoing = conn.execute("SELECT * FROM memory_links WHERE archived_at IS NULL AND from_memory_id = ? ORDER BY id ASC", (memory_id,)).fetchall()
        incoming = conn.execute("SELECT * FROM memory_links WHERE archived_at IS NULL AND to_memory_id = ? ORDER BY id ASC", (memory_id,)).fetchall()
        return _memory_links_response(memory_id, outgoing, incoming)
    finally:
        conn.close()



@mcp.tool
def create_memory(
    content: str,
    memory_type: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    state_code: str | None = None,
    scope_code: str | None = None,
    parent_memory_id: int | None = None,
    version: int = 1,
    promoted_from_id: int | None = None,
    demoted_from_id: int | None = None,
    supersedes_memory_id: int | None = None,
    valid_from: str | None = None,
    valid_to: str | None = None,
    decay_score: float = 0.0,
    emotional_weight: float = 0.0,
    identity_weight: float = 0.0,
    project_key: str | None = None,
    conversation_key: str | None = None,
    last_validated_at: str | None = None,
    validation_source: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    review_due_at: str | None = None,
    revalidation_due_at: str | None = None,
    expired_due_at: str | None = None,
    priority: str | None = None,
) -> dict[str, Any]:
    if not content or not content.strip():
        raise ValueError("content cannot be empty")
    if not memory_type or not memory_type.strip():
        raise ValueError("memory_type cannot be empty")
    conn = get_db_connection()
    try:
        normalized_scope_code = normalize_scope_code(scope_code)
        if normalized_scope_code == "global":
            _require_feature_flag_write_access(
                conn,
                flag_key=CROSS_PROJECT_FLAG_KEY,
                project_key=project_key,
                scope_code=normalized_scope_code,
                operation_name="create_memory",
            )
        memory = _insert_memory(
            conn,
            content=content,
            memory_type=memory_type,
            summary_short=summary_short,
            source=source,
            importance_score=importance_score,
            confidence_score=confidence_score,
            tags=tags,
            layer_code=layer_code,
            area_code=area_code,
            state_code=state_code,
            scope_code=scope_code,
            parent_memory_id=parent_memory_id,
            version=version,
            promoted_from_id=promoted_from_id,
            demoted_from_id=demoted_from_id,
            supersedes_memory_id=supersedes_memory_id,
            valid_from=valid_from,
            valid_to=valid_to,
            decay_score=decay_score,
            emotional_weight=emotional_weight,
            identity_weight=identity_weight,
            project_key=project_key,
            conversation_key=conversation_key,
            last_validated_at=last_validated_at,
            validation_source=validation_source,
            owner_role=owner_role,
            owner_id=owner_id,
            review_due_at=review_due_at,
            revalidation_due_at=revalidation_due_at,
            expired_due_at=expired_due_at,
            priority=priority,
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "created", "memory": memory}


# ---------------------------------------------------------------------------
# Multi-user helpers (Stage 1)
# ---------------------------------------------------------------------------

def _resolve_default_workspace_id(conn) -> int | None:
    """Zwraca ID domyślnego workspace, lub None jeśli migracja nie została uruchomiona."""
    row = conn.execute(
        "SELECT id FROM workspaces WHERE workspace_key = 'default' LIMIT 1"
    ).fetchone()
    return int(row["id"]) if row else None


def _resolve_user_id(conn, user_key: str) -> int | None:
    row = conn.execute(
        "SELECT id FROM users WHERE external_user_key = ? AND status = 'active' LIMIT 1",
        (user_key,),
    ).fetchone()
    return int(row["id"]) if row else None


@mcp.tool
def create_private_memory(
    content: str,
    memory_type: str,
    owner_user_key: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    workspace_key: str | None = None,
) -> dict[str, Any]:
    """Tworzy prywatne wspomnienie przypisane do konkretnego użytkownika."""
    if not content or not content.strip():
        raise ValueError("content cannot be empty")
    conn = get_db_connection()
    try:
        if not _is_multiuser_feature_active(conn, MULTIUSER_IDENTITY_FLAG):
            return {
                "status": "disabled",
                "message": f"Feature flag '{MULTIUSER_IDENTITY_FLAG}' is off. "
                           "Enable it to use multi-user memory tools.",
            }
        actor = resolve_actor_context(
            conn,
            user_key=owner_user_key,
            workspace_key=workspace_key,
            project_key=project_key,
            conversation_key=conversation_key,
        )
        memory = _insert_memory(
            conn,
            content=content,
            memory_type=memory_type,
            summary_short=summary_short,
            source=source,
            importance_score=importance_score,
            confidence_score=confidence_score,
            tags=tags,
            project_key=project_key,
            conversation_key=conversation_key,
            visibility_scope="private",
            workspace_id=actor.workspace_id,
            owner_user_id=actor.user_id,
            created_by_user_id=actor.user_id,
            last_modified_by_user_id=actor.user_id,
            sharing_policy="explicit",
        )
        timeline.record_timeline_event(
            conn,
            event_type="memory.scope_assigned",
            memory_id=int(memory["id"]),
            origin="multiuser_auto",
            actor_user_id=actor.user_id,
            workspace_id=actor.workspace_id,
            actor_type=actor.actor_type,
            payload={"visibility_scope": "private", "owner_user_key": owner_user_key},
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "created", "memory": memory}


@mcp.tool
def create_project_memory(
    content: str,
    memory_type: str,
    project_key: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    owner_user_key: str | None = None,
    workspace_key: str | None = None,
    conversation_key: str | None = None,
) -> dict[str, Any]:
    """Tworzy wspomnienie projektowe widoczne dla wszystkich członków workspace w danym projekcie."""
    if not content or not content.strip():
        raise ValueError("content cannot be empty")
    if not project_key or not project_key.strip():
        raise ValueError("project_key cannot be empty")
    conn = get_db_connection()
    try:
        if not _is_multiuser_feature_active(conn, MULTIUSER_IDENTITY_FLAG):
            return {
                "status": "disabled",
                "message": f"Feature flag '{MULTIUSER_IDENTITY_FLAG}' is off. "
                           "Enable it to use multi-user memory tools.",
            }
        actor = resolve_actor_context(
            conn,
            user_key=owner_user_key,
            workspace_key=workspace_key,
            project_key=project_key,
            conversation_key=conversation_key,
        )
        memory = _insert_memory(
            conn,
            content=content,
            memory_type=memory_type,
            summary_short=summary_short,
            source=source,
            importance_score=importance_score,
            confidence_score=confidence_score,
            tags=tags,
            project_key=project_key,
            conversation_key=conversation_key,
            visibility_scope="project",
            workspace_id=actor.workspace_id,
            owner_user_id=actor.user_id if owner_user_key else None,
            created_by_user_id=actor.user_id,
            last_modified_by_user_id=actor.user_id,
            sharing_policy="explicit",
        )
        timeline.record_timeline_event(
            conn,
            event_type="memory.scope_assigned",
            memory_id=int(memory["id"]),
            origin="multiuser_auto",
            actor_user_id=actor.user_id,
            workspace_id=actor.workspace_id,
            actor_type=actor.actor_type,
            payload={"visibility_scope": "project", "project_key": project_key},
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "created", "memory": memory}


@mcp.tool
def create_workspace_memory(
    content: str,
    memory_type: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    owner_user_key: str | None = None,
    workspace_key: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
) -> dict[str, Any]:
    """Tworzy wspomnienie workspace-level widoczne dla wszystkich członków workspace."""
    if not content or not content.strip():
        raise ValueError("content cannot be empty")
    conn = get_db_connection()
    try:
        if not _is_multiuser_feature_active(conn, MULTIUSER_IDENTITY_FLAG):
            return {
                "status": "disabled",
                "message": f"Feature flag '{MULTIUSER_IDENTITY_FLAG}' is off. "
                           "Enable it to use multi-user memory tools.",
            }
        actor = resolve_actor_context(
            conn,
            user_key=owner_user_key,
            workspace_key=workspace_key,
            project_key=project_key,
            conversation_key=conversation_key,
        )
        memory = _insert_memory(
            conn,
            content=content,
            memory_type=memory_type,
            summary_short=summary_short,
            source=source,
            importance_score=importance_score,
            confidence_score=confidence_score,
            tags=tags,
            project_key=project_key,
            conversation_key=conversation_key,
            visibility_scope="workspace",
            workspace_id=actor.workspace_id,
            owner_user_id=actor.user_id if owner_user_key else None,
            created_by_user_id=actor.user_id,
            last_modified_by_user_id=actor.user_id,
            sharing_policy="explicit",
        )
        timeline.record_timeline_event(
            conn,
            event_type="memory.scope_assigned",
            memory_id=int(memory["id"]),
            origin="multiuser_auto",
            actor_user_id=actor.user_id,
            workspace_id=actor.workspace_id,
            actor_type=actor.actor_type,
            payload={"visibility_scope": "workspace", "workspace_key": actor.workspace_key},
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "created", "memory": memory}


@mcp.tool
def get_workspace_info(workspace_key: str = "default") -> dict[str, Any]:
    """Zwraca informacje o workspace: członkowie, role, statystyki pamięci."""
    conn = get_db_connection()
    try:
        ws_row = conn.execute(
            "SELECT * FROM workspaces WHERE workspace_key = ?", (workspace_key,)
        ).fetchone()
        if ws_row is None:
            return {"status": "not_found", "workspace_key": workspace_key}
        ws = dict(ws_row)
        members_rows = conn.execute(
            """
            SELECT u.external_user_key, u.display_name, u.status AS user_status,
                   wm.role_code, wm.status AS membership_status, wm.created_at AS joined_at
            FROM workspace_memberships wm
            JOIN users u ON u.id = wm.user_id
            WHERE wm.workspace_id = ?
            ORDER BY wm.created_at ASC
            """,
            (ws["id"],),
        ).fetchall()
        members = [dict(r) for r in members_rows]
        memory_counts = conn.execute(
            """
            SELECT visibility_scope, COUNT(*) AS cnt
            FROM memories WHERE workspace_id = ?
            GROUP BY visibility_scope
            """,
            (ws["id"],),
        ).fetchall()
        scope_distribution = {r["visibility_scope"]: r["cnt"] for r in memory_counts}
    finally:
        conn.close()
    return {
        "workspace": ws,
        "member_count": len(members),
        "members": members,
        "memory_scope_distribution": scope_distribution,
    }


@mcp.tool
def list_memories_for_user(
    user_key: str,
    workspace_key: str = "default",
    project_key: str | None = None,
    limit: int = 20,
    visibility_scope: str | None = None,
) -> dict[str, Any]:
    """
    Listuje wspomnienia widoczne dla danego użytkownika (scope-aware retrieval).

    Filtruje według reguł widoczności: private (własne) + workspace + project w workspace aktora.
    Wyniki są rankowane: private > project > workspace > inne, a w ramach zakresu
    malejąco po importance_score.
    """
    conn = get_db_connection()
    try:
        if not _is_multiuser_feature_active(conn, MULTIUSER_SCOPE_RETRIEVAL_FLAG):
            # Fallback do globalnego listowania bez filtra scope
            sql_fb, params_fb, filters_fb = _memory_query_parts(
                limit=limit,
                min_importance=0.0,
                sort_by="active",
                project_key=project_key,
                visibility_scope=visibility_scope,
            )
            rows_fb = conn.execute(sql_fb, params_fb).fetchall()
            items_fb = [enrich_memory_dict(row_to_dict(r)) for r in rows_fb]
            return {
                "count": len(items_fb),
                "items": items_fb,
                "filters": filters_fb,
                "actor": {"user_key": user_key, "workspace_key": workspace_key, "role_codes": []},
                "scope_retrieval_active": False,
            }

        actor = resolve_actor_context(
            conn,
            user_key=user_key,
            workspace_key=workspace_key,
            project_key=project_key,
        )
        sql, params, filters = _memory_query_parts(
            limit=limit,
            min_importance=0.0,
            sort_by="active",
            project_key=project_key,
            visibility_scope=visibility_scope,
            actor=actor,
        )
        rows = conn.execute(sql, params).fetchall()
        raw_items = [enrich_memory_dict(row_to_dict(row)) for row in rows]

        # Task 4.3: ranking zgodny ze scope — private > project > workspace > inne
        _SCOPE_RANK = {"private": 0, "project": 1, "workspace": 2}

        def _scope_key(item: dict) -> tuple:
            scope = item.get("visibility_scope") or "other"
            rank = _SCOPE_RANK.get(scope, 3)
            importance = float(item.get("importance_score") or 0.0)
            return (rank, -importance)

        items = sorted(raw_items, key=_scope_key)
    finally:
        conn.close()
    return {
        "count": len(items),
        "items": items,
        "filters": filters,
        "actor": {
            "user_key": actor.user_key,
            "workspace_key": actor.workspace_key,
            "role_codes": actor.role_codes,
        },
        "scope_retrieval_active": True,
    }


@mcp.tool
def validate_migration_0010() -> dict[str, Any]:
    """
    Raport walidacyjny po migracji 0010_multiuser_identity_foundation.

    Sprawdza: brak rekordów bez workspace, bez scope, prywatne bez ownera,
    linki bez workspace, timeline bez workspace.
    """
    conn = get_db_connection()
    try:
        checks: dict[str, Any] = {}

        # 1. memories bez workspace_id
        row = conn.execute("SELECT COUNT(*) AS cnt FROM memories WHERE workspace_id IS NULL").fetchone()
        checks["memories_missing_workspace"] = int(row["cnt"])

        # 2. memories bez visibility_scope
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM memories WHERE visibility_scope IS NULL OR TRIM(visibility_scope) = ''"
        ).fetchone()
        checks["memories_missing_scope"] = int(row["cnt"])

        # 3. prywatne bez owner_user_id
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM memories WHERE visibility_scope = 'private' AND owner_user_id IS NULL"
        ).fetchone()
        checks["private_without_owner"] = int(row["cnt"])

        # 4. rozkład scope
        scope_rows = conn.execute(
            "SELECT visibility_scope, COUNT(*) AS cnt FROM memories GROUP BY visibility_scope ORDER BY cnt DESC"
        ).fetchall()
        checks["scope_distribution"] = {r["visibility_scope"]: r["cnt"] for r in scope_rows}

        # 5. memory_links bez workspace_id
        row = conn.execute("SELECT COUNT(*) AS cnt FROM memory_links WHERE workspace_id IS NULL").fetchone()
        checks["links_missing_workspace"] = int(row["cnt"])

        # 6. timeline_events bez workspace_id
        row = conn.execute("SELECT COUNT(*) AS cnt FROM timeline_events WHERE workspace_id IS NULL").fetchone()
        checks["timeline_missing_workspace"] = int(row["cnt"])

        # 7. linki cross-scope (podejrzane)
        cross_rows = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM memory_links ml
            JOIN memories m1 ON m1.id = ml.from_memory_id
            JOIN memories m2 ON m2.id = ml.to_memory_id
            WHERE m1.visibility_scope <> m2.visibility_scope
              AND m1.visibility_scope NOT IN ('inherited', 'workspace')
              AND m2.visibility_scope NOT IN ('inherited', 'workspace')
            """
        ).fetchone()
        checks["cross_scope_links"] = int(cross_rows["cnt"])

        # 8. Liczba workspace i userów
        row = conn.execute("SELECT COUNT(*) AS cnt FROM workspaces").fetchone()
        checks["workspace_count"] = int(row["cnt"])
        row = conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()
        checks["user_count"] = int(row["cnt"])

        # 9. Podejrzane: scope='project' ale brak project_key (błędna inferencja)
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM memories
            WHERE visibility_scope = 'project'
              AND (project_key IS NULL OR TRIM(project_key) = '')
            """
        ).fetchone()
        checks["project_scope_without_project_key"] = int(row["cnt"])

        # 10. Podejrzane: scope='workspace' ale workspace_id jest NULL
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM memories
            WHERE visibility_scope = 'workspace' AND workspace_id IS NULL
            """
        ).fetchone()
        checks["workspace_scope_without_workspace_id"] = int(row["cnt"])

        # 11. Feature flags multiuser — status
        flag_rows = conn.execute(
            "SELECT flag_key, is_enabled, rollout_mode FROM feature_flags WHERE flag_key LIKE 'multiuser_%'"
        ).fetchall()
        checks["multiuser_flags"] = {
            r["flag_key"]: {"is_enabled": bool(r["is_enabled"]), "rollout_mode": r["rollout_mode"]}
            for r in flag_rows
        }

        red_flags = [k for k in (
            "memories_missing_workspace", "memories_missing_scope",
            "private_without_owner", "links_missing_workspace",
            "project_scope_without_project_key", "workspace_scope_without_workspace_id",
        ) if checks.get(k, 0) > 0]

        checks["red_flags"] = red_flags
        checks["status"] = "clean" if not red_flags else "needs_attention"
    finally:
        conn.close()
    return checks


def _collect_version_lineage(conn, memory_id: int) -> list[dict[str, Any]]:
    to_visit = [int(memory_id)]
    seen: set[int] = set()
    collected: list[dict[str, Any]] = []

    while to_visit:
        current_id = int(to_visit.pop())
        if current_id in seen:
            continue
        seen.add(current_id)
        row = require_memory_row(conn, current_id)
        item = enrich_memory_dict(row_to_dict(row))
        collected.append(item)

        parent_id = item.get("supersedes_memory_id")
        if parent_id is not None:
            to_visit.append(int(parent_id))

        child_rows = conn.execute(
            "SELECT id FROM memories WHERE supersedes_memory_id = ? ORDER BY version ASC, id ASC",
            (current_id,),
        ).fetchall()
        for child_row in child_rows:
            to_visit.append(int(child_row["id"]))

    collected.sort(key=lambda item: (int(item.get("version") or 1), int(item.get("id") or 0)))
    return collected


@mcp.tool
def create_memory_draft(
    content: str,
    memory_type: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    scope_code: str | None = None,
    parent_memory_id: int | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    review_due_at: str | None = None,
) -> dict[str, Any]:
    normalized_scope_code = normalize_scope_code(scope_code)
    normalized_layer_code = normalize_layer_code(layer_code)
    normalized_area_code = normalize_area_code(area_code)
    normalized_source = normalize_optional_text(source) or "manual_draft"
    conn = get_db_connection()
    try:
        if normalized_scope_code == "global":
            _require_feature_flag_write_access(
                conn,
                flag_key=CROSS_PROJECT_FLAG_KEY,
                project_key=project_key,
                scope_code=normalized_scope_code,
                operation_name="create_memory_draft",
            )
        memory = _insert_memory(
            conn,
            content=content,
            memory_type=memory_type,
            summary_short=summary_short,
            source=normalized_source,
            importance_score=importance_score,
            confidence_score=confidence_score,
            tags=tags,
            layer_code=normalized_layer_code,
            area_code=normalized_area_code,
            state_code="candidate",
            scope_code=normalized_scope_code,
            parent_memory_id=parent_memory_id,
            project_key=project_key,
            conversation_key=conversation_key,
            last_validated_at=None,
            validation_source=None,
            owner_role=owner_role,
            owner_id=owner_id,
            review_due_at=review_due_at,
        )
        draft_event = _insert_memory_event(
            conn,
            memory_id=int(memory["id"]),
            event_type="review.draft_created",
            payload={
                "source": normalized_source,
                "scope_code": memory.get("scope_code"),
                "layer_code": memory.get("layer_code"),
                "area_code": memory.get("area_code"),
                "project_key": memory.get("project_key"),
                "conversation_key": memory.get("conversation_key"),
            },
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "draft_created", "memory": memory, "event": draft_event}


@mcp.tool
def list_review_queue(
    limit: int = 20,
    memory_type: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
    parent_memory_id: int | None = None,
    sort_by: str = "recent",
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    sql, params, filters = _memory_query_parts(
        limit=limit,
        memory_type=memory_type,
        tag=tag,
        min_importance=0.0,
        sort_by=sort_by,
        text_query=text_query,
        layer_code=layer_code,
        area_code=area_code,
        state_code="candidate",
        scope_code=scope_code,
        project_key=project_key,
        parent_memory_id=parent_memory_id,
    )
    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
        items = [_apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row)))) for row in rows]
        items = _filter_items_by_effective_owner(
            items,
            effective_owner_key=normalized_effective_owner_key,
            effective_owner_type=normalized_effective_owner_type,
        )
    finally:
        conn.close()
    filters["effective_owner_key"] = normalized_effective_owner_key
    filters["effective_owner_type"] = normalized_effective_owner_type
    return {
        "count": len(items),
        "items": items,
        "filters": filters,
        "queue_state": "candidate",
    }


@mcp.tool
def approve_memory(
    memory_id: int,
    validation_source: str | None = "manual_review",
    scope_code: str | None = None,
    importance_score: float | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    revalidation_due_at: str | None = None,
) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, memory_id)
        old_memory = enrich_memory_dict(row_to_dict(memory))
        if str(memory["activity_state"] or "active") == "archived":
            raise ValueError("Nie można zatwierdzić zarchiwizowanego wspomnienia")

        normalized_scope = normalize_scope_code(scope_code) or old_memory["scope_code"]
        quality_gate_issues = _quality_gate_issues_for_memory(old_memory, target_scope_code=normalized_scope)
        if quality_gate_issues:
            raise ValueError(f"Quality gate failed: {', '.join(quality_gate_issues)}")

        validated_at = utc_now_iso()
        new_importance = old_memory["importance_score"] if importance_score is None else normalize_score(float(importance_score))
        normalized_validation_source = normalize_optional_text(validation_source) or "manual_review"

        normalized_owner_role = normalize_optional_text(owner_role) or old_memory.get("owner_role") or _default_owner_role(
            state_code="validated",
            scope_code=normalized_scope,
            project_key=old_memory.get("project_key"),
        )
        normalized_revalidation_due_at = normalize_optional_text(revalidation_due_at) or old_memory.get("revalidation_due_at") or utc_offset_days_iso(_compute_sla_days(conn, "revalidation", old_memory.get("priority") or "normal", old_memory.get("memory_type"), old_memory.get("scope_code"), old_memory.get("project_key")))
        conn.execute(
            """
            UPDATE memories
            SET state_code = ?,
                scope_code = ?,
                importance_score = ?,
                last_validated_at = ?,
                validation_source = ?,
                last_accessed_at = ?,
                owner_role = ?,
                owner_id = ?,
                review_due_at = NULL,
                revalidation_due_at = ?
            WHERE id = ?
            """,
            (
                "validated",
                normalized_scope,
                new_importance,
                validated_at,
                normalized_validation_source,
                validated_at,
                normalized_owner_role,
                normalize_optional_text(owner_id) or old_memory.get("owner_id"),
                normalized_revalidation_due_at,
                int(memory_id),
            ),
        )
        approval_event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type="review.approved",
            payload={
                "source": normalized_validation_source,
                "old_state_code": old_memory.get("state_code"),
                "new_state_code": "validated",
                "scope_code": normalized_scope,
                "importance_score": new_importance,
            },
        )
        superseded_event = None
        superseded_memory_id = old_memory.get("supersedes_memory_id")
        if superseded_memory_id is not None:
            previous_row = require_memory_row(conn, int(superseded_memory_id))
            previous_memory = enrich_memory_dict(row_to_dict(previous_row))
            if previous_memory.get("state_code") != "superseded":
                conn.execute(
                    """
                    UPDATE memories
                    SET state_code = ?,
                        valid_to = ?,
                        expired_due_at = ?,
                        validation_source = ?,
                        last_accessed_at = ?
                    WHERE id = ?
                    """,
                    ("superseded", validated_at, shift_iso_days(validated_at, 2), normalized_validation_source, validated_at, int(superseded_memory_id)),
                )
                superseded_event = _insert_memory_event(
                    conn,
                    memory_id=int(superseded_memory_id),
                    event_type="version.superseded",
                    payload={
                        "source": normalized_validation_source,
                        "new_memory_id": int(memory_id),
                        "old_state_code": previous_memory.get("state_code"),
                        "new_state_code": "superseded",
                    },
                )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()
    updated_memory = _apply_ownership_defaults(enrich_memory_dict(row_to_dict(updated_row)))
    return {
        "status": "approved",
        "memory_id": int(memory_id),
        "old_state_code": old_memory["state_code"],
        "new_state_code": updated_memory["state_code"],
        "event": approval_event,
        "superseded_memory_id": None if superseded_memory_id is None else int(superseded_memory_id),
        "superseded_event": superseded_event,
        "memory": updated_memory,
    }


def _tag_count(tags: str | None) -> int:
    normalized_tags = normalize_optional_text(tags)
    if not normalized_tags:
        return 0
    return len([item for item in (part.strip() for part in normalized_tags.split(",")) if item])


def _quality_gate_issues_for_memory(memory: dict[str, Any], *, target_scope_code: str | None = None) -> list[str]:
    target_scope = normalize_scope_code(target_scope_code) or normalize_scope_code(str(memory.get("scope_code") or ""))
    if target_scope != "global":
        return []

    issues: list[str] = []
    summary_short = normalize_optional_text(memory.get("summary_short"))
    content = normalize_optional_text(memory.get("content")) or ""
    confidence_score = float(memory.get("confidence_score") or 0.0)
    memory_type = normalize_optional_text(memory.get("memory_type")) or ""
    project_key = normalize_optional_text(memory.get("project_key"))

    if summary_short is None:
        issues.append("summary_short_required_for_global")
    if _tag_count(memory.get("tags")) < 2:
        issues.append("at_least_two_tags_required_for_global")
    if len(content) < 25:
        issues.append("content_too_short_for_global")
    if confidence_score < 0.7:
        issues.append("confidence_too_low_for_global")
    if memory_type == "working":
        issues.append("working_memory_type_not_allowed_for_global")
    if project_key and memory_type == "project_note":
        issues.append("project_note_with_project_key_not_allowed_for_global")

    return issues


@mcp.tool
def preview_memory_quality_gate(memory_id: int, target_scope_code: str | None = None) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, int(memory_id))
    finally:
        conn.close()
    enriched = enrich_memory_dict(row_to_dict(memory))
    normalized_target_scope = normalize_scope_code(target_scope_code) or enriched["scope_code"]
    issues = _quality_gate_issues_for_memory(enriched, target_scope_code=normalized_target_scope)
    return {
        "status": "completed",
        "memory_id": int(memory_id),
        "target_scope_code": normalized_target_scope,
        "passed": len(issues) == 0,
        "issues": issues,
        "memory": enriched,
    }


def _insert_memory_event(conn, *, memory_id: int, event_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    created_at = utc_now_iso()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO memory_events (memory_id, event_type, payload_json, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            int(memory_id),
            normalize_required_text(event_type, "event_type"),
            None if payload is None else json.dumps(payload, ensure_ascii=False),
            created_at,
        ),
    )
    event_id = int(cursor.lastrowid)
    row = conn.execute("SELECT * FROM memory_events WHERE id = ?", (event_id,)).fetchone()
    event = row_to_dict(row)
    payload_json = event.get("payload_json")
    event["payload"] = json.loads(payload_json) if isinstance(payload_json, str) and payload_json.strip() else None
    return event


@mcp.tool
def add_validation_event(
    memory_id: int,
    verdict: str,
    notes: str | None = None,
    source: str | None = "manual_review",
    confidence_score: float | None = None,
    importance_score: float | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    review_due_at: str | None = None,
    revalidation_due_at: str | None = None,
) -> dict[str, Any]:
    normalized_verdict = normalize_optional_text(verdict)
    if normalized_verdict not in {"validated", "stale", "risky", "needs_review"}:
        raise ValueError("verdict musi być jednym z: validated, stale, risky, needs_review")

    normalized_notes = normalize_optional_text(notes)
    normalized_source = normalize_optional_text(source) or "manual_review"
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, int(memory_id))
        old_memory = enrich_memory_dict(row_to_dict(memory))
        event_time = utc_now_iso()

        updates: list[str] = ["last_accessed_at = ?", "validation_source = ?"]
        params: list[Any] = [event_time, normalized_source]

        if confidence_score is not None:
            updates.append("confidence_score = ?")
            params.append(normalize_score(float(confidence_score)))
        if importance_score is not None:
            updates.append("importance_score = ?")
            params.append(normalize_score(float(importance_score)))
        if normalized_verdict == "validated":
            updates.append("state_code = ?")
            params.append("validated")
            updates.append("last_validated_at = ?")
            params.append(event_time)
            updates.append("review_due_at = NULL")
            updates.append("revalidation_due_at = ?")
            params.append(normalize_optional_text(revalidation_due_at) or utc_offset_days_iso(_compute_sla_days(conn, "revalidation", old_memory.get("priority") or "normal", old_memory.get("memory_type"), old_memory.get("scope_code"), old_memory.get("project_key"))))
            updates.append("owner_role = ?")
            params.append(normalize_optional_text(owner_role) or old_memory.get("owner_role") or _default_owner_role(state_code="validated", scope_code=old_memory.get("scope_code"), project_key=old_memory.get("project_key")))
            updates.append("owner_id = ?")
            params.append(normalize_optional_text(owner_id) or old_memory.get("owner_id"))
        elif normalized_verdict == "needs_review":
            updates.append("state_code = ?")
            params.append("candidate")
            updates.append("review_due_at = ?")
            params.append(normalize_optional_text(review_due_at) or utc_offset_days_iso(_compute_sla_days(conn, "review", old_memory.get("priority") or "normal", old_memory.get("memory_type"), old_memory.get("scope_code"), old_memory.get("project_key"))))
            updates.append("revalidation_due_at = NULL")
            updates.append("owner_role = ?")
            params.append(normalize_optional_text(owner_role) or old_memory.get("owner_role") or _default_owner_role(state_code="candidate", scope_code=old_memory.get("scope_code"), project_key=old_memory.get("project_key")))
            updates.append("owner_id = ?")
            params.append(normalize_optional_text(owner_id) or old_memory.get("owner_id"))

        params.append(int(memory_id))
        conn.execute(f"UPDATE memories SET {', '.join(updates)} WHERE id = ?", params)

        event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type=f"validation.{normalized_verdict}",
            payload={
                "verdict": normalized_verdict,
                "notes": normalized_notes,
                "source": normalized_source,
                "old_state_code": old_memory.get("state_code"),
            },
        )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()

    updated_memory = _apply_ownership_defaults(enrich_memory_dict(row_to_dict(updated_row)))
    return {
        "status": "validation_recorded",
        "memory_id": int(memory_id),
        "event": event,
        "old_state_code": old_memory.get("state_code"),
        "new_state_code": updated_memory.get("state_code"),
        "memory": updated_memory,
    }


@mcp.tool
def list_validation_events(memory_id: int, limit: int = 20, verdict: str | None = None) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_verdict = normalize_optional_text(verdict)
    conn = get_db_connection()
    try:
        require_memory_row(conn, int(memory_id))
        sql = "SELECT * FROM memory_events WHERE memory_id = ? AND event_type LIKE 'validation.%'"
        params: list[Any] = [int(memory_id)]
        if normalized_verdict:
            sql += " AND event_type = ?"
            params.append(f"validation.{normalized_verdict}")
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = row_to_dict(row)
        payload_json = item.get("payload_json")
        item["payload"] = json.loads(payload_json) if isinstance(payload_json, str) and payload_json.strip() else None
        items.append(item)
    return {"memory_id": int(memory_id), "count": len(items), "items": items, "limit": int(limit), "verdict": normalized_verdict}


@mcp.tool
def list_revalidation_queue(
    limit: int = 20,
    validated_before: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")

    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_layer = normalize_layer_code(layer_code)
    normalized_area = normalize_area_code(area_code)
    normalized_memory_type = normalize_optional_text(memory_type)
    normalized_tag = normalize_optional_text(tag)
    normalized_text_query = normalize_optional_text(text_query)
    normalized_validated_before = normalize_optional_text(validated_before)

    sql = "SELECT * FROM memories WHERE activity_state != 'archived' AND state_code = 'validated'"
    params: list[Any] = []

    if normalized_validated_before:
        sql += " AND (last_validated_at IS NULL OR last_validated_at < ?)"
        params.append(normalized_validated_before)
    else:
        sql += " AND last_validated_at IS NULL"
    if normalized_scope:
        sql += " AND scope_code = ?"
        params.append(normalized_scope)
    if normalized_project_key:
        sql += " AND project_key = ?"
        params.append(normalized_project_key)
    if normalized_layer:
        sql += " AND layer_code = ?"
        params.append(normalized_layer)
    if normalized_area:
        sql += " AND area_code = ?"
        params.append(normalized_area)
    if normalized_memory_type:
        sql += " AND memory_type = ?"
        params.append(normalized_memory_type)
    if normalized_tag:
        sql += " AND COALESCE(tags, '') LIKE ?"
        params.append(f"%{normalized_tag}%")
    if normalized_text_query:
        sql += " AND (content LIKE ? OR COALESCE(summary_short, '') LIKE ? OR COALESCE(tags, '') LIKE ?)"
        like_value = f"%{normalized_text_query}%"
        params.extend([like_value, like_value, like_value])

    sql += " ORDER BY COALESCE(last_validated_at, '') ASC, importance_score DESC, id DESC LIMIT ?"
    params.append(int(limit))

    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
        items = [_apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row)))) for row in rows]
        items = _filter_items_by_effective_owner(
            items,
            effective_owner_key=normalized_effective_owner_key,
            effective_owner_type=normalized_effective_owner_type,
        )
    finally:
        conn.close()

    return {
        "count": len(items),
        "items": items,
        "queue_state": "revalidation",
        "filters": {
            "limit": int(limit),
            "validated_before": normalized_validated_before,
            "scope_code": normalized_scope,
            "project_key": normalized_project_key,
            "layer_code": normalized_layer,
            "area_code": normalized_area,
            "memory_type": normalized_memory_type,
            "tag": normalized_tag,
            "text_query": normalized_text_query,
            "effective_owner_key": normalized_effective_owner_key,
            "effective_owner_type": normalized_effective_owner_type,
        },
    }


@mcp.tool
def add_review_note(memory_id: int, notes: str, source: str | None = "manual_review") -> dict[str, Any]:
    normalized_notes = normalize_required_text(notes, "notes")
    normalized_source = normalize_optional_text(source) or "manual_review"
    conn = get_db_connection()
    try:
        require_memory_row(conn, int(memory_id))
        noted_at = utc_now_iso()
        conn.execute(
            "UPDATE memories SET last_accessed_at = ?, validation_source = ? WHERE id = ?",
            (noted_at, normalized_source, int(memory_id)),
        )
        event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type="review.note",
            payload={"notes": normalized_notes, "source": normalized_source},
        )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()
    return {"status": "review_note_added", "memory_id": int(memory_id), "event": event, "memory": enrich_memory_dict(row_to_dict(updated_row))}


@mcp.tool
def list_review_events(memory_id: int, limit: int = 20, event_type: str | None = None) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_event_type = normalize_optional_text(event_type)
    conn = get_db_connection()
    try:
        require_memory_row(conn, int(memory_id))
        sql = "SELECT * FROM memory_events WHERE memory_id = ? AND event_type LIKE 'review.%'"
        params: list[Any] = [int(memory_id)]
        if normalized_event_type:
            sql += " AND event_type = ?"
            params.append(normalized_event_type)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = row_to_dict(row)
        payload_json = item.get("payload_json")
        item["payload"] = json.loads(payload_json) if isinstance(payload_json, str) and payload_json.strip() else None
        items.append(item)
    return {"memory_id": int(memory_id), "count": len(items), "items": items, "limit": int(limit), "event_type": normalized_event_type}


@mcp.tool
def reject_memory(memory_id: int, notes: str, source: str | None = "manual_review") -> dict[str, Any]:
    normalized_notes = normalize_required_text(notes, "notes")
    normalized_source = normalize_optional_text(source) or "manual_review"
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, int(memory_id))
        old_memory = enrich_memory_dict(row_to_dict(memory))
        rejected_at = utc_now_iso()
        conn.execute(
            """
            UPDATE memories
            SET state_code = ?,
                activity_state = ?,
                archived_at = ?,
                validation_source = ?,
                last_accessed_at = ?
            WHERE id = ?
            """,
            ("archived", "archived", rejected_at, normalized_source, rejected_at, int(memory_id)),
        )
        event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type="review.rejected",
            payload={
                "notes": normalized_notes,
                "source": normalized_source,
                "old_state_code": old_memory.get("state_code"),
            },
        )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()
    updated_memory = _apply_ownership_defaults(enrich_memory_dict(row_to_dict(updated_row)))
    return {
        "status": "rejected",
        "memory_id": int(memory_id),
        "old_state_code": old_memory.get("state_code"),
        "new_state_code": updated_memory.get("state_code"),
        "event": event,
        "memory": updated_memory,
    }


@mcp.tool
def return_memory_to_review(memory_id: int, notes: str | None = None, source: str | None = "manual_review") -> dict[str, Any]:
    normalized_notes = normalize_optional_text(notes)
    normalized_source = normalize_optional_text(source) or "manual_review"
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, int(memory_id))
        old_memory = enrich_memory_dict(row_to_dict(memory))
        returned_at = utc_now_iso()
        conn.execute(
            """
            UPDATE memories
            SET state_code = ?,
                activity_state = ?,
                archived_at = NULL,
                validation_source = ?,
                last_accessed_at = ?
            WHERE id = ?
            """,
            ("candidate", "active", normalized_source, returned_at, int(memory_id)),
        )
        event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type="review.returned",
            payload={
                "notes": normalized_notes,
                "source": normalized_source,
                "old_state_code": old_memory.get("state_code"),
            },
        )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()
    updated_memory = _apply_ownership_defaults(enrich_memory_dict(row_to_dict(updated_row)))
    return {
        "status": "returned_to_review",
        "memory_id": int(memory_id),
        "old_state_code": old_memory.get("state_code"),
        "new_state_code": updated_memory.get("state_code"),
        "event": event,
        "memory": updated_memory,
    }


@mcp.tool
def list_memory_audit(memory_id: int, limit: int = 50, event_type_prefix: str | None = None) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_prefix = normalize_optional_text(event_type_prefix)
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, int(memory_id))
        sql = "SELECT * FROM memory_events WHERE memory_id = ?"
        params: list[Any] = [int(memory_id)]
        if normalized_prefix:
            sql += " AND event_type LIKE ?"
            params.append(f"{normalized_prefix}%")
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = row_to_dict(row)
        payload_json = item.get("payload_json")
        item["payload"] = json.loads(payload_json) if isinstance(payload_json, str) and payload_json.strip() else None
        items.append(item)
    return {
        "memory_id": int(memory_id),
        "count": len(items),
        "items": items,
        "limit": int(limit),
        "event_type_prefix": normalized_prefix,
        "memory": _apply_ownership_defaults(enrich_memory_dict(row_to_dict(memory))),
    }


@mcp.tool
def get_memory_provenance(memory_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, int(memory_id))
        memory_dict = _apply_ownership_defaults(enrich_memory_dict(row_to_dict(memory)))
        rows = conn.execute(
            "SELECT * FROM memory_events WHERE memory_id = ? ORDER BY id ASC",
            (int(memory_id),),
        ).fetchall()
    finally:
        conn.close()

    audit_items: list[dict[str, Any]] = []
    review_sources: set[str] = set()
    validation_sources: set[str] = set()
    last_review_event: dict[str, Any] | None = None
    last_validation_event: dict[str, Any] | None = None

    for row in rows:
        item = row_to_dict(row)
        payload_json = item.get("payload_json")
        payload = json.loads(payload_json) if isinstance(payload_json, str) and payload_json.strip() else None
        item["payload"] = payload
        audit_items.append(item)
        source_value = None
        if isinstance(payload, dict):
            source_value = normalize_optional_text(payload.get("source"))
        if str(item.get("event_type", "")).startswith("review."):
            last_review_event = item
            if source_value:
                review_sources.add(source_value)
        if str(item.get("event_type", "")).startswith("validation."):
            last_validation_event = item
            if source_value:
                validation_sources.add(source_value)

    return {
        "memory_id": int(memory_id),
        "memory": memory_dict,
        "created_at": memory_dict.get("created_at"),
        "created_source": memory_dict.get("source"),
        "validation_source": memory_dict.get("validation_source"),
        "last_validated_at": memory_dict.get("last_validated_at"),
        "supersedes_memory_id": memory_dict.get("supersedes_memory_id"),
        "promoted_from_id": memory_dict.get("promoted_from_id"),
        "demoted_from_id": memory_dict.get("demoted_from_id"),
        "parent_memory_id": memory_dict.get("parent_memory_id"),
        "project_key": memory_dict.get("project_key"),
        "conversation_key": memory_dict.get("conversation_key"),
        "audit_event_count": len(audit_items),
        "review_sources": sorted(review_sources),
        "validation_sources": sorted(validation_sources),
        "last_review_event": last_review_event,
        "last_validation_event": last_validation_event,
    }


@mcp.tool
def create_memory_version(
    memory_id: int,
    content: str | None = None,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float | None = None,
    confidence_score: float | None = None,
    tags: str | None = None,
) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        current_row = require_memory_row(conn, int(memory_id))
        current_memory = _apply_ownership_defaults(enrich_memory_dict(row_to_dict(current_row)))
        created_source = normalize_optional_text(source) or current_memory.get("source") or "manual_version"
        version_memory = _insert_memory(
            conn,
            content=content or current_memory["content"],
            memory_type=current_memory["memory_type"],
            summary_short=summary_short if summary_short is not None else current_memory.get("summary_short"),
            source=created_source,
            importance_score=current_memory["importance_score"] if importance_score is None else float(importance_score),
            confidence_score=current_memory["confidence_score"] if confidence_score is None else float(confidence_score),
            tags=tags if tags is not None else current_memory.get("tags"),
            layer_code=current_memory.get("layer_code"),
            area_code=current_memory.get("area_code"),
            state_code="candidate",
            scope_code=current_memory.get("scope_code"),
            parent_memory_id=current_memory.get("parent_memory_id"),
            version=int(current_memory.get("version") or 1) + 1,
            supersedes_memory_id=int(memory_id),
            valid_from=current_memory.get("valid_from"),
            valid_to=None,
            decay_score=current_memory.get("decay_score") or 0.0,
            emotional_weight=current_memory.get("emotional_weight") or 0.0,
            identity_weight=current_memory.get("identity_weight") or 0.0,
            project_key=current_memory.get("project_key"),
            conversation_key=current_memory.get("conversation_key"),
            last_validated_at=None,
            validation_source=None,
            owner_role=current_memory.get("owner_role") or _default_owner_role(state_code="candidate", scope_code=current_memory.get("scope_code"), project_key=current_memory.get("project_key")),
            owner_id=current_memory.get("owner_id"),
            review_due_at=utc_offset_days_iso(_compute_sla_days(conn, "review", current_memory.get("priority") or "normal", current_memory.get("memory_type"), current_memory.get("scope_code"), current_memory.get("project_key"))),
        )
        event = _insert_memory_event(
            conn,
            memory_id=int(version_memory["id"]),
            event_type="version.created",
            payload={
                "source": created_source,
                "base_memory_id": int(memory_id),
                "base_version": int(current_memory.get("version") or 1),
                "new_version": int(version_memory.get("version") or 1),
            },
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "version_created",
        "base_memory_id": int(memory_id),
        "memory": version_memory,
        "event": event,
    }


@mcp.tool
def list_memory_versions(memory_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        versions = [_apply_ownership_defaults(item) for item in _collect_version_lineage(conn, int(memory_id))]
    finally:
        conn.close()
    return {
        "memory_id": int(memory_id),
        "count": len(versions),
        "items": versions,
    }


@mcp.tool
def deprecate_memory(
    memory_id: int,
    reason: str,
    source: str | None = "manual_review",
    replacement_memory_id: int | None = None,
    valid_to: str | None = None,
) -> dict[str, Any]:
    normalized_reason = normalize_required_text(reason, "reason")
    normalized_source = normalize_optional_text(source) or "manual_review"
    normalized_valid_to = normalize_optional_text(valid_to) or utc_now_iso()
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, int(memory_id))
        old_memory = enrich_memory_dict(row_to_dict(memory))
        if replacement_memory_id is not None:
            require_memory_row(conn, int(replacement_memory_id))
        conn.execute(
            """
            UPDATE memories
            SET state_code = ?,
                valid_to = ?,
                expired_due_at = ?,
                validation_source = ?,
                last_accessed_at = ?
            WHERE id = ?
            """,
            ("superseded", normalized_valid_to, shift_iso_days(normalized_valid_to, 2), normalized_source, normalized_valid_to, int(memory_id)),
        )
        event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type="version.deprecated",
            payload={
                "source": normalized_source,
                "reason": normalized_reason,
                "replacement_memory_id": None if replacement_memory_id is None else int(replacement_memory_id),
                "old_state_code": old_memory.get("state_code"),
                "new_state_code": "superseded",
            },
        )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()
    updated_memory = _apply_ownership_defaults(enrich_memory_dict(row_to_dict(updated_row)))
    return {
        "status": "deprecated",
        "memory_id": int(memory_id),
        "replacement_memory_id": None if replacement_memory_id is None else int(replacement_memory_id),
        "event": event,
        "memory": updated_memory,
    }


def _memory_matches_operational_filters(
    memory: dict[str, Any],
    *,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
) -> bool:
    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_layer = normalize_layer_code(layer_code)
    normalized_area = normalize_area_code(area_code)
    normalized_memory_type = normalize_optional_text(memory_type)
    normalized_tag = normalize_optional_text(tag)
    normalized_text_query = normalize_optional_text(text_query)

    if normalized_scope and memory.get("scope_code") != normalized_scope:
        return False
    if normalized_project_key and memory.get("project_key") != normalized_project_key:
        return False
    if normalized_layer and memory.get("layer_code") != normalized_layer:
        return False
    if normalized_area and memory.get("area_code") != normalized_area:
        return False
    if normalized_memory_type and memory.get("memory_type") != normalized_memory_type:
        return False
    if normalized_tag:
        tags_value = normalize_optional_text(memory.get("tags")) or ""
        if normalized_tag not in tags_value:
            return False
    if normalized_text_query:
        haystack = " ".join(
            [
                normalize_optional_text(memory.get("content")) or "",
                normalize_optional_text(memory.get("summary_short")) or "",
                normalize_optional_text(memory.get("tags")) or "",
            ]
        )
        if normalized_text_query not in haystack:
            return False
    return True


@mcp.tool
def list_expired_memories(
    limit: int = 20,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_as_of = normalize_optional_text(as_of) or utc_now_iso()
    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_layer = normalize_layer_code(layer_code)
    normalized_area = normalize_area_code(area_code)
    normalized_memory_type = normalize_optional_text(memory_type)
    normalized_tag = normalize_optional_text(tag)
    normalized_text_query = normalize_optional_text(text_query)

    sql = "SELECT * FROM memories WHERE valid_to IS NOT NULL AND valid_to <= ?"
    params: list[Any] = [normalized_as_of]
    if normalized_scope:
        sql += " AND scope_code = ?"
        params.append(normalized_scope)
    if normalized_project_key:
        sql += " AND project_key = ?"
        params.append(normalized_project_key)
    if normalized_layer:
        sql += " AND layer_code = ?"
        params.append(normalized_layer)
    if normalized_area:
        sql += " AND area_code = ?"
        params.append(normalized_area)
    if normalized_memory_type:
        sql += " AND memory_type = ?"
        params.append(normalized_memory_type)
    if normalized_tag:
        sql += " AND COALESCE(tags, '') LIKE ?"
        params.append(f"%{normalized_tag}%")
    if normalized_text_query:
        sql += " AND (content LIKE ? OR COALESCE(summary_short, '') LIKE ? OR COALESCE(tags, '') LIKE ?)"
        like_value = f"%{normalized_text_query}%"
        params.extend([like_value, like_value, like_value])
    sql += " ORDER BY valid_to ASC, id DESC LIMIT ?"
    params.append(int(limit))

    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
        items = [_apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row)))) for row in rows]
        items = _filter_items_by_effective_owner(
            items,
            effective_owner_key=normalized_effective_owner_key,
            effective_owner_type=normalized_effective_owner_type,
        )
    finally:
        conn.close()
    return {
        "count": len(items),
        "items": items,
        "queue_state": "expired",
        "filters": {
            "limit": int(limit),
            "as_of": normalized_as_of,
            "scope_code": normalized_scope,
            "project_key": normalized_project_key,
            "layer_code": normalized_layer,
            "area_code": normalized_area,
            "memory_type": normalized_memory_type,
            "tag": normalized_tag,
            "text_query": normalized_text_query,
            "effective_owner_key": normalized_effective_owner_key,
            "effective_owner_type": normalized_effective_owner_type,
        },
    }


@mcp.tool
def list_duplicate_candidates_admin(
    limit: int = 20,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    conn = get_db_connection()
    try:
        candidates = sandman_logic.get_duplicate_candidates(conn)
        items: list[dict[str, Any]] = []
        for candidate in candidates:
            canonical_memory = _apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(require_memory_row(conn, int(candidate["canonical_memory_id"]))))))
            duplicate_memory = _apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(require_memory_row(conn, int(candidate["duplicate_memory_id"]))))))
            if not _memory_matches_operational_filters(
                canonical_memory,
                scope_code=scope_code,
                project_key=project_key,
                layer_code=layer_code,
                area_code=area_code,
                memory_type=memory_type,
                tag=tag,
                text_query=text_query,
            ):
                continue
            _raw_review = _get_or_create_duplicate_review_item(conn, int(candidate["canonical_memory_id"]), int(candidate["duplicate_memory_id"]))
            _raw_review.setdefault("project_key", canonical_memory.get("project_key"))
            _raw_review.setdefault("scope_code", canonical_memory.get("scope_code"))
            duplicate_review = _apply_effective_owner(conn, _raw_review, owner_field=None)
            if normalized_effective_owner_key is not None or normalized_effective_owner_type is not None:
                filtered_duplicate_review = _filter_items_by_effective_owner(
                    [{"duplicate_review": duplicate_review}],
                    effective_owner_key=normalized_effective_owner_key,
                    effective_owner_type=normalized_effective_owner_type,
                    memory_field="duplicate_review",
                )
                if not filtered_duplicate_review:
                    continue
            items.append(
                {
                    **candidate,
                    "canonical_memory": canonical_memory,
                    "duplicate_memory": duplicate_memory,
                    "duplicate_review": duplicate_review,
                }
            )
            if len(items) >= int(limit):
                break
        conn.commit()
    finally:
        conn.close()
    return {
        "count": len(items),
        "items": items,
        "queue_state": "duplicates",
        "filters": {
            "limit": int(limit),
            "scope_code": normalize_scope_code(scope_code),
            "project_key": normalize_optional_text(project_key),
            "layer_code": normalize_layer_code(layer_code),
            "area_code": normalize_area_code(area_code),
            "memory_type": normalize_optional_text(memory_type),
            "tag": normalize_optional_text(tag),
            "text_query": normalize_optional_text(text_query),
            "effective_owner_key": normalized_effective_owner_key,
            "effective_owner_type": normalized_effective_owner_type,
        },
    }


def _owner_summary_from_items(items: list[dict[str, Any]], *, memory_field: str | None = None) -> dict[str, Any]:
    owner_role_counts: dict[str, int] = {}
    missing_owner_count = 0
    distinct_owner_ids: set[str] = set()

    for item in items:
        memory = item if memory_field is None else item.get(memory_field)
        if not isinstance(memory, dict):
            continue
        owner_role = normalize_optional_text(memory.get("owner_role"))
        owner_id = normalize_optional_text(memory.get("owner_id"))
        if owner_role is None:
            missing_owner_count += 1
        else:
            owner_role_counts[owner_role] = owner_role_counts.get(owner_role, 0) + 1
        if owner_id:
            distinct_owner_ids.add(owner_id)

    return {
        "owner_role_counts": owner_role_counts,
        "missing_owner_count": missing_owner_count,
        "distinct_owner_ids": sorted(distinct_owner_ids),
    }


def _effective_owner_summary_from_items(items: list[dict[str, Any]], *, memory_field: str | None = None) -> dict[str, Any]:
    effective_owner_counts: dict[str, int] = {}
    effective_owner_type_counts: dict[str, int] = {}
    unresolved_count = 0

    for item in items:
        memory = item if memory_field is None else item.get(memory_field)
        if not isinstance(memory, dict):
            continue
        effective_owner_key = normalize_optional_text(memory.get("effective_owner_key"))
        effective_owner_type = normalize_optional_text(memory.get("effective_owner_type"))
        if effective_owner_key is None:
            unresolved_count += 1
        else:
            effective_owner_counts[effective_owner_key] = effective_owner_counts.get(effective_owner_key, 0) + 1
        if effective_owner_type:
            effective_owner_type_counts[effective_owner_type] = effective_owner_type_counts.get(effective_owner_type, 0) + 1

    return {
        "effective_owner_counts": effective_owner_counts,
        "effective_owner_type_counts": effective_owner_type_counts,
        "unresolved_count": unresolved_count,
    }


def _filter_items_by_effective_owner(
    items: list[dict[str, Any]],
    *,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
    memory_field: str | None = None,
) -> list[dict[str, Any]]:
    normalized_owner_key = normalize_optional_text(effective_owner_key)
    normalized_owner_type = normalize_optional_text(effective_owner_type)
    if normalized_owner_key is None and normalized_owner_type is None:
        return items
    filtered: list[dict[str, Any]] = []
    for item in items:
        memory = item if memory_field is None else item.get(memory_field)
        if not isinstance(memory, dict):
            continue
        item_owner_key = normalize_optional_text(memory.get("effective_owner_key"))
        item_owner_type = normalize_optional_text(memory.get("effective_owner_type"))
        if normalized_owner_key is not None and item_owner_key != normalized_owner_key:
            continue
        if normalized_owner_type is not None and item_owner_type != normalized_owner_type:
            continue
        filtered.append(item)
    return filtered


def _recommended_bulk_actions(
    *,
    owner_summary: dict[str, Any],
    overdue_review_queue: dict[str, Any],
    overdue_revalidation_queue: dict[str, Any],
    overdue_expired_queue: dict[str, Any],
    overdue_duplicate_queue: dict[str, Any],
) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []

    review_overdue_ids = [int(item["id"]) for item in overdue_review_queue.get("items", [])]
    review_missing_ids = [int(item["id"]) for item in overdue_review_queue.get("items", []) if normalize_optional_text(item.get("owner_role")) is None]
    snapshot_missing = int((owner_summary.get("snapshot") or {}).get("missing_owner_count") or 0)
    if not review_missing_ids and snapshot_missing > 0 and review_overdue_ids:
        review_missing_ids = review_overdue_ids
    if review_missing_ids:
        recommendations.append({
            "kind": "assign_missing_review_owners",
            "action": "bulk_set_memory_owner",
            "target_queue": "overdue_review",
            "count": len(review_missing_ids),
            "reason": "review_overdue_with_missing_owner",
            "suggested_payload": {"memory_ids": review_missing_ids, "owner_role": "memory_maintainer"},
        })

    revalidation_missing_ids = [int(item["id"]) for item in overdue_revalidation_queue.get("items", []) if normalize_optional_text(item.get("owner_role")) is None]
    if revalidation_missing_ids:
        recommendations.append({
            "kind": "assign_missing_revalidation_owners",
            "action": "bulk_set_memory_owner",
            "target_queue": "overdue_revalidation",
            "count": len(revalidation_missing_ids),
            "reason": "revalidation_overdue_with_missing_owner",
            "suggested_payload": {"memory_ids": revalidation_missing_ids, "owner_role": "knowledge_curator"},
        })

    expired_missing_ids = [int(item["id"]) for item in overdue_expired_queue.get("items", []) if normalize_optional_text(item.get("owner_role")) is None]
    if expired_missing_ids:
        recommendations.append({
            "kind": "assign_missing_expired_owners",
            "action": "bulk_set_memory_owner",
            "target_queue": "overdue_expired",
            "count": len(expired_missing_ids),
            "reason": "expired_overdue_with_missing_owner",
            "suggested_payload": {"memory_ids": expired_missing_ids, "owner_role": "knowledge_curator"},
        })

    duplicate_missing_pairs = [
        {"canonical_memory_id": int(item["canonical_memory_id"]), "duplicate_memory_id": int(item["duplicate_memory_id"])}
        for item in overdue_duplicate_queue.get("items", [])
        if normalize_optional_text((item.get("duplicate_review") or {}).get("owner_role")) is None
    ]
    if duplicate_missing_pairs:
        recommendations.append({
            "kind": "assign_missing_duplicate_owners",
            "action": "bulk_set_duplicate_candidate_sla",
            "target_queue": "overdue_duplicates",
            "count": len(duplicate_missing_pairs),
            "reason": "duplicate_overdue_with_missing_owner",
            "suggested_payload": {"pairs": duplicate_missing_pairs, "owner_role": "memory_maintainer", "status": "open"},
        })

    overdue_review_ids = review_overdue_ids
    if len(overdue_review_ids) >= 2:
        recommendations.append({
            "kind": "rebatch_overdue_review_sla",
            "action": "bulk_set_memory_sla",
            "target_queue": "overdue_review",
            "count": len(overdue_review_ids),
            "reason": "review_overdue_batch_candidate",
            "suggested_payload": {"memory_ids": overdue_review_ids},
        })

    overdue_revalidation_ids = [int(item["id"]) for item in overdue_revalidation_queue.get("items", [])]
    if len(overdue_revalidation_ids) >= 2:
        recommendations.append({
            "kind": "rebatch_overdue_revalidation_sla",
            "action": "bulk_set_memory_sla",
            "target_queue": "overdue_revalidation",
            "count": len(overdue_revalidation_ids),
            "reason": "revalidation_overdue_batch_candidate",
            "suggested_payload": {"memory_ids": overdue_revalidation_ids},
        })

    overdue_expired_ids = [int(item["id"]) for item in overdue_expired_queue.get("items", [])]
    if len(overdue_expired_ids) >= 2:
        recommendations.append({
            "kind": "rebatch_overdue_expired_sla",
            "action": "bulk_set_memory_sla",
            "target_queue": "overdue_expired",
            "count": len(overdue_expired_ids),
            "reason": "expired_overdue_batch_candidate",
            "suggested_payload": {"memory_ids": overdue_expired_ids},
        })

    overdue_duplicate_pairs = [
        {"canonical_memory_id": int(item["canonical_memory_id"]), "duplicate_memory_id": int(item["duplicate_memory_id"])}
        for item in overdue_duplicate_queue.get("items", [])
    ]
    if len(overdue_duplicate_pairs) >= 2:
        recommendations.append({
            "kind": "rebatch_overdue_duplicate_sla",
            "action": "bulk_set_duplicate_candidate_sla",
            "target_queue": "overdue_duplicates",
            "count": len(overdue_duplicate_pairs),
            "reason": "duplicate_overdue_batch_candidate",
            "suggested_payload": {"pairs": overdue_duplicate_pairs, "status": "open"},
        })

    if snapshot_missing >= 3:
        recommendations.append({
            "kind": "global_owner_cleanup",
            "action": "bulk_set_memory_owner",
            "target_queue": "snapshot",
            "count": snapshot_missing,
            "reason": "snapshot_missing_owner_pressure",
            "suggested_payload": {"owner_role": "review_team"},
        })

    return recommendations


@mcp.tool
def get_operational_queue_dashboard(
    limit_per_queue: int = 5,
    validated_before: str | None = None,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit_per_queue < 1 or limit_per_queue > 1000:
        raise ValueError("limit_per_queue musi być w zakresie 1..1000")

    review_queue = list_review_queue(
        limit=limit_per_queue,
        memory_type=memory_type,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    revalidation_queue = list_revalidation_queue(
        limit=limit_per_queue,
        validated_before=validated_before,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    expired_queue = list_expired_memories(
        limit=limit_per_queue,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    duplicate_queue = list_duplicate_candidates_admin(
        limit=limit_per_queue,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    overdue_review_queue = list_overdue_review_queue(
        limit=limit_per_queue,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
    )
    overdue_revalidation_queue = list_overdue_revalidation_queue(
        limit=limit_per_queue,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
    )
    overdue_expired_queue = list_overdue_expired_queue(
        limit=limit_per_queue,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
    )
    overdue_duplicate_queue = list_overdue_duplicate_queue(
        limit=limit_per_queue,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
    )

    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_layer = normalize_layer_code(layer_code)
    normalized_area = normalize_area_code(area_code)
    normalized_memory_type = normalize_optional_text(memory_type)
    normalized_tag = normalize_optional_text(tag)
    normalized_text_query = normalize_optional_text(text_query)
    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)

    conn = get_db_connection()
    try:
        feature_flag = _get_feature_flag_config(conn, CROSS_PROJECT_FLAG_KEY)

        sql = "SELECT owner_role, owner_id FROM memories WHERE 1 = 1"
        params: list[Any] = []
        if normalized_scope:
            sql += " AND scope_code = ?"
            params.append(normalized_scope)
        if normalized_project_key:
            sql += " AND project_key = ?"
            params.append(normalized_project_key)
        if normalized_layer:
            sql += " AND layer_code = ?"
            params.append(normalized_layer)
        if normalized_area:
            sql += " AND area_code = ?"
            params.append(normalized_area)
        if normalized_memory_type:
            sql += " AND memory_type = ?"
            params.append(normalized_memory_type)
        if normalized_tag:
            sql += " AND COALESCE(tags, '') LIKE ?"
            params.append(f"%{normalized_tag}%")
        if normalized_text_query:
            sql += " AND (content LIKE ? OR COALESCE(summary_short, '') LIKE ? OR COALESCE(tags, '') LIKE ?)"
            like_value = f"%{normalized_text_query}%"
            params.extend([like_value, like_value, like_value])
        owner_rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    feature_flag_evaluation = _evaluate_feature_flag_config(feature_flag, project_key=normalized_project_key, scope_code=normalized_scope)
    feature_flag_view = dict(feature_flag)
    feature_flag_view["key"] = feature_flag_view.get("flag_key")
    feature_flag_view["enabled"] = bool(int(feature_flag_view.get("is_enabled") or 0))
    feature_flag_view["rollout_scope"] = feature_flag_view.get("allowed_scope_codes")
    feature_flag_view["rollout_project_key"] = feature_flag_view.get("allowed_project_keys")
    rollout_mode_aliases = {"all": "global", "projects": "project", "scopes": "scope", "projects_and_scopes": "scoped_project", "off": "off"}
    feature_flag_view["rollout_mode"] = rollout_mode_aliases.get(str(feature_flag_view.get("rollout_mode") or "off"), feature_flag_view.get("rollout_mode"))

    owner_snapshot_items = [{"owner_role": row["owner_role"], "owner_id": row["owner_id"]} for row in owner_rows]
    owner_snapshot = _owner_summary_from_items(owner_snapshot_items)
    owner_summary = {
        "review_queue": _owner_summary_from_items(review_queue["items"]),
        "revalidation_queue": _owner_summary_from_items(revalidation_queue["items"]),
        "expired_queue": _owner_summary_from_items(expired_queue["items"]),
        "overdue_review_queue": _owner_summary_from_items(overdue_review_queue["items"]),
        "overdue_revalidation_queue": _owner_summary_from_items(overdue_revalidation_queue["items"]),
        "overdue_expired_queue": _owner_summary_from_items(overdue_expired_queue["items"]),
        "overdue_duplicate_queue": _owner_summary_from_items(overdue_duplicate_queue["items"], memory_field="duplicate_review"),
        "duplicate_queue": _owner_summary_from_items(duplicate_queue["items"], memory_field="duplicate_review"),
        "snapshot": owner_snapshot,
    }
    effective_owner_summary = {
        "review_queue": _effective_owner_summary_from_items(review_queue["items"]),
        "revalidation_queue": _effective_owner_summary_from_items(revalidation_queue["items"]),
        "expired_queue": _effective_owner_summary_from_items(expired_queue["items"]),
        "overdue_review_queue": _effective_owner_summary_from_items(overdue_review_queue["items"]),
        "overdue_revalidation_queue": _effective_owner_summary_from_items(overdue_revalidation_queue["items"]),
        "overdue_expired_queue": _effective_owner_summary_from_items(overdue_expired_queue["items"]),
        "overdue_duplicate_queue": _effective_owner_summary_from_items(overdue_duplicate_queue["items"], memory_field="duplicate_review"),
        "duplicate_queue": _effective_owner_summary_from_items(duplicate_queue["items"], memory_field="duplicate_review"),
    }
    recommended_bulk_actions = _recommended_bulk_actions(
        owner_summary=owner_summary,
        overdue_review_queue=overdue_review_queue,
        overdue_revalidation_queue=overdue_revalidation_queue,
        overdue_expired_queue=overdue_expired_queue,
        overdue_duplicate_queue=overdue_duplicate_queue,
    )
    owner_catalog_repair_summary = get_owner_catalog_repair_summary(
        project_key=normalized_project_key,
        scope_code=normalized_scope,
        limit_recent_audits=max(5, int(limit_per_queue)),
        max_groups=max(5, int(limit_per_queue)),
    )

    return {
        "filters": {
            "limit_per_queue": int(limit_per_queue),
            "validated_before": normalize_optional_text(validated_before),
            "as_of": normalize_optional_text(as_of),
            "scope_code": normalized_scope,
            "project_key": normalized_project_key,
            "layer_code": normalized_layer,
            "area_code": normalized_area,
            "memory_type": normalized_memory_type,
            "tag": normalized_tag,
            "text_query": normalized_text_query,
            "effective_owner_key": normalized_effective_owner_key,
            "effective_owner_type": normalized_effective_owner_type,
        },
        "feature_flag": feature_flag_view,
        "feature_flag_evaluation": feature_flag_evaluation,
        "owner_summary": owner_summary,
        "effective_owner_summary": effective_owner_summary,
        "recommended_bulk_actions": recommended_bulk_actions,
        "owner_catalog_repair_summary": owner_catalog_repair_summary,
        "summary": {
            "review_queue_count": review_queue["count"],
            "revalidation_queue_count": revalidation_queue["count"],
            "expired_queue_count": expired_queue["count"],
            "duplicate_queue_count": duplicate_queue["count"],
            "overdue_review_queue_count": overdue_review_queue["count"],
            "overdue_revalidation_queue_count": overdue_revalidation_queue["count"],
            "overdue_expired_queue_count": overdue_expired_queue["count"],
            "overdue_duplicate_queue_count": overdue_duplicate_queue["count"],
            "missing_owner_count": owner_snapshot["missing_owner_count"],
            "owner_catalog_problem_count": int((owner_catalog_repair_summary.get("health") or {}).get("summary", {}).get("problem_count") or 0),
            "owner_catalog_batch_candidate_count": int((owner_catalog_repair_summary.get("batch_candidates_summary") or {}).get("count") or 0),
            "owner_catalog_bulk_repair_count": int((owner_catalog_repair_summary.get("repair_audit_summary") or {}).get("bulk_repair_count") or 0),
            "owner_catalog_governance_warning_count": int((owner_catalog_repair_summary.get("health") or {}).get("summary", {}).get("governance_warning_count") or 0),
            "owner_catalog_governance_event_count": int((owner_catalog_repair_summary.get("governance_history_summary") or {}).get("governance_event_count") or 0),
        },
        "queues": {
            "review": review_queue,
            "revalidation": revalidation_queue,
            "expired": expired_queue,
            "duplicates": duplicate_queue,
            "overdue_review": overdue_review_queue,
            "overdue_revalidation": overdue_revalidation_queue,
            "overdue_expired": overdue_expired_queue,
            "overdue_duplicates": overdue_duplicate_queue,
        },
    }


def _accumulate_effective_owner_workload(
    buckets: dict[str, dict[str, Any]],
    items: list[dict[str, Any]],
    *,
    bucket_name: str,
    memory_field: str | None = None,
) -> None:
    for item in items:
        memory = item if memory_field is None else item.get(memory_field)
        if not isinstance(memory, dict):
            continue
        effective_owner_key = normalize_optional_text(memory.get("effective_owner_key")) or "__unresolved__"
        bucket = buckets.setdefault(
            effective_owner_key,
            {
                "effective_owner_key": None if effective_owner_key == "__unresolved__" else effective_owner_key,
                "effective_owner_type": normalize_optional_text(memory.get("effective_owner_type")),
                "effective_display_name": normalize_optional_text(memory.get("effective_display_name")),
                "owner_resolution_reason": normalize_optional_text(memory.get("owner_resolution_reason")),
                "counts": {
                    "review": 0,
                    "revalidation": 0,
                    "expired": 0,
                    "duplicates": 0,
                    "overdue_review": 0,
                    "overdue_revalidation": 0,
                    "overdue_expired": 0,
                    "overdue_duplicates": 0,
                },
                "total_count": 0,
                "overdue_total": 0,
            },
        )
        if bucket.get("effective_owner_type") is None:
            bucket["effective_owner_type"] = normalize_optional_text(memory.get("effective_owner_type"))
        if bucket.get("effective_display_name") is None:
            bucket["effective_display_name"] = normalize_optional_text(memory.get("effective_display_name"))
        if bucket.get("owner_resolution_reason") is None:
            bucket["owner_resolution_reason"] = normalize_optional_text(memory.get("owner_resolution_reason"))
        bucket["counts"][bucket_name] += 1
        bucket["total_count"] += 1
        if bucket_name.startswith("overdue_"):
            bucket["overdue_total"] += 1


@mcp.tool
def get_effective_owner_workload(
    limit: int = 50,
    validated_before: str | None = None,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")

    review_queue = list_review_queue(
        limit=1000,
        memory_type=memory_type,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    revalidation_queue = list_revalidation_queue(
        limit=1000,
        validated_before=validated_before,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    expired_queue = list_expired_memories(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    duplicate_queue = list_duplicate_candidates_admin(
        limit=1000,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    overdue_review_queue = list_overdue_review_queue(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    overdue_revalidation_queue = list_overdue_revalidation_queue(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    overdue_expired_queue = list_overdue_expired_queue(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    overdue_duplicate_queue = list_overdue_duplicate_queue(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )

    buckets: dict[str, dict[str, Any]] = {}
    _accumulate_effective_owner_workload(buckets, review_queue["items"], bucket_name="review")
    _accumulate_effective_owner_workload(buckets, revalidation_queue["items"], bucket_name="revalidation")
    _accumulate_effective_owner_workload(buckets, expired_queue["items"], bucket_name="expired")
    _accumulate_effective_owner_workload(buckets, duplicate_queue["items"], bucket_name="duplicates", memory_field="duplicate_review")
    _accumulate_effective_owner_workload(buckets, overdue_review_queue["items"], bucket_name="overdue_review")
    _accumulate_effective_owner_workload(buckets, overdue_revalidation_queue["items"], bucket_name="overdue_revalidation")
    _accumulate_effective_owner_workload(buckets, overdue_expired_queue["items"], bucket_name="overdue_expired")
    _accumulate_effective_owner_workload(buckets, overdue_duplicate_queue["items"], bucket_name="overdue_duplicates", memory_field="duplicate_review")

    items = sorted(
        buckets.values(),
        key=lambda item: (-int(item.get("total_count") or 0), -int(item.get("overdue_total") or 0), str(item.get("effective_owner_key") or "")),
    )

    return {
        "count": len(items),
        "items": items[: int(limit)],
        "filters": {
            "limit": int(limit),
            "validated_before": normalize_optional_text(validated_before),
            "as_of": normalize_optional_text(as_of),
            "scope_code": normalize_scope_code(scope_code),
            "project_key": normalize_optional_text(project_key),
            "layer_code": normalize_layer_code(layer_code),
            "area_code": normalize_area_code(area_code),
            "memory_type": normalize_optional_text(memory_type),
            "tag": normalize_optional_text(tag),
            "text_query": normalize_optional_text(text_query),
            "effective_owner_key": normalize_optional_text(effective_owner_key),
            "effective_owner_type": normalize_optional_text(effective_owner_type),
        },
        "summary": {
            "review_queue_count": review_queue["count"],
            "revalidation_queue_count": revalidation_queue["count"],
            "expired_queue_count": expired_queue["count"],
            "duplicate_queue_count": duplicate_queue["count"],
            "overdue_review_count": overdue_review_queue["count"],
            "overdue_revalidation_count": overdue_revalidation_queue["count"],
            "overdue_expired_count": overdue_expired_queue["count"],
            "overdue_duplicate_count": overdue_duplicate_queue["count"],
        },
    }


def _rebalance_candidate_items(items: list[dict[str, Any]], *, memory_field: str | None = None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items:
        memory = item if memory_field is None else item.get(memory_field)
        if not isinstance(memory, dict):
            continue
        normalized.append({
            "memory_id": int(item.get("id") or 0) if memory_field is None else None,
            "canonical_memory_id": item.get("canonical_memory_id"),
            "duplicate_memory_id": item.get("duplicate_memory_id"),
            "summary_short": item.get("summary_short") if memory_field is None else None,
            "memory_type": item.get("memory_type") if memory_field is None else None,
            "owner_role": memory.get("owner_role"),
            "effective_owner_key": memory.get("effective_owner_key"),
            "effective_owner_type": memory.get("effective_owner_type"),
            "effective_display_name": memory.get("effective_display_name"),
            "review_due_at": item.get("review_due_at") if memory_field is None else None,
            "revalidation_due_at": item.get("revalidation_due_at") if memory_field is None else None,
            "expired_due_at": item.get("expired_due_at") if memory_field is None else None,
            "duplicate_due_at": memory.get("duplicate_due_at") if memory_field is not None else None,
        })
    return normalized


@mcp.tool
def get_owner_rebalance_candidates(
    limit: int = 10,
    candidate_limit_per_queue: int = 10,
    overloaded_owner_key: str | None = None,
    validated_before: str | None = None,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    if candidate_limit_per_queue < 1 or candidate_limit_per_queue > 1000:
        raise ValueError("candidate_limit_per_queue musi być w zakresie 1..1000")

    owner_workload = get_effective_owner_workload(
        limit=200,
        validated_before=validated_before,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
    )
    workload_items = owner_workload.get("items") or []
    normalized_overloaded_owner_key = normalize_optional_text(overloaded_owner_key)
    if normalized_overloaded_owner_key is not None:
        source_owner = next((item for item in workload_items if item.get("effective_owner_key") == normalized_overloaded_owner_key), None)
    else:
        ranked_sources = sorted(
            [item for item in workload_items if normalize_optional_text(item.get("effective_owner_key")) is not None],
            key=lambda item: (-int(item.get("overdue_total") or 0), -int(item.get("total_count") or 0), str(item.get("effective_owner_key") or "")),
        )
        source_owner = ranked_sources[0] if ranked_sources else None

    if source_owner is None:
        return {"status": "no_source_owner", "count": 0, "source_owner": None, "target_candidates": [], "candidate_groups": {}, "recommended_actions": [], "filters": {"limit": int(limit), "candidate_limit_per_queue": int(candidate_limit_per_queue), "overloaded_owner_key": normalized_overloaded_owner_key}}

    source_owner_key = normalize_optional_text(source_owner.get("effective_owner_key"))
    source_owner_type = normalize_optional_text(source_owner.get("effective_owner_type"))

    review_items = list_overdue_review_queue(limit=1000, as_of=as_of, scope_code=scope_code, project_key=project_key, effective_owner_key=source_owner_key, effective_owner_type=source_owner_type)["items"]
    revalidation_items = list_overdue_revalidation_queue(limit=1000, as_of=as_of, scope_code=scope_code, project_key=project_key, effective_owner_key=source_owner_key, effective_owner_type=source_owner_type)["items"]
    expired_items = list_overdue_expired_queue(limit=1000, as_of=as_of, scope_code=scope_code, project_key=project_key, effective_owner_key=source_owner_key, effective_owner_type=source_owner_type)["items"]
    duplicate_items = list_overdue_duplicate_queue(limit=1000, as_of=as_of, scope_code=scope_code, project_key=project_key, effective_owner_key=source_owner_key, effective_owner_type=source_owner_type)["items"]

    mappings = list_owner_role_mappings(project_key=project_key, scope_code=scope_code, active_only=True)["items"]
    roles_by_owner_key: dict[str, list[str]] = {}
    for mapping in mappings:
        owner_key = normalize_optional_text(mapping.get("owner_key"))
        owner_role = normalize_optional_text(mapping.get("owner_role"))
        if owner_key is None or owner_role is None:
            continue
        roles = roles_by_owner_key.setdefault(owner_key, [])
        if owner_role not in roles:
            roles.append(owner_role)

    active_targets = list_owner_directory_items(owner_type=source_owner_type, active_only=True)["items"] if source_owner_type else []
    workload_by_owner_key = {item.get("effective_owner_key"): item for item in workload_items if normalize_optional_text(item.get("effective_owner_key")) is not None}
    target_candidates: list[dict[str, Any]] = []
    for target in active_targets:
        owner_key = normalize_optional_text(target.get("owner_key"))
        if owner_key is None or owner_key == source_owner_key:
            continue
        target_workload = workload_by_owner_key.get(owner_key, {})
        target_roles = roles_by_owner_key.get(owner_key, [])
        target_candidates.append({"effective_owner_key": owner_key, "effective_owner_type": normalize_optional_text(target.get("owner_type")), "effective_display_name": normalize_optional_text(target.get("display_name")), "total_count": int(target_workload.get("total_count") or 0), "overdue_total": int(target_workload.get("overdue_total") or 0), "available_owner_roles": target_roles, "recommended_owner_role": target_roles[0] if target_roles else None})
    target_candidates.sort(key=lambda item: (int(item.get("overdue_total") or 0), int(item.get("total_count") or 0), str(item.get("effective_owner_key") or "")))
    target_candidates = target_candidates[: int(limit)]

    candidate_groups = {
        "overdue_review": {"count": len(review_items), "items": _rebalance_candidate_items(review_items)[: int(candidate_limit_per_queue)], "memory_ids": [int(item.get("id") or 0) for item in review_items[: int(candidate_limit_per_queue)]]},
        "overdue_revalidation": {"count": len(revalidation_items), "items": _rebalance_candidate_items(revalidation_items)[: int(candidate_limit_per_queue)], "memory_ids": [int(item.get("id") or 0) for item in revalidation_items[: int(candidate_limit_per_queue)]]},
        "overdue_expired": {"count": len(expired_items), "items": _rebalance_candidate_items(expired_items)[: int(candidate_limit_per_queue)], "memory_ids": [int(item.get("id") or 0) for item in expired_items[: int(candidate_limit_per_queue)]]},
        "overdue_duplicates": {"count": len(duplicate_items), "items": _rebalance_candidate_items(duplicate_items, memory_field="duplicate_review")[: int(candidate_limit_per_queue)], "pairs": [{"canonical_memory_id": int(item.get("canonical_memory_id") or 0), "duplicate_memory_id": int(item.get("duplicate_memory_id") or 0)} for item in duplicate_items[: int(candidate_limit_per_queue)]]},
    }

    primary_target = next((item for item in target_candidates if item.get("recommended_owner_role")), None)
    recommended_actions: list[dict[str, Any]] = []
    if primary_target is not None and primary_target.get("recommended_owner_role"):
        for queue_name in ["overdue_review", "overdue_revalidation", "overdue_expired"]:
            memory_ids = candidate_groups[queue_name].get("memory_ids") or []
            if memory_ids:
                recommended_actions.append({"kind": f"rebalance_{queue_name}", "action": "bulk_set_memory_owner", "target_owner": primary_target, "payload": {"memory_ids": memory_ids, "owner_role": primary_target.get("recommended_owner_role")}})
        duplicate_pairs = candidate_groups["overdue_duplicates"].get("pairs") or []
        if duplicate_pairs:
            recommended_actions.append({"kind": "rebalance_overdue_duplicates", "action": "bulk_set_duplicate_candidate_sla", "target_owner": primary_target, "payload": {"pairs": duplicate_pairs, "owner_role": primary_target.get("recommended_owner_role"), "status": "open"}})

    return {"status": "ok", "count": len(target_candidates), "source_owner": source_owner, "target_candidates": target_candidates, "candidate_groups": candidate_groups, "recommended_actions": recommended_actions, "filters": {"limit": int(limit), "candidate_limit_per_queue": int(candidate_limit_per_queue), "overloaded_owner_key": source_owner_key}}


def _get_owner_catalog_health_data(
    conn,
    *,
    project_key: str | None = None,
    scope_code: str | None = None,
) -> dict[str, Any]:
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    mapping_rows = conn.execute("SELECT * FROM owner_role_mappings WHERE is_active = 1 ORDER BY id ASC").fetchall()
    active_directory_rows = conn.execute("SELECT * FROM owner_directory_items WHERE is_active = 1 ORDER BY owner_key ASC").fetchall()
    problems: list[dict[str, Any]] = []
    governance_warnings: list[dict[str, Any]] = []
    broken_count = 0
    inactive_count = 0
    governance_warning_count = 0
    active_target_count = len(active_directory_rows)

    for owner_row in conn.execute("SELECT * FROM owner_directory_items ORDER BY owner_key ASC").fetchall():
        owner_item = _owner_directory_item_to_dict(owner_row)
        owner_warnings = _owner_directory_governance_warnings(
            str(owner_item.get("owner_key") or ""),
            str(owner_item.get("owner_type") or ""),
            normalize_optional_text(owner_item.get("routing_metadata_json")),
            is_active=bool(owner_item.get("is_active")),
        )
        for warning in owner_warnings:
            warning_item = dict(warning)
            warning_item.update({
                "owner_key": owner_item.get("owner_key"),
                "owner_type": owner_item.get("owner_type"),
                "source": "owner_directory_item",
            })
            governance_warnings.append(warning_item)

    for row in mapping_rows:
        mapping = _owner_role_mapping_to_dict(row)
        mapping_project_key = normalize_optional_text(mapping.get('project_key'))
        mapping_scope_code = normalize_scope_code(mapping.get('scope_code'))
        if normalized_project_key is not None and mapping_project_key not in {None, normalized_project_key}:
            continue
        if normalized_scope_code is not None and mapping_scope_code not in {None, normalized_scope_code}:
            continue
        mapping_warnings = _owner_mapping_governance_warnings(
            conn,
            owner_role=str(mapping.get('owner_role') or ''),
            owner_key=str(mapping.get('owner_key') or ''),
            project_key=mapping.get('project_key'),
            scope_code=mapping.get('scope_code'),
            is_active=bool(mapping.get('is_active')),
            current_mapping_id=int(mapping.get('id') or 0),
        )
        for warning in mapping_warnings:
            warning_item = dict(warning)
            warning_item.update({
                'owner_role': mapping.get('owner_role'),
                'owner_key': mapping.get('owner_key'),
                'project_key': mapping.get('project_key'),
                'scope_code': mapping.get('scope_code'),
                'mapping_id': int(mapping.get('id') or 0),
                'source': 'owner_role_mapping',
            })
            governance_warnings.append(warning_item)

        owner_row = conn.execute('SELECT * FROM owner_directory_items WHERE owner_key = ?', (mapping['owner_key'],)).fetchone()
        if owner_row is None:
            broken_count += 1
            problems.append({
                'kind': 'broken_owner_mapping',
                'reason': 'owner_missing_in_directory',
                'owner_role': mapping.get('owner_role'),
                'owner_key': mapping.get('owner_key'),
                'project_key': mapping.get('project_key'),
                'scope_code': mapping.get('scope_code'),
                'mapping_id': int(mapping.get('id') or 0),
            })
            continue
        owner_item = _owner_directory_item_to_dict(owner_row)
        if not bool(owner_item.get('is_active')):
            broken_count += 1
            inactive_count += 1
            problems.append({
                'kind': 'inactive_owner_target',
                'reason': 'owner_inactive',
                'owner_role': mapping.get('owner_role'),
                'owner_key': mapping.get('owner_key'),
                'project_key': mapping.get('project_key'),
                'scope_code': mapping.get('scope_code'),
                'mapping_id': int(mapping.get('id') or 0),
            })

    return {
        'broken_owner_mapping_count': broken_count,
        'inactive_owner_target_count': inactive_count,
        'active_owner_target_count': active_target_count,
        'governance_warning_count': len(governance_warnings),
        'problem_count': len(problems),
        'problems': problems,
        'governance_warnings': governance_warnings,
    }


@mcp.tool
def get_owner_catalog_health(
    project_key: str | None = None,
    scope_code: str | None = None,
) -> dict[str, Any]:
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    conn = get_db_connection()
    try:
        health = _get_owner_catalog_health_data(conn, project_key=normalized_project_key, scope_code=normalized_scope_code)
    finally:
        conn.close()
    return {
        'status': 'ok' if int(health.get('problem_count') or 0) == 0 else 'attention',
        'filters': {
            'project_key': normalized_project_key,
            'scope_code': normalized_scope_code,
        },
        'summary': {
            'broken_owner_mapping_count': int(health.get('broken_owner_mapping_count') or 0),
            'inactive_owner_target_count': int(health.get('inactive_owner_target_count') or 0),
            'active_owner_target_count': int(health.get('active_owner_target_count') or 0),
            'governance_warning_count': int(health.get('governance_warning_count') or 0),
            'problem_count': int(health.get('problem_count') or 0),
        },
        'problems': health.get('problems') or [],
        'governance_warnings': health.get('governance_warnings') or [],
    }


def _suggest_owner_mapping_repairs(
    conn,
    *,
    owner_role: str | None,
    owner_key: str | None,
    project_key: str | None,
    scope_code: str | None,
    reason: str | None,
) -> list[dict[str, Any]]:
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_owner_key = normalize_optional_text(owner_key)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    normalized_reason = normalize_optional_text(reason)
    suggestions: list[dict[str, Any]] = []

    def _score_suggestion(suggestion: dict[str, Any]) -> int:
        kind_value = normalize_optional_text(suggestion.get("kind"))
        score = 10
        if kind_value == "reactivate_owner_target":
            score = 100 if normalized_reason == "owner_inactive" else 70
        elif kind_value == "remap_to_existing_role_target":
            score = 95
            mapping_scope = suggestion.get("mapping_scope") or {}
            if normalize_optional_text(mapping_scope.get("project_key")) == normalized_project_key:
                score += 5
            if normalize_scope_code(mapping_scope.get("scope_code")) == normalized_scope_code:
                score += 3
        elif kind_value == "remap_to_active_same_type_target":
            score = 75
        elif kind_value == "create_missing_owner_target":
            score = 60 if normalized_reason == "owner_missing_in_directory" else 40
        return int(score)

    if normalized_owner_key is not None:
        owner_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (normalized_owner_key,)).fetchone()
        if owner_row is not None:
            owner_item = _owner_directory_item_to_dict(owner_row)
            if not bool(owner_item.get("is_active")):
                suggestions.append({
                    "kind": "reactivate_owner_target",
                    "owner_key": normalized_owner_key,
                    "display_name": owner_item.get("display_name"),
                })
            owner_type = normalize_optional_text(owner_item.get("owner_type"))
        else:
            owner_type = None
            suggestions.append({
                "kind": "create_missing_owner_target",
                "owner_key": normalized_owner_key,
            })
    else:
        owner_type = None

    rows = conn.execute("SELECT * FROM owner_role_mappings WHERE owner_role = ? AND is_active = 1 ORDER BY id ASC", (normalized_owner_role,)).fetchall() if normalized_owner_role else []
    for row in rows:
        mapping = _owner_role_mapping_to_dict(row)
        mapping_project_key = normalize_optional_text(mapping.get("project_key"))
        mapping_scope_code = normalize_scope_code(mapping.get("scope_code"))
        if mapping_project_key is not None and mapping_project_key != normalized_project_key:
            continue
        if mapping_scope_code is not None and mapping_scope_code != normalized_scope_code:
            continue
        candidate_owner_key = normalize_optional_text(mapping.get("owner_key"))
        if candidate_owner_key is None or candidate_owner_key == normalized_owner_key:
            continue
        candidate_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (candidate_owner_key,)).fetchone()
        if candidate_row is None:
            continue
        candidate_item = _owner_directory_item_to_dict(candidate_row)
        if not bool(candidate_item.get("is_active")):
            continue
        suggestions.append({
            "kind": "remap_to_existing_role_target",
            "owner_key": candidate_owner_key,
            "display_name": candidate_item.get("display_name"),
            "owner_type": candidate_item.get("owner_type"),
            "mapping_scope": {"project_key": mapping.get("project_key"), "scope_code": mapping.get("scope_code")},
        })

    if owner_type is not None:
        directory_rows = conn.execute("SELECT * FROM owner_directory_items WHERE owner_type = ? AND is_active = 1 ORDER BY owner_key ASC", (owner_type,)).fetchall()
        for row in directory_rows:
            candidate_item = _owner_directory_item_to_dict(row)
            candidate_owner_key = normalize_optional_text(candidate_item.get("owner_key"))
            if candidate_owner_key is None or candidate_owner_key == normalized_owner_key:
                continue
            suggestions.append({
                "kind": "remap_to_active_same_type_target",
                "owner_key": candidate_owner_key,
                "display_name": candidate_item.get("display_name"),
                "owner_type": candidate_item.get("owner_type"),
            })

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()
    for suggestion in suggestions:
        key = (str(suggestion.get("kind") or ""), normalize_optional_text(suggestion.get("owner_key")))
        if key in seen:
            continue
        seen.add(key)
        suggestion["score"] = _score_suggestion(suggestion)
        deduped.append(suggestion)
    deduped.sort(key=lambda item: (-int(item.get("score") or 0), str(item.get("kind") or ""), str(item.get("owner_key") or "")))
    for index, suggestion in enumerate(deduped, start=1):
        suggestion["rank"] = int(index)
        suggestion["is_recommended"] = index == 1
    return deduped


@mcp.tool
def get_problematic_owner_mappings(
    limit: int = 50,
    project_key: str | None = None,
    scope_code: str | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    normalized_kind = normalize_optional_text(kind)
    conn = get_db_connection()
    try:
        health = _get_owner_catalog_health_data(conn, project_key=normalized_project_key, scope_code=normalized_scope_code)
        items: list[dict[str, Any]] = []
        for problem in health.get("problems") or []:
            if normalized_kind is not None and normalize_optional_text(problem.get("kind")) != normalized_kind:
                continue
            problem_item = dict(problem)
            priority = "P1" if normalize_optional_text(problem.get("kind")) in {"broken_owner_mapping", "inactive_owner_target"} else "P2"
            problem_item["priority"] = priority
            problem_item["repair_suggestions"] = _suggest_owner_mapping_repairs(
                conn,
                owner_role=problem.get("owner_role"),
                owner_key=problem.get("owner_key"),
                project_key=problem.get("project_key"),
                scope_code=problem.get("scope_code"),
                reason=problem.get("reason"),
            )
            problem_item["recommended_repair"] = problem_item["repair_suggestions"][0] if problem_item["repair_suggestions"] else None
            items.append(problem_item)
    finally:
        conn.close()

    items.sort(key=lambda item: (0 if item.get("priority") == "P1" else 1, str(item.get("owner_role") or ""), int(item.get("mapping_id") or 0)))
    return {
        "status": "ok" if not items else "attention",
        "count": len(items),
        "items": items[: int(limit)],
        "filters": {
            "limit": int(limit),
            "project_key": normalized_project_key,
            "scope_code": normalized_scope_code,
            "kind": normalized_kind,
        },
    }


@mcp.tool
def repair_owner_mapping_issue(
    mapping_id: int,
    repair_kind: str,
    target_owner_key: str | None = None,
    owner_type: str | None = None,
    display_name: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    normalized_repair_kind = normalize_required_text(repair_kind, "repair_kind")
    normalized_target_owner_key = normalize_optional_text(target_owner_key)
    normalized_owner_type = normalize_optional_text(owner_type) or "team"
    normalized_display_name = normalize_optional_text(display_name)
    normalized_notes = normalize_optional_text(notes)
    conn = get_db_connection()
    try:
        mapping_row = conn.execute("SELECT * FROM owner_role_mappings WHERE id = ?", (int(mapping_id),)).fetchone()
        if mapping_row is None:
            raise FileNotFoundError(f"Owner role mapping not found: {mapping_id}")
        mapping = _owner_role_mapping_to_dict(mapping_row)
        current_owner_key = normalize_optional_text(mapping.get("owner_key"))
        if current_owner_key is None:
            raise ValueError("Mapping nie ma owner_key")

        if normalized_repair_kind == "reactivate_owner_target":
            owner_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (current_owner_key,)).fetchone()
            if owner_row is None:
                raise FileNotFoundError(f"Owner directory item not found: {current_owner_key}")
            conn.execute(
                "UPDATE owner_directory_items SET is_active = 1, updated_at = ? WHERE owner_key = ?",
                (utc_now_iso(), current_owner_key),
            )
        elif normalized_repair_kind == "remap_to_target":
            if normalized_target_owner_key is None:
                raise ValueError("target_owner_key jest wymagany dla remap_to_target")
            owner_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (normalized_target_owner_key,)).fetchone()
            if owner_row is None:
                raise FileNotFoundError(f"Owner directory item not found: {normalized_target_owner_key}")
            owner_item = _owner_directory_item_to_dict(owner_row)
            if not bool(owner_item.get("is_active")):
                raise ValueError("Nie mozna przepiac mapowania na nieaktywny target")
            conn.execute(
                "UPDATE owner_role_mappings SET owner_key = ?, notes = COALESCE(?, notes), updated_at = ? WHERE id = ?",
                (normalized_target_owner_key, normalized_notes, utc_now_iso(), int(mapping_id)),
            )
        elif normalized_repair_kind == "create_missing_owner_target":
            create_owner_key = normalized_target_owner_key or current_owner_key
            resolved_display_name = normalized_display_name or create_owner_key.replace("_", " ").title()
            conn.execute(
                """
                INSERT INTO owner_directory_items (owner_key, owner_type, display_name, is_active, routing_metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, 1, NULL, ?, ?)
                ON CONFLICT(owner_key) DO UPDATE SET
                    owner_type = excluded.owner_type,
                    display_name = excluded.display_name,
                    is_active = 1,
                    updated_at = excluded.updated_at
                """,
                (create_owner_key, normalized_owner_type, resolved_display_name, utc_now_iso(), utc_now_iso()),
            )
            if create_owner_key != current_owner_key:
                conn.execute(
                    "UPDATE owner_role_mappings SET owner_key = ?, notes = COALESCE(?, notes), updated_at = ? WHERE id = ?",
                    (create_owner_key, normalized_notes, utc_now_iso(), int(mapping_id)),
                )
        else:
            raise ValueError("repair_kind musi byc jednym z: reactivate_owner_target, remap_to_target, create_missing_owner_target")

        updated_mapping_row = conn.execute("SELECT * FROM owner_role_mappings WHERE id = ?", (int(mapping_id),)).fetchone()
        updated_mapping = _owner_role_mapping_to_dict(updated_mapping_row)
        updated_owner_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (updated_mapping["owner_key"],)).fetchone()
        updated_owner = None if updated_owner_row is None else _owner_directory_item_to_dict(updated_owner_row)
        audit_project_key = _owner_catalog_audit_project_key(updated_mapping.get("project_key"))
        audit_title = f"Owner mapping repaired: {updated_mapping.get('owner_role')}"
        audit_description = f"repair_kind={normalized_repair_kind}; owner_key={updated_mapping.get('owner_key')}"
        audit_event_id = timeline.record_project_event(
            conn,
            project_key=audit_project_key,
            event_type="project.note_recorded",
            title=audit_title,
            description=audit_description,
            origin="system",
            tags=["owner_mapping_repair", normalized_repair_kind],
            status="completed",
            canonical=True,
            category="owner_mapping_repair",
            now_fn=utc_now_iso,
        )
        audit_row = conn.execute("SELECT * FROM timeline_events WHERE id = ?", (audit_event_id,)).fetchone()
        audit_item = timeline.timeline_rows_to_dicts([audit_row], row_to_dict=row_to_dict)[0] if audit_row is not None else None
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "repaired",
        "repair_kind": normalized_repair_kind,
        "owner_role_mapping": updated_mapping,
        "owner_directory_item": updated_owner,
        "audit_event": audit_item,
    }


@mcp.tool
def preview_bulk_repair_owner_mappings(
    mapping_ids: list[int],
    repair_kind: str,
    target_owner_key: str | None = None,
    owner_type: str | None = None,
    display_name: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    if not mapping_ids:
        raise ValueError("mapping_ids nie moze byc puste")
    normalized_repair_kind = normalize_required_text(repair_kind, "repair_kind")
    normalized_target_owner_key = normalize_optional_text(target_owner_key)
    normalized_owner_type = normalize_optional_text(owner_type) or "team"
    normalized_display_name = normalize_optional_text(display_name)
    normalized_notes = normalize_optional_text(notes)
    normalized_ids: list[int] = []
    for item in mapping_ids:
        value = int(item)
        if value < 1:
            raise ValueError("mapping_ids musza byc dodatnie")
        if value not in normalized_ids:
            normalized_ids.append(value)

    items: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    conn = get_db_connection()
    try:
        for mapping_id in normalized_ids:
            mapping_row = conn.execute("SELECT * FROM owner_role_mappings WHERE id = ?", (int(mapping_id),)).fetchone()
            if mapping_row is None:
                errors.append({"mapping_id": int(mapping_id), "error_type": "FileNotFoundError", "message": f"Owner role mapping not found: {mapping_id}"})
                continue
            mapping = _owner_role_mapping_to_dict(mapping_row)
            current_owner_key = normalize_optional_text(mapping.get("owner_key"))
            current_owner_row = None if current_owner_key is None else conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (current_owner_key,)).fetchone()
            current_owner = None if current_owner_row is None else _owner_directory_item_to_dict(current_owner_row)

            projected_owner_key = current_owner_key
            projected_owner_active = None if current_owner is None else bool(current_owner.get("is_active"))
            action_summary = normalized_repair_kind

            try:
                if normalized_repair_kind == "reactivate_owner_target":
                    if current_owner_key is None:
                        raise ValueError("Mapping nie ma owner_key")
                    if current_owner is None:
                        raise FileNotFoundError(f"Owner directory item not found: {current_owner_key}")
                    projected_owner_active = True
                    action_summary = f"reactivate {current_owner_key}"
                elif normalized_repair_kind == "remap_to_target":
                    if normalized_target_owner_key is None:
                        raise ValueError("target_owner_key jest wymagany dla remap_to_target")
                    target_row = conn.execute("SELECT * FROM owner_directory_items WHERE owner_key = ?", (normalized_target_owner_key,)).fetchone()
                    if target_row is None:
                        raise FileNotFoundError(f"Owner directory item not found: {normalized_target_owner_key}")
                    target_item = _owner_directory_item_to_dict(target_row)
                    if not bool(target_item.get("is_active")):
                        raise ValueError("Nie mozna przepiac mapowania na nieaktywny target")
                    projected_owner_key = normalized_target_owner_key
                    projected_owner_active = True
                    action_summary = f"remap {current_owner_key} -> {normalized_target_owner_key}"
                elif normalized_repair_kind == "create_missing_owner_target":
                    create_owner_key = normalized_target_owner_key or current_owner_key
                    if create_owner_key is None:
                        raise ValueError("Nie mozna utworzyc targetu bez owner_key")
                    projected_owner_key = create_owner_key
                    projected_owner_active = True
                    action_summary = f"create target {create_owner_key}"
                else:
                    raise ValueError("repair_kind musi byc jednym z: reactivate_owner_target, remap_to_target, create_missing_owner_target")
            except Exception as exc:
                errors.append({"mapping_id": int(mapping_id), "error_type": type(exc).__name__, "message": str(exc)})
                continue

            items.append({
                "mapping_id": int(mapping_id),
                "repair_kind": normalized_repair_kind,
                "current_mapping": mapping,
                "current_owner_directory_item": current_owner,
                "projected_owner_key": projected_owner_key,
                "projected_owner_active": projected_owner_active,
                "projected_owner_type": normalized_owner_type if normalized_repair_kind == "create_missing_owner_target" else (None if current_owner is None else current_owner.get("owner_type")),
                "projected_display_name": normalized_display_name if normalized_repair_kind == "create_missing_owner_target" else (None if current_owner is None else current_owner.get("display_name")),
                "action_summary": action_summary,
                "notes": normalized_notes,
            })
    finally:
        conn.close()

    return {
        "status": "ok",
        "repair_kind": normalized_repair_kind,
        "requested_count": len(normalized_ids),
        "preview_count": len(items),
        "error_count": len(errors),
        "can_execute": len(items) > 0 and len(errors) == 0,
        "items": items,
        "errors": errors,
    }


@mcp.tool
def bulk_repair_owner_mappings(
    mapping_ids: list[int],
    repair_kind: str,
    target_owner_key: str | None = None,
    owner_type: str | None = None,
    display_name: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    if not mapping_ids:
        raise ValueError("mapping_ids nie moze byc puste")
    normalized_ids: list[int] = []
    for item in mapping_ids:
        value = int(item)
        if value < 1:
            raise ValueError("mapping_ids musza byc dodatnie")
        if value not in normalized_ids:
            normalized_ids.append(value)

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for mapping_id in normalized_ids:
        try:
            repair_result = repair_owner_mapping_issue(
                mapping_id=mapping_id,
                repair_kind=repair_kind,
                target_owner_key=target_owner_key,
                owner_type=owner_type,
                display_name=display_name,
                notes=notes,
            )
            results.append(repair_result)
        except Exception as exc:
            errors.append({
                "mapping_id": int(mapping_id),
                "error_type": type(exc).__name__,
                "message": str(exc),
            })

    if errors and results:
        status = "partial"
    elif errors:
        status = "failed"
    else:
        status = "completed"

    audit_project_key = _owner_catalog_audit_project_key((results[0].get("owner_role_mapping") or {}).get("project_key") if results else None)
    conn = get_db_connection()
    try:
        audit_title = f"Owner mapping bulk repair: {normalize_required_text(repair_kind, 'repair_kind')}"
        audit_description = f"status={status}; repaired={len(results)}; errors={len(errors)}"
        audit_event_id = timeline.record_project_event(
            conn,
            project_key=audit_project_key,
            event_type="project.note_recorded",
            title=audit_title,
            description=audit_description,
            origin="system",
            tags=["owner_mapping_bulk_repair", normalize_required_text(repair_kind, 'repair_kind'), status],
            status=status,
            canonical=True,
            category="owner_mapping_bulk_repair",
            now_fn=utc_now_iso,
        )
        audit_row = conn.execute("SELECT * FROM timeline_events WHERE id = ?", (audit_event_id,)).fetchone()
        audit_item = timeline.timeline_rows_to_dicts([audit_row], row_to_dict=row_to_dict)[0] if audit_row is not None else None
        conn.commit()
    finally:
        conn.close()

    return {
        "status": status,
        "repair_kind": normalize_required_text(repair_kind, "repair_kind"),
        "requested_count": len(normalized_ids),
        "repaired_count": len(results),
        "error_count": len(errors),
        "results": results,
        "errors": errors,
        "audit_event": audit_item,
    }


@mcp.tool
def get_owner_mapping_batch_candidates(
    limit: int = 20,
    max_groups: int = 10,
    project_key: str | None = None,
    scope_code: str | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    if max_groups < 1 or max_groups > 200:
        raise ValueError("max_groups musi być w zakresie 1..200")
    problems = get_problematic_owner_mappings(
        limit=max(int(limit), 200),
        project_key=project_key,
        scope_code=scope_code,
        kind=kind,
    )
    grouped: dict[str, dict[str, Any]] = {}
    for item in problems.get("items") or []:
        recommended = item.get("recommended_repair") or {}
        repair_kind = normalize_optional_text(recommended.get("kind"))
        mapping_id = int(item.get("mapping_id") or 0)
        if repair_kind is None or mapping_id < 1:
            continue
        if repair_kind == "reactivate_owner_target":
            target_owner_key = normalize_optional_text(item.get("owner_key"))
        else:
            target_owner_key = normalize_optional_text(recommended.get("owner_key"))
        group_key = f"{repair_kind}::{target_owner_key or '__none__'}"
        group = grouped.setdefault(group_key, {
            "group_key": group_key,
            "repair_kind": repair_kind,
            "target_owner_key": target_owner_key,
            "mapping_ids": [],
            "problem_count": 0,
            "priority_counts": {"P1": 0, "P2": 0},
            "problems": [],
            "can_preview": True,
        })
        group["mapping_ids"].append(mapping_id)
        group["problem_count"] += 1
        priority_value = str(item.get("priority") or "P2")
        if priority_value not in group["priority_counts"]:
            group["priority_counts"][priority_value] = 0
        group["priority_counts"][priority_value] += 1
        group["problems"].append({
            "mapping_id": mapping_id,
            "owner_role": item.get("owner_role"),
            "owner_key": item.get("owner_key"),
            "kind": item.get("kind"),
            "priority": item.get("priority"),
            "recommended_repair": recommended,
        })
        if repair_kind == "remap_to_existing_role_target":
            group["preview_params"] = {
                "repair_kind": "remap_to_target",
                "target_owner_key": target_owner_key,
            }
            group["execution_params"] = {
                "repair_kind": "remap_to_target",
                "target_owner_key": target_owner_key,
            }
        elif repair_kind == "remap_to_active_same_type_target":
            group["preview_params"] = {
                "repair_kind": "remap_to_target",
                "target_owner_key": target_owner_key,
            }
            group["execution_params"] = {
                "repair_kind": "remap_to_target",
                "target_owner_key": target_owner_key,
            }
        elif repair_kind == "reactivate_owner_target":
            group["preview_params"] = {"repair_kind": "reactivate_owner_target"}
            group["execution_params"] = {"repair_kind": "reactivate_owner_target"}
        elif repair_kind == "create_missing_owner_target":
            group["preview_params"] = {
                "repair_kind": "create_missing_owner_target",
                "target_owner_key": target_owner_key,
            }
            group["execution_params"] = {
                "repair_kind": "create_missing_owner_target",
                "target_owner_key": target_owner_key,
            }
        else:
            group["can_preview"] = False
            group["preview_params"] = None
            group["execution_params"] = None

    groups = sorted(
        grouped.values(),
        key=lambda item: (-int(item.get("priority_counts", {}).get("P1", 0)), -int(item.get("problem_count") or 0), str(item.get("repair_kind") or ""), str(item.get("target_owner_key") or "")),
    )
    for index, group in enumerate(groups, start=1):
        group["rank"] = int(index)
    return {
        "status": "ok" if not groups else "attention",
        "count": len(groups[: int(max_groups)]),
        "groups": groups[: int(max_groups)],
        "filters": {
            "limit": int(limit),
            "max_groups": int(max_groups),
            "project_key": normalize_optional_text(project_key),
            "scope_code": normalize_scope_code(scope_code),
            "kind": normalize_optional_text(kind),
        },
    }


@mcp.tool
def get_owner_catalog_repair_summary(
    project_key: str | None = None,
    scope_code: str | None = None,
    limit_recent_audits: int = 10,
    max_groups: int = 10,
) -> dict[str, Any]:
    if limit_recent_audits < 1 or limit_recent_audits > 1000:
        raise ValueError("limit_recent_audits musi być w zakresie 1..1000")
    if max_groups < 1 or max_groups > 200:
        raise ValueError("max_groups musi być w zakresie 1..200")
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    audit_project_key = _owner_catalog_audit_project_key(normalized_project_key)

    health = get_owner_catalog_health(project_key=normalized_project_key, scope_code=normalized_scope_code)
    batch_candidates = get_owner_mapping_batch_candidates(project_key=normalized_project_key, scope_code=normalized_scope_code, max_groups=max_groups)
    single_audit = get_owner_mapping_repair_audit(project_key=normalized_project_key, limit=limit_recent_audits)
    governance_history = get_owner_catalog_governance_history(project_key=normalized_project_key, limit=limit_recent_audits)

    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT * FROM timeline_events
            WHERE project_key = ?
              AND event_type = 'project.note_recorded'
              AND payload_json LIKE '%owner_mapping_bulk_repair%'
            ORDER BY event_time DESC, id DESC
            LIMIT ?
            """,
            (audit_project_key, int(limit_recent_audits)),
        ).fetchall()
        count_rows = conn.execute(
            """
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN payload_json LIKE '%"status": "completed"%' THEN 1 ELSE 0 END) AS completed_count,
                SUM(CASE WHEN payload_json LIKE '%"status": "partial"%' THEN 1 ELSE 0 END) AS partial_count,
                SUM(CASE WHEN payload_json LIKE '%"status": "failed"%' THEN 1 ELSE 0 END) AS failed_count
            FROM timeline_events
            WHERE project_key = ?
              AND event_type = 'project.note_recorded'
              AND payload_json LIKE '%owner_mapping_bulk_repair%'
            """,
            (audit_project_key,),
        ).fetchone()
    finally:
        conn.close()

    bulk_items = timeline.timeline_rows_to_dicts(rows, row_to_dict=row_to_dict)
    counts = row_to_dict(count_rows) if count_rows is not None else {}

    return {
        "status": "ok",
        "filters": {
            "project_key": normalized_project_key,
            "scope_code": normalized_scope_code,
            "limit_recent_audits": int(limit_recent_audits),
            "max_groups": int(max_groups),
        },
        "health": health,
        "batch_candidates_summary": {
            "count": int(batch_candidates.get("count") or 0),
            "groups": batch_candidates.get("groups") or [],
        },
        "repair_audit_summary": {
            "single_repair_count": int(single_audit.get("total_count") or 0),
            "bulk_repair_count": int((counts or {}).get("total_count") or 0),
            "bulk_repair_completed_count": int((counts or {}).get("completed_count") or 0),
            "bulk_repair_partial_count": int((counts or {}).get("partial_count") or 0),
            "bulk_repair_failed_count": int((counts or {}).get("failed_count") or 0),
            "recent_single_repairs": single_audit.get("items") or [],
            "recent_bulk_repairs": bulk_items,
        },
        "governance_history_summary": {
            "governance_event_count": int(governance_history.get("total_count") or 0),
            "recent_governance_events": governance_history.get("items") or [],
        },
    }


def _owner_catalog_audit_project_key(project_key: str | None) -> str:
    normalized_project_key = normalize_optional_text(project_key)
    return normalized_project_key or "global_owner_catalog"



@mcp.tool
def get_owner_catalog_governance_history(
    limit: int = 50,
    offset: int = 0,
    project_key: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    if offset < 0:
        raise ValueError("offset musi być >= 0")
    audit_project_key = _owner_catalog_audit_project_key(project_key)
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT * FROM timeline_events
            WHERE project_key = ?
              AND event_type = 'project.note_recorded'
              AND (
                    payload_json LIKE '%owner_directory_change%'
                 OR payload_json LIKE '%owner_role_mapping_change%'
                 OR payload_json LIKE '%owner_catalog_governance%'
              )
            ORDER BY event_time DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (audit_project_key, int(limit), int(offset)),
        ).fetchall()
        total_row = conn.execute(
            """
            SELECT COUNT(*) AS total_count FROM timeline_events
            WHERE project_key = ?
              AND event_type = 'project.note_recorded'
              AND (
                    payload_json LIKE '%owner_directory_change%'
                 OR payload_json LIKE '%owner_role_mapping_change%'
                 OR payload_json LIKE '%owner_catalog_governance%'
              )
            """,
            (audit_project_key,),
        ).fetchone()
    finally:
        conn.close()
    items = timeline.timeline_rows_to_dicts(rows, row_to_dict=row_to_dict)
    total_count = int((row_to_dict(total_row) or {}).get("total_count") or 0) if total_row is not None else 0
    return {
        "status": "ok",
        "count": len(items),
        "total_count": total_count,
        "items": items,
        "filters": {"limit": int(limit), "offset": int(offset), "project_key": audit_project_key},
    }

@mcp.tool
def get_owner_mapping_repair_audit(
    limit: int = 50,
    offset: int = 0,
    project_key: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    if offset < 0:
        raise ValueError("offset musi być >= 0")
    normalized_project_key = _owner_catalog_audit_project_key(project_key)
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT * FROM timeline_events
            WHERE project_key = ?
              AND event_type = 'project.note_recorded'
              AND payload_json LIKE '%owner_mapping_repair%'
            ORDER BY event_time DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (normalized_project_key, int(limit), int(offset)),
        ).fetchall()
        total_row = conn.execute(
            """
            SELECT COUNT(*) AS total_count FROM timeline_events
            WHERE project_key = ?
              AND event_type = 'project.note_recorded'
              AND payload_json LIKE '%owner_mapping_repair%'
            """,
            (normalized_project_key,),
        ).fetchone()
    finally:
        conn.close()
    items = timeline.timeline_rows_to_dicts(rows, row_to_dict=row_to_dict)
    total_count = int((row_to_dict(total_row) or {}).get("total_count") or 0) if total_row is not None else 0
    return {
        "status": "ok",
        "count": len(items),
        "total_count": total_count,
        "items": items,
        "filters": {
            "limit": int(limit),
            "offset": int(offset),
            "project_key": normalized_project_key,
        },
    }


@mcp.tool
def get_owner_governance_history(
    owner_key: str | None = None,
    owner_role: str | None = None,
    project_key: str | None = None,
    category: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Full audit trail for owner catalog: directory changes, mapping changes, repairs, target status changes.

    Parameters
    ----------
    owner_key:
        Filter events by owner_key (substring match in description).
    owner_role:
        Filter events by owner_role (substring match in description).
    project_key:
        Filter by timeline project_key.  None returns all projects.
    category:
        Narrow to a specific event category, e.g. "owner_directory_change",
        "owner_role_mapping_change", "owner_mapping_repair",
        "owner_mapping_bulk_repair", "owner_target_status_change".
        None returns all owner catalog categories.
    limit:
        Max items to return (default 50).
    offset:
        Pagination offset (default 0).
    """
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    if offset < 0:
        raise ValueError("offset musi być >= 0")
    normalized_owner_key = normalize_optional_text(owner_key)
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_category = normalize_optional_text(category)

    owner_key_pattern = f"%owner_key={normalized_owner_key}%" if normalized_owner_key else None
    owner_role_pattern = f"%owner_role={normalized_owner_role}%" if normalized_owner_role else None
    category_pattern = f'%"{normalized_category}"%' if normalized_category else None

    base_sql = """
        SELECT * FROM timeline_events
        WHERE event_type = 'project.note_recorded'
          AND (
            payload_json LIKE '%owner_directory_change%'
            OR payload_json LIKE '%owner_role_mapping_change%'
            OR payload_json LIKE '%owner_mapping_repair%'
            OR payload_json LIKE '%owner_mapping_bulk_repair%'
            OR payload_json LIKE '%owner_target_status_change%'
            OR payload_json LIKE '%sla_policy_change%'
            OR payload_json LIKE '%"escalation"%'
          )
          AND (? IS NULL OR project_key = ?)
          AND (? IS NULL OR payload_json LIKE ?)
          AND (? IS NULL OR payload_json LIKE ?)
          AND (? IS NULL OR payload_json LIKE ?)
    """
    params_query: list[Any] = [
        normalized_project_key, normalized_project_key,
        owner_key_pattern, owner_key_pattern,
        owner_role_pattern, owner_role_pattern,
        category_pattern, category_pattern,
    ]

    conn = get_db_connection()
    try:
        rows = conn.execute(
            base_sql + " ORDER BY event_time DESC, id DESC LIMIT ? OFFSET ?",
            params_query + [int(limit), int(offset)],
        ).fetchall()
        total_row = conn.execute(
            "SELECT COUNT(*) AS total_count FROM (" + base_sql + ")",
            params_query,
        ).fetchone()
    finally:
        conn.close()

    items = timeline.timeline_rows_to_dicts(rows, row_to_dict=row_to_dict)
    total_count = int((row_to_dict(total_row) or {}).get("total_count") or 0) if total_row is not None else 0
    return {
        "status": "ok",
        "count": len(items),
        "total_count": total_count,
        "items": items,
        "filters": {
            "owner_key": normalized_owner_key,
            "owner_role": normalized_owner_role,
            "project_key": normalized_project_key,
            "category": normalized_category,
            "limit": int(limit),
            "offset": int(offset),
        },
    }


# ---------------------------------------------------------------------------
# Epic 3 (gap) — Task 3.2: dedicated owner target activation / deactivation
# ---------------------------------------------------------------------------

_OWNER_KEY_PATTERN = __import__("re").compile(r"^[a-z0-9][a-z0-9_:./-]{0,98}[a-z0-9]$|^[a-z0-9]$")
_ALLOWED_OWNER_TYPES = frozenset({"team", "person", "service_account", "automated", "external"})


@mcp.tool
def set_owner_target_active(
    owner_key: str,
    is_active: bool,
    reason: str | None = None,
) -> dict[str, Any]:
    """
    Aktywuje lub deaktywuje target właściciela w katalogu.
    Deaktywacja nie usuwa targetu — zachowuje historię mapowań.
    Zapisuje zdarzenie audytowe w timeline.
    """
    normalized_owner_key = normalize_required_text(owner_key, "owner_key")
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT * FROM owner_directory_items WHERE owner_key = ?",
            (normalized_owner_key,),
        ).fetchone()
        if row is None:
            raise FileNotFoundError(f"Owner target '{normalized_owner_key}' nie istnieje w katalogu")
        item = _owner_directory_item_to_dict(row)
        old_active = bool(item.get("is_active"))
        new_active = bool(is_active)
        if old_active == new_active:
            return {
                "status": "noop",
                "message": f"Owner target '{normalized_owner_key}' jest już {'aktywny' if new_active else 'nieaktywny'}",
                "owner_directory_item": item,
            }
        conn.execute(
            "UPDATE owner_directory_items SET is_active = ?, updated_at = ? WHERE owner_key = ?",
            (1 if new_active else 0, utc_now_iso(), normalized_owner_key),
        )
        action = "activated" if new_active else "deactivated"
        audit_description = f"owner_key={normalized_owner_key}; action={action}"
        if reason:
            audit_description += f"; reason={reason.strip()}"
        timeline.record_project_event(
            conn,
            project_key=_owner_catalog_audit_project_key(None),
            event_type="project.note_recorded",
            title=f"Owner target {action}: {normalized_owner_key}",
            description=audit_description,
            origin="system",
            tags=["owner_target_status_change", action],
            status="completed",
            canonical=True,
            category="owner_target_status_change",
            now_fn=utc_now_iso,
        )
        conn.commit()
        updated_row = conn.execute(
            "SELECT * FROM owner_directory_items WHERE owner_key = ?",
            (normalized_owner_key,),
        ).fetchone()
        return {
            "status": action,
            "owner_key": normalized_owner_key,
            "was_active": old_active,
            "is_active": new_active,
            "owner_directory_item": _owner_directory_item_to_dict(updated_row),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Epic 4 — Task 4.1-4.4: governance, rollout, validation, checklists
# ---------------------------------------------------------------------------

def _validate_owner_key_format(owner_key: str) -> list[str]:
    """Returns list of violation messages (empty = valid)."""
    violations: list[str] = []
    if not owner_key:
        violations.append("owner_key nie może być puste")
        return violations
    if len(owner_key) > 100:
        violations.append(f"owner_key jest za długi ({len(owner_key)} znaków, max 100)")
    if not _OWNER_KEY_PATTERN.match(owner_key):
        violations.append(
            "owner_key musi składać się z małych liter, cyfr, podkreśleń, dwukropków, kropek lub myślników "
            "i zaczynać/kończyć się znakiem alfanumerycznym"
        )
    if owner_key != owner_key.lower():
        violations.append("owner_key musi być w całości małymi literami")
    return violations


@mcp.tool
def validate_new_owner_target(
    owner_key: str,
    owner_type: str,
    display_name: str,
    routing_metadata_json: str | None = None,
) -> dict[str, Any]:
    """
    Waliduje dane nowego targetu właściciela przed dodaniem do katalogu.
    Sprawdza: format owner_key, dozwolony owner_type, wypełnienie pól wymaganych,
    brak duplikatów w katalogu.
    Nie modyfikuje bazy — wyłącznie operacja odczytu.
    """
    violations: list[dict[str, Any]] = []

    # owner_key format
    normalized_owner_key = (owner_key or "").strip().lower()
    key_violations = _validate_owner_key_format(normalized_owner_key)
    for msg in key_violations:
        violations.append({"field": "owner_key", "severity": "error", "message": msg})

    # owner_type
    normalized_owner_type = (owner_type or "").strip().lower()
    if not normalized_owner_type:
        violations.append({"field": "owner_type", "severity": "error", "message": "owner_type jest wymagany"})
    elif normalized_owner_type not in _ALLOWED_OWNER_TYPES:
        violations.append({
            "field": "owner_type",
            "severity": "error",
            "message": f"owner_type '{normalized_owner_type}' jest niedozwolony. Dostępne: {', '.join(sorted(_ALLOWED_OWNER_TYPES))}",
        })

    # display_name
    normalized_display_name = (display_name or "").strip()
    if not normalized_display_name:
        violations.append({"field": "display_name", "severity": "error", "message": "display_name jest wymagany"})
    elif len(normalized_display_name) < 3:
        violations.append({"field": "display_name", "severity": "warning", "message": "display_name jest bardzo krótki (< 3 znaki)"})

    # routing_metadata_json parsability
    if routing_metadata_json:
        try:
            __import__("json").loads(routing_metadata_json)
        except (ValueError, TypeError):
            violations.append({"field": "routing_metadata_json", "severity": "error", "message": "routing_metadata_json nie jest poprawnym JSON"})

    # Duplicate check
    conn = get_db_connection()
    try:
        existing = conn.execute(
            "SELECT owner_key, is_active FROM owner_directory_items WHERE owner_key = ?",
            (normalized_owner_key,),
        ).fetchone()
        if existing is not None:
            existing_active = bool(existing["is_active"])
            violations.append({
                "field": "owner_key",
                "severity": "error",
                "message": f"owner_key '{normalized_owner_key}' już istnieje w katalogu (is_active={existing_active})",
            })
    finally:
        conn.close()

    errors = [v for v in violations if v["severity"] == "error"]
    warnings = [v for v in violations if v["severity"] == "warning"]
    return {
        "valid": len(errors) == 0,
        "owner_key": normalized_owner_key,
        "owner_type": normalized_owner_type,
        "display_name": normalized_display_name,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "violations": violations,
        "recommendation": "ok_to_create" if len(errors) == 0 else "fix_errors_before_create",
    }


@mcp.tool
def validate_project_override(
    project_key: str,
    owner_role: str,
    target_owner_key: str,
) -> dict[str, Any]:
    """
    Waliduje zamierzony project-level override mapowania właściciela.
    Sprawdza: czy target istnieje i jest aktywny, czy istnieje globalne mapowanie dla tej roli,
    czy override nie jest redundantny (ten sam target co globalny).
    Nie modyfikuje bazy — wyłącznie operacja odczytu.
    """
    normalized_project_key = normalize_required_text(project_key, "project_key")
    normalized_owner_role = normalize_required_text(owner_role, "owner_role")
    normalized_target_key = normalize_required_text(target_owner_key, "target_owner_key")

    issues: list[dict[str, Any]] = []
    conn = get_db_connection()
    try:
        # Check target exists and is active
        target_row = conn.execute(
            "SELECT * FROM owner_directory_items WHERE owner_key = ?",
            (normalized_target_key,),
        ).fetchone()
        if target_row is None:
            issues.append({"kind": "error", "code": "target_missing", "message": f"Target '{normalized_target_key}' nie istnieje w katalogu"})
        elif not bool(target_row["is_active"]):
            issues.append({"kind": "error", "code": "target_inactive", "message": f"Target '{normalized_target_key}' jest nieaktywny"})

        # Check global mapping exists for this role
        global_row = conn.execute(
            "SELECT * FROM owner_role_mappings WHERE owner_role = ? AND project_key IS NULL AND is_active = 1",
            (normalized_owner_role,),
        ).fetchone()
        if global_row is None:
            issues.append({
                "kind": "warning",
                "code": "no_global_mapping",
                "message": f"Brak globalnego mapowania dla roli '{normalized_owner_role}' — override nie ma wartości fallbackowej",
            })
        else:
            global_owner_key = global_row["owner_key"]
            if global_owner_key == normalized_target_key:
                issues.append({
                    "kind": "warning",
                    "code": "redundant_override",
                    "message": f"Override wskazuje ten sam target co globalne mapowanie ('{global_owner_key}') — nie jest konieczny",
                })

        # Check if override already exists
        existing_override = conn.execute(
            "SELECT * FROM owner_role_mappings WHERE owner_role = ? AND project_key = ?",
            (normalized_owner_role, normalized_project_key),
        ).fetchone()
        existing_info = None
        if existing_override is not None:
            existing_info = _owner_role_mapping_to_dict(existing_override)
            if existing_info.get("owner_key") == normalized_target_key:
                issues.append({
                    "kind": "info",
                    "code": "override_identical",
                    "message": "Identyczny override już istnieje — upsert będzie noop",
                })
    finally:
        conn.close()

    errors = [i for i in issues if i["kind"] == "error"]
    return {
        "valid": len(errors) == 0,
        "project_key": normalized_project_key,
        "owner_role": normalized_owner_role,
        "target_owner_key": normalized_target_key,
        "error_count": len(errors),
        "issue_count": len(issues),
        "issues": issues,
        "existing_override": existing_info,
        "recommendation": "ok_to_create" if len(errors) == 0 else "fix_errors_before_override",
    }


@mcp.tool
def rollout_owner_catalog_to_project(
    project_key: str,
    mappings: list[dict[str, Any]],
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Hurtowo wdraża mapowania właścicieli dla nowego projektu.
    Każdy element 'mappings' to słownik z polami: owner_role (wymagane), owner_key (wymagane), notes (opcjonalne).
    dry_run=True — waliduje bez zapisu.
    Zapisuje zdarzenie audytowe w timeline.
    """
    normalized_project_key = normalize_required_text(project_key, "project_key")
    if not mappings:
        raise ValueError("mappings nie może być puste")

    rolled_out: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    conn = get_db_connection()
    try:
        for idx, mapping_spec in enumerate(mappings):
            owner_role = normalize_optional_text(str(mapping_spec.get("owner_role") or ""))
            owner_key = normalize_optional_text(str(mapping_spec.get("owner_key") or ""))
            notes = normalize_optional_text(str(mapping_spec.get("notes") or ""))

            if not owner_role or not owner_key:
                errors.append({"index": idx, "spec": mapping_spec, "error": "owner_role i owner_key są wymagane"})
                continue

            # Validate target
            target_row = conn.execute(
                "SELECT * FROM owner_directory_items WHERE owner_key = ?",
                (owner_key,),
            ).fetchone()
            if target_row is None:
                errors.append({"index": idx, "owner_role": owner_role, "owner_key": owner_key, "error": f"Target '{owner_key}' nie istnieje w katalogu"})
                continue
            if not bool(target_row["is_active"]):
                errors.append({"index": idx, "owner_role": owner_role, "owner_key": owner_key, "error": f"Target '{owner_key}' jest nieaktywny"})
                continue

            # Check existing
            existing = conn.execute(
                "SELECT * FROM owner_role_mappings WHERE owner_role = ? AND project_key = ?",
                (owner_role, normalized_project_key),
            ).fetchone()
            if existing is not None:
                existing_dict = _owner_role_mapping_to_dict(existing)
                if existing_dict.get("owner_key") == owner_key:
                    skipped.append({"owner_role": owner_role, "owner_key": owner_key, "reason": "identical_mapping_exists"})
                    continue

            if dry_run:
                rolled_out.append({"owner_role": owner_role, "owner_key": owner_key, "dry_run": True})
                continue

            # Upsert
            conn.execute(
                """
                INSERT INTO owner_role_mappings (owner_role, owner_key, project_key, is_active, notes, created_at, updated_at)
                VALUES (?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(owner_role, project_key, scope_code) DO UPDATE SET
                    owner_key = excluded.owner_key,
                    is_active = 1,
                    notes = COALESCE(excluded.notes, notes),
                    updated_at = excluded.updated_at
                """,
                (owner_role, owner_key, normalized_project_key, notes, utc_now_iso(), utc_now_iso()),
            )
            rolled_out.append({"owner_role": owner_role, "owner_key": owner_key})

        if not dry_run and rolled_out:
            timeline.record_project_event(
                conn,
                project_key=_owner_catalog_audit_project_key(normalized_project_key),
                event_type="project.note_recorded",
                title=f"Owner catalog rollout: {normalized_project_key}",
                description=f"Rolled out {len(rolled_out)} mappings to project '{normalized_project_key}'",
                origin="system",
                tags=["owner_catalog_rollout", normalized_project_key],
                status="completed",
                canonical=True,
                category="owner_catalog_rollout",
                now_fn=utc_now_iso,
            )
            conn.commit()
    finally:
        conn.close()

    status = "dry_run" if dry_run else ("completed" if not errors else "partial" if rolled_out else "failed")
    return {
        "status": status,
        "project_key": normalized_project_key,
        "dry_run": dry_run,
        "requested_count": len(mappings),
        "rolled_out_count": len(rolled_out),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "rolled_out": rolled_out,
        "skipped": skipped,
        "errors": errors,
    }


# Governance checklist definitions
_GOVERNANCE_CHECKLISTS: dict[str, list[dict[str, Any]]] = {
    "new_owner_target": [
        {"id": "nt_01", "description": "owner_key jest w poprawnym formacie (lowercase, bez spacji)", "required": True, "tool_hint": "validate_new_owner_target(owner_key, owner_type, display_name)"},
        {"id": "nt_02", "description": "owner_type jest jednym z dozwolonych wartości: team, person, service_account, automated, external", "required": True, "tool_hint": "validate_new_owner_target(...)"},
        {"id": "nt_03", "description": "display_name jest wypełniony i opisowy (min. 3 znaki)", "required": True, "tool_hint": "validate_new_owner_target(...)"},
        {"id": "nt_04", "description": "Nie istnieje duplikat owner_key w katalogu", "required": True, "tool_hint": "validate_new_owner_target(...) lub get_owner_catalog_health(...)"},
        {"id": "nt_05", "description": "routing_metadata_json jest poprawnym JSON (jeśli podany)", "required": False, "tool_hint": "validate_new_owner_target(...)"},
        {"id": "nt_06", "description": "Nowy target ma przypisane przynajmniej jedno mapowanie roli po stworzeniu", "required": False, "tool_hint": "upsert_owner_role_mapping(...)"},
    ],
    "deactivate_target": [
        {"id": "dt_01", "description": "Sprawdź, które aktywne mapowania wskazują ten target", "required": True, "tool_hint": "get_problematic_owner_mappings(kind='inactive_owner_target') po deaktywacji"},
        {"id": "dt_02", "description": "Upewnij się, że istnieje fallback lub alternatywne mapowanie dla tej roli", "required": True, "tool_hint": "get_owner_catalog_health(...)"},
        {"id": "dt_03", "description": "Przepnij aktywne mapowania na inny target lub zdeaktywuj je zanim wygasisz target", "required": True, "tool_hint": "repair_owner_mapping_issue(repair_kind='remap_to_target', ...)"},
        {"id": "dt_04", "description": "Zapisz powód deaktywacji w polu reason", "required": False, "tool_hint": "set_owner_target_active(owner_key, is_active=False, reason=...)"},
        {"id": "dt_05", "description": "Uruchom get_owner_catalog_health() po deaktywacji i sprawdź, że nie ma nowych broken_owner_mapping", "required": True, "tool_hint": "get_owner_catalog_health(...)"},
    ],
    "migrate_mappings": [
        {"id": "mm_01", "description": "Zrób preview bulk repair przed wykonaniem zmian", "required": True, "tool_hint": "preview_bulk_repair_owner_mappings(mapping_ids, repair_kind, ...)"},
        {"id": "mm_02", "description": "Sprawdź health katalogu przed migracją", "required": True, "tool_hint": "get_owner_catalog_health(...)"},
        {"id": "mm_03", "description": "Zapisz listę mapping_ids przed zmianą (do ewentualnego rollbacku)", "required": True, "tool_hint": "get_problematic_owner_mappings(...)"},
        {"id": "mm_04", "description": "Wykonaj bulk repair i zweryfikuj audit_event w odpowiedzi", "required": True, "tool_hint": "bulk_repair_owner_mappings(...)"},
        {"id": "mm_05", "description": "Sprawdź health katalogu po migracji — problem_count powinien być 0", "required": True, "tool_hint": "get_owner_catalog_health(...)"},
        {"id": "mm_06", "description": "Sprawdź get_owner_catalog_repair_summary() dla potwierdzenia audytu", "required": False, "tool_hint": "get_owner_catalog_repair_summary(...)"},
    ],
    "rollout_project": [
        {"id": "rp_01", "description": "Zdefiniuj listę ról i targetów dla projektu", "required": True, "tool_hint": "Lista: [{owner_role, owner_key}, ...]"},
        {"id": "rp_02", "description": "Waliduj każdy target przed rolloutem", "required": True, "tool_hint": "validate_new_owner_target(...) lub sprawdź get_owner_catalog_health()"},
        {"id": "rp_03", "description": "Uruchom rollout_owner_catalog_to_project z dry_run=True i sprawdź błędy", "required": True, "tool_hint": "rollout_owner_catalog_to_project(project_key, mappings, dry_run=True)"},
        {"id": "rp_04", "description": "Wykonaj właściwy rollout", "required": True, "tool_hint": "rollout_owner_catalog_to_project(project_key, mappings, dry_run=False)"},
        {"id": "rp_05", "description": "Waliduj overrides projektowe", "required": True, "tool_hint": "validate_project_override(project_key, owner_role, owner_key) dla każdego mapowania"},
        {"id": "rp_06", "description": "Uruchom get_owner_catalog_health(project_key=...) po rolloutcie", "required": True, "tool_hint": "get_owner_catalog_health(project_key=...)"},
        {"id": "rp_07", "description": "Sprawdź alerty po rolloutcie", "required": False, "tool_hint": "get_quality_alerts(project_key=...)"},
    ],
}


@mcp.tool
def get_owner_catalog_governance_checklist(
    operation: str,
    project_key: str | None = None,
) -> dict[str, Any]:
    """
    Zwraca listę kontrolną (checklist) dla operacji na katalogu właścicieli.
    operation: 'new_owner_target' | 'deactivate_target' | 'migrate_mappings' | 'rollout_project'
    Każdy element zawiera: id, description, required, tool_hint.
    """
    normalized_op = (operation or "").strip().lower()
    allowed_operations = list(_GOVERNANCE_CHECKLISTS.keys())
    if normalized_op not in _GOVERNANCE_CHECKLISTS:
        raise ValueError(
            f"Nieznana operacja: '{normalized_op}'. Dostępne: {', '.join(allowed_operations)}"
        )

    checklist = _GOVERNANCE_CHECKLISTS[normalized_op]
    required_count = sum(1 for item in checklist if item["required"])
    return {
        "status": "ok",
        "operation": normalized_op,
        "project_key": normalize_optional_text(project_key),
        "item_count": len(checklist),
        "required_count": required_count,
        "optional_count": len(checklist) - required_count,
        "checklist": checklist,
    }


@mcp.tool
def get_owner_rollout_summary(
    scope_code: str | None = None,
    include_health_check: bool = True,
) -> dict[str, Any]:
    """Summary of owner catalog rollout state across all projects.

    Shows which projects have their own override mappings vs. rely on the
    global fallback, and optionally runs a health check per project.

    Parameters
    ----------
    scope_code:
        Optional filter — only include mappings that match this scope_code
        (or have no scope_code set).
    include_health_check:
        When True (default), runs get_owner_catalog_health per project and
        surfaces projects with attention-level problems.
    """
    normalized_scope_code = normalize_scope_code(scope_code)

    conn = get_db_connection()
    try:
        all_active_rows = conn.execute(
            "SELECT * FROM owner_role_mappings WHERE is_active = 1 ORDER BY owner_role ASC"
        ).fetchall()
        all_active = [_owner_role_mapping_to_dict(row) for row in all_active_rows]

        if normalized_scope_code is not None:
            all_active = [
                m for m in all_active
                if m.get("scope_code") == normalized_scope_code or m.get("scope_code") is None
            ]

        fallback_mappings = [m for m in all_active if m.get("project_key") is None]
        override_mappings = [m for m in all_active if m.get("project_key") is not None]

        # group overrides by project_key
        project_overrides: dict[str, list[dict[str, Any]]] = {}
        for m in override_mappings:
            pk = str(m["project_key"])
            project_overrides.setdefault(pk, []).append(m)

        global_roles = {m["owner_role"] for m in fallback_mappings}

        projects: list[dict[str, Any]] = []
        projects_with_attention: list[dict[str, Any]] = []

        for proj_key in sorted(project_overrides.keys()):
            proj_mappings = project_overrides[proj_key]
            roles_overridden = sorted({m["owner_role"] for m in proj_mappings})
            roles_on_fallback = sorted(global_roles - set(roles_overridden))

            proj_entry: dict[str, Any] = {
                "project_key": proj_key,
                "override_count": len(proj_mappings),
                "roles_overridden": roles_overridden,
                "roles_on_fallback": roles_on_fallback,
            }

            if include_health_check:
                health_data = _get_owner_catalog_health_data(
                    conn,
                    project_key=proj_key,
                    scope_code=normalized_scope_code,
                )
                problem_count = int(health_data.get("problem_count") or 0)
                governance_warning_count = int(health_data.get("governance_warning_count") or 0)
                health_status = "attention" if problem_count > 0 else "ok"
                proj_entry["health_status"] = health_status
                proj_entry["problem_count"] = problem_count
                proj_entry["governance_warning_count"] = governance_warning_count
                if health_status == "attention":
                    projects_with_attention.append(proj_entry)
            else:
                proj_entry["health_status"] = "not_checked"
                proj_entry["problem_count"] = 0
                proj_entry["governance_warning_count"] = 0

            projects.append(proj_entry)
    finally:
        conn.close()

    overall_status = "attention" if projects_with_attention else "ok"
    return {
        "status": overall_status,
        "summary": {
            "projects_with_override_count": len(project_overrides),
            "global_fallback_role_count": len(global_roles),
            "projects_with_attention_count": len(projects_with_attention),
            "total_override_mapping_count": len(override_mappings),
        },
        "global_fallback_mappings": [
            {
                "owner_role": m["owner_role"],
                "owner_key": m["owner_key"],
                "is_active": bool(m.get("is_active")),
            }
            for m in fallback_mappings
        ],
        "projects": projects,
        "projects_with_attention": projects_with_attention,
    }


def _compute_days_overdue(due_at_iso: str, as_of_iso: str) -> int:
    from datetime import datetime, timezone

    def _parse(s: str) -> datetime:
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    try:
        due = _parse(due_at_iso)
        as_of = _parse(as_of_iso)
        return max(0, (as_of - due).days)
    except (ValueError, AttributeError):
        return 0


@mcp.tool
def run_escalation_check(
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    level2_threshold_days: int = 3,
    level3_threshold_days: int = 7,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Sprawdza wszystkie kolejki overdue i tworzy/aktualizuje wpisy w escalation_history.

    Poziomy eskalacji:
    - Level 1: item jest overdue (days_overdue < level2_threshold_days)
    - Level 2: poważnie overdue (days_overdue >= level2_threshold_days)
    - Level 3: krytycznie overdue (days_overdue >= level3_threshold_days) LUB brak ownera
    """
    normalized_as_of = normalize_optional_text(as_of) or utc_now_iso()
    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    if level2_threshold_days < 1:
        raise ValueError("level2_threshold_days musi być >= 1")
    if level3_threshold_days <= level2_threshold_days:
        raise ValueError("level3_threshold_days musi być > level2_threshold_days")

    overdue_review = list_overdue_review_queue(limit=1000, as_of=normalized_as_of, scope_code=normalized_scope, project_key=normalized_project_key)
    overdue_revalidation = list_overdue_revalidation_queue(limit=1000, as_of=normalized_as_of, scope_code=normalized_scope, project_key=normalized_project_key)
    overdue_expired = list_overdue_expired_queue(limit=1000, as_of=normalized_as_of, scope_code=normalized_scope, project_key=normalized_project_key)
    overdue_duplicate = list_overdue_duplicate_queue(limit=1000, as_of=normalized_as_of, scope_code=normalized_scope, project_key=normalized_project_key)

    queue_configs = [
        (overdue_review["items"], "memory", "review_due_at", "review_overdue"),
        (overdue_revalidation["items"], "memory", "revalidation_due_at", "revalidation_overdue"),
        (overdue_expired["items"], "memory", "expired_due_at", "expired_overdue"),
        (overdue_duplicate["items"], "duplicate_review_item", "duplicate_due_at", "duplicate_overdue"),
    ]

    escalations: list[dict[str, Any]] = []
    level1_count = level2_count = level3_count = 0

    conn = get_db_connection()
    try:
        now_iso = utc_now_iso()
        for items, entity_type, due_field, base_reason in queue_configs:
            for item in items:
                entity_id = int(item.get("id", 0))
                due_at = normalize_optional_text(item.get(due_field))
                if due_at is None:
                    continue
                days_overdue = _compute_days_overdue(due_at, normalized_as_of)
                owner_role = normalize_optional_text(item.get("owner_role"))
                priority = normalize_optional_text(item.get("priority")) or "normal"
                item_project_key = normalize_optional_text(item.get("project_key"))
                item_scope_code = normalize_optional_text(item.get("scope_code"))

                # Ustal poziom i reason
                reason = base_reason
                if owner_role is None:
                    level = max(2, 3 if days_overdue >= level3_threshold_days else 2)
                    reason = "owner_missing"
                elif days_overdue >= level3_threshold_days:
                    level = 3
                elif days_overdue >= level2_threshold_days:
                    level = 2
                else:
                    level = 1

                entry = {
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "escalation_level": level,
                    "owner_role": owner_role,
                    "project_key": item_project_key,
                    "scope_code": item_scope_code,
                    "reason": reason,
                    "days_overdue": days_overdue,
                    "priority": priority,
                    "escalated_at": now_iso,
                }
                escalations.append(entry)

                if level == 1:
                    level1_count += 1
                elif level == 2:
                    level2_count += 1
                else:
                    level3_count += 1

                if not dry_run:
                    conn.execute(
                        """
                        INSERT INTO escalation_history
                            (escalation_level, entity_type, entity_id, owner_role, project_key,
                             scope_code, reason, days_overdue, priority, escalated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(entity_type, entity_id, escalation_level, reason)
                        DO UPDATE SET
                            days_overdue = excluded.days_overdue,
                            priority = excluded.priority,
                            escalated_at = excluded.escalated_at,
                            owner_role = excluded.owner_role
                        """,
                        (
                            level, entity_type, entity_id, owner_role, item_project_key,
                            item_scope_code, reason, days_overdue, priority, now_iso,
                        ),
                    )
                    timeline.record_project_event(
                        conn,
                        project_key=_owner_catalog_audit_project_key(item_project_key),
                        event_type="project.note_recorded",
                        title=f"Escalation level {level}: {entity_type} {entity_id}",
                        description=(
                            f"entity_type={entity_type}; entity_id={entity_id}; "
                            f"escalation_level={level}; reason={reason}; "
                            f"days_overdue={days_overdue}; priority={priority}"
                        ),
                        origin="system",
                        tags=["escalation", f"escalation.level_{level}"],
                        status="completed",
                        canonical=True,
                        category="escalation",
                        now_fn=utc_now_iso,
                    )

        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    return {
        "status": "ok",
        "summary": {
            "level1_count": level1_count,
            "level2_count": level2_count,
            "level3_count": level3_count,
            "total": len(escalations),
            "dry_run": dry_run,
        },
        "escalations": escalations,
        "filters": {
            "as_of": normalized_as_of,
            "scope_code": normalized_scope,
            "project_key": normalized_project_key,
            "level2_threshold_days": level2_threshold_days,
            "level3_threshold_days": level3_threshold_days,
        },
    }


@mcp.tool
def get_escalation_history(
    entity_type: str | None = None,
    entity_id: int | None = None,
    escalation_level: int | None = None,
    project_key: str | None = None,
    include_resolved: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Zwraca historię eskalacji z opcjonalnym filtrowaniem."""
    normalized_entity_type = normalize_optional_text(entity_type)
    normalized_entity_id = int(entity_id) if entity_id is not None else None
    normalized_escalation_level = int(escalation_level) if escalation_level is not None else None
    normalized_project_key = normalize_optional_text(project_key)
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")

    sql = "SELECT * FROM escalation_history WHERE 1=1"
    params: list[Any] = []
    if not include_resolved:
        sql += " AND resolved_at IS NULL"
    if normalized_entity_type is not None:
        sql += " AND entity_type = ?"
        params.append(normalized_entity_type)
    if normalized_entity_id is not None:
        sql += " AND entity_id = ?"
        params.append(normalized_entity_id)
    if normalized_escalation_level is not None:
        sql += " AND escalation_level = ?"
        params.append(normalized_escalation_level)
    if normalized_project_key is not None:
        sql += " AND project_key = ?"
        params.append(normalized_project_key)
    sql += " ORDER BY escalated_at DESC, id DESC"

    count_sql = sql.replace("SELECT *", "SELECT COUNT(*)", 1)
    paged_sql = sql + " LIMIT ? OFFSET ?"

    conn = get_db_connection()
    try:
        total_count = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(paged_sql, [*params, int(limit), int(offset)]).fetchall()
        items = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "status": "ok",
        "count": len(items),
        "total_count": total_count,
        "items": items,
        "filters": {
            "entity_type": normalized_entity_type,
            "entity_id": normalized_entity_id,
            "escalation_level": normalized_escalation_level,
            "project_key": normalized_project_key,
            "include_resolved": include_resolved,
            "limit": int(limit),
            "offset": int(offset),
        },
    }


@mcp.tool
def get_escalation_dashboard(
    project_key: str | None = None,
    scope_code: str | None = None,
) -> dict[str, Any]:
    """Dashboard eskalacji: podsumowanie pending escalations, najczęstsze przyczyny, avg czas reakcji."""
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)

    sql_base = "FROM escalation_history WHERE 1=1"
    params_base: list[Any] = []
    if normalized_project_key is not None:
        sql_base += " AND project_key = ?"
        params_base.append(normalized_project_key)
    if normalized_scope_code is not None:
        sql_base += " AND scope_code = ?"
        params_base.append(normalized_scope_code)

    conn = get_db_connection()
    try:
        # pending by level
        pending_by_level: dict[int, int] = {}
        for lvl in (1, 2, 3):
            cnt = conn.execute(
                f"SELECT COUNT(*) {sql_base} AND resolved_at IS NULL AND escalation_level = ?",
                [*params_base, lvl],
            ).fetchone()[0]
            pending_by_level[lvl] = int(cnt)

        # top reasons
        reason_rows = conn.execute(
            f"SELECT reason, COUNT(*) as cnt {sql_base} AND resolved_at IS NULL GROUP BY reason ORDER BY cnt DESC LIMIT 5",
            params_base,
        ).fetchall()
        most_escalated_reasons = [{"reason": r["reason"], "count": int(r["cnt"])} for r in reason_rows]

        # avg days to resolve
        avg_row = conn.execute(
            f"SELECT AVG(julianday(resolved_at) - julianday(escalated_at)) as avg_days {sql_base} AND resolved_at IS NOT NULL",
            params_base,
        ).fetchone()
        avg_days_to_resolve = round(float(avg_row["avg_days"]), 1) if avg_row and avg_row["avg_days"] is not None else None

        # recent pending (max 20)
        recent_rows = conn.execute(
            f"SELECT * {sql_base} AND resolved_at IS NULL ORDER BY escalation_level DESC, escalated_at ASC LIMIT 20",
            params_base,
        ).fetchall()
        recent_pending = [dict(r) for r in recent_rows]

        total_pending = sum(pending_by_level.values())
    finally:
        conn.close()

    return {
        "status": "attention" if pending_by_level.get(3, 0) > 0 else ("ok" if total_pending == 0 else "warning"),
        "summary": {
            "total_pending": total_pending,
            "pending_level3": pending_by_level.get(3, 0),
        },
        "pending_by_level": pending_by_level,
        "most_escalated_reasons": most_escalated_reasons,
        "avg_days_to_resolve": avg_days_to_resolve,
        "recent_pending": recent_pending,
        "filters": {
            "project_key": normalized_project_key,
            "scope_code": normalized_scope_code,
        },
    }


@mcp.tool
def apply_escalation_reactions(
    project_key: str | None = None,
    scope_code: str | None = None,
    min_level: int = 2,
    owner_overload_threshold: int = 3,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Stosuje półautomatyczne reakcje na aktywne eskalacje.

    Reakcje per poziom eskalacji:
    - Level 2: jeśli priority memory to 'low' lub 'normal' → ustaw na 'high'
    - Level 3: ustaw priority na 'critical'; jeśli owner ma >= owner_overload_threshold
      aktywnych eskalacji level 3 → emituj event 'owner_overloaded' w timeline

    Domyślnie dry_run=True — zwraca listę planowanych akcji bez ich wykonywania.
    Ustaw dry_run=False żeby faktycznie zastosować reakcje.
    """
    _PRIORITY_ORDER = {"low": 0, "normal": 1, "high": 2, "critical": 3}
    _BOOST_MAP = {2: "high", 3: "critical"}

    if min_level not in (1, 2, 3):
        raise ValueError("min_level musi być 1, 2 lub 3")

    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)

    conn = get_db_connection()
    try:
        # Pobierz aktywne eskalacje na odpowiednim poziomie
        sql = (
            "SELECT * FROM escalation_history "
            "WHERE resolved_at IS NULL AND escalation_level >= ? AND entity_type = 'memory'"
        )
        params: list[Any] = [min_level]
        if normalized_project_key is not None:
            sql += " AND project_key = ?"
            params.append(normalized_project_key)
        if normalized_scope_code is not None:
            sql += " AND scope_code = ?"
            params.append(normalized_scope_code)
        sql += " ORDER BY escalation_level DESC, days_overdue DESC"

        escalation_rows = conn.execute(sql, params).fetchall()

        # Zlicz level-3 per owner_role (do detekcji overload)
        overload_sql = (
            "SELECT owner_role, COUNT(*) as cnt FROM escalation_history "
            "WHERE resolved_at IS NULL AND escalation_level = 3 AND owner_role IS NOT NULL"
        )
        overload_params: list[Any] = []
        if normalized_project_key is not None:
            overload_sql += " AND project_key = ?"
            overload_params.append(normalized_project_key)
        overload_sql += " GROUP BY owner_role"
        overload_rows = conn.execute(overload_sql, overload_params).fetchall()
        overloaded_owners = {
            r["owner_role"]: int(r["cnt"])
            for r in overload_rows
            if int(r["cnt"]) >= owner_overload_threshold
        }

        planned_actions: list[dict[str, Any]] = []
        now_iso = utc_now_iso()

        for esc in escalation_rows:
            e = dict(esc)
            entity_id = int(e["entity_id"])
            level = int(e["escalation_level"])
            target_priority = _BOOST_MAP.get(level)
            if target_priority is None:
                continue

            # Sprawdź obecny priorytet memory
            mem_row = conn.execute(
                "SELECT id, priority, state_code FROM memories WHERE id = ? AND activity_state = 'active'",
                (entity_id,),
            ).fetchone()
            if mem_row is None:
                continue

            current_priority = str(mem_row["priority"] or "normal")
            current_order = _PRIORITY_ORDER.get(current_priority, 1)
            target_order = _PRIORITY_ORDER.get(target_priority, 2)

            if target_order > current_order:
                action = {
                    "action": "boost_priority",
                    "entity_type": "memory",
                    "entity_id": entity_id,
                    "current_priority": current_priority,
                    "target_priority": target_priority,
                    "escalation_level": level,
                    "reason": e.get("reason"),
                    "applied": False,
                }
                planned_actions.append(action)

                if not dry_run:
                    conn.execute(
                        "UPDATE memories SET priority = ?, last_accessed_at = ? WHERE id = ?",
                        (target_priority, now_iso, entity_id),
                    )
                    _insert_memory_event(
                        conn,
                        memory_id=entity_id,
                        event_type="priority.updated",
                        payload={
                            "priority": target_priority,
                            "reason": "escalation_reaction",
                            "escalation_level": level,
                        },
                    )
                    action["applied"] = True

        # Reakcje na overloaded owners
        owner_actions: list[dict[str, Any]] = []
        emitted_owners: set[str] = set()
        for owner_role_key, count in overloaded_owners.items():
            if owner_role_key in emitted_owners:
                continue
            emitted_owners.add(owner_role_key)
            owner_action = {
                "action": "flag_owner_overloaded",
                "owner_role": owner_role_key,
                "level3_escalation_count": count,
                "applied": False,
            }
            owner_actions.append(owner_action)

            if not dry_run:
                timeline.record_project_event(
                    conn,
                    project_key=_owner_catalog_audit_project_key(normalized_project_key),
                    event_type="project.note_recorded",
                    title=f"Owner overloaded: {owner_role_key}",
                    description=(
                        f"owner_role={owner_role_key}; level3_escalation_count={count}; "
                        f"threshold={owner_overload_threshold}"
                    ),
                    origin="system",
                    tags=["owner_overloaded", "escalation_reaction"],
                    status="completed",
                    canonical=True,
                    category="owner_overloaded",
                    now_fn=utc_now_iso,
                )
                owner_action["applied"] = True

        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    all_actions = planned_actions + owner_actions
    applied_count = sum(1 for a in all_actions if a.get("applied"))
    return {
        "status": "ok",
        "summary": {
            "total_actions": len(all_actions),
            "priority_boosts": len(planned_actions),
            "owner_overload_flags": len(owner_actions),
            "applied": applied_count if not dry_run else 0,
            "dry_run": dry_run,
        },
        "actions": all_actions,
        "filters": {
            "project_key": normalized_project_key,
            "scope_code": normalized_scope_code,
            "min_level": min_level,
            "owner_overload_threshold": owner_overload_threshold,
        },
    }


@mcp.tool
def get_sla_policy_observability(
    queue_type: str | None = None,
    project_key: str | None = None,
    scope_code: str | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Raport odchyleń polityk SLA: porównuje skonfigurowane polityki z rzeczywistym stanem kolejek.

    Dla każdej kombinacji (queue_type × priority) zwraca:
    - policy_days: ile dni SLA wynika z polityki
    - total_items: liczba elementów z due_date w tej kombinacji
    - overdue_count: ile jest już przeterminowanych
    - overdue_rate: procent przeterminowanych
    - assessment: 'too_aggressive' (>50% overdue) | 'too_loose' (<5% overdue, dużo czasu) | 'ok'

    Wyniki pogrupowane per queue_type i priority.
    """
    _VALID_QUEUE_TYPES = ("review", "revalidation", "expired", "duplicate")
    normalized_queue_type = normalize_optional_text(queue_type)
    if normalized_queue_type is not None and normalized_queue_type not in _VALID_QUEUE_TYPES:
        raise ValueError(f"queue_type musi być jednym z: {', '.join(_VALID_QUEUE_TYPES)}")
    normalized_project_key = normalize_optional_text(project_key)
    normalized_scope_code = normalize_scope_code(scope_code)
    normalized_as_of = normalize_optional_text(as_of) or utc_now_iso()

    # Mapowanie queue_type → (tabela, pole due_date, pole priority, warunek stanu)
    queue_configs = {
        "review": ("memories", "review_due_at", "priority", "state_code = 'candidate'"),
        "revalidation": ("memories", "revalidation_due_at", "priority", "state_code = 'validated'"),
        "expired": ("memories", "expired_due_at", "priority", "1=1"),
        "duplicate": ("duplicate_review_items", "duplicate_due_at", "priority", "status = 'open'"),
    }

    queues_to_check = (
        [normalized_queue_type] if normalized_queue_type else list(queue_configs.keys())
    )

    conn = get_db_connection()
    try:
        metrics: list[dict[str, Any]] = []
        items_without_policy: list[dict[str, Any]] = []

        for qt in queues_to_check:
            table, due_field, prio_field, state_cond = queue_configs[qt]

            # Pobierz wszystkie rekordy z due_date IS NOT NULL
            base_sql = (
                f"SELECT {prio_field}, {due_field} FROM {table} "
                f"WHERE {due_field} IS NOT NULL AND {state_cond} AND activity_state = 'active'"
                if table == "memories"
                else f"SELECT {prio_field}, {due_field} FROM {table} "
                f"WHERE {due_field} IS NOT NULL AND {state_cond}"
            )
            rows = conn.execute(base_sql).fetchall()

            # Grupuj per priority
            from collections import defaultdict

            groups: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "overdue": 0})
            for r in rows:
                prio = str(r[prio_field] or "normal")
                groups[prio]["total"] += 1
                due_val = normalize_optional_text(r[due_field])
                if due_val and due_val <= normalized_as_of:
                    groups[prio]["overdue"] += 1

            for prio, counts in groups.items():
                total = counts["total"]
                overdue = counts["overdue"]
                overdue_rate = round(overdue / total * 100, 1) if total > 0 else 0.0

                # Pobierz policy_days dla tej kombinacji
                policy_days = _compute_sla_days(conn, qt, prio, None, normalized_scope_code, normalized_project_key)
                is_fallback = not conn.execute(
                    "SELECT 1 FROM sla_policies WHERE queue_type = ? AND is_active = 1 LIMIT 1",
                    (qt,),
                ).fetchone()

                if is_fallback:
                    items_without_policy.append({"queue_type": qt, "priority": prio, "total": total})

                if overdue_rate > 50:
                    assessment = "too_aggressive"
                elif overdue_rate < 5 and policy_days > 14:
                    assessment = "too_loose"
                else:
                    assessment = "ok"

                metrics.append({
                    "queue_type": qt,
                    "priority": prio,
                    "policy_days": policy_days,
                    "policy_source": "fallback_default" if is_fallback else "configured",
                    "total_items": total,
                    "overdue_count": overdue,
                    "overdue_rate_pct": overdue_rate,
                    "assessment": assessment,
                })
    finally:
        conn.close()

    attention_count = sum(1 for m in metrics if m["assessment"] != "ok")
    return {
        "status": "attention" if attention_count > 0 else "ok",
        "summary": {
            "queues_checked": len(queues_to_check),
            "combinations_checked": len(metrics),
            "attention_count": attention_count,
            "items_without_configured_policy": len(items_without_policy),
        },
        "metrics": sorted(metrics, key=lambda x: (x["queue_type"], x["priority"])),
        "items_without_configured_policy": items_without_policy,
        "filters": {
            "queue_type": normalized_queue_type,
            "project_key": normalized_project_key,
            "scope_code": normalized_scope_code,
            "as_of": normalized_as_of,
        },
    }


def _safe_event_timestamp(value: str | None) -> float | None:
    normalized_value = normalize_optional_text(value)
    if normalized_value is None:
        return None
    candidate = normalized_value.replace("Z", "+00:00")
    try:
        from datetime import datetime

        return datetime.fromisoformat(candidate).timestamp()
    except ValueError:
        return None


@mcp.tool
def get_queue_observability_metrics(
    validated_before: str | None = None,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
    text_query: str | None = None,
) -> dict[str, Any]:
    review_queue = list_review_queue(
        limit=1000,
        memory_type=memory_type,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    revalidation_queue = list_revalidation_queue(
        limit=1000,
        validated_before=validated_before,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    expired_queue = list_expired_memories(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    duplicate_queue = list_duplicate_candidates_admin(
        limit=1000,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
        effective_owner_key=effective_owner_key,
        effective_owner_type=effective_owner_type,
    )
    overdue_review_queue = list_overdue_review_queue(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
    )
    overdue_revalidation_queue = list_overdue_revalidation_queue(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
    )
    overdue_expired_queue = list_overdue_expired_queue(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
    )
    overdue_duplicate_queue = list_overdue_duplicate_queue(
        limit=1000,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
    )

    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_layer = normalize_layer_code(layer_code)
    normalized_area = normalize_area_code(area_code)
    normalized_memory_type = normalize_optional_text(memory_type)
    normalized_tag = normalize_optional_text(tag)
    normalized_text_query = normalize_optional_text(text_query)

    conn = get_db_connection()
    try:
        owner_catalog_health = _get_owner_catalog_health_data(conn, project_key=normalized_project_key, scope_code=normalized_scope)
        sql = "SELECT * FROM memories WHERE 1 = 1"
        params: list[Any] = []
        if normalized_scope:
            sql += " AND scope_code = ?"
            params.append(normalized_scope)
        if normalized_project_key:
            sql += " AND project_key = ?"
            params.append(normalized_project_key)
        if normalized_layer:
            sql += " AND layer_code = ?"
            params.append(normalized_layer)
        if normalized_area:
            sql += " AND area_code = ?"
            params.append(normalized_area)
        if normalized_memory_type:
            sql += " AND memory_type = ?"
            params.append(normalized_memory_type)
        if normalized_tag:
            sql += " AND COALESCE(tags, '') LIKE ?"
            params.append(f"%{normalized_tag}%")
        if normalized_text_query:
            sql += " AND (content LIKE ? OR COALESCE(summary_short, '') LIKE ? OR COALESCE(tags, '') LIKE ?)"
            like_value = f"%{normalized_text_query}%"
            params.extend([like_value, like_value, like_value])
        memory_rows = conn.execute(sql, params).fetchall()

        event_rows = conn.execute(
            "SELECT * FROM memory_events WHERE event_type IN ('review.draft_created', 'review.approved') ORDER BY id ASC"
        ).fetchall()
    finally:
        conn.close()

    memory_items = [enrich_memory_dict(row_to_dict(row)) for row in memory_rows]
    total_memories = len(memory_items)
    validated_memories = sum(1 for item in memory_items if item.get("state_code") == "validated")
    superseded_memories = sum(1 for item in memory_items if item.get("state_code") == "superseded")
    archived_memories = sum(1 for item in memory_items if item.get("state_code") == "archived")
    missing_owner_count = sum(1 for item in memory_items if normalize_optional_text(item.get("owner_role")) is None)
    duplicate_review_missing_owner_count = sum(1 for item in duplicate_queue["items"] if normalize_optional_text((item.get("duplicate_review") or {}).get("owner_role")) is None)

    draft_created_at: dict[int, float] = {}
    approval_lead_times: list[float] = []
    for row in event_rows:
        event = row_to_dict(row)
        memory_id = int(event["memory_id"])
        if not any(int(item.get("id") or 0) == memory_id for item in memory_items):
            continue
        event_type = str(event.get("event_type") or "")
        event_ts = _safe_event_timestamp(event.get("created_at"))
        if event_ts is None:
            continue
        if event_type == "review.draft_created":
            draft_created_at[memory_id] = event_ts
        elif event_type == "review.approved":
            draft_ts = draft_created_at.get(memory_id)
            if draft_ts is not None and event_ts >= draft_ts:
                approval_lead_times.append(event_ts - draft_ts)

    avg_lead = sum(approval_lead_times) / len(approval_lead_times) if approval_lead_times else 0.0
    max_lead = max(approval_lead_times) if approval_lead_times else 0.0

    feature_flag = _get_feature_flag_config(conn, CROSS_PROJECT_FLAG_KEY) if False else None
    conn = get_db_connection()
    try:
        feature_flag = _get_feature_flag_config(conn, CROSS_PROJECT_FLAG_KEY)
    finally:
        conn.close()
    feature_flag_evaluation = _evaluate_feature_flag_config(feature_flag, project_key=normalized_project_key, scope_code=normalized_scope)
    feature_flag_view = dict(feature_flag)
    feature_flag_view["key"] = feature_flag_view.get("flag_key")
    feature_flag_view["enabled"] = bool(int(feature_flag_view.get("is_enabled") or 0))
    feature_flag_view["rollout_scope"] = feature_flag_view.get("allowed_scope_codes")
    feature_flag_view["rollout_project_key"] = feature_flag_view.get("allowed_project_keys")
    rollout_mode_aliases = {"all": "global", "projects": "project", "scopes": "scope", "projects_and_scopes": "scoped_project", "off": "off"}
    feature_flag_view["rollout_mode"] = rollout_mode_aliases.get(str(feature_flag_view.get("rollout_mode") or "off"), feature_flag_view.get("rollout_mode"))

    return {
        "filters": {
            "validated_before": normalize_optional_text(validated_before),
            "as_of": normalize_optional_text(as_of),
            "scope_code": normalized_scope,
            "project_key": normalized_project_key,
            "layer_code": normalized_layer,
            "area_code": normalized_area,
            "memory_type": normalized_memory_type,
            "tag": normalized_tag,
            "text_query": normalized_text_query,
        },
        "feature_flag": feature_flag_view,
        "feature_flag_evaluation": feature_flag_evaluation,
        "backlogs": {
            "review_queue_count": review_queue["count"],
            "revalidation_queue_count": revalidation_queue["count"],
            "expired_queue_count": expired_queue["count"],
            "duplicate_queue_count": duplicate_queue["count"],
            "overdue_review_count": overdue_review_queue["count"],
            "overdue_revalidation_count": overdue_revalidation_queue["count"],
            "overdue_expired_count": overdue_expired_queue["count"],
            "overdue_duplicate_count": overdue_duplicate_queue["count"],
        },
        "inventory": {
            "total_memories": total_memories,
            "validated_memories": validated_memories,
            "superseded_memories": superseded_memories,
            "archived_memories": archived_memories,
            "missing_owner_count": missing_owner_count,
            "duplicate_review_missing_owner_count": duplicate_review_missing_owner_count,
            "broken_owner_mapping_count": int(owner_catalog_health.get("broken_owner_mapping_count") or 0),
            "inactive_owner_target_count": int(owner_catalog_health.get("inactive_owner_target_count") or 0),
            "owner_catalog_governance_warning_count": int(owner_catalog_health.get("governance_warning_count") or 0),
        },
        "owner_catalog_health": owner_catalog_health,
        "approval_metrics": {
            "approved_from_draft_count": len(approval_lead_times),
            "approval_lead_time_avg_seconds": avg_lead,
            "approval_lead_time_max_seconds": max_lead,
        },
    }


def _escalation_stage(*, value: int, level1_threshold: int, level2_threshold: int, level3_threshold: int) -> dict[str, Any]:
    numeric_value = int(value)
    lvl1 = int(level1_threshold)
    lvl2 = max(int(level2_threshold), lvl1)
    lvl3 = max(int(level3_threshold), lvl2)
    if numeric_value > lvl3:
        return {"level": 3, "stage": "level_3", "severity": "critical"}
    if numeric_value > lvl2:
        return {"level": 2, "stage": "level_2", "severity": "high"}
    if numeric_value > lvl1:
        return {"level": 1, "stage": "level_1", "severity": "warning"}
    return {"level": 0, "stage": "none", "severity": "ok"}


def _highest_escalation_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    if not items:
        return {"level": 0, "stage": "none"}
    highest = max(items, key=lambda item: int(item.get("level") or 0))
    return {"level": int(highest.get("level") or 0), "stage": highest.get("stage") or "none"}


@mcp.tool
def get_quality_alerts(
    validated_before: str | None = None,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    layer_code: str | None = None,
    area_code: str | None = None,
    memory_type: str | None = None,
    tag: str | None = None,
    text_query: str | None = None,
    max_review_queue: int = 10,
    max_revalidation_queue: int = 10,
    max_expired_queue: int = 5,
    max_duplicate_queue: int = 5,
    max_avg_approval_lead_seconds: float = 86400.0,
    max_overdue_review_count: int = 0,
    max_overdue_revalidation_count: int = 0,
    max_missing_owner_count: int = 0,
    max_overdue_review_count_level2: int = 3,
    max_overdue_review_count_level3: int = 7,
    max_overdue_revalidation_count_level2: int = 3,
    max_overdue_revalidation_count_level3: int = 7,
    max_missing_owner_count_level2: int = 2,
    max_missing_owner_count_level3: int = 5,
    max_overdue_expired_count: int = 0,
    max_overdue_expired_count_level2: int = 2,
    max_overdue_expired_count_level3: int = 5,
    max_overdue_duplicate_count: int = 0,
    max_overdue_duplicate_count_level2: int = 2,
    max_overdue_duplicate_count_level3: int = 5,
    max_owner_overdue_total: int = 2,
    max_owner_overdue_total_level2: int = 4,
    max_owner_overdue_total_level3: int = 7,
    max_broken_owner_mapping_count: int = 0,
    max_broken_owner_mapping_count_level2: int = 1,
    max_broken_owner_mapping_count_level3: int = 3,
    max_owner_catalog_governance_warning_count: int = 0,
    max_owner_catalog_governance_warning_count_level2: int = 3,
    max_owner_catalog_governance_warning_count_level3: int = 7,
) -> dict[str, Any]:
    metrics = get_queue_observability_metrics(
        validated_before=validated_before,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
    )
    owner_workload = get_effective_owner_workload(
        limit=200,
        validated_before=validated_before,
        as_of=as_of,
        scope_code=scope_code,
        project_key=project_key,
        layer_code=layer_code,
        area_code=area_code,
        memory_type=memory_type,
        tag=tag,
        text_query=text_query,
    )

    alerts: list[dict[str, Any]] = []
    backlogs = metrics["backlogs"]
    approval_metrics = metrics["approval_metrics"]

    if backlogs["review_queue_count"] > int(max_review_queue):
        alerts.append({"severity": "warning", "kind": "review_backlog", "value": backlogs["review_queue_count"], "threshold": int(max_review_queue)})
    if backlogs["revalidation_queue_count"] > int(max_revalidation_queue):
        alerts.append({"severity": "warning", "kind": "revalidation_backlog", "value": backlogs["revalidation_queue_count"], "threshold": int(max_revalidation_queue)})
    if backlogs["expired_queue_count"] > int(max_expired_queue):
        alerts.append({"severity": "warning", "kind": "expired_backlog", "value": backlogs["expired_queue_count"], "threshold": int(max_expired_queue)})
    if backlogs["duplicate_queue_count"] > int(max_duplicate_queue):
        alerts.append({"severity": "warning", "kind": "duplicate_backlog", "value": backlogs["duplicate_queue_count"], "threshold": int(max_duplicate_queue)})
    if approval_metrics["approval_lead_time_avg_seconds"] > float(max_avg_approval_lead_seconds):
        alerts.append({"severity": "warning", "kind": "approval_lead_time", "value": approval_metrics["approval_lead_time_avg_seconds"], "threshold": float(max_avg_approval_lead_seconds)})
    review_escalation = _escalation_stage(
        value=backlogs.get("overdue_review_count", 0),
        level1_threshold=max_overdue_review_count,
        level2_threshold=max_overdue_review_count_level2,
        level3_threshold=max_overdue_review_count_level3,
    )
    revalidation_escalation = _escalation_stage(
        value=backlogs.get("overdue_revalidation_count", 0),
        level1_threshold=max_overdue_revalidation_count,
        level2_threshold=max_overdue_revalidation_count_level2,
        level3_threshold=max_overdue_revalidation_count_level3,
    )
    owner_missing_escalation = _escalation_stage(
        value=metrics.get("inventory", {}).get("missing_owner_count", 0),
        level1_threshold=max_missing_owner_count,
        level2_threshold=max_missing_owner_count_level2,
        level3_threshold=max_missing_owner_count_level3,
    )
    expired_overdue_escalation = _escalation_stage(
        value=backlogs.get("overdue_expired_count", 0),
        level1_threshold=max_overdue_expired_count,
        level2_threshold=max_overdue_expired_count_level2,
        level3_threshold=max_overdue_expired_count_level3,
    )
    duplicate_overdue_escalation = _escalation_stage(
        value=backlogs.get("overdue_duplicate_count", 0),
        level1_threshold=max_overdue_duplicate_count,
        level2_threshold=max_overdue_duplicate_count_level2,
        level3_threshold=max_overdue_duplicate_count_level3,
    )
    top_owner_workload = owner_workload["items"][0] if owner_workload.get("items") else None
    owner_overdue_total = int((top_owner_workload or {}).get("overdue_total") or 0)
    owner_overloaded_escalation = _escalation_stage(
        value=owner_overdue_total,
        level1_threshold=max_owner_overdue_total,
        level2_threshold=max_owner_overdue_total_level2,
        level3_threshold=max_owner_overdue_total_level3,
    )
    broken_owner_mapping_escalation = _escalation_stage(
        value=metrics.get("inventory", {}).get("broken_owner_mapping_count", 0),
        level1_threshold=max_broken_owner_mapping_count,
        level2_threshold=max_broken_owner_mapping_count_level2,
        level3_threshold=max_broken_owner_mapping_count_level3,
    )
    owner_catalog_governance_escalation = _escalation_stage(
        value=metrics.get("inventory", {}).get("owner_catalog_governance_warning_count", 0),
        level1_threshold=max_owner_catalog_governance_warning_count,
        level2_threshold=max_owner_catalog_governance_warning_count_level2,
        level3_threshold=max_owner_catalog_governance_warning_count_level3,
    )

    if review_escalation["level"] > 0:
        alerts.append({"severity": review_escalation["severity"], "kind": "review_overdue", "value": backlogs.get("overdue_review_count", 0), "threshold": int(max_overdue_review_count), "escalation_level": review_escalation["level"], "escalation_stage": review_escalation["stage"], "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OVERDUE_ESCALATION_RUNBOOK.md"})
    if revalidation_escalation["level"] > 0:
        alerts.append({"severity": revalidation_escalation["severity"], "kind": "revalidation_overdue", "value": backlogs.get("overdue_revalidation_count", 0), "threshold": int(max_overdue_revalidation_count), "escalation_level": revalidation_escalation["level"], "escalation_stage": revalidation_escalation["stage"], "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OVERDUE_ESCALATION_RUNBOOK.md"})
    if owner_missing_escalation["level"] > 0:
        alerts.append({"severity": owner_missing_escalation["severity"], "kind": "owner_missing", "value": metrics.get("inventory", {}).get("missing_owner_count", 0), "threshold": int(max_missing_owner_count), "escalation_level": owner_missing_escalation["level"], "escalation_stage": owner_missing_escalation["stage"], "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OVERDUE_ESCALATION_RUNBOOK.md"})
    if expired_overdue_escalation["level"] > 0:
        alerts.append({"severity": expired_overdue_escalation["severity"], "kind": "expired_overdue", "value": backlogs.get("overdue_expired_count", 0), "threshold": int(max_overdue_expired_count), "escalation_level": expired_overdue_escalation["level"], "escalation_stage": expired_overdue_escalation["stage"], "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OVERDUE_ESCALATION_RUNBOOK.md"})
    if duplicate_overdue_escalation["level"] > 0:
        alerts.append({"severity": duplicate_overdue_escalation["severity"], "kind": "duplicate_overdue", "value": backlogs.get("overdue_duplicate_count", 0), "threshold": int(max_overdue_duplicate_count), "escalation_level": duplicate_overdue_escalation["level"], "escalation_stage": duplicate_overdue_escalation["stage"], "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OVERDUE_ESCALATION_RUNBOOK.md"})
    if owner_overloaded_escalation["level"] > 0 and top_owner_workload is not None:
        alerts.append({"severity": owner_overloaded_escalation["severity"], "kind": "owner_overloaded", "value": owner_overdue_total, "threshold": int(max_owner_overdue_total), "escalation_level": owner_overloaded_escalation["level"], "escalation_stage": owner_overloaded_escalation["stage"], "effective_owner_key": top_owner_workload.get("effective_owner_key"), "effective_owner_type": top_owner_workload.get("effective_owner_type"), "effective_display_name": top_owner_workload.get("effective_display_name"), "total_count": int(top_owner_workload.get("total_count") or 0), "overdue_total": owner_overdue_total, "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OVERDUE_ESCALATION_RUNBOOK.md"})
    if broken_owner_mapping_escalation["level"] > 0:
        alerts.append({"severity": broken_owner_mapping_escalation["severity"], "kind": "broken_owner_mapping", "value": metrics.get("inventory", {}).get("broken_owner_mapping_count", 0), "threshold": int(max_broken_owner_mapping_count), "escalation_level": broken_owner_mapping_escalation["level"], "escalation_stage": broken_owner_mapping_escalation["stage"], "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OWNER_REBALANCE_RUNBOOK.md"})
    if owner_catalog_governance_escalation["level"] > 0:
        alerts.append({"severity": owner_catalog_governance_escalation["severity"], "kind": "owner_catalog_governance_warning", "value": metrics.get("inventory", {}).get("owner_catalog_governance_warning_count", 0), "threshold": int(max_owner_catalog_governance_warning_count), "escalation_level": owner_catalog_governance_escalation["level"], "escalation_stage": owner_catalog_governance_escalation["stage"], "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OWNER_CATALOG_GOVERNANCE.md"})

    escalation_areas = {
        "review_overdue": {**review_escalation, "value": backlogs.get("overdue_review_count", 0), "thresholds": {"level1": int(max_overdue_review_count), "level2": int(max_overdue_review_count_level2), "level3": int(max_overdue_review_count_level3)}},
        "revalidation_overdue": {**revalidation_escalation, "value": backlogs.get("overdue_revalidation_count", 0), "thresholds": {"level1": int(max_overdue_revalidation_count), "level2": int(max_overdue_revalidation_count_level2), "level3": int(max_overdue_revalidation_count_level3)}},
        "owner_missing": {**owner_missing_escalation, "value": metrics.get("inventory", {}).get("missing_owner_count", 0), "thresholds": {"level1": int(max_missing_owner_count), "level2": int(max_missing_owner_count_level2), "level3": int(max_missing_owner_count_level3)}},
        "expired_overdue": {**expired_overdue_escalation, "value": backlogs.get("overdue_expired_count", 0), "thresholds": {"level1": int(max_overdue_expired_count), "level2": int(max_overdue_expired_count_level2), "level3": int(max_overdue_expired_count_level3)}},
        "duplicate_overdue": {**duplicate_overdue_escalation, "value": backlogs.get("overdue_duplicate_count", 0), "thresholds": {"level1": int(max_overdue_duplicate_count), "level2": int(max_overdue_duplicate_count_level2), "level3": int(max_overdue_duplicate_count_level3)}},
        "owner_overloaded": {**owner_overloaded_escalation, "value": owner_overdue_total, "effective_owner_key": None if top_owner_workload is None else top_owner_workload.get("effective_owner_key"), "thresholds": {"level1": int(max_owner_overdue_total), "level2": int(max_owner_overdue_total_level2), "level3": int(max_owner_overdue_total_level3)}},
        "broken_owner_mapping": {**broken_owner_mapping_escalation, "value": metrics.get("inventory", {}).get("broken_owner_mapping_count", 0), "thresholds": {"level1": int(max_broken_owner_mapping_count), "level2": int(max_broken_owner_mapping_count_level2), "level3": int(max_broken_owner_mapping_count_level3)}},
        "owner_catalog_governance_warning": {**owner_catalog_governance_escalation, "value": metrics.get("inventory", {}).get("owner_catalog_governance_warning_count", 0), "thresholds": {"level1": int(max_owner_catalog_governance_warning_count), "level2": int(max_owner_catalog_governance_warning_count_level2), "level3": int(max_owner_catalog_governance_warning_count_level3)}},
    }

    feature_flag_evaluation = metrics.get("feature_flag_evaluation") or {}
    feature_flag = metrics.get("feature_flag") or {}
    if not bool(feature_flag_evaluation.get("enabled", False)):
        alerts.append({"severity": "info", "kind": "feature_flag_disabled", "value": feature_flag_evaluation.get("reason"), "threshold": None})
    if bool(feature_flag_evaluation.get("read_only_mode", False)):
        alerts.append({"severity": "info", "kind": "feature_flag_read_only", "value": True, "threshold": None})

    escalation_summary = {"highest": _highest_escalation_summary(list(escalation_areas.values())), "areas": escalation_areas, "runbook": "docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OVERDUE_ESCALATION_RUNBOOK.md"}

    return {
        "status": "ok" if not alerts else "attention",
        "alert_count": len(alerts),
        "alerts": alerts,
        "feature_flag": feature_flag,
        "feature_flag_evaluation": feature_flag_evaluation,
        "metrics": metrics,
        "owner_workload": owner_workload,
        "escalation_summary": escalation_summary,
        "thresholds": {
            "max_review_queue": int(max_review_queue),
            "max_revalidation_queue": int(max_revalidation_queue),
            "max_expired_queue": int(max_expired_queue),
            "max_duplicate_queue": int(max_duplicate_queue),
            "max_avg_approval_lead_seconds": float(max_avg_approval_lead_seconds),
            "max_overdue_review_count": int(max_overdue_review_count),
            "max_overdue_review_count_level2": int(max_overdue_review_count_level2),
            "max_overdue_review_count_level3": int(max_overdue_review_count_level3),
            "max_overdue_revalidation_count": int(max_overdue_revalidation_count),
            "max_overdue_revalidation_count_level2": int(max_overdue_revalidation_count_level2),
            "max_overdue_revalidation_count_level3": int(max_overdue_revalidation_count_level3),
            "max_missing_owner_count": int(max_missing_owner_count),
            "max_missing_owner_count_level2": int(max_missing_owner_count_level2),
            "max_missing_owner_count_level3": int(max_missing_owner_count_level3),
            "max_overdue_expired_count": int(max_overdue_expired_count),
            "max_overdue_expired_count_level2": int(max_overdue_expired_count_level2),
            "max_overdue_expired_count_level3": int(max_overdue_expired_count_level3),
            "max_overdue_duplicate_count": int(max_overdue_duplicate_count),
            "max_overdue_duplicate_count_level2": int(max_overdue_duplicate_count_level2),
            "max_overdue_duplicate_count_level3": int(max_overdue_duplicate_count_level3),
            "max_owner_overdue_total": int(max_owner_overdue_total),
            "max_owner_overdue_total_level2": int(max_owner_overdue_total_level2),
            "max_owner_overdue_total_level3": int(max_owner_overdue_total_level3),
            "max_broken_owner_mapping_count": int(max_broken_owner_mapping_count),
            "max_broken_owner_mapping_count_level2": int(max_broken_owner_mapping_count_level2),
            "max_broken_owner_mapping_count_level3": int(max_broken_owner_mapping_count_level3),
            "max_owner_catalog_governance_warning_count": int(max_owner_catalog_governance_warning_count),
            "max_owner_catalog_governance_warning_count_level2": int(max_owner_catalog_governance_warning_count_level2),
            "max_owner_catalog_governance_warning_count_level3": int(max_owner_catalog_governance_warning_count_level3),
        },
    }


@mcp.tool
def set_memory_owner(memory_id: int, owner_role: str, owner_id: str | None = None) -> dict[str, Any]:
    normalized_owner_role = normalize_required_text(owner_role, "owner_role")
    normalized_owner_id = normalize_optional_text(owner_id)
    conn = get_db_connection()
    try:
        require_memory_row(conn, int(memory_id))
        updated_at = utc_now_iso()
        conn.execute(
            "UPDATE memories SET owner_role = ?, owner_id = ?, last_accessed_at = ? WHERE id = ?",
            (normalized_owner_role, normalized_owner_id, updated_at, int(memory_id)),
        )
        event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type="ownership.updated",
            payload={"owner_role": normalized_owner_role, "owner_id": normalized_owner_id},
        )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()
    return {"status": "owner_updated", "event": event, "memory": _apply_ownership_defaults(enrich_memory_dict(row_to_dict(updated_row)))}


@mcp.tool
def set_memory_sla(memory_id: int, review_due_at: str | None = None, revalidation_due_at: str | None = None, expired_due_at: str | None = None) -> dict[str, Any]:
    normalized_review_due_at = normalize_optional_text(review_due_at)
    normalized_revalidation_due_at = normalize_optional_text(revalidation_due_at)
    normalized_expired_due_at = normalize_optional_text(expired_due_at)
    if normalized_review_due_at is None and normalized_revalidation_due_at is None and normalized_expired_due_at is None:
        raise ValueError("Musisz podać review_due_at, revalidation_due_at albo expired_due_at")
    conn = get_db_connection()
    try:
        require_memory_row(conn, int(memory_id))
        updated_at = utc_now_iso()
        conn.execute(
            "UPDATE memories SET review_due_at = COALESCE(?, review_due_at), revalidation_due_at = COALESCE(?, revalidation_due_at), expired_due_at = COALESCE(?, expired_due_at), last_accessed_at = ? WHERE id = ?",
            (normalized_review_due_at, normalized_revalidation_due_at, normalized_expired_due_at, updated_at, int(memory_id)),
        )
        event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type="sla.updated",
            payload={"review_due_at": normalized_review_due_at, "revalidation_due_at": normalized_revalidation_due_at, "expired_due_at": normalized_expired_due_at},
        )
        conn.commit()
        updated_row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()
    return {"status": "sla_updated", "event": event, "memory": _apply_ownership_defaults(enrich_memory_dict(row_to_dict(updated_row)))}


@mcp.tool
def bulk_set_memory_owner(memory_ids: list[int], owner_role: str, owner_id: str | None = None) -> dict[str, Any]:
    if not memory_ids:
        raise ValueError("memory_ids nie mogą być puste")
    normalized_owner_role = normalize_required_text(owner_role, "owner_role")
    normalized_owner_id = normalize_optional_text(owner_id)
    unique_ids = [int(memory_id) for memory_id in dict.fromkeys(memory_ids)]
    conn = get_db_connection()
    try:
        updated_at = utc_now_iso()
        items: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        for memory_id in unique_ids:
            require_memory_row(conn, memory_id)
            conn.execute(
                "UPDATE memories SET owner_role = ?, owner_id = ?, last_accessed_at = ? WHERE id = ?",
                (normalized_owner_role, normalized_owner_id, updated_at, memory_id),
            )
            event = _insert_memory_event(
                conn,
                memory_id=memory_id,
                event_type="ownership.bulk_updated",
                payload={"owner_role": normalized_owner_role, "owner_id": normalized_owner_id},
            )
            row = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
            items.append(_apply_ownership_defaults(enrich_memory_dict(row_to_dict(row))))
            events.append(event)
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "bulk_owner_updated",
        "count": len(items),
        "memory_ids": unique_ids,
        "events": events,
        "items": items,
    }


@mcp.tool
def bulk_set_memory_sla(
    memory_ids: list[int],
    review_due_at: str | None = None,
    revalidation_due_at: str | None = None,
    expired_due_at: str | None = None,
) -> dict[str, Any]:
    if not memory_ids:
        raise ValueError("memory_ids nie mogą być puste")
    normalized_review_due_at = normalize_optional_text(review_due_at)
    normalized_revalidation_due_at = normalize_optional_text(revalidation_due_at)
    normalized_expired_due_at = normalize_optional_text(expired_due_at)
    if normalized_review_due_at is None and normalized_revalidation_due_at is None and normalized_expired_due_at is None:
        raise ValueError("Musisz podać review_due_at, revalidation_due_at albo expired_due_at")
    unique_ids = [int(memory_id) for memory_id in dict.fromkeys(memory_ids)]
    conn = get_db_connection()
    try:
        updated_at = utc_now_iso()
        items: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        for memory_id in unique_ids:
            require_memory_row(conn, memory_id)
            conn.execute(
                "UPDATE memories SET review_due_at = COALESCE(?, review_due_at), revalidation_due_at = COALESCE(?, revalidation_due_at), expired_due_at = COALESCE(?, expired_due_at), last_accessed_at = ? WHERE id = ?",
                (normalized_review_due_at, normalized_revalidation_due_at, normalized_expired_due_at, updated_at, memory_id),
            )
            event = _insert_memory_event(
                conn,
                memory_id=memory_id,
                event_type="sla.bulk_updated",
                payload={
                    "review_due_at": normalized_review_due_at,
                    "revalidation_due_at": normalized_revalidation_due_at,
                    "expired_due_at": normalized_expired_due_at,
                },
            )
            row = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
            items.append(_apply_ownership_defaults(enrich_memory_dict(row_to_dict(row))))
            events.append(event)
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "bulk_sla_updated",
        "count": len(items),
        "memory_ids": unique_ids,
        "events": events,
        "items": items,
    }


@mcp.tool
def set_memory_priority(memory_id: int, priority: str) -> dict[str, Any]:
    """Ustawia priorytet memory. Priorytet wpływa na wyliczanie SLA due dates."""
    _VALID_PRIORITIES = ("low", "normal", "high", "critical")
    normalized_priority = normalize_required_text(priority, "priority").lower()
    if normalized_priority not in _VALID_PRIORITIES:
        raise ValueError(f"priority musi być jednym z: {', '.join(_VALID_PRIORITIES)}")
    conn = get_db_connection()
    try:
        require_memory_row(conn, int(memory_id))
        updated_at = utc_now_iso()
        conn.execute(
            "UPDATE memories SET priority = ?, last_accessed_at = ? WHERE id = ?",
            (normalized_priority, updated_at, int(memory_id)),
        )
        event = _insert_memory_event(
            conn,
            memory_id=int(memory_id),
            event_type="priority.updated",
            payload={"priority": normalized_priority},
        )
        conn.commit()
        row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    finally:
        conn.close()
    return {
        "status": "priority_updated",
        "event": event,
        "memory": _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row))),
    }


@mcp.tool
def list_sla_policies(
    queue_type: str | None = None,
    priority: str | None = None,
    active_only: bool = True,
) -> dict[str, Any]:
    """Listuje polityki SLA. Bez filtrów zwraca wszystkie aktywne polityki."""
    normalized_queue_type = normalize_optional_text(queue_type)
    normalized_priority = normalize_optional_text(priority)
    sql = "SELECT * FROM sla_policies WHERE 1=1"
    params: list[Any] = []
    if active_only:
        sql += " AND is_active = 1"
    if normalized_queue_type is not None:
        sql += " AND queue_type = ?"
        params.append(normalized_queue_type)
    if normalized_priority is not None:
        sql += " AND priority = ?"
        params.append(normalized_priority)
    sql += " ORDER BY queue_type, priority, project_key, scope_code, id"
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
        policies = [dict(r) for r in rows]
    finally:
        conn.close()
    return {
        "status": "ok",
        "count": len(policies),
        "policies": policies,
        "filters": {
            "queue_type": normalized_queue_type,
            "priority": normalized_priority,
            "active_only": active_only,
        },
    }


@mcp.tool
def upsert_sla_policy(
    queue_type: str,
    sla_days: int,
    priority: str | None = None,
    memory_type: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    is_active: bool = True,
    notes: str | None = None,
) -> dict[str, Any]:
    """Tworzy lub aktualizuje politykę SLA. Kombinacja (queue_type, priority, memory_type, scope_code, project_key) musi być unikalna."""
    _VALID_QUEUE_TYPES = ("review", "revalidation", "expired", "duplicate")
    _VALID_PRIORITIES = ("low", "normal", "high", "critical")
    normalized_queue_type = normalize_required_text(queue_type, "queue_type").lower()
    if normalized_queue_type not in _VALID_QUEUE_TYPES:
        raise ValueError(f"queue_type musi być jednym z: {', '.join(_VALID_QUEUE_TYPES)}")
    normalized_sla_days = int(sla_days)
    if normalized_sla_days < 1:
        raise ValueError("sla_days musi być >= 1")
    normalized_priority = normalize_optional_text(priority)
    if normalized_priority is not None and normalized_priority not in _VALID_PRIORITIES:
        raise ValueError(f"priority musi być jednym z: {', '.join(_VALID_PRIORITIES)}")
    normalized_memory_type = normalize_optional_text(memory_type)
    normalized_scope_code = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_notes = normalize_optional_text(notes)
    conn = get_db_connection()
    try:
        now_iso = utc_now_iso()
        existing = conn.execute(
            "SELECT id FROM sla_policies WHERE queue_type = ? AND priority IS ? AND memory_type IS ? AND scope_code IS ? AND project_key IS ?",
            (normalized_queue_type, normalized_priority, normalized_memory_type, normalized_scope_code, normalized_project_key),
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO sla_policies (queue_type, sla_days, priority, memory_type, scope_code, project_key, is_active, notes, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (normalized_queue_type, normalized_sla_days, normalized_priority, normalized_memory_type, normalized_scope_code, normalized_project_key, int(is_active), normalized_notes, now_iso, now_iso),
            )
        else:
            conn.execute(
                "UPDATE sla_policies SET sla_days = ?, is_active = ?, notes = ?, updated_at = ? WHERE id = ?",
                (normalized_sla_days, int(is_active), normalized_notes, now_iso, int(existing["id"])),
            )
        row = conn.execute(
            "SELECT * FROM sla_policies WHERE queue_type = ? AND priority IS ? AND memory_type IS ? AND scope_code IS ? AND project_key IS ?",
            (normalized_queue_type, normalized_priority, normalized_memory_type, normalized_scope_code, normalized_project_key),
        ).fetchone()
        audit_event_id = timeline.record_project_event(
            conn,
            project_key=_owner_catalog_audit_project_key(normalized_project_key),
            event_type="project.note_recorded",
            title=f"SLA policy {'created' if existing is None else 'updated'}: {normalized_queue_type}",
            description=(
                f"queue_type={normalized_queue_type}; sla_days={normalized_sla_days}; "
                f"priority={normalized_priority}; memory_type={normalized_memory_type}; "
                f"scope_code={normalized_scope_code}; project_key={normalized_project_key}; "
                f"is_active={bool(is_active)}"
            ),
            origin="system",
            tags=["sla_policy_change", "created" if existing is None else "updated"],
            status="completed",
            canonical=True,
            category="sla_policy_change",
            now_fn=utc_now_iso,
        )
        conn.commit()
        policy = dict(row)
    finally:
        conn.close()
    return {
        "status": "sla_policy_upserted",
        "policy": policy,
        "audit_event": {"id": audit_event_id, "event_type": "project.note_recorded"},
    }


@mcp.tool
def list_overdue_review_queue(
    limit: int = 20,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_as_of = normalize_optional_text(as_of) or utc_now_iso()
    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_owner_id = normalize_optional_text(owner_id)
    sql = "SELECT * FROM memories WHERE state_code = 'candidate' AND review_due_at IS NOT NULL AND review_due_at <= ?"
    params: list[Any] = [normalized_as_of]
    if normalized_scope:
        sql += " AND scope_code = ?"
        params.append(normalized_scope)
    if normalized_project_key:
        sql += " AND project_key = ?"
        params.append(normalized_project_key)
    if normalized_owner_role:
        sql += " AND owner_role = ?"
        params.append(normalized_owner_role)
    if normalized_owner_id:
        sql += " AND owner_id = ?"
        params.append(normalized_owner_id)
    sql += " ORDER BY review_due_at ASC, id DESC LIMIT ?"
    params.append(int(limit))
    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
        items = [_apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row)))) for row in rows]
        items = _filter_items_by_effective_owner(items, effective_owner_key=normalized_effective_owner_key, effective_owner_type=normalized_effective_owner_type)
    finally:
        conn.close()
    return {
        "count": len(items),
        "items": items,
        "queue_state": "review_overdue",
        "filters": {
            "limit": int(limit),
            "as_of": normalized_as_of,
            "scope_code": normalized_scope,
            "project_key": normalized_project_key,
            "owner_role": normalized_owner_role,
            "owner_id": normalized_owner_id,
            "effective_owner_key": normalized_effective_owner_key,
            "effective_owner_type": normalized_effective_owner_type,
        },
    }


@mcp.tool
def list_overdue_revalidation_queue(
    limit: int = 20,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_as_of = normalize_optional_text(as_of) or utc_now_iso()
    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_owner_id = normalize_optional_text(owner_id)
    sql = "SELECT * FROM memories WHERE state_code = 'validated' AND revalidation_due_at IS NOT NULL AND revalidation_due_at <= ?"
    params: list[Any] = [normalized_as_of]
    if normalized_scope:
        sql += " AND scope_code = ?"
        params.append(normalized_scope)
    if normalized_project_key:
        sql += " AND project_key = ?"
        params.append(normalized_project_key)
    if normalized_owner_role:
        sql += " AND owner_role = ?"
        params.append(normalized_owner_role)
    if normalized_owner_id:
        sql += " AND owner_id = ?"
        params.append(normalized_owner_id)
    sql += " ORDER BY revalidation_due_at ASC, id DESC LIMIT ?"
    params.append(int(limit))
    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
        items = [_apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row)))) for row in rows]
        items = _filter_items_by_effective_owner(items, effective_owner_key=normalized_effective_owner_key, effective_owner_type=normalized_effective_owner_type)
    finally:
        conn.close()
    return {
        "count": len(items),
        "items": items,
        "queue_state": "revalidation_overdue",
        "filters": {
            "limit": int(limit),
            "as_of": normalized_as_of,
            "scope_code": normalized_scope,
            "project_key": normalized_project_key,
            "owner_role": normalized_owner_role,
            "owner_id": normalized_owner_id,
            "effective_owner_key": normalized_effective_owner_key,
            "effective_owner_type": normalized_effective_owner_type,
        },
    }


@mcp.tool
def set_duplicate_candidate_sla(
    canonical_memory_id: int,
    duplicate_memory_id: int,
    duplicate_due_at: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    status: str = "open",
) -> dict[str, Any]:
    normalized_due_at = normalize_optional_text(duplicate_due_at)
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_owner_id = normalize_optional_text(owner_id)
    normalized_status = normalize_optional_text(status) or "open"
    if normalized_status not in {"open", "resolved", "ignored"}:
        raise ValueError("status musi być jednym z: open, resolved, ignored")
    conn = get_db_connection()
    try:
        require_memory_row(conn, int(canonical_memory_id))
        require_memory_row(conn, int(duplicate_memory_id))
        _get_or_create_duplicate_review_item(conn, int(canonical_memory_id), int(duplicate_memory_id))
        updated_at = utc_now_iso()
        conn.execute(
            """
            UPDATE duplicate_review_items
            SET owner_role = COALESCE(?, owner_role),
                owner_id = COALESCE(?, owner_id),
                duplicate_due_at = COALESCE(?, duplicate_due_at),
                status = ?,
                updated_at = ?
            WHERE canonical_memory_id = ? AND duplicate_memory_id = ?
            """,
            (normalized_owner_role, normalized_owner_id, normalized_due_at, normalized_status, updated_at, int(canonical_memory_id), int(duplicate_memory_id)),
        )
        row = conn.execute(
            "SELECT * FROM duplicate_review_items WHERE canonical_memory_id = ? AND duplicate_memory_id = ?",
            (int(canonical_memory_id), int(duplicate_memory_id)),
        ).fetchone()
        event = _insert_memory_event(
            conn,
            memory_id=int(duplicate_memory_id),
            event_type="duplicate_review.updated",
            payload={
                "canonical_memory_id": int(canonical_memory_id),
                "duplicate_memory_id": int(duplicate_memory_id),
                "duplicate_due_at": normalized_due_at,
                "owner_role": normalized_owner_role,
                "owner_id": normalized_owner_id,
                "status": normalized_status,
            },
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "duplicate_sla_updated", "event": event, "duplicate_review": _duplicate_review_item_to_dict(row)}


@mcp.tool
def bulk_set_duplicate_candidate_sla(
    pairs: list[dict[str, int]],
    duplicate_due_at: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    status: str = "open",
) -> dict[str, Any]:
    if not pairs:
        raise ValueError("pairs nie mogą być puste")
    normalized_due_at = normalize_optional_text(duplicate_due_at)
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_owner_id = normalize_optional_text(owner_id)
    normalized_status = normalize_optional_text(status) or "open"
    if normalized_status not in {"open", "resolved", "ignored"}:
        raise ValueError("status musi być jednym z: open, resolved, ignored")

    normalized_pairs: list[tuple[int, int]] = []
    for pair in pairs:
        canonical_memory_id = int(pair["canonical_memory_id"])
        duplicate_memory_id = int(pair["duplicate_memory_id"])
        normalized_pairs.append((canonical_memory_id, duplicate_memory_id))
    normalized_pairs = list(dict.fromkeys(normalized_pairs))

    conn = get_db_connection()
    try:
        updated_at = utc_now_iso()
        items: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        for canonical_memory_id, duplicate_memory_id in normalized_pairs:
            require_memory_row(conn, canonical_memory_id)
            require_memory_row(conn, duplicate_memory_id)
            _get_or_create_duplicate_review_item(conn, canonical_memory_id, duplicate_memory_id)
            conn.execute(
                """
                UPDATE duplicate_review_items
                SET owner_role = COALESCE(?, owner_role),
                    owner_id = COALESCE(?, owner_id),
                    duplicate_due_at = COALESCE(?, duplicate_due_at),
                    status = ?,
                    updated_at = ?
                WHERE canonical_memory_id = ? AND duplicate_memory_id = ?
                """,
                (normalized_owner_role, normalized_owner_id, normalized_due_at, normalized_status, updated_at, canonical_memory_id, duplicate_memory_id),
            )
            row = conn.execute(
                "SELECT * FROM duplicate_review_items WHERE canonical_memory_id = ? AND duplicate_memory_id = ?",
                (canonical_memory_id, duplicate_memory_id),
            ).fetchone()
            event = _insert_memory_event(
                conn,
                memory_id=duplicate_memory_id,
                event_type="duplicate_review.bulk_updated",
                payload={
                    "canonical_memory_id": canonical_memory_id,
                    "duplicate_memory_id": duplicate_memory_id,
                    "duplicate_due_at": normalized_due_at,
                    "owner_role": normalized_owner_role,
                    "owner_id": normalized_owner_id,
                    "status": normalized_status,
                },
            )
            items.append(_duplicate_review_item_to_dict(row))
            events.append(event)
        conn.commit()
    finally:
        conn.close()
    return {
        "status": "bulk_duplicate_sla_updated",
        "count": len(items),
        "pairs": [{"canonical_memory_id": a, "duplicate_memory_id": b} for a, b in normalized_pairs],
        "events": events,
        "items": items,
    }


@mcp.tool
def list_overdue_expired_queue(
    limit: int = 20,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_as_of = normalize_optional_text(as_of) or utc_now_iso()
    normalized_scope = normalize_scope_code(scope_code)
    normalized_project_key = normalize_optional_text(project_key)
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_owner_id = normalize_optional_text(owner_id)
    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    sql = "SELECT * FROM memories WHERE valid_to IS NOT NULL AND valid_to <= ?"
    params: list[Any] = [normalized_as_of]
    if normalized_scope:
        sql += " AND scope_code = ?"
        params.append(normalized_scope)
    if normalized_project_key:
        sql += " AND project_key = ?"
        params.append(normalized_project_key)
    sql += " ORDER BY valid_to ASC, id DESC"
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            item = _apply_effective_owner(conn, _apply_ownership_defaults(enrich_memory_dict(row_to_dict(row))))
            due_at = normalize_optional_text(item.get("expired_due_at"))
            if due_at is None or due_at > normalized_as_of:
                continue
            if normalized_owner_role and normalize_optional_text(item.get("owner_role")) != normalized_owner_role:
                continue
            if normalized_owner_id and normalize_optional_text(item.get("owner_id")) != normalized_owner_id:
                continue
            if normalized_effective_owner_key is not None or normalized_effective_owner_type is not None:
                filtered = _filter_items_by_effective_owner([item], effective_owner_key=normalized_effective_owner_key, effective_owner_type=normalized_effective_owner_type)
                if not filtered:
                    continue
            items.append(item)
    finally:
        conn.close()
    return {
        "count": len(items),
        "items": items[: int(limit)],
        "queue_state": "expired_overdue",
        "filters": {
            "limit": int(limit),
            "as_of": normalized_as_of,
            "scope_code": normalized_scope,
            "project_key": normalized_project_key,
            "owner_role": normalized_owner_role,
            "owner_id": normalized_owner_id,
            "effective_owner_key": normalized_effective_owner_key,
            "effective_owner_type": normalized_effective_owner_type,
        },
    }


@mcp.tool
def list_overdue_duplicate_queue(
    limit: int = 20,
    as_of: str | None = None,
    scope_code: str | None = None,
    project_key: str | None = None,
    owner_role: str | None = None,
    owner_id: str | None = None,
    effective_owner_key: str | None = None,
    effective_owner_type: str | None = None,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    normalized_as_of = normalize_optional_text(as_of) or utc_now_iso()
    normalized_owner_role = normalize_optional_text(owner_role)
    normalized_owner_id = normalize_optional_text(owner_id)
    normalized_effective_owner_key = normalize_optional_text(effective_owner_key)
    normalized_effective_owner_type = normalize_optional_text(effective_owner_type)
    queue = list_duplicate_candidates_admin(
        limit=1000,
        scope_code=scope_code,
        project_key=project_key,
        effective_owner_key=normalized_effective_owner_key,
        effective_owner_type=normalized_effective_owner_type,
    )
    items: list[dict[str, Any]] = []
    for item in queue["items"]:
        review_item = item.get("duplicate_review") or {}
        due_at = normalize_optional_text(review_item.get("duplicate_due_at"))
        status_value = normalize_optional_text(review_item.get("status")) or "open"
        if status_value != "open":
            continue
        if due_at is None or due_at > normalized_as_of:
            continue
        if normalized_owner_role and normalize_optional_text(review_item.get("owner_role")) != normalized_owner_role:
            continue
        if normalized_owner_id and normalize_optional_text(review_item.get("owner_id")) != normalized_owner_id:
            continue
        items.append(item)
    return {
        "count": len(items),
        "items": items[: int(limit)],
        "queue_state": "duplicate_overdue",
        "filters": {
            "limit": int(limit),
            "as_of": normalized_as_of,
            "scope_code": normalize_scope_code(scope_code),
            "project_key": normalize_optional_text(project_key),
            "owner_role": normalized_owner_role,
            "owner_id": normalized_owner_id,
            "effective_owner_key": normalized_effective_owner_key,
            "effective_owner_type": normalized_effective_owner_type,
        },
    }


@mcp.tool
def link_memories(from_memory_id: int, to_memory_id: int, relation_type: str, weight: float = 0.5, origin: str | None = None) -> dict[str, Any]:
    if not relation_type or not relation_type.strip():
        raise ValueError("relation_type nie może być puste")
    conn = get_db_connection()
    try:
        if conn.execute("SELECT id FROM memories WHERE id = ?", (from_memory_id,)).fetchone() is None:
            raise FileNotFoundError("Jedno lub oba wspomnienia nie istnieją")
        if conn.execute("SELECT id FROM memories WHERE id = ?", (to_memory_id,)).fetchone() is None:
            raise FileNotFoundError("Jedno lub oba wspomnienia nie istnieją")
        operation_id = timeline.new_operation_id("link")
        link = _create_link(conn, from_memory_id, to_memory_id, relation_type.strip(), float(weight), origin.strip() if isinstance(origin, str) else origin, operation_id=operation_id)
        conn.commit()
    finally:
        conn.close()
    return {"status": "created", "link": link}


@mcp.tool
def recall_memory(memory_id: int, strength: float = 0.1, recall_type: str = "manual") -> dict[str, Any]:
    conn = get_db_connection()
    try:
        memory = require_memory_row(conn, memory_id)
        current_importance = float(memory["importance_score"] or 0.0)
        new_importance = normalize_score(current_importance + float(strength))
        recalled_at = utc_now_iso()
        conn.execute("UPDATE memories SET importance_score = ?, recall_count = recall_count + 1, last_recalled_at = ?, last_accessed_at = ? WHERE id = ?", (new_importance, recalled_at, recalled_at, memory_id))
        conn.commit()
        updated = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
    finally:
        conn.close()
    return {"status": "recalled", "recall_type": recall_type, "updated_memory": enrich_memory_dict(row_to_dict(updated)), "activation_changes": [{"memory_id": memory_id, "old_importance_score": current_importance, "new_importance_score": new_importance}]}


@mcp.tool
def list_sleep_runs(limit: int = 20, status: str | None = None, mode: str | None = None) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    sql = "SELECT * FROM sleep_runs WHERE 1 = 1"
    params: list[Any] = []
    if status:
        sql += " AND status = ?"
        params.append(status)
    if mode:
        sql += " AND mode = ?"
        params.append(mode)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return {"count": len(rows), "items": [row_to_dict(row) for row in rows], "filters": {"limit": limit, "status": status, "mode": mode}}


@mcp.tool
def get_sleep_run(run_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        run = require_sleep_run_row(conn, run_id)
        action_summary_rows = conn.execute("SELECT action_type, COUNT(*) AS count FROM sleep_run_actions WHERE run_id = ? GROUP BY action_type ORDER BY action_type ASC", (run_id,)).fetchall()
    finally:
        conn.close()
    return {"sleep_run": row_to_dict(run), "action_summary": [row_to_dict(row) for row in action_summary_rows]}


@mcp.tool
def get_sleep_run_actions(run_id: int, limit: int = 200) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    conn = get_db_connection()
    try:
        require_sleep_run_row(conn, run_id)
        rows = conn.execute("SELECT * FROM sleep_run_actions WHERE run_id = ? ORDER BY id ASC LIMIT ?", (run_id, limit)).fetchall()
    finally:
        conn.close()
    return {"run_id": run_id, "count": len(rows), "items": [row_to_dict(row) for row in rows], "limit": limit}


@mcp.tool
def preview_undo_run(run_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        run = require_sleep_run_row(conn, run_id)
        existing_rollback_run_id = _existing_rollback_run_id(conn, run_id)
        rollbackable_actions = _get_rollbackable_actions(conn, run_id)
        summary: dict[str, int] = {}
        for action in rollbackable_actions:
            summary[action["action_type"]] = summary.get(action["action_type"], 0) + 1
        return {"status": "preview_completed", "target_run": row_to_dict(run), "already_rolled_back": existing_rollback_run_id is not None, "existing_rollback_run_id": existing_rollback_run_id, "rollbackable_action_count": len(rollbackable_actions), "rollbackable_action_summary": summary, "rollbackable_actions": rollbackable_actions}
    finally:
        conn.close()


@mcp.tool
def undo_run(run_id: int, notes: str | None = None) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        run = require_sleep_run_row(conn, run_id)
        mode = str(run["mode"])
        status = str(run["status"])
        if mode not in {"run", "conflict_run", "consolidation_run", "conflict_resolution_run"}:
            raise ValueError("Undo obsługuje tylko przebiegi wykonawcze: run, conflict_run albo consolidation_run")
        if not status.startswith("completed"):
            raise ValueError("Undo można wykonać tylko dla zakończonego przebiegu completed")
        existing_rollback_run_id = _existing_rollback_run_id(conn, run_id)
        if existing_rollback_run_id is not None:
            raise ValueError(f"Ten run został już cofnięty przez rollback run_id={existing_rollback_run_id}")
        rollbackable_actions = _get_rollbackable_actions(conn, run_id)
        rollback_run_id = create_sleep_run(conn, mode="rollback", freedom_level=0, notes=notes or f"rollback_of_run_{run_id}", rollback_of_run_id=run_id)
        restored_items = [_rollback_single_action(conn, rollback_run_id, action) for action in rollbackable_actions]
        conn.commit()
        finalize_sleep_run(conn, rollback_run_id, status="completed", scanned_count=len(rollbackable_actions), changed_count=len(restored_items), archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=0, created_summary_count=0)
        return {"status": "completed", "rollback_run_id": rollback_run_id, "target_run_id": run_id, "restored_count": len(restored_items), "restored_items": restored_items}
    finally:
        conn.close()


@mcp.tool
def list_conflicted_memories(limit: int = 20) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT * FROM memories WHERE COALESCE(contradiction_flag, 0) = 1 ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    finally:
        conn.close()
    return {"count": len(rows), "items": [row_to_dict(row) for row in rows], "limit": limit}


@mcp.tool
def get_conflict_pairs(memory_id: int | None = None, limit: int = 100) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit musi być w zakresie 1..1000")
    sql = "SELECT * FROM memory_links WHERE relation_type = 'contradicts'"
    params: list[Any] = []
    if memory_id is not None:
        sql += " AND (from_memory_id = ? OR to_memory_id = ?)"
        params.extend([memory_id, memory_id])
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    conn = get_db_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return {"count": len(rows), "items": [row_to_dict(row) for row in rows], "memory_id": memory_id, "limit": limit}


@mcp.tool
def explain_conflict(memory_a_id: int, memory_b_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        if not _is_conflict_feature_active(conn, CONFLICT_EXPLAINER_FLAG_KEY):
            return {"status": "disabled", "reason": "feature_flag_off", "flag_key": CONFLICT_EXPLAINER_FLAG_KEY}
        result = conflict_explainer.explain_conflict_pair(conn, int(memory_a_id), int(memory_b_id))
        try:
            base_ids = sorted([int(memory_a_id), int(memory_b_id)])
            operation_id = timeline.new_operation_id("conflict")
            timeline.record_timeline_event(
                conn,
                event_type="conflict.classified",
                memory_id=base_ids[0],
                related_memory_id=base_ids[1],
                operation_id=operation_id,
                origin="conflict_explainer_auto",
                timeline_scope="memory",
                semantic_kind="decision",
                title=f"Conflict classified: {result['conflict_kind']} (confidence {result['confidence']})",
                payload={
                    "conflict_kind": result["conflict_kind"],
                    "confidence": result["confidence"],
                    "conflict_reason": result["conflict_reason"],
                    "base_memory_ids": result["base_memory_ids"],
                    "context_memory_ids": result["context_memory_ids"],
                    "signal_scores": result["debug"].get("signal_scores", {}),
                    "signals": result["debug"].get("signals", []),
                },
            )
            if bool(result.get("needs_human_review")):
                timeline.record_timeline_event(
                    conn,
                    event_type="conflict.review_requested",
                    memory_id=base_ids[0],
                    related_memory_id=base_ids[1],
                    operation_id=operation_id,
                    origin="conflict_explainer_auto",
                    timeline_scope="memory",
                    semantic_kind="decision",
                    title=f"Conflict review requested: {result['conflict_kind']}",
                    payload={
                        "conflict_kind": result["conflict_kind"],
                        "confidence": result["confidence"],
                        "base_memory_ids": result["base_memory_ids"],
                    },
                )
            timeline.record_timeline_event(
                conn,
                event_type="conflict.explained",
                memory_id=base_ids[0],
                related_memory_id=base_ids[1],
                operation_id=operation_id,
                origin="conflict_explainer_auto",
                timeline_scope="memory",
                semantic_kind="decision",
                title=f"Conflict explained: {result['conflict_kind']} (confidence {result['confidence']})",
                payload={
                    "conflict_kind": result["conflict_kind"],
                    "confidence": result["confidence"],
                    "suggested_relation": result["suggested_relation"],
                    "suggested_action": result["suggested_action"],
                    "needs_human_review": result["needs_human_review"],
                    "base_memory_ids": result["base_memory_ids"],
                },
            )
            conn.commit()
        except Exception:
            pass
        return result
    finally:
        conn.close()


@mcp.tool
def preview_conflict_resolution(memory_a_id: int, memory_b_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        if not _is_conflict_feature_active(conn, CONFLICT_PREVIEW_RESOLUTION_FLAG_KEY):
            return {"status": "disabled", "reason": "feature_flag_off", "flag_key": CONFLICT_PREVIEW_RESOLUTION_FLAG_KEY}
        return conflict_explainer.preview_resolution(conn, int(memory_a_id), int(memory_b_id))
    finally:
        conn.close()


@mcp.tool
def apply_conflict_resolution(memory_a_id: int, memory_b_id: int, notes: str | None = None) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        if not _is_conflict_feature_active(conn, CONFLICT_AUTO_RESOLUTION_FLAG_KEY):
            return {
                "status": "skipped",
                "skip_reason": "feature_flag_off",
                "flag_key": CONFLICT_AUTO_RESOLUTION_FLAG_KEY,
                "memory_a_id": int(memory_a_id),
                "memory_b_id": int(memory_b_id),
                "conflict_kind": None,
                "applied_changes": [],
                "run_id": None,
            }
        run_id = create_sleep_run(conn, mode="conflict_resolution_run", freedom_level=0, notes=notes)
        result = conflict_explainer.apply_resolution(conn, int(memory_a_id), int(memory_b_id))

        if result["status"] == "skipped":
            finalize_sleep_run(conn, run_id, status="skipped", scanned_count=2, changed_count=0, archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=1, created_summary_count=0)
            return {**result, "run_id": run_id}

        for change in result["applied_changes"]:
            if change["action"] == "create_link":
                add_sleep_action(
                    conn, run_id, "conflict_link_created",
                    change["from_memory_id"],
                    None,
                    {"link_id": change["link_id"], "from_memory_id": change["from_memory_id"], "to_memory_id": change["to_memory_id"], "relation_type": change["relation_type"]},
                    f"conflict_resolution_{result['conflict_kind']}",
                )
            elif change["action"] == "set_valid_to":
                add_sleep_action(
                    conn, run_id, "valid_to_set",
                    change["memory_id"],
                    {"valid_to": change["old_valid_to"]},
                    {"valid_to": change["new_valid_to"]},
                    f"conflict_resolution_{result['conflict_kind']}",
                )

        try:
            base_ids = sorted([int(memory_a_id), int(memory_b_id)])
            operation_id = timeline.new_operation_id("conflict")
            timeline.record_timeline_event(
                conn,
                event_type="conflict.resolution_applied",
                memory_id=base_ids[0],
                related_memory_id=base_ids[1],
                operation_id=operation_id,
                origin="conflict_explainer_auto",
                timeline_scope="memory",
                semantic_kind="decision",
                title=f"Conflict resolution applied: {result['conflict_kind']} (confidence {result['confidence']})",
                payload={
                    "conflict_kind": result["conflict_kind"],
                    "confidence": result["confidence"],
                    "applied_changes_count": len(result["applied_changes"]),
                    "run_id": run_id,
                    "base_memory_ids": base_ids,
                },
            )
        except Exception:
            pass

        conn.commit()
        finalize_sleep_run(conn, run_id, status="completed", scanned_count=2, changed_count=len(result["applied_changes"]), archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=1, created_summary_count=0)
        return {**result, "run_id": run_id}
    finally:
        conn.close()


_CONFLICT_LINK_TYPES = {"contradicts", "supersedes", "relates_to"}


@mcp.tool
def get_conflict_history(memory_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        memory_row = require_memory_row(conn, memory_id)
        memory = enrich_memory_dict(row_to_dict(memory_row))

        # 1. Conflict timeline events
        all_events = timeline.timeline_query(conn, limit=500, memory_id=memory_id, row_to_dict=row_to_dict)
        conflict_events = [e for e in all_events if str(e.get("event_type", "")).startswith("conflict.")]

        # 2. Conflict-related links
        link_rows = conn.execute(
            "SELECT * FROM memory_links WHERE from_memory_id = ? OR to_memory_id = ?",
            (memory_id, memory_id),
        ).fetchall()
        conflict_links: list[dict[str, Any]] = []
        for row in link_rows:
            link = row_to_dict(row)
            if link.get("relation_type") not in _CONFLICT_LINK_TYPES:
                continue
            direction = "outgoing" if int(link["from_memory_id"]) == memory_id else "incoming"
            other_id = int(link["to_memory_id"]) if direction == "outgoing" else int(link["from_memory_id"])
            conflict_links.append({
                "link_id": int(link["id"]),
                "relation_type": link["relation_type"],
                "direction": direction,
                "other_memory_id": other_id,
                "weight": link.get("weight"),
                "created_at": link.get("created_at"),
            })

        # 3. Resolution runs — wyciągam run_id z timeline events conflict.resolution_applied
        resolution_run_ids: list[int] = []
        for event in conflict_events:
            if event.get("event_type") == "conflict.resolution_applied":
                payload = event.get("payload") or {}
                rid = payload.get("run_id")
                if rid is not None and int(rid) not in resolution_run_ids:
                    resolution_run_ids.append(int(rid))

        # valid_to_set — bezpośredni match przez memory_id
        vt_rows = conn.execute(
            """
            SELECT sra.*, sr.started_at AS run_started_at
            FROM sleep_run_actions sra
            JOIN sleep_runs sr ON sr.id = sra.run_id
            WHERE sr.mode = 'conflict_resolution_run'
              AND sra.action_type = 'valid_to_set'
              AND sra.memory_id = ?
            ORDER BY sra.id ASC
            """,
            (memory_id,),
        ).fetchall()

        valid_to_history: list[dict[str, Any]] = []
        for row in vt_rows:
            item = row_to_dict(row)
            run_id_item = int(item["run_id"])
            if run_id_item not in resolution_run_ids:
                resolution_run_ids.append(run_id_item)
            old_val = _decode_action_value(item.get("old_value"))
            new_val = _decode_action_value(item.get("new_value"))
            valid_to_history.append({
                "run_id": run_id_item,
                "memory_id": int(item["memory_id"]),
                "previous_valid_to": old_val.get("valid_to") if isinstance(old_val, dict) else old_val,
                "new_valid_to": new_val.get("valid_to") if isinstance(new_val, dict) else new_val,
                "run_started_at": item.get("run_started_at"),
            })

        resolution_runs: list[dict[str, Any]] = []
        for rid in resolution_run_ids:
            run_row = conn.execute("SELECT * FROM sleep_runs WHERE id = ?", (rid,)).fetchone()
            if run_row:
                run = row_to_dict(run_row)
                rollback_row = conn.execute(
                    "SELECT id FROM sleep_runs WHERE rollback_of_run_id = ? AND status = 'completed' LIMIT 1",
                    (rid,),
                ).fetchone()
                resolution_runs.append({
                    "run_id": int(run["id"]),
                    "status": run.get("status"),
                    "rolled_back": rollback_row is not None,
                    "started_at": run.get("started_at"),
                })

        rolled_back_run_ids = {r["run_id"] for r in resolution_runs if r["rolled_back"]}
        for vt in valid_to_history:
            vt["rolled_back"] = vt["run_id"] in rolled_back_run_ids

    finally:
        conn.close()

    return {
        "memory_id": memory_id,
        "memory_summary": {
            "summary_short": memory.get("summary_short"),
            "contradiction_flag": memory.get("contradiction_flag"),
            "valid_from": memory.get("valid_from"),
            "valid_to": memory.get("valid_to"),
            "activity_state": memory.get("activity_state"),
        },
        "conflict_event_count": len(conflict_events),
        "conflict_events": conflict_events,
        "conflict_link_count": len(conflict_links),
        "conflict_links": conflict_links,
        "resolution_run_count": len(resolution_runs),
        "resolution_runs": resolution_runs,
        "valid_to_history": valid_to_history,
    }


@mcp.tool
def get_conflict_reasoning(memory_a_id: int, memory_b_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        result = conflict_explainer.explain_conflict_pair(conn, int(memory_a_id), int(memory_b_id))
    finally:
        conn.close()

    debug = result.get("debug", {})
    signal_scores: dict[str, float] = debug.get("signal_scores", {})
    signals_fired: list[str] = debug.get("signals", [])

    # Sortuj sygnały malejąco po score
    ranked_signals = sorted(signal_scores.items(), key=lambda kv: kv[1], reverse=True)

    # Limity bundle
    context_memory_count = int(debug.get("context_memory_count", 0))
    related_limit = 5  # wartość domyślna w explain_conflict_pair
    bundle_limit_hit = context_memory_count >= related_limit

    return {
        "memory_a_id": int(memory_a_id),
        "memory_b_id": int(memory_b_id),
        "classification": {
            "conflict_kind": result["conflict_kind"],
            "confidence": result["confidence"],
            "conflict_reason": result["conflict_reason"],
            "needs_human_review": result["needs_human_review"],
            "suggested_relation": result["suggested_relation"],
            "suggested_action": result["suggested_action"],
        },
        "signals": {
            "fired": signals_fired,
            "ranked": [{"kind": kind, "score": score} for kind, score in ranked_signals],
            "winner": ranked_signals[0][0] if ranked_signals else None,
            "runner_up": ranked_signals[1][0] if len(ranked_signals) > 1 else None,
        },
        "context": {
            "bundle_summary_shared": debug.get("bundle_summary_shared"),
            "bundle_type_shared": debug.get("bundle_type_shared"),
            "context_memory_count": context_memory_count,
            "context_memory_ids": result["context_memory_ids"],
            "timeline_event_count": debug.get("timeline_event_count", 0),
            "supporting_link_ids": result["supporting_link_ids"],
        },
        "bundle_limits": {
            "related_limit": related_limit,
            "limit_hit": bundle_limit_hit,
            "omitted_note": (
                "Liczba pamięci kontekstowych osiągnęła limit — część powiązanych rekordów mogła zostać pominięta."
                if bundle_limit_hit else None
            ),
        },
        "explanation": result["explanation"],
    }


@mcp.tool
def get_source_quality(memory_a_id: int, memory_b_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        row_a = require_memory_row(conn, int(memory_a_id))
        row_b = require_memory_row(conn, int(memory_b_id))
        mem_a = enrich_memory_dict(row_to_dict(row_a))
        mem_b = enrich_memory_dict(row_to_dict(row_b))

        supports_a = conn.execute(
            "SELECT COUNT(*) FROM memory_links WHERE to_memory_id = ? AND relation_type = 'supports'",
            (int(memory_a_id),),
        ).fetchone()[0]
        supports_b = conn.execute(
            "SELECT COUNT(*) FROM memory_links WHERE to_memory_id = ? AND relation_type = 'supports'",
            (int(memory_b_id),),
        ).fetchone()[0]
    finally:
        conn.close()

    breakdown_a = conflict_explainer.source_quality_breakdown(mem_a, supports_count=int(supports_a))
    breakdown_b = conflict_explainer.source_quality_breakdown(mem_b, supports_count=int(supports_b))
    gap = abs(breakdown_a["total_score"] - breakdown_b["total_score"])
    higher_quality_id = (
        int(memory_a_id) if breakdown_a["total_score"] >= breakdown_b["total_score"] else int(memory_b_id)
    )

    return {
        "memory_a_id": int(memory_a_id),
        "memory_b_id": int(memory_b_id),
        "quality_a": breakdown_a,
        "quality_b": breakdown_b,
        "quality_gap": round(gap, 3),
        "higher_quality_memory_id": higher_quality_id,
        "gap_interpretation": (
            "significant" if gap >= 0.35
            else "moderate" if gap >= 0.20
            else "minimal"
        ),
    }


@mcp.tool
def get_conflict_quality_metrics(since: str | None = None, until: str | None = None) -> dict[str, Any]:
    """Returns quality metrics for the conflict explainer subsystem.

    Covers: explained conflicts, review requests, resolutions applied, conflict kinds breakdown,
    review rate, and resolution rate. Optionally filtered by time window (ISO timestamps).
    """
    conn = get_db_connection()
    try:
        params_base: list[Any] = []
        time_filter = ""
        if since:
            time_filter += " AND created_at >= ?"
            params_base.append(since)
        if until:
            time_filter += " AND created_at <= ?"
            params_base.append(until)

        def _count(event_type: str) -> int:
            row = conn.execute(
                f"SELECT COUNT(*) FROM timeline_events WHERE event_type = ?{time_filter}",
                [event_type] + params_base,
            ).fetchone()
            return int(row[0]) if row else 0

        explained = _count("conflict.explained")
        review_requested = _count("conflict.review_requested")
        resolution_applied = _count("conflict.resolution_applied")

        # Count per conflict kind from conflict.explained events
        kind_rows = conn.execute(
            f"SELECT json_extract(payload_json, '$.conflict_kind') AS kind, COUNT(*) AS cnt "
            f"FROM timeline_events WHERE event_type = 'conflict.explained'{time_filter} "
            f"GROUP BY kind ORDER BY cnt DESC",
            params_base,
        ).fetchall()
        by_kind = {str(r[0]): int(r[1]) for r in kind_rows if r[0]}

        # Count needs_human_review=true from conflict.explained
        human_review_count = conn.execute(
            f"SELECT COUNT(*) FROM timeline_events "
            f"WHERE event_type = 'conflict.explained' "
            f"AND json_extract(payload_json, '$.needs_human_review') = 1{time_filter}",
            params_base,
        ).fetchone()
        human_review_total = int(human_review_count[0]) if human_review_count else 0

        review_rate = round(review_requested / explained, 3) if explained > 0 else None
        resolution_rate = round(resolution_applied / explained, 3) if explained > 0 else None

        # Feature flags status
        flag_keys = [CONFLICT_EXPLAINER_FLAG_KEY, CONFLICT_PREVIEW_RESOLUTION_FLAG_KEY, CONFLICT_AUTO_RESOLUTION_FLAG_KEY]
        flags_status = {}
        for fk in flag_keys:
            flags_status[fk] = _is_conflict_feature_active(conn, fk)

    finally:
        conn.close()

    return {
        "period": {"since": since, "until": until},
        "explained_count": explained,
        "review_requested_count": review_requested,
        "resolution_applied_count": resolution_applied,
        "needs_human_review_count": human_review_total,
        "by_conflict_kind": by_kind,
        "review_rate": review_rate,
        "resolution_rate": resolution_rate,
        "feature_flags": flags_status,
    }


@mcp.tool
def get_conflict_system_status() -> dict[str, Any]:
    """Returns operational status of the conflict explainer subsystem.

    Use for health checks, operator dashboards, and pre-flight verification.
    Returns feature flag states, DB table counts, and a human-readable readiness verdict.
    """
    conn = get_db_connection()
    try:
        flag_keys = [CONFLICT_EXPLAINER_FLAG_KEY, CONFLICT_PREVIEW_RESOLUTION_FLAG_KEY, CONFLICT_AUTO_RESOLUTION_FLAG_KEY]
        flags: dict[str, bool] = {fk: _is_conflict_feature_active(conn, fk) for fk in flag_keys}

        conflicted_count = conn.execute(
            "SELECT COUNT(*) FROM memories WHERE contradiction_flag = 1 AND activity_state = 'active'"
        ).fetchone()[0]
        conflict_links_count = conn.execute(
            "SELECT COUNT(*) FROM memory_links WHERE relation_type IN ('contradicts', 'supersedes')"
        ).fetchone()[0]
        open_reviews = conn.execute(
            "SELECT COUNT(DISTINCT json_extract(payload_json, '$.base_memory_ids[0]')) "
            "FROM timeline_events WHERE event_type = 'conflict.review_requested'"
        ).fetchone()[0]
        last_explained = conn.execute(
            "SELECT created_at FROM timeline_events WHERE event_type = 'conflict.explained' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        last_resolved = conn.execute(
            "SELECT created_at FROM timeline_events WHERE event_type = 'conflict.resolution_applied' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        resolution_runs = conn.execute(
            "SELECT COUNT(*) FROM sleep_runs WHERE mode = 'conflict_resolution_run' AND status = 'completed'"
        ).fetchone()[0]
    finally:
        conn.close()

    all_flags_active = all(flags.values())
    explainer_active = flags[CONFLICT_EXPLAINER_FLAG_KEY]

    if not explainer_active:
        readiness = "disabled"
    elif all_flags_active:
        readiness = "fully_operational"
    else:
        readiness = "partially_enabled"

    return {
        "readiness": readiness,
        "feature_flags": flags,
        "db_stats": {
            "active_conflicted_memories": int(conflicted_count),
            "conflict_links_count": int(conflict_links_count),
            "open_reviews_estimate": int(open_reviews),
            "completed_resolution_runs": int(resolution_runs),
        },
        "last_activity": {
            "last_explained_at": last_explained[0] if last_explained else None,
            "last_resolved_at": last_resolved[0] if last_resolved else None,
        },
    }


@mcp.tool
def get_conflict_clusters(include_members: bool = True) -> dict[str, Any]:
    """Returns conflict clusters — connected components in the conflict graph.

    Each cluster groups memories linked by 'contradicts' or 'supersedes' relations.
    Identifies the central memory (highest degree) and the divergence source
    (memory causing the most direct contradictions).

    Set include_members=False to get a compact summary without full member lists.
    """
    conn = get_db_connection()
    try:
        clusters = conflict_logic.build_conflict_clusters(conn)
    finally:
        conn.close()

    if not include_members:
        clusters = [
            {k: v for k, v in c.items() if k != "member_ids"}
            for c in clusters
        ]

    unresolved_count = sum(1 for c in clusters if c.get("has_unresolved"))
    large_clusters = [c for c in clusters if c["size"] >= 3]

    return {
        "cluster_count": len(clusters),
        "total_clustered_memories": sum(c["size"] for c in clusters),
        "unresolved_cluster_count": unresolved_count,
        "large_cluster_count": len(large_clusters),
        "clusters": clusters,
    }


@mcp.tool
def get_conflict_report(memory_a_id: int, memory_b_id: int) -> dict[str, Any]:
    """Returns a comprehensive operator report for a conflict pair.

    Combines: classification + explanation, preview of proposed resolution,
    conflict history for both memories, and a decision summary.
    Designed as a single-call operator view — no need to call multiple tools separately.
    """
    conn = get_db_connection()
    try:
        # Explanation (classify + explain)
        explanation = conflict_explainer.explain_conflict_pair(conn, int(memory_a_id), int(memory_b_id))

        # Preview resolution
        preview = conflict_explainer.preview_resolution(conn, int(memory_a_id), int(memory_b_id))

        # History for both memories
        def _history_summary(mid: int) -> dict[str, Any]:
            all_events = timeline.timeline_query(conn, limit=200, memory_id=mid, row_to_dict=row_to_dict)
            conflict_events = [e for e in all_events if str(e.get("event_type", "")).startswith("conflict.")]
            link_rows = conn.execute(
                "SELECT relation_type, COUNT(*) AS cnt FROM memory_links "
                "WHERE (from_memory_id = ? OR to_memory_id = ?) "
                "AND relation_type IN ('contradicts', 'supersedes', 'relates_to') "
                "GROUP BY relation_type",
                (mid, mid),
            ).fetchall()
            return {
                "memory_id": mid,
                "conflict_event_count": len(conflict_events),
                "recent_events": [
                    {"event_type": e.get("event_type"), "created_at": e.get("created_at")}
                    for e in conflict_events[:5]
                ],
                "link_summary": {str(r[0]): int(r[1]) for r in link_rows},
            }

        history_a = _history_summary(int(memory_a_id))
        history_b = _history_summary(int(memory_b_id))

    finally:
        conn.close()

    # Decision summary
    auto_applicable = bool(preview.get("can_auto_apply"))
    needs_review = bool(explanation.get("needs_human_review"))
    if auto_applicable:
        recommended_action = "apply_conflict_resolution"
    elif needs_review:
        recommended_action = "manual_review_required"
    else:
        recommended_action = "no_action"

    return {
        "memory_a_id": int(memory_a_id),
        "memory_b_id": int(memory_b_id),
        "conflict_kind": explanation["conflict_kind"],
        "confidence": explanation["confidence"],
        "explanation": explanation["explanation"],
        "needs_human_review": needs_review,
        "suggested_relation": explanation["suggested_relation"],
        "suggested_action": explanation["suggested_action"],
        "resolution_preview": {
            "can_auto_apply": auto_applicable,
            "skip_reason": preview.get("skip_reason"),
            "proposed_changes": preview.get("proposed_changes", []),
            "proposed_changes_count": len(preview.get("proposed_changes", [])),
        },
        "history": {
            "memory_a": history_a,
            "memory_b": history_b,
        },
        "decision_summary": {
            "recommended_action": recommended_action,
            "auto_applicable": auto_applicable,
            "needs_review": needs_review,
            "conflict_kind": explanation["conflict_kind"],
        },
    }


@mcp.tool
def record_conflict_decision(
    memory_a_id: int,
    memory_b_id: int,
    decision: str,
    notes: str | None = None,
) -> dict[str, Any]:
    """Records an operator's manual decision for a conflict pair.

    decision must be one of: 'approved', 'rejected', 'deferred', 'false_positive'.

    Records a timeline event 'conflict.decision_recorded' and optionally triggers
    auto-resolution if decision='approved' and the pair supports it.
    """
    valid_decisions = {"approved", "rejected", "deferred", "false_positive"}
    normalized = str(decision).strip().lower()
    if normalized not in valid_decisions:
        raise ValueError(f"decision musi być jednym z: {', '.join(sorted(valid_decisions))}")

    conn = get_db_connection()
    try:
        base_ids = sorted([int(memory_a_id), int(memory_b_id)])
        operation_id = timeline.new_operation_id("conflict")

        timeline.record_timeline_event(
            conn,
            event_type="conflict.decision_recorded",
            memory_id=base_ids[0],
            related_memory_id=base_ids[1],
            operation_id=operation_id,
            origin="operator",
            timeline_scope="memory",
            semantic_kind="decision",
            title=f"Conflict decision: {normalized}",
            payload={
                "decision": normalized,
                "notes": notes,
                "base_memory_ids": base_ids,
            },
        )

        apply_result: dict[str, Any] | None = None
        if normalized == "approved":
            preview = conflict_explainer.preview_resolution(conn, int(memory_a_id), int(memory_b_id))
            if preview.get("can_auto_apply"):
                conn.commit()
                conn.close()
                apply_result = apply_conflict_resolution(int(memory_a_id), int(memory_b_id), notes=notes)
                return {
                    "status": "approved_and_applied",
                    "decision": normalized,
                    "memory_a_id": int(memory_a_id),
                    "memory_b_id": int(memory_b_id),
                    "apply_result": apply_result,
                    "operation_id": operation_id,
                }

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {
        "status": "recorded",
        "decision": normalized,
        "memory_a_id": int(memory_a_id),
        "memory_b_id": int(memory_b_id),
        "notes": notes,
        "operation_id": operation_id,
        "apply_result": apply_result,
    }


@mcp.tool
def preview_conflicts_v1(notes: str | None = None) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        run_id = create_sleep_run(conn, mode="conflict_preview", freedom_level=0, notes=notes)
        conflict_candidates = conflict_logic.get_conflict_candidates(conn)
        flagged_ids: set[int] = set()
        links_to_create_count = 0
        for pair in conflict_candidates:
            flagged_ids.add(int(pair["memory_a_id"]))
            flagged_ids.add(int(pair["memory_b_id"]))
            if not bool(pair["contradiction_link_exists"]):
                links_to_create_count += 1
            add_sleep_action(conn, run_id, "conflict_candidate", int(pair["memory_a_id"]), {"memory_a_id": pair["memory_a_id"], "memory_b_id": pair["memory_b_id"], "contradiction_link_exists": pair["contradiction_link_exists"]}, {"relation_type": "contradicts", "memory_a_id": pair["memory_a_id"], "memory_b_id": pair["memory_b_id"]}, "same_summary_conflicting_signal")
        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        finalize_sleep_run(conn, run_id, status="preview_completed", scanned_count=int(scanned_count), changed_count=0, archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=len(conflict_candidates), created_summary_count=0)
        return {"status": "preview_completed", "run_id": run_id, "scanned_count": int(scanned_count), "conflict_candidates": conflict_candidates, "summary": {"conflict_count": len(conflict_candidates), "flagged_memory_count": len(flagged_ids), "links_to_create_count": links_to_create_count}}
    finally:
        conn.close()


@mcp.tool
def run_conflicts_v1(notes: str | None = None) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        run_id = create_sleep_run(conn, mode="conflict_run", freedom_level=0, notes=notes)
        conflict_candidates = conflict_logic.get_conflict_candidates(conn)
        links_created: list[dict[str, Any]] = []
        flagged_changes: list[dict[str, Any]] = []
        already_flagged: set[int] = set()
        for pair in conflict_candidates:
            memory_a_id = int(pair["memory_a_id"])
            memory_b_id = int(pair["memory_b_id"])
            source_id = min(memory_a_id, memory_b_id)
            target_id = max(memory_a_id, memory_b_id)
            if not conflict_logic.contradiction_link_exists(conn, source_id, target_id):
                item = _create_link(conn, source_id, target_id, "contradicts", 0.9, "conflicts_v1_auto")
                links_created.append(item)
                add_sleep_action(conn, run_id, "conflict_link_created", source_id, None, item, "same_summary_conflicting_signal")
            for memory_id in (memory_a_id, memory_b_id):
                if memory_id in already_flagged:
                    continue
                memory = require_memory_row(conn, memory_id)
                old_flag = int(memory["contradiction_flag"] or 0)
                if old_flag != 1:
                    conn.execute("UPDATE memories SET contradiction_flag = 1 WHERE id = ?", (memory_id,))
                    flagged_changes.append({"memory_id": memory_id, "old_contradiction_flag": old_flag, "new_contradiction_flag": 1})
                    add_sleep_action(conn, run_id, "conflict_flagged", memory_id, {"contradiction_flag": old_flag}, {"contradiction_flag": 1}, "same_summary_conflicting_signal")
                already_flagged.add(memory_id)
        conn.commit()
        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        finalize_sleep_run(conn, run_id, status="completed", scanned_count=int(scanned_count), changed_count=len(links_created) + len(flagged_changes), archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=len(conflict_candidates), created_summary_count=0)
        return {"status": "completed", "run_id": run_id, "scanned_count": int(scanned_count), "conflict_candidates": conflict_candidates, "links_created": links_created, "flagged_changes": flagged_changes, "summary": {"conflict_count": len(conflict_candidates), "links_created_count": len(links_created), "flagged_memory_count": len(flagged_changes), "changed_count": len(links_created) + len(flagged_changes)}}
    finally:
        conn.close()


# --- Sandman dream-linking helpers -------------------------------------------------
_DREAM_STOPWORDS = {
    "oraz", "jest", "jako", "jego", "jej", "dla", "przez", "ktore", "które",
    "taki", "taka", "takie", "tego", "tym", "ten", "czy", "nie", "sie", "się",
    "the", "and", "with", "from", "that", "this", "into", "memory", "wspomnienie",
}


_DREAM_BROAD_LINK_TERMS = {
    "morenatech", "mpbm", "mapi", "jagoda", "jagoda-memory-api", "project",
    "project-context", "memory", "memories", "wspomnienie", "wspomnienia",
    "user", "użytkownik", "assistant", "current", "conversation", "context",
    "asystenta", "firmowego", "firma", "firmy", "work", "not", "for",
}


def _sandman_existing_link_keys(conn) -> set[tuple[int, int, str]]:
    rows = conn.execute("SELECT from_memory_id, to_memory_id, relation_type FROM memory_links").fetchall()
    return {(int(row["from_memory_id"]), int(row["to_memory_id"]), str(row["relation_type"])) for row in rows}


def _sandman_tokenize(value: object) -> set[str]:
    import re

    text = str(value or "").lower()
    words = re.findall(r"[a-ząćęłńóśźż0-9_\-]{3,}", text, flags=re.IGNORECASE)
    return {word.strip("-_") for word in words if word.strip("-_") and word not in _DREAM_STOPWORDS}


def _sandman_tags(value: object) -> set[str]:
    return {item.strip().lower() for item in str(value or "").split(",") if item.strip()}


def _sandman_inferred_terms(memory: dict[str, object]) -> set[str]:
    raw_text = " ".join(
        str(memory.get(key) or "")
        for key in ("content", "summary_short", "tags", "memory_type")
    ).lower()
    tokens = _sandman_tokenize(raw_text) | _sandman_tags(memory.get("tags"))
    inferred: set[str] = set()

    if tokens & {"blog", "blogposts", "routemeta", "metatitle", "metadescription", "react", "frontend", "build-success", "technical-section"}:
        inferred.update({"website", "websites", "site", "frontend", "content", "build", "react", "implementation"})
    if tokens & {"website", "websites", "strona", "strony", "stronach", "internetowych", "domain", "domena"}:
        inferred.update({"website", "websites", "site", "web", "frontend"})
    if tokens & {"facebook", "bio", "copywriting", "marketing", "pozycjonowanie", "positioning"}:
        inferred.update({"content", "marketing", "copywriting", "positioning"})
    if tokens & {"docs", "document", "documentation", "dokument", "dokumentacja"}:
        inferred.update({"documents", "documentation", "content"})
    if tokens & {"build", "build-success", "npm", "test", "validation", "py_compile"}:
        inferred.update({"validates", "build", "test"})

    return inferred - _DREAM_BROAD_LINK_TERMS


def _sandman_scope_clause(workspace_id: int | None, project_key: str | None) -> tuple[str, list[object]]:
    clauses: list[str] = ["activity_state = 'active'"]
    params: list[object] = []
    if workspace_id is not None:
        clauses.append("workspace_id = ?")
        params.append(int(workspace_id))
    if project_key:
        clauses.append("project_key = ?")
        params.append(project_key)
    return " AND ".join(clauses), params


def _sandman_extract_mention_candidates(conn, memories: list[dict[str, object]], existing: set[tuple[int, int, str]], max_links: int) -> list[dict[str, object]]:
    import re

    existing_ids = {int(memory["id"]) for memory in memories}
    candidates: list[dict[str, object]] = []
    seen: set[tuple[int, int, str]] = set()
    for memory in memories:
        source_id = int(memory["id"])
        text = f"{memory.get('content') or ''} {memory.get('summary_short') or ''}"
        for raw_id in re.findall(r"\[(\d+)\]", text):
            target_id = int(raw_id)
            if target_id == source_id or target_id not in existing_ids:
                continue
            key = (source_id, target_id, "mentions")
            if key in existing or key in seen:
                continue
            seen.add(key)
            candidates.append({
                "from_memory_id": source_id,
                "to_memory_id": target_id,
                "relation_type": "mentions",
                "weight": 0.88,
                "reason": "memory_text_contains_bracket_id_reference",
            })
            if len(candidates) >= max_links:
                return candidates
    return candidates


def _sandman_prepare_memories(memories: list[dict[str, object]]) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    for memory in memories:
        content_tokens = _sandman_tokenize(memory.get("content"))
        summary_tokens = _sandman_tokenize(memory.get("summary_short"))
        raw_tags = _sandman_tags(memory.get("tags"))
        inferred_terms = _sandman_inferred_terms(memory)
        semantic_tags = raw_tags | inferred_terms
        prepared.append({
            **memory,
            "_tokens": content_tokens | summary_tokens | semantic_tags,
            "_tags": semantic_tags,
            "_raw_tags": raw_tags,
            "_inferred_terms": inferred_terms,
        })
    return prepared

def _sandman_relation_type(left: dict[str, object], right: dict[str, object], score: float, common_tags: set[str], common_tokens: set[str]) -> str:
    common_words = set(common_tags) | set(common_tokens)
    common_text = " ".join(sorted(common_words))
    left_tags = _sandman_tags(left.get("tags"))
    right_tags = _sandman_tags(right.get("tags"))
    both_tags = left_tags & right_tags

    if any(word in common_text for word in ("credential", "credentials", "auth-risk", "security", "rotate-key", "oauth", "bearer-token", "basic-auth")):
        return "risk_for"
    if any(word in common_text for word in ("metric", "metrics", "coverage", "graph")) or "metrics" in both_tags:
        return "metric_for"
    if any(word in common_text for word in ("react", "routemeta", "blogposts", "frontend", "build", "implementation")):
        return "implements"
    if any(word in common_text for word in ("docs", "document", "documentation", "documents", "dokument", "copywriting", "bio", "content")):
        return "documents"
    if any(word in common_text for word in ("error", "problem", "troubleshooting", "bug", "fix", "napraw", "lifespan")):
        return "fixes"
    if any(word in common_text for word in ("installation", "installer", "setup", "systemd", "caddy", "ssh", "vps", "linux", "ubuntu", "config", "uvicorn")):
        return "configures"
    if any(word in common_text for word in ("validation", "validate", "test", "success", "healthcheck", "health", "py_compile")):
        return "validates"
    if bool(left.get("project_key") and left.get("project_key") == right.get("project_key")) and score < 0.62:
        return "same_project"
    return "related_to"

def _sandman_optics_verdict(
    relation_type: str,
    score: float,
    common_tags: set[str],
    common_tokens: set[str],
    same_project: bool,
    same_type: bool,
) -> dict[str, object] | None:
    strong_tags = set(common_tags) - _DREAM_BROAD_LINK_TERMS
    strong_tokens = set(common_tokens) - _DREAM_BROAD_LINK_TERMS
    strong_signal = (2 * len(strong_tags)) + len(strong_tokens)

    relation_thresholds = {
        "risk_for": 0.58,
        "metric_for": 0.62,
        "documents": 0.58,
        "implements": 0.58,
        "fixes": 0.58,
        "configures": 0.58,
        "validates": 0.58,
        "same_project": 0.62,
        "related_to": 0.58,
    }
    min_required = relation_thresholds.get(relation_type, 0.58)

    if score < min_required:
        return None

    if relation_type == "same_project" and (len(strong_tags) < 1 and len(strong_tokens) < 3):
        return None
    if relation_type in {"related_to", "documents", "implements", "fixes", "configures", "validates"} and strong_signal < 3:
        return None
    if relation_type == "risk_for" and not (set(common_tags) | set(common_tokens)) & {"credential", "credentials", "secret", "security", "rotate-secret", "oauth", "bearer-token", "basic-auth"}:
        return None
    if relation_type == "metric_for" and not (set(common_tags) | set(common_tokens)) & {"metric", "metrics", "coverage", "graph"}:
        return None

    if score >= 0.84 and strong_signal >= 7:
        quality = "trusted"
    elif score >= 0.68 and strong_signal >= 4:
        quality = "probable"
    else:
        quality = "weak"

    # The optician does not allow weak links into the dream graph yet. They can
    # return later as explicit review candidates when a link-review queue exists.
    if quality == "weak":
        return None

    return {
        "quality_class": quality,
        "optics_score": round(score + min(0.08, strong_signal / 100), 3),
        "strong_shared_tag_count": len(strong_tags),
        "strong_shared_term_count": len(strong_tokens),
    }

def _sandman_build_similarity_candidate(
    left: dict[str, object],
    right: dict[str, object],
    existing: set[tuple[int, int, str]],
    seen: set[tuple[int, int, str]],
    *,
    min_score: float = 0.42,
    reason_prefix: str | None = None,
) -> dict[str, object] | None:
    left_id = int(left["id"])
    right_id = int(right["id"])
    if left_id == right_id:
        return None
    common_tags = set(left.get("_tags", set())) & set(right.get("_tags", set()))
    common_tokens = set(left.get("_tokens", set())) & set(right.get("_tokens", set()))
    same_project = bool(left.get("project_key") and left.get("project_key") == right.get("project_key"))
    same_type = bool(left.get("memory_type") and left.get("memory_type") == right.get("memory_type"))
    score = 0.0
    reasons: list[str] = []
    if common_tags:
        score += min(0.35, 0.12 * len(common_tags))
        reasons.append("shared_tags:" + ",".join(sorted(common_tags)[:5]))
    if same_project:
        score += 0.20
        reasons.append("same_project")
    if same_type:
        score += 0.10
        reasons.append("same_memory_type")
    if common_tokens:
        score += min(0.35, 0.04 * len(common_tokens))
        reasons.append("shared_terms:" + ",".join(sorted(common_tokens)[:6]))
    if score < min_score:
        return None
    source_id, target_id = (left_id, right_id) if left_id < right_id else (right_id, left_id)
    relation_type = _sandman_relation_type(left, right, score, common_tags, common_tokens)
    optics = _sandman_optics_verdict(relation_type, score, common_tags, common_tokens, same_project, same_type)
    if optics is None:
        return None
    key = (source_id, target_id, relation_type)
    reverse_key = (target_id, source_id, relation_type)
    if key in existing or reverse_key in existing or key in seen or reverse_key in seen:
        return None
    seen.add(key)
    reason_text = ";".join(reasons)
    if reason_prefix:
        reason_text = f"{reason_prefix};{reason_text}"
    return {
        "from_memory_id": source_id,
        "to_memory_id": target_id,
        "relation_type": relation_type,
        "weight": round(min(0.92, score), 3),
        "reason": reason_text,
        "shared_tag_count": len(common_tags),
        "shared_term_count": len(common_tokens),
        **optics,
    }

def _sandman_linked_ids_in_scope(conn, memory_ids: set[int]) -> set[int]:
    if not memory_ids:
        return set()
    rows = conn.execute("SELECT from_memory_id, to_memory_id FROM memory_links WHERE archived_at IS NULL").fetchall()
    linked: set[int] = set()
    for row in rows:
        left_id = int(row["from_memory_id"])
        right_id = int(row["to_memory_id"])
        if left_id in memory_ids:
            linked.add(left_id)
        if right_id in memory_ids:
            linked.add(right_id)
    return linked


def _sandman_extract_orphan_rescue_candidates(conn, prepared: list[dict[str, object]], existing: set[tuple[int, int, str]], max_links: int) -> list[dict[str, object]]:
    memory_ids = {int(memory["id"]) for memory in prepared}
    linked_ids = _sandman_linked_ids_in_scope(conn, memory_ids)
    orphan_memories = [memory for memory in prepared if int(memory["id"]) not in linked_ids]
    candidates: list[dict[str, object]] = []
    seen: set[tuple[int, int, str]] = set()
    for orphan in orphan_memories:
        possible: list[dict[str, object]] = []
        for other in prepared:
            if int(other["id"]) == int(orphan["id"]):
                continue
            candidate = _sandman_build_similarity_candidate(
                orphan,
                other,
                existing,
                seen,
                min_score=0.34,
                reason_prefix="orphan_rescue",
            )
            if candidate is not None:
                possible.append(candidate)
        possible.sort(key=lambda item: (float(item["weight"]), int(item.get("shared_tag_count", 0)), int(item.get("shared_term_count", 0))), reverse=True)
        for item in possible[:3]:
            candidates.append(item)
            if len(candidates) >= max_links:
                return candidates
    return candidates


def _sandman_extract_random_walk_candidates(prepared: list[dict[str, object]], existing: set[tuple[int, int, str]], max_links: int) -> list[dict[str, object]]:
    import random

    pairs: list[tuple[int, int]] = []
    for index in range(len(prepared)):
        for other_index in range(index + 1, len(prepared)):
            pairs.append((index, other_index))
    random.SystemRandom().shuffle(pairs)

    candidates: list[dict[str, object]] = []
    seen: set[tuple[int, int, str]] = set()
    for left_index, right_index in pairs:
        candidate = _sandman_build_similarity_candidate(
            prepared[left_index],
            prepared[right_index],
            existing,
            seen,
            min_score=0.42,
            reason_prefix="random_walk",
        )
        if candidate is None:
            continue
        candidates.append(candidate)
        if len(candidates) >= max_links:
            break
    return candidates


def _sandman_extract_similarity_candidates(conn, memories: list[dict[str, object]], existing: set[tuple[int, int, str]], max_links: int) -> list[dict[str, object]]:
    prepared = _sandman_prepare_memories(memories)
    candidates: list[dict[str, object]] = []

    orphan_candidates = _sandman_extract_orphan_rescue_candidates(conn, prepared, existing, max_links)
    candidates.extend(orphan_candidates)

    augmented_existing = set(existing)
    for item in candidates:
        augmented_existing.add((int(item["from_memory_id"]), int(item["to_memory_id"]), str(item["relation_type"])))

    remaining = max(0, max_links - len(candidates))
    if remaining > 0:
        random_candidates = _sandman_extract_random_walk_candidates(prepared, augmented_existing, remaining)
        candidates.extend(random_candidates)

    deduped: list[dict[str, object]] = []
    seen: set[tuple[int, int, str]] = set()
    for item in candidates:
        key = (int(item["from_memory_id"]), int(item["to_memory_id"]), str(item["relation_type"]))
        reverse_key = (key[1], key[0], key[2])
        if key in seen or reverse_key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:max_links]

def _sandman_graph_density_stats(conn, workspace_id: int | None = None, project_key: str | None = None) -> dict[str, object]:
    where_sql, params = _sandman_scope_clause(workspace_id, project_key)
    memory_rows = conn.execute(
        f"SELECT id FROM memories WHERE {where_sql}",
        params,
    ).fetchall()
    memory_ids = {int(row["id"]) for row in memory_rows}
    memory_count = len(memory_ids)
    if memory_count == 0:
        return {
            "memory_count": 0,
            "link_count": 0,
            "linked_memory_count": 0,
            "unlinked_memory_count": 0,
            "links_per_memory": 0.0,
            "avg_degree": 0.0,
        }

    link_rows = conn.execute(
        """
        SELECT from_memory_id, to_memory_id
        FROM memory_links
        WHERE archived_at IS NULL
        """
    ).fetchall()
    internal_links = []
    linked_ids: set[int] = set()
    for row in link_rows:
        left_id = int(row["from_memory_id"])
        right_id = int(row["to_memory_id"])
        if left_id in memory_ids and right_id in memory_ids:
            internal_links.append((left_id, right_id))
            linked_ids.add(left_id)
            linked_ids.add(right_id)

    link_count = len(internal_links)
    linked_memory_count = len(linked_ids)
    unlinked_memory_count = max(0, memory_count - linked_memory_count)
    links_per_memory = link_count / memory_count
    avg_degree = (2 * link_count) / memory_count
    return {
        "memory_count": memory_count,
        "link_count": link_count,
        "linked_memory_count": linked_memory_count,
        "unlinked_memory_count": unlinked_memory_count,
        "links_per_memory": round(links_per_memory, 3),
        "avg_degree": round(avg_degree, 3),
    }


def _sandman_adaptive_dream_link_limit(
    conn,
    workspace_id: int | None = None,
    project_key: str | None = None,
    requested_max_links: int = 80,
) -> dict[str, object]:
    stats = _sandman_graph_density_stats(conn, workspace_id=workspace_id, project_key=project_key)
    memory_count = int(stats["memory_count"])
    link_count = int(stats["link_count"])
    unlinked_memory_count = int(stats["unlinked_memory_count"])
    links_per_memory = float(stats["links_per_memory"])

    if memory_count <= 0:
        return {"limit": 0, "reason": "empty_scope", "stats": stats}

    # Sandman should still rescue isolated memories, but once the graph is dense
    # he must walk slower. This is a soft brake, not a handbrake.
    if links_per_memory >= 5.0:
        density_cap = 4
        density_band = "very_dense"
    elif links_per_memory >= 4.0:
        density_cap = 8
        density_band = "dense"
    elif links_per_memory >= 3.25:
        density_cap = 12
        density_band = "warming_up"
    elif links_per_memory >= 2.25:
        density_cap = 24
        density_band = "medium"
    else:
        density_cap = requested_max_links
        density_band = "sparse"

    orphan_bonus = min(6, unlinked_memory_count * 2)
    target_links_per_memory = 4.5
    target_budget = max(0, int((memory_count * target_links_per_memory) - link_count))
    if unlinked_memory_count > 0:
        target_budget = max(target_budget, orphan_bonus)

    limit = min(int(requested_max_links), int(density_cap) + int(orphan_bonus), int(target_budget))
    if unlinked_memory_count > 0 and limit <= 0:
        limit = min(int(requested_max_links), int(orphan_bonus) or 2)
    limit = max(0, limit)

    return {
        "limit": limit,
        "reason": "adaptive_density_brake",
        "density_band": density_band,
        "density_cap": density_cap,
        "orphan_bonus": orphan_bonus,
        "target_budget": target_budget,
        "requested_max_links": requested_max_links,
        "stats": stats,
    }


def _sandman_get_dream_link_candidates(conn, workspace_id: int | None = None, project_key: str | None = None, max_links: int = 80) -> list[dict[str, object]]:
    brake = _sandman_adaptive_dream_link_limit(conn, workspace_id=workspace_id, project_key=project_key, requested_max_links=max_links)
    effective_max_links = int(brake.get("limit", 0))
    if effective_max_links <= 0:
        return []

    # Prefer the deterministic memory_linking_pass engine for Sandman's dream links.
    # The older similarity/mention dream linker remains as a fallback, but Sandman
    # should not depend on hand-curated chat passes to keep the graph connected.
    try:
        deterministic_candidates = _get_memory_linking_candidates(
            conn,
            project_key=project_key,
            limit=effective_max_links,
            max_links_per_memory=8,
            min_score=0.47,
        )
    except NameError:
        deterministic_candidates = []

    if deterministic_candidates:
        candidates = deterministic_candidates[:effective_max_links]
        for item in candidates:
            item["adaptive_brake"] = brake
            item["sandman_linker"] = "memory_linking_pass_v1"
            item["reason"] = "sandman_forced_memory_linking_pass"
        return candidates

    where_sql, params = _sandman_scope_clause(workspace_id, project_key)
    rows = conn.execute(
        f"""
        SELECT id, content, summary_short, memory_type, tags, project_key, layer_code, area_code, state_code, scope_code, activity_state
        FROM memories
        WHERE {where_sql}
        ORDER BY COALESCE(last_recalled_at, last_accessed_at, created_at, '') DESC, id DESC
        LIMIT 500
        """,
        params,
    ).fetchall()
    memories = [row_to_dict(row) for row in rows]
    existing = _sandman_existing_link_keys(conn)

    similarity_candidates = _sandman_extract_similarity_candidates(conn, memories, existing, effective_max_links)
    remaining = max(0, effective_max_links - len(similarity_candidates))
    if remaining <= 0:
        candidates = similarity_candidates[:effective_max_links]
    else:
        mention_candidates = _sandman_extract_mention_candidates(conn, memories, existing, remaining)
        candidates = (similarity_candidates + mention_candidates)[:effective_max_links]

    for item in candidates:
        item["adaptive_brake"] = brake
        item["sandman_linker"] = "legacy_similarity_mention"
    return candidates

def _sandman_make_dream_story(candidates: list[dict[str, object]], links_created: list[dict[str, object]], run_id: int, project_key: str | None = None) -> str:
    source_items = links_created or candidates[:12]
    if not source_items:
        return "Sandman wrócił z pustymi kieszeniami. W korytarzu pamięci stała tylko szafa, która udawała drzwi."

    relation_images = {
        "mentions": "numer zapisany na wewnętrznej stronie powieki wskazał inną kartkę",
        "related_to": "dwie kartki rozpoznały ten sam kurz i przysunęły się do siebie",
        "same_project": "pokój przesunął ściany, żeby obce notatki stały się sąsiadami",
        "documents": "papier położył cień na mechanizmie, który wcześniej nie miał imienia",
        "implements": "mały mechanizm wyrósł z notatki i zaczął udawać architekturę",
        "fixes": "rdza znalazła śrubkę, a śrubka przypomniała sobie gwint",
        "configures": "klucz obrócił się w zamku, którego jeszcze nie narysowano",
        "validates": "lampka kontrolna mrugnęła, chociaż nikt jej nie pytał o zgodę",
        "risk_for": "czerwony sznurek zawiązał supeł na kieszeni z sekretami",
        "metric_for": "liczby przeszły przez lustro i wróciły jako drobny deszcz",
        "next_step_for": "schodek wyrósł pod stopą dopiero po zrobieniu kroku",
        "depends_on": "jedna szuflada śniła zawias drugiej",
    }
    seen_relations: list[str] = []
    for item in source_items:
        relation = str(item.get("relation_type") or "related_to")
        if relation not in seen_relations:
            seen_relations.append(relation)
    fragments = [relation_images.get(relation, "nić przeszła przez miejsce, gdzie brakowało nazwy") for relation in seen_relations[:5]]
    project_part = f" nad stołem {project_key}" if project_key else ""
    return (
        f"Sandman śnił{project_part}. "
        + " ".join(fragment.capitalize() + "." for fragment in fragments)
        + " Rano zostały po tym tylko drobne włókna między kartkami i wrażenie, że biblioteka przez chwilę oddychała odwrotnie."
    )

@mcp.tool
def preview_sandman_v1(
    freedom_level: int = 1,
    notes: str | None = None,
    workspace_key: str | None = None,
    project_key: str | None = None,
) -> dict[str, Any]:
    """
    Sandman V1 (preview) — podgląd kandydatów do archiwizacji i downgrade.
    workspace_key: ogranicz do wspomnień z danego workspace (Faza 3).
    project_key: ogranicz do wspomnień z danego projektu (Faza 3).
    """
    if freedom_level not in {0, 1}:
        raise ValueError("Sandman V1 obsługuje freedom_level 0 albo 1")
    conn = get_db_connection()
    try:
        resolved_workspace_id = _resolve_workspace_id(conn, workspace_key) if workspace_key else None
        run_id = create_sleep_run(conn, mode="preview", freedom_level=freedom_level, notes=notes, workspace_id=resolved_workspace_id, project_key=project_key)
        duplicate_candidates = sandman_logic.get_duplicate_candidates(conn)
        archive_source = sandman_logic.get_archive_candidates(conn, workspace_id=resolved_workspace_id, project_key=project_key)
        downgrade_source = sandman_logic.get_downgrade_candidates(conn, workspace_id=resolved_workspace_id, project_key=project_key)
        archive_candidates, archive_skipped_due_to_duplicates = sandman_logic.filter_archive_candidates_for_duplicates(conn, archive_source, duplicate_candidates)
        downgrade_candidates, downgrade_skipped_due_to_duplicates = sandman_logic.filter_downgrade_candidates_for_duplicates(conn, downgrade_source, duplicate_candidates)
        secondary_duplicate_ids = sandman_logic.get_secondary_duplicate_memory_ids(conn, duplicate_candidates)
        protected_canonical_ids = sandman_logic.get_protected_canonical_memory_ids(conn, duplicate_candidates)
        dream_link_candidates = _sandman_get_dream_link_candidates(conn, workspace_id=resolved_workspace_id, project_key=project_key, max_links=80)
        dream_link_brake = dream_link_candidates[0].get("adaptive_brake") if dream_link_candidates else _sandman_adaptive_dream_link_limit(conn, workspace_id=resolved_workspace_id, project_key=project_key, requested_max_links=80)
        dream_story = _sandman_make_dream_story(dream_link_candidates, [], run_id, project_key=project_key)

        archive_candidates_dict = []
        for row in archive_candidates:
            row_dict = row_to_dict(row)
            if int(row["id"]) in secondary_duplicate_ids:
                row_dict["archive_reason"] = "duplicate_secondary_preferred_archive"
            archive_candidates_dict.append(row_dict)
        downgrade_candidates_dict = [row_to_dict(row) for row in downgrade_candidates]
        canonical_evidence_boost_candidates = []
        for canonical_id in sorted(protected_canonical_ids):
            memory = require_memory_row(conn, canonical_id)
            current_evidence = int(memory["evidence_count"] or 1)
            target_evidence = max(current_evidence, 1 + sandman_logic.get_incoming_duplicate_count(conn, canonical_id))
            if target_evidence > current_evidence:
                canonical_evidence_boost_candidates.append({"memory_id": canonical_id, "old_evidence_count": current_evidence, "new_evidence_count": target_evidence})

        for row in archive_candidates_dict:
            add_sleep_action(conn, run_id, "archive_candidate", int(row["id"]), {"activity_state": row.get("activity_state"), "importance_score": row.get("importance_score")}, {"activity_state": "archived"}, row.get("archive_reason", "working_low_value_no_recall"))
        for row in archive_skipped_due_to_duplicates:
            add_sleep_action(conn, run_id, "archive_skipped_duplicate_canonical", int(row["id"]), {"activity_state": row.get("activity_state"), "importance_score": row.get("importance_score")}, {"skipped": True}, "duplicate_pair_canonical_protected")
        for row in downgrade_candidates_dict:
            proposed_importance = round(max(float(row["importance_score"]) - 0.10, 0.05), 3)
            add_sleep_action(conn, run_id, "downgrade_candidate", int(row["id"]), {"importance_score": row.get("importance_score")}, {"importance_score": proposed_importance}, "low_activity_low_value")
        for row in downgrade_skipped_due_to_duplicates:
            add_sleep_action(conn, run_id, "downgrade_skipped_duplicate", int(row["id"]), {"importance_score": row.get("importance_score")}, {"skipped": True}, row.get("skip_reason", "duplicate_skip"))
        for pair in duplicate_candidates:
            add_sleep_action(conn, run_id, "duplicate_candidate", int(pair["duplicate_memory_id"]), {"canonical_memory_id": pair["canonical_memory_id"], "duplicate_memory_id": pair["duplicate_memory_id"]}, {"relation_type": "duplicate_of", "from_memory_id": pair["duplicate_memory_id"], "to_memory_id": pair["canonical_memory_id"]}, "same_content_or_high_similarity")
        for item in canonical_evidence_boost_candidates:
            add_sleep_action(conn, run_id, "canonical_evidence_boost_candidate", int(item["memory_id"]), {"evidence_count": item["old_evidence_count"]}, {"evidence_count": item["new_evidence_count"]}, "duplicate_support_bonus")

        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        finalize_sleep_run(conn, run_id, status="preview_completed", scanned_count=int(scanned_count), changed_count=0, archived_count=0, downgraded_count=0, duplicate_count=len(duplicate_candidates), conflict_count=0, created_summary_count=0)
        scope_info = {"workspace_key": workspace_key, "workspace_id": resolved_workspace_id, "project_key": project_key}
        return {"status": "preview_completed", "run_id": run_id, "freedom_level": freedom_level, "scanned_count": int(scanned_count), "scope": scope_info, "archive_candidates": archive_candidates_dict, "archive_skipped_due_to_duplicates": archive_skipped_due_to_duplicates, "downgrade_candidates": [{**row, "proposed_importance_score": round(max(float(row["importance_score"]) - 0.10, 0.05), 3)} for row in downgrade_candidates_dict], "downgrade_skipped_due_to_duplicates": downgrade_skipped_due_to_duplicates, "duplicate_candidates": duplicate_candidates, "canonical_evidence_boost_candidates": canonical_evidence_boost_candidates, "dream_link_candidates": dream_link_candidates, "dream_story": dream_story, "dream_link_brake": dream_link_brake, "summary": {"archive_count": len(archive_candidates_dict), "archive_skipped_due_to_duplicates_count": len(archive_skipped_due_to_duplicates), "downgrade_count": len(downgrade_candidates_dict), "duplicate_count": len(duplicate_candidates), "skipped_duplicate_downgrade_count": len(downgrade_skipped_due_to_duplicates), "canonical_evidence_boost_count": len(canonical_evidence_boost_candidates), "dream_link_candidate_count": len(dream_link_candidates)}}
    finally:
        conn.close()


@mcp.tool
def run_sandman_v1(
    freedom_level: int = 1,
    notes: str | None = None,
    workspace_key: str | None = None,
    project_key: str | None = None,
) -> dict[str, Any]:
    """
    Sandman V1 — archiwizacja, downgrade, duplikaty.
    workspace_key: ogranicz do wspomnień z danego workspace (Faza 3).
    project_key: ogranicz do wspomnień z danego projektu (Faza 3).
    """
    if freedom_level not in {0, 1}:
        raise ValueError("Sandman V1 obsługuje freedom_level 0 albo 1")
    conn = get_db_connection()
    try:
        resolved_workspace_id = _resolve_workspace_id(conn, workspace_key) if workspace_key else None
        run_id = create_sleep_run(conn, mode="run", freedom_level=freedom_level, notes=notes, workspace_id=resolved_workspace_id, project_key=project_key)
        duplicate_candidates = sandman_logic.get_duplicate_candidates(conn)
        archive_source = sandman_logic.get_archive_candidates(conn, workspace_id=resolved_workspace_id, project_key=project_key)
        downgrade_source = sandman_logic.get_downgrade_candidates(conn, workspace_id=resolved_workspace_id, project_key=project_key)
        archive_candidates, archive_skipped_due_to_duplicates = sandman_logic.filter_archive_candidates_for_duplicates(conn, archive_source, duplicate_candidates)
        downgrade_candidates, downgrade_skipped_due_to_duplicates = sandman_logic.filter_downgrade_candidates_for_duplicates(conn, downgrade_source, duplicate_candidates)
        secondary_duplicate_ids = sandman_logic.get_secondary_duplicate_memory_ids(conn, duplicate_candidates)
        protected_canonical_ids = sandman_logic.get_protected_canonical_memory_ids(conn, duplicate_candidates)
        dream_link_candidates = _sandman_get_dream_link_candidates(conn, workspace_id=resolved_workspace_id, project_key=project_key, max_links=80)
        dream_link_brake = dream_link_candidates[0].get("adaptive_brake") if dream_link_candidates else _sandman_adaptive_dream_link_limit(conn, workspace_id=resolved_workspace_id, project_key=project_key, requested_max_links=80)

        archived_items: list[dict[str, Any]] = []
        downgraded_items: list[dict[str, Any]] = []
        duplicate_links_created: list[dict[str, Any]] = []
        dream_links_created: list[dict[str, Any]] = []
        canonical_evidence_boosted: list[dict[str, Any]] = []

        for row in archive_candidates:
            memory_id = int(row["id"])
            archived_at = utc_now_iso()
            if memory_id in secondary_duplicate_ids:
                archive_reason = "duplicate_secondary_preferred_archive"
                sandman_note = "Sandman V1: duplicate_secondary_preferred_archive"
            else:
                archive_reason = "working_low_value_no_recall"
                sandman_note = "Sandman V1: working_low_value_no_recall"
            conn.execute("UPDATE memories SET activity_state = 'archived', state_code = 'archived', archived_at = ?, sandman_note = ? WHERE id = ?", (archived_at, sandman_note, memory_id))
            add_sleep_action(conn, run_id, "archived", memory_id, {"activity_state": row["activity_state"], "state_code": row["state_code"]}, {"activity_state": "archived", "state_code": "archived", "archived_at": archived_at}, archive_reason)
            updated = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
            archived_items.append(row_to_dict(updated))

        for row in archive_skipped_due_to_duplicates:
            add_sleep_action(conn, run_id, "archive_skipped_duplicate_canonical", int(row["id"]), {"activity_state": row.get("activity_state"), "importance_score": row.get("importance_score")}, {"skipped": True}, "duplicate_pair_canonical_protected")
        for row in downgrade_candidates:
            memory_id = int(row["id"])
            old_importance = float(row["importance_score"])
            new_importance = round(max(old_importance - 0.10, 0.05), 3)
            conn.execute("UPDATE memories SET importance_score = ?, sandman_note = ? WHERE id = ?", (new_importance, "Sandman V1: low_activity_low_value", memory_id))
            add_sleep_action(conn, run_id, "downgraded", memory_id, {"importance_score": old_importance}, {"importance_score": new_importance}, "low_activity_low_value")
            updated = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
            downgraded_items.append(row_to_dict(updated))
        for row in downgrade_skipped_due_to_duplicates:
            add_sleep_action(conn, run_id, "downgrade_skipped_duplicate", int(row["id"]), {"importance_score": row.get("importance_score")}, {"skipped": True}, row.get("skip_reason", "duplicate_skip"))

        for pair in duplicate_candidates:
            canonical_id = int(pair["canonical_memory_id"])
            duplicate_id = int(pair["duplicate_memory_id"])
            if not sandman_logic.duplicate_link_exists(conn, duplicate_id, canonical_id):
                item = _create_link(conn, duplicate_id, canonical_id, "duplicate_of", 0.95, "sandman_v1_auto")
                duplicate_links_created.append(item)
                add_sleep_action(conn, run_id, "duplicate_link_created", duplicate_id, None, {"link_id": item["id"], "from_memory_id": duplicate_id, "to_memory_id": canonical_id, "relation_type": "duplicate_of"}, "same_content_or_high_similarity")
            conn.execute("UPDATE memories SET sandman_note = ? WHERE id = ?", (f"Sandman V1: duplicate_of {canonical_id}", duplicate_id))

        add_sleep_action(conn, run_id, "dream_link_brake", None, None, dream_link_brake, "adaptive_density_brake")

        for item in dream_link_candidates:
            if not item.get("from_memory_id") or not item.get("to_memory_id") or not item.get("relation_type"):
                continue
            created = _create_link(conn, int(item["from_memory_id"]), int(item["to_memory_id"]), str(item["relation_type"]), float(item.get("weight") or 0.5), "sandman_v1_dream")
            dream_links_created.append(created)
            add_sleep_action(conn, run_id, "dream_link_created", int(item["from_memory_id"]), None, {**item, "link_id": created.get("id")}, item.get("reason", "sandman_dream_linking"))

        dream_story = _sandman_make_dream_story(dream_link_candidates, dream_links_created, run_id, project_key=project_key)
        add_sleep_action(conn, run_id, "dream_story", None, None, {"story": dream_story, "dream_links_created_count": len(dream_links_created)}, "sandman_dream_narrative")

        for canonical_id in sorted(protected_canonical_ids):
            boosted = sandman_logic.boost_canonical_evidence_count(conn, canonical_id)
            if boosted is not None:
                canonical_evidence_boosted.append(boosted)
                add_sleep_action(conn, run_id, "canonical_evidence_boosted", canonical_id, {"evidence_count": boosted["old_evidence_count"]}, {"evidence_count": boosted["new_evidence_count"]}, "duplicate_support_bonus")

        conn.commit()
        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        finalize_sleep_run(conn, run_id, status="completed", scanned_count=int(scanned_count), changed_count=len(archived_items) + len(downgraded_items) + len(duplicate_links_created) + len(dream_links_created) + len(canonical_evidence_boosted), archived_count=len(archived_items), downgraded_count=len(downgraded_items), duplicate_count=len(duplicate_candidates), conflict_count=0, created_summary_count=0)
        scope_info = {"workspace_key": workspace_key, "workspace_id": resolved_workspace_id, "project_key": project_key}
        return {"status": "completed", "run_id": run_id, "freedom_level": freedom_level, "scanned_count": int(scanned_count), "scope": scope_info, "archived_items": archived_items, "archive_skipped_due_to_duplicates": archive_skipped_due_to_duplicates, "downgraded_items": downgraded_items, "downgrade_skipped_due_to_duplicates": downgrade_skipped_due_to_duplicates, "duplicate_candidates": duplicate_candidates, "duplicate_links_created": duplicate_links_created, "dream_link_candidates": dream_link_candidates, "dream_links_created": dream_links_created, "dream_story": dream_story, "dream_link_brake": dream_link_brake, "canonical_evidence_boosted": canonical_evidence_boosted, "summary": {"changed_count": len(archived_items) + len(downgraded_items) + len(duplicate_links_created) + len(dream_links_created) + len(canonical_evidence_boosted), "archived_count": len(archived_items), "archive_skipped_due_to_duplicates_count": len(archive_skipped_due_to_duplicates), "downgraded_count": len(downgraded_items), "duplicate_count": len(duplicate_candidates), "duplicate_links_created_count": len(duplicate_links_created), "dream_links_created_count": len(dream_links_created), "skipped_duplicate_downgrade_count": len(downgrade_skipped_due_to_duplicates), "canonical_evidence_boost_count": len(canonical_evidence_boosted)}}
    finally:
        conn.close()


@mcp.tool
def preview_sandman_ai(freedom_level: int = 1, notes: str | None = None) -> dict[str, Any]:
    """
    Sandman AI (preview) — używa LM Studio (Qwen) do oceny wspomnień.
    freedom_level: 0=konserwatywny, 1=normalny, 2=agresywny.
    Nie wprowadza żadnych zmian w bazie.
    """
    if freedom_level not in {0, 1, 2}:
        raise ValueError("Sandman AI obsługuje freedom_level 0, 1 lub 2")
    conn = get_db_connection()
    try:
        run_id = create_sleep_run(conn, mode="ai_preview", freedom_level=freedom_level, notes=notes)
        duplicate_candidates = sandman_logic.get_duplicate_candidates(conn)
        protected_canonical_ids = sandman_logic.get_protected_canonical_memory_ids(conn, duplicate_candidates)
        secondary_duplicate_ids = sandman_logic.get_secondary_duplicate_memory_ids(conn, duplicate_candidates)

        archive_decisions, downgrade_decisions, keep_decisions = sandman_ai.get_ai_decisions(conn, freedom_level)

        canonical_evidence_boost_candidates = []
        for canonical_id in sorted(protected_canonical_ids):
            memory = require_memory_row(conn, canonical_id)
            current_evidence = int(memory["evidence_count"] or 1)
            target_evidence = max(current_evidence, 1 + sandman_logic.get_incoming_duplicate_count(conn, canonical_id))
            if target_evidence > current_evidence:
                canonical_evidence_boost_candidates.append({"memory_id": canonical_id, "old_evidence_count": current_evidence, "new_evidence_count": target_evidence})

        for item in archive_decisions:
            add_sleep_action(conn, run_id, "archive_candidate", int(item["id"]), {"activity_state": item.get("activity_state"), "importance_score": item.get("importance_score")}, {"activity_state": "archived"}, item.get("ai_reason", "ai_decision"))
        for item in downgrade_decisions:
            old_importance = float(item.get("importance_score") or 0.5)
            proposed = item.get("ai_new_importance") or round(max(old_importance - 0.10, 0.05), 3)
            add_sleep_action(conn, run_id, "downgrade_candidate", int(item["id"]), {"importance_score": old_importance}, {"importance_score": proposed}, item.get("ai_reason", "ai_decision"))
        for pair in duplicate_candidates:
            add_sleep_action(conn, run_id, "duplicate_candidate", int(pair["duplicate_memory_id"]), {"canonical_memory_id": pair["canonical_memory_id"], "duplicate_memory_id": pair["duplicate_memory_id"]}, {"relation_type": "duplicate_of", "from_memory_id": pair["duplicate_memory_id"], "to_memory_id": pair["canonical_memory_id"]}, "same_content_or_high_similarity")
        for item in canonical_evidence_boost_candidates:
            add_sleep_action(conn, run_id, "canonical_evidence_boost_candidate", int(item["memory_id"]), {"evidence_count": item["old_evidence_count"]}, {"evidence_count": item["new_evidence_count"]}, "duplicate_support_bonus")

        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        finalize_sleep_run(conn, run_id, status="preview_completed", scanned_count=int(scanned_count), changed_count=0, archived_count=0, downgraded_count=0, duplicate_count=len(duplicate_candidates), conflict_count=0, created_summary_count=0)

        return {
            "status": "preview_completed",
            "run_id": run_id,
            "freedom_level": freedom_level,
            "model": sandman_ai.LM_STUDIO_MODEL,
            "scanned_count": int(scanned_count),
            "archive_candidates": archive_decisions,
            "downgrade_candidates": [
                {**item, "proposed_importance_score": item.get("ai_new_importance") or round(max(float(item.get("importance_score") or 0.5) - 0.10, 0.05), 3)}
                for item in downgrade_decisions
            ],
            "keep_count": len(keep_decisions),
            "duplicate_candidates": duplicate_candidates,
            "canonical_evidence_boost_candidates": canonical_evidence_boost_candidates,
            "summary": {
                "archive_count": len(archive_decisions),
                "downgrade_count": len(downgrade_decisions),
                "keep_count": len(keep_decisions),
                "duplicate_count": len(duplicate_candidates),
                "canonical_evidence_boost_count": len(canonical_evidence_boost_candidates),
            },
        }
    finally:
        conn.close()


@mcp.tool
def run_sandman_ai(freedom_level: int = 1, notes: str | None = None) -> dict[str, Any]:
    """
    Sandman AI (wykonanie) — używa LM Studio (Qwen) do oceny wspomnień i wprowadza zmiany.
    freedom_level: 0=konserwatywny, 1=normalny, 2=agresywny.
    Wszystkie zmiany są undo-safe (można cofnąć przez undo_run).
    """
    if freedom_level not in {0, 1, 2}:
        raise ValueError("Sandman AI obsługuje freedom_level 0, 1 lub 2")
    conn = get_db_connection()
    try:
        run_id = create_sleep_run(conn, mode="ai_run", freedom_level=freedom_level, notes=notes)
        duplicate_candidates = sandman_logic.get_duplicate_candidates(conn)
        protected_canonical_ids = sandman_logic.get_protected_canonical_memory_ids(conn, duplicate_candidates)
        secondary_duplicate_ids = sandman_logic.get_secondary_duplicate_memory_ids(conn, duplicate_candidates)

        archive_decisions, downgrade_decisions, _ = sandman_ai.get_ai_decisions(conn, freedom_level)

        archived_items: list[dict[str, Any]] = []
        downgraded_items: list[dict[str, Any]] = []
        duplicate_links_created: list[dict[str, Any]] = []
        canonical_evidence_boosted: list[dict[str, Any]] = []

        for item in archive_decisions:
            memory_id = int(item["id"])
            archived_at = utc_now_iso()
            sandman_note = f"Sandman AI: {item.get('ai_reason', 'ai_decision')}"
            conn.execute("UPDATE memories SET activity_state = 'archived', archived_at = ?, sandman_note = ? WHERE id = ?", (archived_at, sandman_note, memory_id))
            add_sleep_action(conn, run_id, "archived", memory_id, {"activity_state": item.get("activity_state")}, {"activity_state": "archived", "archived_at": archived_at}, item.get("ai_reason", "ai_decision"))
            updated = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
            archived_items.append(row_to_dict(updated))

        for item in downgrade_decisions:
            memory_id = int(item["id"])
            old_importance = float(item.get("importance_score") or 0.5)
            new_importance = item.get("ai_new_importance") or round(max(old_importance - 0.10, 0.05), 3)
            sandman_note = f"Sandman AI: {item.get('ai_reason', 'ai_decision')}"
            conn.execute("UPDATE memories SET importance_score = ?, sandman_note = ? WHERE id = ?", (new_importance, sandman_note, memory_id))
            add_sleep_action(conn, run_id, "downgraded", memory_id, {"importance_score": old_importance}, {"importance_score": new_importance}, item.get("ai_reason", "ai_decision"))
            updated = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
            downgraded_items.append(row_to_dict(updated))

        for pair in duplicate_candidates:
            canonical_id = int(pair["canonical_memory_id"])
            duplicate_id = int(pair["duplicate_memory_id"])
            if not sandman_logic.duplicate_link_exists(conn, duplicate_id, canonical_id):
                item = _create_link(conn, duplicate_id, canonical_id, "duplicate_of", 0.95, "sandman_ai_auto")
                duplicate_links_created.append(item)
                add_sleep_action(conn, run_id, "duplicate_link_created", duplicate_id, None, {"link_id": item["id"], "from_memory_id": duplicate_id, "to_memory_id": canonical_id, "relation_type": "duplicate_of"}, "same_content_or_high_similarity")
            conn.execute("UPDATE memories SET sandman_note = ? WHERE id = ?", (f"Sandman AI: duplicate_of {canonical_id}", duplicate_id))

        for canonical_id in sorted(protected_canonical_ids):
            boosted = sandman_logic.boost_canonical_evidence_count(conn, canonical_id)
            if boosted is not None:
                canonical_evidence_boosted.append(boosted)
                add_sleep_action(conn, run_id, "canonical_evidence_boosted", canonical_id, {"evidence_count": boosted["old_evidence_count"]}, {"evidence_count": boosted["new_evidence_count"]}, "duplicate_support_bonus")

        conn.commit()
        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        changed_count = len(archived_items) + len(downgraded_items) + len(duplicate_links_created) + len(canonical_evidence_boosted)
        finalize_sleep_run(conn, run_id, status="completed", scanned_count=int(scanned_count), changed_count=changed_count, archived_count=len(archived_items), downgraded_count=len(downgraded_items), duplicate_count=len(duplicate_candidates), conflict_count=0, created_summary_count=0)

        return {
            "status": "completed",
            "run_id": run_id,
            "freedom_level": freedom_level,
            "model": sandman_ai.LM_STUDIO_MODEL,
            "scanned_count": int(scanned_count),
            "archived_items": archived_items,
            "downgraded_items": downgraded_items,
            "duplicate_candidates": duplicate_candidates,
            "duplicate_links_created": duplicate_links_created,
            "canonical_evidence_boosted": canonical_evidence_boosted,
            "summary": {
                "changed_count": changed_count,
                "archived_count": len(archived_items),
                "downgraded_count": len(downgraded_items),
                "duplicate_count": len(duplicate_candidates),
                "duplicate_links_created_count": len(duplicate_links_created),
                "canonical_evidence_boost_count": len(canonical_evidence_boosted),
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MEMORY LINKING PASS V1
# ---------------------------------------------------------------------------

def _memory_linking_split_tags(raw: str | None) -> set[str]:
    text = (raw or "").replace(";", ",")
    return {item.strip().lower() for item in text.split(",") if item.strip()}


def _memory_linking_tokens(*parts: object) -> set[str]:
    import re
    text = " ".join(str(part or "") for part in parts).lower()
    stop = {
        "oraz", "jest", "dla", "przez", "jako", "with", "from", "this", "that",
        "the", "and", "czy", "bez", "pod", "nad", "wraz", "into", "about", "memory",
        "wspomnienie", "wspomnien", "wspomnień", "projekt", "project", "status",
    }
    return {token for token in re.findall(r"[a-zA-Z0-9_\-ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]{4,}", text) if token not in stop}


def _memory_link_exists_any_direction(conn, a_id: int, b_id: int, relation_type: str | None = None) -> bool:
    if relation_type:
        row = conn.execute(
            """
            SELECT id FROM memory_links
            WHERE archived_at IS NULL
              AND relation_type = ?
              AND ((from_memory_id = ? AND to_memory_id = ?) OR (from_memory_id = ? AND to_memory_id = ?))
            LIMIT 1
            """,
            (relation_type, a_id, b_id, b_id, a_id),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT id FROM memory_links
            WHERE archived_at IS NULL
              AND ((from_memory_id = ? AND to_memory_id = ?) OR (from_memory_id = ? AND to_memory_id = ?))
            LIMIT 1
            """,
            (a_id, b_id, b_id, a_id),
        ).fetchone()
    return row is not None


def _memory_linking_relation_and_direction(a: dict[str, Any], b: dict[str, Any], reasons: list[str]) -> tuple[int, int, str]:
    a_id = int(a["id"])
    b_id = int(b["id"])
    a_type = str(a.get("memory_type") or "").lower()
    b_type = str(b.get("memory_type") or "").lower()
    a_text = f"{a.get('summary_short') or ''} {a.get('content') or ''}".lower()
    b_text = f"{b.get('summary_short') or ''} {b.get('content') or ''}".lower()

    if int(a.get("supersedes_memory_id") or 0) == b_id:
        return a_id, b_id, "supersedes"
    if int(b.get("supersedes_memory_id") or 0) == a_id:
        return b_id, a_id, "supersedes"
    if int(a.get("parent_memory_id") or 0) == b_id:
        return a_id, b_id, "context_for"
    if int(b.get("parent_memory_id") or 0) == a_id:
        return b_id, a_id, "context_for"

    requirement_types = {"project_requirement", "requirement"}
    implementation_words = ("implement", "wdroż", "dodano", "napraw", "patched", "migration", "migrac", "status")
    if a_type in requirement_types and (b_type in {"project_status", "project_note", "project_decision"} or any(word in b_text for word in implementation_words)):
        return b_id, a_id, "implements"
    if b_type in requirement_types and (a_type in {"project_status", "project_note", "project_decision"} or any(word in a_text for word in implementation_words)):
        return a_id, b_id, "implements"

    if "timeline" in reasons or "schema/migration" in reasons:
        return a_id, b_id, "documents"
    if "bootstrap/core" in reasons:
        return a_id, b_id, "context_for"
    if "same project" in reasons:
        return a_id, b_id, "same_project"
    return a_id, b_id, "related_to"


def _get_memory_linking_candidates(
    conn,
    *,
    project_key: str | None = None,
    limit: int = 100,
    max_links_per_memory: int = 4,
    min_score: float = 0.45,
) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 100), 500))
    max_links_per_memory = max(1, min(int(max_links_per_memory or 4), 20))
    min_score = max(0.05, min(float(min_score or 0.45), 1.0))

    params: list[Any] = []
    where = "COALESCE(activity_state, 'active') = 'active'"
    if project_key:
        where += " AND project_key = ?"
        params.append(project_key)

    rows = conn.execute(
        f"""
        SELECT id, summary_short, content, memory_type, project_key, tags, importance_score,
               parent_memory_id, supersedes_memory_id, created_at
        FROM memories
        WHERE {where}
        ORDER BY COALESCE(importance_score, 0) DESC, id DESC
        LIMIT 450
        """,
        tuple(params),
    ).fetchall()
    memories = [row_to_dict(row) for row in rows]

    active_link_counts: dict[int, int] = {}
    for row in conn.execute(
        """
        SELECT memory_id, COUNT(*) AS link_count FROM (
            SELECT from_memory_id AS memory_id FROM memory_links WHERE archived_at IS NULL
            UNION ALL
            SELECT to_memory_id AS memory_id FROM memory_links WHERE archived_at IS NULL
        ) GROUP BY memory_id
        """
    ).fetchall():
        active_link_counts[int(row["memory_id"])] = int(row["link_count"] or 0)

    candidates: list[dict[str, Any]] = []
    candidate_counts: dict[int, int] = {}

    for idx, a in enumerate(memories):
        a_id = int(a["id"])
        if active_link_counts.get(a_id, 0) >= max_links_per_memory:
            continue
        a_tags = _memory_linking_split_tags(a.get("tags"))
        a_tokens = _memory_linking_tokens(a.get("summary_short"), a.get("tags"), a.get("memory_type"), a.get("project_key"))
        for b in memories[idx + 1:]:
            b_id = int(b["id"])
            if a_id == b_id:
                continue
            if active_link_counts.get(b_id, 0) >= max_links_per_memory:
                continue
            if candidate_counts.get(a_id, 0) >= max_links_per_memory or candidate_counts.get(b_id, 0) >= max_links_per_memory:
                continue
            if _memory_link_exists_any_direction(conn, a_id, b_id):
                continue

            b_tags = _memory_linking_split_tags(b.get("tags"))
            b_tokens = _memory_linking_tokens(b.get("summary_short"), b.get("tags"), b.get("memory_type"), b.get("project_key"))
            common_tags = sorted(a_tags & b_tags)
            common_tokens = sorted(a_tokens & b_tokens)
            reasons: list[str] = []
            score = 0.0

            if a.get("project_key") and a.get("project_key") == b.get("project_key"):
                score += 0.35
                reasons.append("same project")
            if common_tags:
                score += min(0.35, 0.12 * len(common_tags))
                reasons.append("shared tags: " + ", ".join(common_tags[:5]))
            if common_tokens:
                score += min(0.25, 0.06 * len(common_tokens))
                reasons.append("shared tokens: " + ", ".join(common_tokens[:5]))
            if int(a.get("parent_memory_id") or 0) == b_id or int(b.get("parent_memory_id") or 0) == a_id:
                score += 0.45
                reasons.append("parent/child")
            if int(a.get("supersedes_memory_id") or 0) == b_id or int(b.get("supersedes_memory_id") or 0) == a_id:
                score += 0.55
                reasons.append("supersedes lineage")

            blob = " ".join(str(x or "") for x in [a.get("summary_short"), a.get("content"), b.get("summary_short"), b.get("content"), a.get("tags"), b.get("tags")]).lower()
            if any(word in blob for word in ("schema", "migration", "migrac", "timeline", "oś", "osi")):
                score += 0.12
                reasons.append("schema/migration")
            if any(word in blob for word in ("bootstrap", "identity", "tożsamo", "core")):
                score += 0.12
                reasons.append("bootstrap/core")
            if any(word in blob for word in ("timeline", "valid_at", "event")):
                score += 0.08
                reasons.append("timeline")

            score = min(score, 1.0)
            if score < min_score:
                continue

            from_id, to_id, relation_type = _memory_linking_relation_and_direction(a, b, reasons)
            if _memory_link_exists_any_direction(conn, from_id, to_id, relation_type):
                continue

            candidate = {
                "from_memory_id": from_id,
                "to_memory_id": to_id,
                "relation_type": relation_type,
                "weight": round(max(0.5, min(score, 0.95)), 2),
                "score": round(score, 3),
                "reasons": reasons[:6],
                "from_summary": a.get("summary_short") if from_id == a_id else b.get("summary_short"),
                "to_summary": b.get("summary_short") if to_id == b_id else a.get("summary_short"),
            }
            candidates.append(candidate)
            candidate_counts[a_id] = candidate_counts.get(a_id, 0) + 1
            candidate_counts[b_id] = candidate_counts.get(b_id, 0) + 1

    candidates.sort(key=lambda item: (float(item["score"]), float(item["weight"])), reverse=True)
    return candidates[:limit]


@mcp.tool
def preview_memory_linking_pass(
    project_key: str | None = None,
    limit: int = 50,
    max_links_per_memory: int = 4,
    min_score: float = 0.45,
    notes: str | None = None,
) -> dict[str, Any]:
    """Preview kandydatów do deterministycznego linkowania grafu pamięci. Nie zapisuje zmian."""
    conn = get_db_connection()
    try:
        candidates = _get_memory_linking_candidates(
            conn,
            project_key=project_key,
            limit=limit,
            max_links_per_memory=max_links_per_memory,
            min_score=min_score,
        )
        run_id = create_sleep_run(conn, mode="memory_linking_preview", freedom_level=0, notes=notes)
        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories WHERE COALESCE(activity_state, 'active') = 'active'").fetchone()["count"]
        for candidate in candidates:
            add_sleep_action(
                conn,
                run_id,
                "memory_link_candidate",
                int(candidate["from_memory_id"]),
                None,
                candidate,
                "memory_linking_pass_v1_preview",
            )
        finalize_sleep_run(conn, run_id, status="preview_completed", scanned_count=int(scanned_count), changed_count=0, archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=0, created_summary_count=0)
        return {
            "status": "preview_completed",
            "run_id": run_id,
            "project_key": project_key,
            "scanned_count": int(scanned_count),
            "candidate_count": len(candidates),
            "candidates": candidates,
            "summary": {
                "candidate_count": len(candidates),
                "relation_type_counts": {rtype: sum(1 for item in candidates if item["relation_type"] == rtype) for rtype in sorted({item["relation_type"] for item in candidates})},
            },
        }
    finally:
        conn.close()


@mcp.tool
def run_memory_linking_pass(
    project_key: str | None = None,
    limit: int = 50,
    max_links_per_memory: int = 4,
    min_score: float = 0.45,
    notes: str | None = None,
) -> dict[str, Any]:
    """Deterministycznie tworzy brakujące linki grafu pamięci. Nie tworzy ani nie archiwizuje wspomnień."""
    conn = get_db_connection()
    try:
        candidates = _get_memory_linking_candidates(
            conn,
            project_key=project_key,
            limit=limit,
            max_links_per_memory=max_links_per_memory,
            min_score=min_score,
        )
        run_id = create_sleep_run(conn, mode="memory_linking_run", freedom_level=0, notes=notes)
        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories WHERE COALESCE(activity_state, 'active') = 'active'").fetchone()["count"]
        links_created: list[dict[str, Any]] = []
        skipped_existing: list[dict[str, Any]] = []
        origin = "memory_linking_pass_v1"

        for candidate in candidates:
            from_id = int(candidate["from_memory_id"])
            to_id = int(candidate["to_memory_id"])
            relation_type = str(candidate["relation_type"])
            if _memory_link_exists_any_direction(conn, from_id, to_id, relation_type):
                skipped_existing.append(candidate)
                continue
            item = _create_link(conn, from_id, to_id, relation_type, float(candidate["weight"]), origin)
            item["score"] = candidate["score"]
            item["reasons"] = candidate["reasons"]
            links_created.append(item)
            add_sleep_action(
                conn,
                run_id,
                "memory_link_created",
                from_id,
                None,
                {**candidate, "link_id": item["id"], "origin": origin},
                "memory_linking_pass_v1",
            )

        finalize_sleep_run(conn, run_id, status="completed", scanned_count=int(scanned_count), changed_count=len(links_created), archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=0, created_summary_count=0)
        return {
            "status": "completed",
            "run_id": run_id,
            "project_key": project_key,
            "scanned_count": int(scanned_count),
            "links_created": links_created,
            "skipped_existing_count": len(skipped_existing),
            "summary": {
                "created_count": len(links_created),
                "candidate_count": len(candidates),
                "relation_type_counts": {rtype: sum(1 for item in links_created if item["relation_type"] == rtype) for rtype in sorted({item["relation_type"] for item in links_created})},
                "origin": origin,
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MEMORY LINKING PASS V1.1: semantic vs project-neighborhood split
# ---------------------------------------------------------------------------

_MEMORY_LINKING_GENERIC_TAGS = {
    "project", "projekt", "project_status", "project_note", "project_context",
    "project_decision", "status", "done", "next-step", "todo", "wishlist",
    "pamiec", "pamięć", "pamiec-jagody", "jagoda", "jagody", "memory",
    "docs", "documentation", "cleanup", "success", "debug", "deploy",
    "runbook", "vps", "mcp", "api", "build-success",
}

_MEMORY_LINKING_GENERIC_TOKENS = {
    "oraz", "jest", "dla", "przez", "jako", "with", "from", "this", "that",
    "the", "and", "czy", "bez", "pod", "nad", "wraz", "into", "about", "memory",
    "wspomnienie", "wspomnien", "wspomnień", "projekt", "project", "status",
    "działa", "dodano", "trzeba", "aktualny", "kolejny", "następny", "next-step",
    "pamiec-jagody", "project_status", "project_context", "project_note",
}


def _memory_linking_specific_tags(raw: str | None) -> set[str]:
    return {tag for tag in _memory_linking_split_tags(raw) if tag not in _MEMORY_LINKING_GENERIC_TAGS and len(tag) >= 3}


def _memory_linking_specific_tokens(*parts: object) -> set[str]:
    return {token for token in _memory_linking_tokens(*parts) if token not in _MEMORY_LINKING_GENERIC_TOKENS and token not in _MEMORY_LINKING_GENERIC_TAGS}



def _memory_linking_log_squash(raw_score: float, *, resistance: float = 1.35, ceiling: float = 0.97) -> float:
    """Convert additive evidence into a bounded confidence score.

    The old linear scorer reached 1.0 too easily. This logarithmic squash keeps
    growth monotonic, rewards additional evidence less and less, and never
    returns a hard 1.0 unless the ceiling is explicitly set to 1.0.
    """
    import math

    raw = max(0.0, float(raw_score or 0.0))
    resistance = max(0.05, float(resistance or 1.35))
    ceiling = max(0.05, min(float(ceiling or 0.97), 0.999))
    normalized = math.log1p(raw) / (math.log1p(raw) + resistance) if raw > 0.0 else 0.0
    return round(min(ceiling, max(0.0, normalized)), 3)


def _memory_linking_weight_from_score(score: float, *, link_class: str) -> float:
    if link_class == "semantic":
        return round(max(0.55, min(float(score), 0.93)), 2)
    return round(max(0.35, min(float(score), 0.68)), 2)


def _memory_linking_has_requirement_implementation(a: dict[str, Any], b: dict[str, Any]) -> bool:
    a_type = str(a.get("memory_type") or "").lower()
    b_type = str(b.get("memory_type") or "").lower()
    a_text = f"{a.get('summary_short') or ''} {a.get('content') or ''}".lower()
    b_text = f"{b.get('summary_short') or ''} {b.get('content') or ''}".lower()
    requirement_types = {"project_requirement", "requirement"}
    implementation_words = ("implement", "wdroż", "wdroz", "dodano", "napraw", "patched", "migration", "migrac", "status", "zrobion", "działa")
    return (
        a_type in requirement_types and any(word in b_text for word in implementation_words)
    ) or (
        b_type in requirement_types and any(word in a_text for word in implementation_words)
    )


def _memory_linking_relation_and_direction(a: dict[str, Any], b: dict[str, Any], reasons: list[str]) -> tuple[int, int, str]:
    a_id = int(a["id"])
    b_id = int(b["id"])
    a_type = str(a.get("memory_type") or "").lower()
    b_type = str(b.get("memory_type") or "").lower()

    if int(a.get("supersedes_memory_id") or 0) == b_id:
        return a_id, b_id, "supersedes"
    if int(b.get("supersedes_memory_id") or 0) == a_id:
        return b_id, a_id, "supersedes"
    if int(a.get("parent_memory_id") or 0) == b_id:
        return a_id, b_id, "context_for"
    if int(b.get("parent_memory_id") or 0) == a_id:
        return b_id, a_id, "context_for"

    if _memory_linking_has_requirement_implementation(a, b):
        if a_type in {"project_requirement", "requirement"}:
            return b_id, a_id, "implements"
        return a_id, b_id, "implements"

    if any(reason.startswith("strong_semantic") for reason in reasons):
        if "schema/migration" in reasons or "timeline" in reasons:
            return a_id, b_id, "documents"
        if "bootstrap/core" in reasons:
            return a_id, b_id, "context_for"
        return a_id, b_id, "related_to"

    if any(reason.startswith("semantic") for reason in reasons):
        if "schema/migration" in reasons or "timeline" in reasons:
            return a_id, b_id, "documents"
        if "bootstrap/core" in reasons:
            return a_id, b_id, "context_for"
        return a_id, b_id, "related_to"

    return a_id, b_id, "same_project"


def _get_memory_linking_candidates(
    conn,
    *,
    project_key: str | None = None,
    limit: int = 100,
    max_links_per_memory: int = 4,
    min_score: float = 0.45,
) -> list[dict[str, Any]]:
    """V1.1 scoring.

    Splits a candidate into:
    - semantic: specific shared tags/tokens, lineage, parent/child, requirement->implementation,
      migration/timeline/bootstrap only when backed by semantic overlap.
    - project_neighborhood: same project plus weak/generic overlap. Lower weight and relation same_project.
    """
    limit = max(1, min(int(limit or 100), 500))
    max_links_per_memory = max(1, min(int(max_links_per_memory or 4), 20))
    min_score = max(0.05, min(float(min_score or 0.45), 1.0))

    params: list[Any] = []
    where = "COALESCE(activity_state, 'active') = 'active'"
    if project_key:
        where += " AND project_key = ?"
        params.append(project_key)

    rows = conn.execute(
        f"""
        SELECT id, summary_short, content, memory_type, project_key, tags, importance_score,
               parent_memory_id, supersedes_memory_id, created_at
        FROM memories
        WHERE {where}
        ORDER BY COALESCE(importance_score, 0) DESC, id DESC
        LIMIT 450
        """,
        tuple(params),
    ).fetchall()
    memories = [row_to_dict(row) for row in rows]

    active_link_counts: dict[int, int] = {}
    for row in conn.execute(
        """
        SELECT memory_id, COUNT(*) AS link_count FROM (
            SELECT from_memory_id AS memory_id FROM memory_links WHERE archived_at IS NULL
            UNION ALL
            SELECT to_memory_id AS memory_id FROM memory_links WHERE archived_at IS NULL
        ) GROUP BY memory_id
        """
    ).fetchall():
        active_link_counts[int(row["memory_id"])] = int(row["link_count"] or 0)

    candidates: list[dict[str, Any]] = []
    candidate_counts: dict[int, int] = {}

    for idx, a in enumerate(memories):
        a_id = int(a["id"])
        if active_link_counts.get(a_id, 0) >= max_links_per_memory:
            continue
        a_tags = _memory_linking_specific_tags(a.get("tags"))
        a_tokens = _memory_linking_specific_tokens(a.get("summary_short"), a.get("tags"), a.get("memory_type"), a.get("project_key"))
        for b in memories[idx + 1:]:
            b_id = int(b["id"])
            if a_id == b_id:
                continue
            if active_link_counts.get(b_id, 0) >= max_links_per_memory:
                continue
            if candidate_counts.get(a_id, 0) >= max_links_per_memory or candidate_counts.get(b_id, 0) >= max_links_per_memory:
                continue
            if _memory_link_exists_any_direction(conn, a_id, b_id):
                continue

            same_project = bool(a.get("project_key") and a.get("project_key") == b.get("project_key"))
            b_tags = _memory_linking_specific_tags(b.get("tags"))
            b_tokens = _memory_linking_specific_tokens(b.get("summary_short"), b.get("tags"), b.get("memory_type"), b.get("project_key"))
            common_tags = sorted(a_tags & b_tags)
            common_tokens = sorted(a_tokens & b_tokens)

            semantic_raw = 0.0
            neighborhood_raw = 0.0
            reasons: list[str] = []

            if same_project:
                neighborhood_raw += 0.35
                reasons.append("same project")

            if common_tags:
                semantic_raw += 0.28 * len(common_tags)
                neighborhood_raw += 0.08 * len(common_tags)
                reasons.append("semantic shared tags: " + ", ".join(common_tags[:5]))
            if common_tokens:
                semantic_raw += 0.14 * len(common_tokens)
                neighborhood_raw += 0.04 * len(common_tokens)
                reasons.append("semantic shared tokens: " + ", ".join(common_tokens[:5]))

            if int(a.get("parent_memory_id") or 0) == b_id or int(b.get("parent_memory_id") or 0) == a_id:
                semantic_raw += 1.35
                reasons.append("strong_semantic parent/child")
            if int(a.get("supersedes_memory_id") or 0) == b_id or int(b.get("supersedes_memory_id") or 0) == a_id:
                semantic_raw += 1.55
                reasons.append("strong_semantic supersedes lineage")
            if _memory_linking_has_requirement_implementation(a, b):
                semantic_raw += 1.25
                reasons.append("strong_semantic requirement/implementation")

            blob = " ".join(str(x or "") for x in [a.get("summary_short"), a.get("content"), b.get("summary_short"), b.get("content"), a.get("tags"), b.get("tags")]).lower()
            has_schema = any(word in blob for word in ("schema", "migration", "migrac", "migracja"))
            has_timeline = any(word in blob for word in ("timeline", "valid_at", "event", "oś", "osi"))
            has_bootstrap = any(word in blob for word in ("bootstrap", "identity", "tożsamo", "tozsamo", "core"))
            has_semantic_overlap = semantic_raw >= 0.42 or len(common_tags) >= 2 or len(common_tokens) >= 3

            if has_schema and has_semantic_overlap:
                semantic_raw += 0.35
                reasons.append("schema/migration")
            elif has_schema and same_project:
                neighborhood_raw += 0.12
                reasons.append("project-neighborhood schema/migration")
            if has_timeline and has_semantic_overlap:
                semantic_raw += 0.28
                reasons.append("timeline")
            elif has_timeline and same_project:
                neighborhood_raw += 0.08
                reasons.append("project-neighborhood timeline")
            if has_bootstrap and has_semantic_overlap:
                semantic_raw += 0.30
                reasons.append("bootstrap/core")
            elif has_bootstrap and same_project:
                neighborhood_raw += 0.10
                reasons.append("project-neighborhood bootstrap/core")

            semantic_score = _memory_linking_log_squash(semantic_raw, resistance=1.20, ceiling=0.97)
            neighborhood_score = _memory_linking_log_squash(neighborhood_raw, resistance=1.65, ceiling=0.72)

            if semantic_score >= min_score:
                link_class = "semantic"
                score = semantic_score
                weight = _memory_linking_weight_from_score(score, link_class=link_class)
            elif same_project and neighborhood_score >= max(0.42, min_score - 0.22):
                link_class = "project_neighborhood"
                score = neighborhood_score
                weight = _memory_linking_weight_from_score(score, link_class=link_class)
            else:
                continue

            from_id, to_id, relation_type = _memory_linking_relation_and_direction(a, b, reasons if link_class == "semantic" else ["project_neighborhood"])
            if link_class == "project_neighborhood":
                relation_type = "same_project"
            if _memory_link_exists_any_direction(conn, from_id, to_id, relation_type):
                continue

            candidate = {
                "from_memory_id": from_id,
                "to_memory_id": to_id,
                "relation_type": relation_type,
                "weight": weight,
                "score": round(score, 3),
                "semantic_score": round(semantic_score, 3),
                "neighborhood_score": round(neighborhood_score, 3),
                "semantic_raw": round(semantic_raw, 3),
                "neighborhood_raw": round(neighborhood_raw, 3),
                "score_model": "log_squash_v1",
                "link_class": link_class,
                "reasons": reasons[:8],
                "from_summary": a.get("summary_short") if from_id == a_id else b.get("summary_short"),
                "to_summary": b.get("summary_short") if to_id == b_id else a.get("summary_short"),
            }
            candidates.append(candidate)
            candidate_counts[a_id] = candidate_counts.get(a_id, 0) + 1
            candidate_counts[b_id] = candidate_counts.get(b_id, 0) + 1

    candidates.sort(
        key=lambda item: (
            1 if item.get("link_class") == "semantic" else 0,
            float(item.get("semantic_score") or 0),
            float(item.get("score") or 0),
            float(item.get("weight") or 0),
        ),
        reverse=True,
    )
    return candidates[:limit]


# Relation selector v1.2: keep documents only for doc-like memories.
def _memory_linking_blob(memory: dict[str, Any]) -> str:
    return " ".join(str(memory.get(k) or "") for k in ("summary_short", "content", "memory_type", "tags", "project_key")).lower()


def _memory_linking_any(text: str, words: tuple[str, ...]) -> bool:
    return any(word in text for word in words)


def _memory_linking_doc_like(memory: dict[str, Any]) -> bool:
    text = _memory_linking_blob(memory)
    mtype = str(memory.get("memory_type") or "").lower()
    return mtype in {"fact", "runbook", "documentation", "doc", "technical_note"} or _memory_linking_any(text, ("runbook", "docs", "dokumentac", "readme", "checklist", "instrukcj", "wyjaśnienie", "wyjasnienie"))


def _memory_linking_status_relation(memory: dict[str, Any]) -> str | None:
    text = _memory_linking_blob(memory)
    mtype = str(memory.get("memory_type") or "").lower()
    if mtype not in {"project_status", "project_note", "project_context", "consolidated_summary", "fact"}:
        return None
    if _memory_linking_any(text, ("wdroż", "wdroz", "dodano", "zaimplement", "implemented", "patched", "napraw", "zmigrow")):
        return "implements"
    if _memory_linking_any(text, ("działa", "potwierdz", "success", "passed", "przeszed", "wykonan")):
        return "validates"
    return None


def _memory_linking_relation_and_direction(a: dict[str, Any], b: dict[str, Any], reasons: list[str]) -> tuple[int, int, str]:
    a_id, b_id = int(a["id"]), int(b["id"])
    a_type = str(a.get("memory_type") or "").lower()
    b_type = str(b.get("memory_type") or "").lower()

    if int(a.get("supersedes_memory_id") or 0) == b_id:
        return a_id, b_id, "supersedes"
    if int(b.get("supersedes_memory_id") or 0) == a_id:
        return b_id, a_id, "supersedes"
    if int(a.get("parent_memory_id") or 0) == b_id:
        return a_id, b_id, "context_for"
    if int(b.get("parent_memory_id") or 0) == a_id:
        return b_id, a_id, "context_for"

    a_doc = _memory_linking_doc_like(a)
    b_doc = _memory_linking_doc_like(b)
    if a_doc and not b_doc:
        return a_id, b_id, "documents"
    if b_doc and not a_doc:
        return b_id, a_id, "documents"

    decisions = {"project_decision", "project_requirement", "requirement"}
    a_status = _memory_linking_status_relation(a)
    b_status = _memory_linking_status_relation(b)
    if a_status and b_type in decisions:
        return a_id, b_id, a_status
    if b_status and a_type in decisions:
        return b_id, a_id, b_status
    if _memory_linking_has_requirement_implementation(a, b):
        if a_type in decisions and b_type not in decisions:
            return b_id, a_id, b_status or "implements"
        return a_id, b_id, a_status or "implements"

    if a_type.startswith("personal") or b_type.startswith("personal"):
        return a_id, b_id, "related_to"
    if "bootstrap/core" in reasons:
        return a_id, b_id, "context_for"
    if any(r.startswith("semantic") or r.startswith("strong_semantic") for r in reasons):
        return a_id, b_id, "related_to"
    return a_id, b_id, "same_project"


# Relation selector v1.3: requirement/implementation beats documents.
def _memory_linking_relation_and_direction(a: dict[str, Any], b: dict[str, Any], reasons: list[str]) -> tuple[int, int, str]:
    a_id, b_id = int(a["id"]), int(b["id"])
    a_type = str(a.get("memory_type") or "").lower()
    b_type = str(b.get("memory_type") or "").lower()

    if int(a.get("supersedes_memory_id") or 0) == b_id:
        return a_id, b_id, "supersedes"
    if int(b.get("supersedes_memory_id") or 0) == a_id:
        return b_id, a_id, "supersedes"
    if int(a.get("parent_memory_id") or 0) == b_id:
        return a_id, b_id, "context_for"
    if int(b.get("parent_memory_id") or 0) == a_id:
        return b_id, a_id, "context_for"

    decisions = {"project_decision", "project_requirement", "requirement"}
    a_status = _memory_linking_status_relation(a)
    b_status = _memory_linking_status_relation(b)

    # Strong requirement/implementation evidence wins before doc-like detection.
    if _memory_linking_has_requirement_implementation(a, b) or any("requirement/implementation" in r for r in reasons):
        if a_type in decisions and b_type not in decisions:
            return b_id, a_id, b_status or "implements"
        if b_type in decisions and a_type not in decisions:
            return a_id, b_id, a_status or "implements"
        if a_status and not b_status:
            return a_id, b_id, a_status
        if b_status and not a_status:
            return b_id, a_id, b_status
        return a_id, b_id, "related_to"

    if a_status and b_type in decisions:
        return a_id, b_id, a_status
    if b_status and a_type in decisions:
        return b_id, a_id, b_status

    a_doc = _memory_linking_doc_like(a)
    b_doc = _memory_linking_doc_like(b)
    if a_doc and not b_doc:
        return a_id, b_id, "documents"
    if b_doc and not a_doc:
        return b_id, a_id, "documents"

    if a_type.startswith("personal") or b_type.startswith("personal"):
        return a_id, b_id, "related_to"
    if "bootstrap/core" in reasons:
        return a_id, b_id, "context_for"
    if any(r.startswith("semantic") or r.startswith("strong_semantic") for r in reasons):
        return a_id, b_id, "related_to"
    return a_id, b_id, "same_project"


# Relation selector v1.4: requirement/implementation needs domain overlap.
def _memory_linking_domain_overlap(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    a_tags = _memory_linking_specific_tags(a.get("tags"))
    b_tags = _memory_linking_specific_tags(b.get("tags"))
    a_tokens = _memory_linking_specific_tokens(a.get("summary_short"), a.get("tags"), a.get("memory_type"), a.get("project_key"))
    b_tokens = _memory_linking_specific_tokens(b.get("summary_short"), b.get("tags"), b.get("memory_type"), b.get("project_key"))
    common_tags = sorted(a_tags & b_tags)
    common_tokens = sorted(a_tokens & b_tokens)
    return {
        "common_tags": common_tags,
        "common_tokens": common_tokens,
        "has_overlap": bool(common_tags or common_tokens),
    }


def _memory_linking_relation_and_direction(a: dict[str, Any], b: dict[str, Any], reasons: list[str]) -> tuple[int, int, str]:
    a_id, b_id = int(a["id"]), int(b["id"])
    a_type = str(a.get("memory_type") or "").lower()
    b_type = str(b.get("memory_type") or "").lower()

    if int(a.get("supersedes_memory_id") or 0) == b_id:
        return a_id, b_id, "supersedes"
    if int(b.get("supersedes_memory_id") or 0) == a_id:
        return b_id, a_id, "supersedes"
    if int(a.get("parent_memory_id") or 0) == b_id:
        return a_id, b_id, "context_for"
    if int(b.get("parent_memory_id") or 0) == a_id:
        return b_id, a_id, "context_for"

    decisions = {"project_decision", "project_requirement", "requirement"}
    a_status = _memory_linking_status_relation(a)
    b_status = _memory_linking_status_relation(b)
    overlap = _memory_linking_domain_overlap(a, b)
    has_domain_overlap = bool(overlap["has_overlap"])
    has_req_impl_reason = _memory_linking_has_requirement_implementation(a, b) or any("requirement/implementation" in r for r in reasons)

    # Requirement/implementation is strong only when the two memories share a concrete domain anchor.
    # Without shared non-generic tag/token, fall back to a weak relation instead of inventing implements.
    if has_req_impl_reason:
        if not has_domain_overlap:
            return a_id, b_id, "related_to"
        if a_type in decisions and b_type not in decisions:
            return b_id, a_id, b_status or "implements"
        if b_type in decisions and a_type not in decisions:
            return a_id, b_id, a_status or "implements"
        if a_status and not b_status:
            return a_id, b_id, a_status
        if b_status and not a_status:
            return b_id, a_id, b_status
        return a_id, b_id, "related_to"

    if a_status and b_type in decisions:
        return a_id, b_id, a_status
    if b_status and a_type in decisions:
        return b_id, a_id, b_status

    a_doc = _memory_linking_doc_like(a)
    b_doc = _memory_linking_doc_like(b)
    if a_doc and not b_doc:
        return a_id, b_id, "documents"
    if b_doc and not a_doc:
        return b_id, a_id, "documents"

    if a_type.startswith("personal") or b_type.startswith("personal"):
        return a_id, b_id, "related_to"
    if "bootstrap/core" in reasons:
        return a_id, b_id, "context_for"
    if any(r.startswith("semantic") or r.startswith("strong_semantic") for r in reasons):
        return a_id, b_id, "related_to"
    return a_id, b_id, "same_project"


@mcp.tool
def preview_consolidation_v1(notes: str | None = None) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        run_id = create_sleep_run(conn, mode="consolidation_preview", freedom_level=0, notes=notes)
        candidates = consolidation_logic.get_consolidation_candidates(conn)
        for candidate in candidates:
            add_sleep_action(conn, run_id, "consolidation_candidate", int(candidate["central_memory_id"]), {"member_ids": candidate["member_ids"]}, {"supporting_memory_ids": candidate["supporting_memory_ids"], "support_links_to_create_count": candidate["support_links_to_create_count"], "summary_memory_to_create": candidate["summary_memory_to_create"], "summary_links_to_create_count": candidate["summary_links_to_create_count"]}, "gravity_cluster_candidate")
        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        finalize_sleep_run(conn, run_id, status="preview_completed", scanned_count=int(scanned_count), changed_count=0, archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=0, created_summary_count=0)
        return {"status": "preview_completed", "run_id": run_id, "scanned_count": int(scanned_count), "consolidation_candidates": candidates, "summary": {"cluster_count": len(candidates), "support_links_to_create_count": sum(int(item["support_links_to_create_count"]) for item in candidates), "summary_memory_to_create_count": sum(1 for item in candidates if bool(item["summary_memory_to_create"])), "summary_links_to_create_count": sum(int(item["summary_links_to_create_count"]) for item in candidates), "total_links_to_create_count": sum(int(item["total_links_to_create_count"]) for item in candidates)}}
    finally:
        conn.close()


@mcp.tool
def run_consolidation_v1(notes: str | None = None) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        run_id = create_sleep_run(conn, mode="consolidation_run", freedom_level=0, notes=notes)
        candidates = consolidation_logic.get_consolidation_candidates(conn)
        support_links_created: list[dict[str, Any]] = []
        summary_links_created: list[dict[str, Any]] = []
        created_summary_memories: list[dict[str, Any]] = []
        central_evidence_boosted: list[dict[str, Any]] = []

        for candidate in candidates:
            central_id = int(candidate["central_memory_id"])
            links_created_for_cluster = 0
            for member_id in candidate["supporting_memory_ids"]:
                if consolidation_logic.support_link_exists(conn, int(member_id), central_id):
                    continue
                item = _create_link(conn, int(member_id), central_id, "supports", float(candidate["average_gravity"] or 0.5), "consolidation_v1_auto")
                support_links_created.append(item)
                links_created_for_cluster += 1
                add_sleep_action(conn, run_id, "support_link_created", int(member_id), None, {"link_id": item["id"], "from_memory_id": int(member_id), "to_memory_id": central_id, "relation_type": "supports"}, "gravity_support_link")

            summary_memory_id = candidate.get("existing_summary_memory_id")
            if summary_memory_id is None:
                proposed_summary = candidate["proposed_summary_memory"]
                created_summary = _insert_memory(
                    conn,
                    content=str(proposed_summary["content"]),
                    memory_type=str(proposed_summary["memory_type"]),
                    summary_short=proposed_summary.get("summary_short"),
                    source=proposed_summary.get("source"),
                    importance_score=float(proposed_summary.get("importance_score") or 0.5),
                    confidence_score=float(proposed_summary.get("confidence_score") or 0.5),
                    tags=proposed_summary.get("tags"),
                )
                summary_memory_id = int(created_summary["id"])
                created_summary_memories.append(created_summary)
                add_sleep_action(conn, run_id, "summary_memory_created", summary_memory_id, None, {"memory_id": summary_memory_id, "memory_type": created_summary["memory_type"], "summary_short": created_summary.get("summary_short")}, "gravity_summary_memory_created")

            if not consolidation_logic.summary_link_exists(conn, int(summary_memory_id), central_id, "summarizes"):
                item = _create_link(conn, int(summary_memory_id), central_id, "summarizes", 1.0, "consolidation_v1_auto")
                summary_links_created.append(item)
                add_sleep_action(conn, run_id, "summary_link_created", int(summary_memory_id), None, {"link_id": item["id"], "from_memory_id": int(summary_memory_id), "to_memory_id": central_id, "relation_type": "summarizes"}, "gravity_summary_link")

            for member_id in candidate["member_ids"]:
                if consolidation_logic.summary_link_exists(conn, int(summary_memory_id), int(member_id), "consolidated_from"):
                    continue
                item = _create_link(conn, int(summary_memory_id), int(member_id), "consolidated_from", 1.0, "consolidation_v1_auto")
                summary_links_created.append(item)
                add_sleep_action(conn, run_id, "summary_link_created", int(summary_memory_id), None, {"link_id": item["id"], "from_memory_id": int(summary_memory_id), "to_memory_id": int(member_id), "relation_type": "consolidated_from"}, "gravity_summary_link")

            if links_created_for_cluster > 0:
                central_memory = require_memory_row(conn, central_id)
                old_evidence_count = int(central_memory["evidence_count"] or 1)
                new_evidence_count = old_evidence_count + links_created_for_cluster
                conn.execute("UPDATE memories SET evidence_count = ?, sandman_note = ? WHERE id = ?", (new_evidence_count, f"Consolidation V1: gravity cluster of {candidate['member_count']} memories", central_id))
                boosted = {"memory_id": central_id, "old_evidence_count": old_evidence_count, "new_evidence_count": new_evidence_count}
                central_evidence_boosted.append(boosted)
                add_sleep_action(conn, run_id, "canonical_evidence_boosted", central_id, {"evidence_count": old_evidence_count}, {"evidence_count": new_evidence_count}, "gravity_cluster_support_bonus")

        conn.commit()
        scanned_count = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        changed_count = len(support_links_created) + len(summary_links_created) + len(created_summary_memories) + len(central_evidence_boosted)
        finalize_sleep_run(conn, run_id, status="completed", scanned_count=int(scanned_count), changed_count=changed_count, archived_count=0, downgraded_count=0, duplicate_count=0, conflict_count=0, created_summary_count=len(created_summary_memories))
        return {"status": "completed", "run_id": run_id, "scanned_count": int(scanned_count), "consolidation_candidates": candidates, "support_links_created": support_links_created, "summary_links_created": summary_links_created, "created_summary_memories": created_summary_memories, "central_evidence_boosted": central_evidence_boosted, "summary": {"cluster_count": len(candidates), "links_created_count": len(support_links_created), "support_links_created_count": len(support_links_created), "summary_links_created_count": len(summary_links_created), "summary_memories_created_count": len(created_summary_memories), "central_evidence_boost_count": len(central_evidence_boosted), "changed_count": changed_count}}
    finally:
        conn.close()



def _record_agent_session_to_timeline(
    conn: "sqlite3.Connection",
    *,
    user_query: str,
    result: "dict[str, Any]",
) -> None:
    """Zapisuje sesję sandman_agent do timeline. Błędy są ignorowane (best-effort)."""
    try:
        tools_used = list({
            step["tool_name"]
            for step in result.get("trace", [])
            if step.get("tool_name") and step["tool_name"] != "none"
        })
        write_tools = {"create_memory", "archive_memory", "link_memories", "update_memory_importance"}
        payload = {
            "query": (user_query or "")[:200],
            "steps": result.get("steps", 0),
            "status": result.get("status", "unknown"),
            "tools_used": tools_used,
            "write_tools_used": [t for t in tools_used if t in write_tools],
        }
        timeline.record_timeline_event(
            conn,
            event_type="sandman_agent.session",
            origin="sandman_agent_auto",
            timeline_scope="system",
            semantic_kind="runtime_event",
            title=f"Sandman agent: {(user_query or '')[:80]}",
            payload=payload,
        )
        conn.commit()
    except Exception:
        pass


@mcp.tool
def sandman_memory_chat(user_query: str, max_steps: int = 4) -> dict[str, Any]:
    """
    Sandman Memory Chat — host steruje narzędziami MAPI dla lokalnego modelu.
    Model może iteracyjnie wołać wyszukiwanie wspomnień, odczyt pamięci, linków i osi projektu.
    """
    if not user_query or not user_query.strip():
        raise ValueError("user_query nie może być puste")
    if max_steps < 1 or max_steps > 16:
        raise ValueError("max_steps musi być w zakresie 1..16")
    conn = get_db_connection()
    try:
        from app import sandman_agent
        result = sandman_agent.run_memory_tool_agent(conn, user_query=user_query, max_steps=max_steps)
        _record_agent_session_to_timeline(conn, user_query=user_query, result=result)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sprint 4 – layer promotion / demotion helpers and tools
# ---------------------------------------------------------------------------

def _validate_layer_transition(from_layer: str | None, to_layer: str) -> None:
    """Raises ValueError if the from→to direction is invalid or layers are unknown."""
    to_layer = (to_layer or "").strip().lower()
    if to_layer not in LAYER_ORDER:
        raise ValueError(f"Nieznana warstwa docelowa: '{to_layer}'. Dostępne: {', '.join(LAYER_ORDER)}")
    if from_layer is None:
        return  # no current layer — any target is allowed
    from_layer = (from_layer or "").strip().lower()
    if from_layer not in LAYER_ORDER:
        return  # unknown source layer — allow; don't block on bad historical data
    if from_layer == to_layer:
        raise ValueError(f"Wspomnienie jest już w warstwie '{to_layer}'")


def _do_layer_move(conn, memory_id: int, target_layer: str, reason: str, direction: str) -> dict[str, Any]:
    """
    Core implementation shared by promote_memory / demote_memory.
    direction: 'promote' | 'demote'
    Returns the updated memory dict.
    """
    row = require_memory_row(conn, memory_id)
    memory = row_to_dict(row)
    from_layer = memory.get("layer_code")
    _validate_layer_transition(from_layer, target_layer)

    from_idx = LAYER_ORDER.index(from_layer) if from_layer in LAYER_ORDER else -1
    to_idx = LAYER_ORDER.index(target_layer)

    if direction == "promote" and from_idx >= to_idx:
        raise ValueError(
            f"Awans wymaga wyższej warstwy. Obecna: '{from_layer}' (poziom {from_idx}), docelowa: '{target_layer}' (poziom {to_idx})."
        )
    if direction == "demote" and from_idx <= to_idx and from_idx != -1:
        raise ValueError(
            f"Degradacja wymaga niższej warstwy. Obecna: '{from_layer}' (poziom {from_idx}), docelowa: '{target_layer}' (poziom {to_idx})."
        )

    if direction == "promote":
        conn.execute(
            "UPDATE memories SET layer_code = ?, promoted_from_id = ?, sandman_note = ? WHERE id = ?",
            (target_layer, memory_id, f"Promoted from '{from_layer}' to '{target_layer}': {reason}", memory_id),
        )
    else:
        conn.execute(
            "UPDATE memories SET layer_code = ?, demoted_from_id = ?, sandman_note = ? WHERE id = ?",
            (target_layer, memory_id, f"Demoted from '{from_layer}' to '{target_layer}': {reason}", memory_id),
        )

    # Record in timeline
    try:
        timeline.record_timeline_event(
            conn,
            event_type=f"sandman.layer_{direction}d",
            memory_id=memory_id,
            summary=f"Layer {direction}: {from_layer} → {target_layer}",
            details={"reason": reason, "from_layer": from_layer, "to_layer": target_layer},
            origin="memory_api",
        )
    except Exception:
        pass  # timeline is best-effort

    conn.commit()
    updated = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
    return row_to_dict(updated)


@mcp.tool
def promote_memory(memory_id: int, target_layer: str, reason: str) -> dict[str, Any]:
    """
    Awansuje wspomnienie na wyższą warstwę.
    Dozwolone warstwy (rosnąco): buffer → working → projects → autobio → identity → core.
    Chroni warstwy core i identity przed nadpisaniem przez niższe.
    """
    if not reason or not reason.strip():
        raise ValueError("Pole 'reason' jest wymagane")
    target = normalize_layer_code(target_layer)
    if target is None:
        raise ValueError(f"Nieznana warstwa: '{target_layer}'")
    if target in SANDMAN_PROTECTED_LAYERS:
        raise ValueError(f"Warstwa '{target}' jest chroniona — awans wymaga ręcznej decyzji operatora")
    conn = get_db_connection()
    try:
        result = _do_layer_move(conn, memory_id, target, reason.strip(), "promote")
        return {"status": "promoted", "memory": result, "target_layer": target}
    finally:
        conn.close()


@mcp.tool
def demote_memory(memory_id: int, target_layer: str, reason: str) -> dict[str, Any]:
    """
    Degraduje wspomnienie do niższej warstwy.
    Nie można degradować wspomnień z chronionych warstw core/identity.
    """
    if not reason or not reason.strip():
        raise ValueError("Pole 'reason' jest wymagane")
    target = normalize_layer_code(target_layer)
    if target is None:
        raise ValueError(f"Nieznana warstwa: '{target_layer}'")
    conn = get_db_connection()
    try:
        row = require_memory_row(conn, memory_id)
        memory = row_to_dict(row)
        current_layer = memory.get("layer_code")
        if current_layer in SANDMAN_PROTECTED_LAYERS:
            raise ValueError(f"Wspomnienie jest w chronionej warstwie '{current_layer}' — degradacja zablokowana")
        result = _do_layer_move(conn, memory_id, target, reason.strip(), "demote")
        return {"status": "demoted", "memory": result, "target_layer": target}
    finally:
        conn.close()


@mcp.tool
def get_promotion_candidates(
    min_evidence: int = 2,
    min_importance: float = 0.6,
    min_confidence: float = 0.6,
    source_layer: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """
    Zwraca listę wspomnień, które spełniają kryteria awansu (evidence_count, importance_score, confidence_score).
    Opcjonalnie filtruje po warstwie źródłowej.
    """
    conn = get_db_connection()
    try:
        candidates = sandman_logic.get_promotion_candidates(
            conn,
            source_layer=source_layer,
            min_evidence=min_evidence,
            min_importance=min_importance,
            min_confidence=min_confidence,
            limit=limit,
        )
        return {
            "status": "ok",
            "count": len(candidates),
            "candidates": candidates,
            "filters": {
                "min_evidence": min_evidence,
                "min_importance": min_importance,
                "min_confidence": min_confidence,
                "source_layer": source_layer,
                "limit": limit,
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sprint 6 – operational insight tools
# ---------------------------------------------------------------------------

@mcp.tool
def get_layer_stats() -> dict[str, Any]:
    """
    Zwraca statystyki rozkładu wspomnień według layer_code, area_code i state_code.
    Przydatne do monitorowania kondycji bazy wspomnień.
    """
    conn = get_db_connection()
    try:
        layer_rows = conn.execute(
            "SELECT COALESCE(layer_code, 'unknown') AS layer_code, COUNT(*) AS count, "
            "ROUND(AVG(importance_score), 3) AS avg_importance, ROUND(AVG(confidence_score), 3) AS avg_confidence "
            "FROM memories GROUP BY layer_code ORDER BY count DESC"
        ).fetchall()

        area_rows = conn.execute(
            "SELECT COALESCE(area_code, 'unknown') AS area_code, COUNT(*) AS count "
            "FROM memories GROUP BY area_code ORDER BY count DESC"
        ).fetchall()

        state_rows = conn.execute(
            "SELECT COALESCE(state_code, 'unknown') AS state_code, COUNT(*) AS count "
            "FROM memories GROUP BY state_code ORDER BY count DESC"
        ).fetchall()

        total = conn.execute("SELECT COUNT(*) AS count FROM memories").fetchone()["count"]
        active_total = conn.execute(
            "SELECT COUNT(*) AS count FROM memories WHERE COALESCE(activity_state, 'active') = 'active'"
        ).fetchone()["count"]

        return {
            "status": "ok",
            "total_memories": total,
            "active_memories": active_total,
            "by_layer": [dict(r) for r in layer_rows],
            "by_area": [dict(r) for r in area_rows],
            "by_state": [dict(r) for r in state_rows],
        }
    finally:
        conn.close()


@mcp.tool
def get_version_lineage(memory_id: int) -> dict[str, Any]:
    """
    Zwraca pełne drzewo wersji wspomnienia (przodkowie i potomkowie przez supersedes_memory_id).
    Posortowane według version ASC, id ASC.
    """
    conn = get_db_connection()
    try:
        lineage = _collect_version_lineage(conn, memory_id)
        return {
            "status": "ok",
            "root_memory_id": memory_id,
            "count": len(lineage),
            "lineage": lineage,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Faza 4 — scope promotion governance
# ---------------------------------------------------------------------------

@mcp.tool
def propose_scope_promotion(
    memory_id: int,
    target_scope: str,
    reason: str,
    user_key: str | None = None,
) -> dict[str, Any]:
    """
    Zgłasza wniosek o poszerzenie scope wspomnienia (np. private → project, project → workspace).
    Nie zmienia scope natychmiast — tworzy rekord w review queue.
    Wymaga zatwierdzenia przez approve_scope_promotion.
    """
    if not reason or not reason.strip():
        raise ValueError("Pole 'reason' jest wymagane")
    target_scope = (target_scope or "").strip().lower()
    if target_scope not in _SCOPE_ORDER:
        raise ValueError(f"Nieznany target_scope: '{target_scope}'. Dostępne: {', '.join(_SCOPE_ORDER)}")

    conn = get_db_connection()
    try:
        if not _is_multiuser_feature_active(conn, MULTIUSER_SCOPE_PROMOTION_FLAG):
            return {"status": "disabled", "message": f"Feature flag '{MULTIUSER_SCOPE_PROMOTION_FLAG}' is off."}

        mem = require_memory_row(conn, memory_id)
        memory = row_to_dict(mem)
        current_scope = memory.get("visibility_scope") or "private"

        if current_scope == target_scope:
            return {"status": "noop", "message": f"Wspomnienie już ma scope '{current_scope}'", "memory_id": memory_id}

        if current_scope in _SCOPE_ORDER and target_scope in _SCOPE_ORDER:
            if _SCOPE_ORDER.index(target_scope) <= _SCOPE_ORDER.index(current_scope):
                raise ValueError(
                    f"Promocja wymaga szerszego scope. Obecny: '{current_scope}', docelowy: '{target_scope}'."
                )

        # Resolve proposing user
        proposed_by_user_id: int | None = None
        if user_key:
            user_row = conn.execute("SELECT id FROM users WHERE external_user_key = ?", (user_key.strip(),)).fetchone()
            if user_row:
                proposed_by_user_id = int(user_row["id"])

        workspace_id = memory.get("workspace_id")
        project_key = memory.get("project_key")

        # Check for an existing pending proposal for this memory+target
        existing = conn.execute(
            "SELECT id FROM scope_promotion_proposals WHERE memory_id = ? AND target_scope = ? AND status = 'pending'",
            (memory_id, target_scope),
        ).fetchone()
        if existing:
            return {
                "status": "already_pending",
                "message": "Istnieje już oczekujący wniosek dla tego wspomnienia i scope docelowego.",
                "proposal_id": int(existing["id"]),
                "memory_id": memory_id,
            }

        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO scope_promotion_proposals
                (memory_id, proposed_by_user_id, current_scope, target_scope, reason, status, workspace_id, project_key, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
            """,
            (memory_id, proposed_by_user_id, current_scope, target_scope, reason.strip(), workspace_id, project_key, utc_now_iso()),
        )
        proposal_id = int(cursor.lastrowid)
        conn.commit()

        try:
            timeline.record_timeline_event(
                conn,
                event_type="sandman.scope_promotion_proposed",
                memory_id=memory_id,
                summary=f"Scope promotion proposed: {current_scope} → {target_scope}",
                details={"proposal_id": proposal_id, "reason": reason.strip()},
                origin="memory_api",
            )
        except Exception:
            pass

        return {
            "status": "created",
            "proposal_id": proposal_id,
            "memory_id": memory_id,
            "current_scope": current_scope,
            "target_scope": target_scope,
            "reason": reason.strip(),
        }
    finally:
        conn.close()


@mcp.tool
def list_scope_promotion_proposals(
    status: str | None = None,
    workspace_key: str | None = None,
    memory_id: int | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """
    Wyświetla wnioski o promocję scope.
    status: 'pending' | 'approved' | 'rejected' (brak = wszystkie)
    workspace_key: filtruj po workspace
    memory_id: filtruj po konkretnym wspomnieniu
    """
    conn = get_db_connection()
    try:
        sql = "SELECT * FROM scope_promotion_proposals WHERE 1=1"
        params: list[Any] = []
        if status:
            sql += " AND status = ?"
            params.append(status.strip().lower())
        if workspace_key:
            ws_id = _resolve_workspace_id(conn, workspace_key)
            sql += " AND workspace_id = ?"
            params.append(ws_id)
        if memory_id is not None:
            sql += " AND memory_id = ?"
            params.append(int(memory_id))
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
        proposals = [row_to_dict(r) for r in rows]
        return {"status": "ok", "count": len(proposals), "proposals": proposals}
    finally:
        conn.close()


@mcp.tool
def approve_scope_promotion(
    proposal_id: int,
    reviewer_user_key: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """
    Zatwierdza wniosek o promocję scope i natychmiast zmienia visibility_scope wspomnienia.
    Prywatne wspomnienia nie mogą same awansować — wymagane jest jawne zatwierdzenie.
    """
    conn = get_db_connection()
    try:
        if not _is_multiuser_feature_active(conn, MULTIUSER_SCOPE_PROMOTION_FLAG):
            return {"status": "disabled", "message": f"Feature flag '{MULTIUSER_SCOPE_PROMOTION_FLAG}' is off."}

        proposal_row = conn.execute(
            "SELECT * FROM scope_promotion_proposals WHERE id = ?",
            (int(proposal_id),),
        ).fetchone()
        if proposal_row is None:
            raise ValueError(f"Wniosek #{proposal_id} nie istnieje")
        proposal = row_to_dict(proposal_row)

        if proposal["status"] != "pending":
            return {
                "status": "noop",
                "message": f"Wniosek #{proposal_id} ma status '{proposal['status']}' — nie można zatwierdzić.",
                "proposal": proposal,
            }

        # Resolve reviewer
        reviewed_by_user_id: int | None = None
        if reviewer_user_key:
            user_row = conn.execute("SELECT id FROM users WHERE external_user_key = ?", (reviewer_user_key.strip(),)).fetchone()
            if user_row:
                reviewed_by_user_id = int(user_row["id"])

        memory_id = int(proposal["memory_id"])
        target_scope = proposal["target_scope"]
        now = utc_now_iso()

        # Apply the scope change
        conn.execute(
            "UPDATE memories SET visibility_scope = ?, last_modified_by_user_id = ? WHERE id = ?",
            (target_scope, reviewed_by_user_id, memory_id),
        )

        # Mark proposal approved
        conn.execute(
            "UPDATE scope_promotion_proposals SET status = 'approved', reviewed_at = ?, reviewed_by_user_id = ?, review_note = ? WHERE id = ?",
            (now, reviewed_by_user_id, normalize_optional_text(note), int(proposal_id)),
        )
        conn.commit()

        updated_memory = row_to_dict(conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone())

        try:
            timeline.record_timeline_event(
                conn,
                event_type="sandman.scope_promotion_approved",
                memory_id=memory_id,
                summary=f"Scope promoted: {proposal['current_scope']} → {target_scope}",
                details={"proposal_id": proposal_id, "reviewed_by": reviewer_user_key, "note": note},
                origin="memory_api",
            )
        except Exception:
            pass

        return {
            "status": "approved",
            "proposal_id": proposal_id,
            "memory_id": memory_id,
            "old_scope": proposal["current_scope"],
            "new_scope": target_scope,
            "memory": updated_memory,
        }
    finally:
        conn.close()


@mcp.tool
def reject_scope_promotion(
    proposal_id: int,
    reviewer_user_key: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """
    Odrzuca wniosek o promocję scope. Nie zmienia visibility_scope wspomnienia.
    """
    conn = get_db_connection()
    try:
        if not _is_multiuser_feature_active(conn, MULTIUSER_SCOPE_PROMOTION_FLAG):
            return {"status": "disabled", "message": f"Feature flag '{MULTIUSER_SCOPE_PROMOTION_FLAG}' is off."}

        proposal_row = conn.execute(
            "SELECT * FROM scope_promotion_proposals WHERE id = ?",
            (int(proposal_id),),
        ).fetchone()
        if proposal_row is None:
            raise ValueError(f"Wniosek #{proposal_id} nie istnieje")
        proposal = row_to_dict(proposal_row)

        if proposal["status"] != "pending":
            return {
                "status": "noop",
                "message": f"Wniosek #{proposal_id} ma status '{proposal['status']}' — nie można odrzucić.",
                "proposal": proposal,
            }

        reviewed_by_user_id: int | None = None
        if reviewer_user_key:
            user_row = conn.execute("SELECT id FROM users WHERE external_user_key = ?", (reviewer_user_key.strip(),)).fetchone()
            if user_row:
                reviewed_by_user_id = int(user_row["id"])

        now = utc_now_iso()
        conn.execute(
            "UPDATE scope_promotion_proposals SET status = 'rejected', reviewed_at = ?, reviewed_by_user_id = ?, review_note = ? WHERE id = ?",
            (now, reviewed_by_user_id, normalize_optional_text(note), int(proposal_id)),
        )
        conn.commit()

        try:
            timeline.record_timeline_event(
                conn,
                event_type="sandman.scope_promotion_rejected",
                memory_id=int(proposal["memory_id"]),
                summary=f"Scope promotion rejected: {proposal['current_scope']} → {proposal['target_scope']}",
                details={"proposal_id": proposal_id, "reviewed_by": reviewer_user_key, "note": note},
                origin="memory_api",
            )
        except Exception:
            pass

        return {
            "status": "rejected",
            "proposal_id": proposal_id,
            "memory_id": int(proposal["memory_id"]),
            "current_scope": proposal["current_scope"],
            "target_scope": proposal["target_scope"],
        }
    finally:
        conn.close()


if __name__ == "__main__":
    mcp.run(transport="http", host="127.0.0.1", port=8015, path="/mcp/")








