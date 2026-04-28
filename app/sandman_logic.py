from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

from app.conflict_logic import (
    contradiction_link_exists,
    has_conflict_signal,
    normalize_summary_key,
    normalize_text_for_conflict,
)
from app.memory_store import require_memory_row, row_to_dict
from app.schemas import SANDMAN_PROTECTED_LAYERS, SANDMAN_PROTECTED_STATES


def _build_scope_filter(workspace_id: int | None, project_key: str | None) -> tuple[str, list]:
    """Returns (sql_fragment, params) for optional workspace/project scope filtering."""
    clauses: list[str] = []
    params: list = []
    if workspace_id is not None:
        clauses.append("workspace_id = ?")
        params.append(int(workspace_id))
    if project_key is not None:
        clauses.append("project_key = ?")
        params.append(str(project_key))
    if not clauses:
        return "", []
    return " AND " + " AND ".join(clauses), params


def get_archive_candidates(
    conn: sqlite3.Connection,
    workspace_id: int | None = None,
    project_key: str | None = None,
) -> list[sqlite3.Row]:
    """Zwraca kandydatów do archiwizacji.

    Sprint 5: wyklucza warstwy chronione (core, identity) oraz state_code=validated.
    Faza 3: opcjonalne filtrowanie po workspace_id / project_key.
    """
    protected_layers_sql = ", ".join(f"'{lc}'" for lc in sorted(SANDMAN_PROTECTED_LAYERS))
    protected_states_sql = ", ".join(f"'{sc}'" for sc in sorted(SANDMAN_PROTECTED_STATES))
    scope_sql, scope_params = _build_scope_filter(workspace_id, project_key)
    return conn.execute(
        f"""
        SELECT *
        FROM memories
        WHERE memory_type = 'working'
          AND importance_score <= 0.35
          AND recall_count = 0
          AND COALESCE(activity_state, 'active') = 'active'
          AND COALESCE(contradiction_flag, 0) = 0
          AND (layer_code IS NULL OR layer_code NOT IN ({protected_layers_sql}))
          AND (state_code IS NULL OR state_code NOT IN ({protected_states_sql}))
          {scope_sql}
        ORDER BY importance_score ASC, id ASC
        """,
        scope_params,
    ).fetchall()


def get_downgrade_candidates(
    conn: sqlite3.Connection,
    workspace_id: int | None = None,
    project_key: str | None = None,
) -> list[sqlite3.Row]:
    """Zwraca kandydatów do obniżenia importance_score.

    Sprint 5: wyklucza warstwy chronione (core, identity) oraz state_code=validated.
    Faza 3: opcjonalne filtrowanie po workspace_id / project_key.
    """
    protected_layers_sql = ", ".join(f"'{lc}'" for lc in sorted(SANDMAN_PROTECTED_LAYERS))
    protected_states_sql = ", ".join(f"'{sc}'" for sc in sorted(SANDMAN_PROTECTED_STATES))
    scope_sql, scope_params = _build_scope_filter(workspace_id, project_key)
    return conn.execute(
        f"""
        SELECT *
        FROM memories
        WHERE COALESCE(activity_state, 'active') = 'active'
          AND recall_count <= 1
          AND importance_score > 0.35
          AND importance_score <= 0.55
          AND COALESCE(contradiction_flag, 0) = 0
          AND (layer_code IS NULL OR layer_code NOT IN ({protected_layers_sql}))
          AND (state_code IS NULL OR state_code NOT IN ({protected_states_sql}))
          {scope_sql}
        ORDER BY importance_score ASC, id ASC
        """,
        scope_params,
    ).fetchall()


def get_promotion_candidates(
    conn: sqlite3.Connection,
    *,
    source_layer: str | None = None,
    workspace_id: int | None = None,
    project_key: str | None = None,
    min_evidence: int = 2,
    min_importance: float = 0.6,
    min_confidence: float = 0.6,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Zwraca wspomnienia kwalifikujące się do awansu na wyższą warstwę.

    Kryteria domyślne:
    - evidence_count >= min_evidence (domyślnie 2 — ktoś potwierdził)
    - importance_score >= min_importance (domyślnie 0.6)
    - confidence_score >= min_confidence (domyślnie 0.6)
    - activity_state = 'active'
    - contradiction_flag = 0
    - state_code != 'archived', 'superseded', 'conflicted'
    - opcjonalne: source_layer, workspace_id, project_key
    """
    params: list[Any] = [int(min_evidence), float(min_importance), float(min_confidence)]
    extra_filters = ""
    if source_layer:
        extra_filters += " AND layer_code = ?"
        params.append(str(source_layer).strip().lower())
    scope_sql, scope_params = _build_scope_filter(workspace_id, project_key)
    if scope_sql:
        extra_filters += scope_sql
        params.extend(scope_params)

    params.append(int(limit))
    rows = conn.execute(
        f"""
        SELECT *
        FROM memories
        WHERE evidence_count >= ?
          AND importance_score >= ?
          AND confidence_score >= ?
          AND COALESCE(activity_state, 'active') = 'active'
          AND COALESCE(contradiction_flag, 0) = 0
          AND (state_code IS NULL OR state_code NOT IN ('archived', 'superseded', 'conflicted'))
          {extra_filters}
        ORDER BY evidence_count DESC, importance_score DESC, id ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [row_to_dict(row) for row in rows]


def get_content_token_set(text: str | None) -> set[str]:
    normalized = normalize_text_for_conflict(text)
    return {token for token in normalized.split() if len(token) >= 3}


def are_duplicate_contents(text_a: str | None, text_b: str | None) -> bool:
    norm_a = normalize_text_for_conflict(text_a)
    norm_b = normalize_text_for_conflict(text_b)

    if not norm_a or not norm_b:
        return False
    if norm_a == norm_b:
        return True
    if has_conflict_signal(norm_a, norm_b):
        return False

    tokens_a = get_content_token_set(norm_a)
    tokens_b = get_content_token_set(norm_b)
    if not tokens_a or not tokens_b:
        return False

    union = tokens_a | tokens_b
    if not union:
        return False

    jaccard = len(tokens_a & tokens_b) / len(union)
    if jaccard >= 0.85:
        return True

    shorter_len = min(len(norm_a), len(norm_b))
    longer_len = max(len(norm_a), len(norm_b))
    if shorter_len >= 20 and shorter_len / longer_len >= 0.85:
        if norm_a in norm_b or norm_b in norm_a:
            return True

    return False


def duplicate_link_exists(conn: sqlite3.Connection, from_memory_id: int, to_memory_id: int) -> bool:
    row = conn.execute(
        """
        SELECT id
        FROM memory_links
        WHERE from_memory_id = ?
          AND to_memory_id = ?
          AND relation_type = 'duplicate_of'
        LIMIT 1
        """,
        (from_memory_id, to_memory_id),
    ).fetchone()
    return row is not None


def _safe_timestamp(value: str | None) -> float:
    if not value:
        return 0.0
    candidate = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate).timestamp()
    except ValueError:
        return 0.0


def _content_length(value: str | None) -> int:
    if value is None:
        return 0
    return len(str(value).strip())


def _consolidated_from_count(conn: sqlite3.Connection, memory_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM memory_links
        WHERE from_memory_id = ?
          AND relation_type = 'consolidated_from'
        """,
        (int(memory_id),),
    ).fetchone()
    return int(row["count"] or 0)


def _load_memory_profiles(conn: sqlite3.Connection, memory_ids: set[int]) -> dict[int, dict[str, Any]]:
    if not memory_ids:
        return {}
    placeholders = ",".join("?" for _ in memory_ids)
    rows = conn.execute(
        f"""
        SELECT *
        FROM memories
        WHERE id IN ({placeholders})
        """,
        tuple(sorted(int(item) for item in memory_ids)),
    ).fetchall()
    profiles: dict[int, dict[str, Any]] = {}
    for row in rows:
        item = row_to_dict(row)
        memory_id = int(item["id"])
        item["consolidated_from_count"] = _consolidated_from_count(conn, memory_id)
        item["content_length"] = _content_length(item.get("content"))
        item["created_at_ts"] = _safe_timestamp(item.get("created_at"))
        profiles[memory_id] = item
    return profiles


def _canonical_sort_key(profile: dict[str, Any]) -> tuple[Any, ...]:
    memory_type = str(profile.get("memory_type") or "")
    if memory_type == "consolidated_summary":
        return (
            -int(profile.get("consolidated_from_count") or 0),
            -int(profile.get("content_length") or 0),
            -float(profile.get("confidence_score") or 0.0),
            -float(profile.get("evidence_count") or 1),
            -float(profile.get("importance_score") or 0.0),
            -float(profile.get("created_at_ts") or 0.0),
            -int(profile.get("id") or 0),
        )

    return (
        -float(profile.get("evidence_count") or 1),
        -float(profile.get("confidence_score") or 0.0),
        -float(profile.get("importance_score") or 0.0),
        -int(profile.get("recall_count") or 0),
        float(profile.get("created_at_ts") or 0.0),
        int(profile.get("id") or 0),
    )


def _pick_component_canonical(component_ids: set[int], profiles: dict[int, dict[str, Any]]) -> int:
    ordered = sorted(component_ids, key=lambda item: _canonical_sort_key(profiles[int(item)]))
    return int(ordered[0])


def _connected_components(adjacency: dict[int, set[int]]) -> list[set[int]]:
    pending = set(adjacency.keys())
    components: list[set[int]] = []
    while pending:
        start = pending.pop()
        stack = [start]
        component = {start}
        while stack:
            current = stack.pop()
            for neighbor in adjacency.get(current, set()):
                if neighbor in component:
                    continue
                component.add(neighbor)
                if neighbor in pending:
                    pending.remove(neighbor)
                stack.append(neighbor)
        components.append(component)
    return components


def get_duplicate_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            a.id AS memory_a_id,
            b.id AS memory_b_id,
            a.memory_type AS memory_type,
            a.summary_short AS summary_a,
            b.summary_short AS summary_b,
            a.content AS content_a,
            b.content AS content_b,
            a.tags AS tags_a,
            b.tags AS tags_b,
            a.contradiction_flag AS contradiction_flag_a,
            b.contradiction_flag AS contradiction_flag_b
        FROM memories a
        JOIN memories b
          ON a.id < b.id
         AND a.memory_type = b.memory_type
         -- Scope isolation: only compare memories within the same visibility scope
         AND COALESCE(a.visibility_scope, 'private') = COALESCE(b.visibility_scope, 'private')
         -- Private memories: same owner only
         AND (COALESCE(a.visibility_scope, 'private') != 'private' OR a.owner_user_id IS b.owner_user_id)
         -- Workspace memories: same workspace only
         AND (COALESCE(a.visibility_scope, 'private') != 'workspace' OR a.workspace_id IS b.workspace_id)
         -- Project memories: same project only
         AND (COALESCE(a.visibility_scope, 'private') != 'project' OR a.project_key IS b.project_key)
        WHERE COALESCE(a.activity_state, 'active') <> 'archived'
          AND COALESCE(b.activity_state, 'active') <> 'archived'
        ORDER BY a.id ASC, b.id ASC
        """
    ).fetchall()

    pair_lookup: dict[frozenset[int], dict[str, Any]] = {}
    adjacency: dict[int, set[int]] = {}
    memory_ids: set[int] = set()

    for row in rows:
        item = row_to_dict(row)
        memory_a_id = int(item["memory_a_id"])
        memory_b_id = int(item["memory_b_id"])

        if int(item.get("contradiction_flag_a") or 0) == 1:
            continue
        if int(item.get("contradiction_flag_b") or 0) == 1:
            continue
        if contradiction_link_exists(conn, memory_a_id, memory_b_id):
            continue

        content_a = item.get("content_a")
        content_b = item.get("content_b")
        summary_a = normalize_summary_key(item.get("summary_a"))
        summary_b = normalize_summary_key(item.get("summary_b"))

        same_normalized_content = normalize_text_for_conflict(content_a) == normalize_text_for_conflict(content_b)
        high_similarity_same_topic = bool(summary_a) and summary_a == summary_b and are_duplicate_contents(content_a, content_b)

        if not same_normalized_content and not high_similarity_same_topic:
            continue

        pair_lookup[frozenset({memory_a_id, memory_b_id})] = item
        adjacency.setdefault(memory_a_id, set()).add(memory_b_id)
        adjacency.setdefault(memory_b_id, set()).add(memory_a_id)
        memory_ids.add(memory_a_id)
        memory_ids.add(memory_b_id)

    profiles = _load_memory_profiles(conn, memory_ids)
    components = _connected_components(adjacency)

    candidates: list[dict[str, Any]] = []
    for component in components:
        canonical_id = _pick_component_canonical(component, profiles)
        for member_id in sorted(component):
            if member_id == canonical_id:
                continue
            pair = pair_lookup.get(frozenset({canonical_id, member_id}))
            if pair is None:
                continue
            candidate = dict(pair)
            candidate["canonical_memory_id"] = canonical_id
            candidate["duplicate_memory_id"] = int(member_id)
            candidate["relation_type"] = "duplicate_of"
            candidates.append(candidate)

    return candidates


def get_canonical_memory_ids(duplicate_candidates: list[dict[str, Any]]) -> set[int]:
    canonical_ids = {int(pair["canonical_memory_id"]) for pair in duplicate_candidates}
    duplicate_ids = {int(pair["duplicate_memory_id"]) for pair in duplicate_candidates}
    return canonical_ids - duplicate_ids


def get_protected_canonical_memory_ids(conn: sqlite3.Connection, duplicate_candidates: list[dict[str, Any]]) -> set[int]:
    ids = set(get_canonical_memory_ids(duplicate_candidates))
    linked_rows = conn.execute(
        """
        SELECT DISTINCT to_memory_id
        FROM memory_links
        WHERE relation_type = 'duplicate_of'
        """
    ).fetchall()
    ids.update(int(row["to_memory_id"]) for row in linked_rows)
    return ids


def get_secondary_duplicate_memory_ids(conn: sqlite3.Connection, duplicate_candidates: list[dict[str, Any]]) -> set[int]:
    ids = {int(pair["duplicate_memory_id"]) for pair in duplicate_candidates}
    linked_rows = conn.execute(
        """
        SELECT DISTINCT from_memory_id
        FROM memory_links
        WHERE relation_type = 'duplicate_of'
        """
    ).fetchall()
    ids.update(int(row["from_memory_id"]) for row in linked_rows)
    return ids


def filter_archive_candidates_for_duplicates(
    conn: sqlite3.Connection,
    archive_candidates: list[sqlite3.Row],
    duplicate_candidates: list[dict[str, Any]],
) -> tuple[list[sqlite3.Row], list[dict[str, Any]]]:
    protected_canonical_ids = get_protected_canonical_memory_ids(conn, duplicate_candidates)
    secondary_duplicate_ids = get_secondary_duplicate_memory_ids(conn, duplicate_candidates)

    filtered_map: dict[int, sqlite3.Row] = {}
    skipped: list[dict[str, Any]] = []

    for row in archive_candidates:
        row_id = int(row["id"])
        row_dict = row_to_dict(row)
        if row_id in protected_canonical_ids:
            row_dict["skip_reason"] = "duplicate_pair_canonical_protected"
            skipped.append(row_dict)
        else:
            filtered_map[row_id] = row

    if secondary_duplicate_ids:
        placeholders = ",".join("?" for _ in secondary_duplicate_ids)
        preferred_rows = conn.execute(
            f"""
            SELECT *
            FROM memories
            WHERE id IN ({placeholders})
              AND COALESCE(activity_state, 'active') = 'active'
              AND recall_count <= 1
              AND importance_score <= 0.55
              AND COALESCE(contradiction_flag, 0) = 0
            ORDER BY importance_score ASC, id ASC
            """,
            tuple(sorted(secondary_duplicate_ids)),
        ).fetchall()
        for row in preferred_rows:
            row_id = int(row["id"])
            if row_id not in protected_canonical_ids:
                filtered_map[row_id] = row

    filtered = sorted(filtered_map.values(), key=lambda row: (float(row["importance_score"]), int(row["id"])))
    return filtered, skipped


def filter_downgrade_candidates_for_duplicates(
    conn: sqlite3.Connection,
    downgrade_candidates: list[sqlite3.Row],
    duplicate_candidates: list[dict[str, Any]],
) -> tuple[list[sqlite3.Row], list[dict[str, Any]]]:
    protected_canonical_ids = get_protected_canonical_memory_ids(conn, duplicate_candidates)
    secondary_duplicate_ids = get_secondary_duplicate_memory_ids(conn, duplicate_candidates)

    filtered: list[sqlite3.Row] = []
    skipped: list[dict[str, Any]] = []

    for row in downgrade_candidates:
        row_id = int(row["id"])
        row_dict = row_to_dict(row)
        if row_id in protected_canonical_ids:
            row_dict["skip_reason"] = "duplicate_pair_canonical_protected"
            skipped.append(row_dict)
        elif row_id in secondary_duplicate_ids:
            row_dict["skip_reason"] = "duplicate_pair_secondary_prefer_archive"
            skipped.append(row_dict)
        else:
            filtered.append(row)

    return filtered, skipped


def get_incoming_duplicate_count(conn: sqlite3.Connection, canonical_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM memory_links
        WHERE to_memory_id = ?
          AND relation_type = 'duplicate_of'
        """,
        (canonical_id,),
    ).fetchone()
    return int(row["count"])


def boost_canonical_evidence_count(conn: sqlite3.Connection, canonical_id: int) -> dict[str, Any] | None:
    memory = require_memory_row(conn, canonical_id)
    current_evidence = int(memory["evidence_count"] or 1)
    target_evidence = max(current_evidence, 1 + get_incoming_duplicate_count(conn, canonical_id))

    if target_evidence == current_evidence:
        return None

    conn.execute(
        """
        UPDATE memories
        SET evidence_count = ?,
            sandman_note = ?
        WHERE id = ?
        """,
        (target_evidence, f"Sandman V1: canonical evidence boosted to {target_evidence}", canonical_id),
    )

    return {
        "memory_id": canonical_id,
        "old_evidence_count": current_evidence,
        "new_evidence_count": target_evidence,
    }
