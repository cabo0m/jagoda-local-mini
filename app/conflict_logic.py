from __future__ import annotations

import re
import sqlite3
from typing import Any

from app.memory_store import row_to_dict

CONTRADICTION_PHRASE_PAIRS: tuple[tuple[str, str], ...] = (
    ("działa", "nie działa"),
    ("jest", "nie jest"),
    ("ma", "nie ma"),
    ("można", "nie można"),
    ("mozna", "nie mozna"),
    ("aktywny", "nieaktywny"),
    ("aktywne", "nieaktywne"),
    ("włączone", "wyłączone"),
    ("wlaczone", "wylaczone"),
    ("enabled", "disabled"),
    ("works", "does not work"),
    ("is active", "is inactive"),
    ("has", "does not have"),
    ("true", "false"),
    ("tak", "nie"),
)

_MEMORY_CONTEXT_COLUMNS: tuple[str, ...] = (
    "id",
    "summary_short",
    "memory_type",
    "content",
    "source",
    "created_at",
    "valid_from",
    "valid_to",
    "contradiction_flag",
    "project_key",
    "confidence_score",
    "evidence_count",
)

_DIRECT_LINK_COLUMNS: tuple[str, ...] = (
    "id",
    "from_memory_id",
    "to_memory_id",
    "relation_type",
    "weight",
    "created_at",
    "archived_at",
)


def normalize_text_for_conflict(text: str | None) -> str:
    raw = (text or "").casefold()
    raw = re.sub(r"[^\w\s]", " ", raw, flags=re.UNICODE)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def normalize_summary_key(text: str | None) -> str:
    return normalize_text_for_conflict(text)


def normalize_project_key(value: str | None) -> str:
    return (value or "").strip().casefold()


def normalize_tag_set(tags: str | None) -> set[str]:
    if not tags:
        return set()
    parts = re.split(r"[,\s;|]+", tags.casefold())
    return {part.strip() for part in parts if len(part.strip()) >= 3}


def contradiction_link_exists(conn: sqlite3.Connection, memory_a_id: int, memory_b_id: int) -> bool:
    row = conn.execute(
        """
        SELECT id
        FROM memory_links
        WHERE relation_type = 'contradicts'
          AND (
                (from_memory_id = ? AND to_memory_id = ?)
                OR
                (from_memory_id = ? AND to_memory_id = ?)
          )
        LIMIT 1
        """,
        (memory_a_id, memory_b_id, memory_b_id, memory_a_id),
    ).fetchone()
    return row is not None


def duplicate_relation_exists_between(conn: sqlite3.Connection, memory_a_id: int, memory_b_id: int) -> bool:
    row = conn.execute(
        """
        SELECT id
        FROM memory_links
        WHERE relation_type = 'duplicate_of'
          AND (
                (from_memory_id = ? AND to_memory_id = ?)
                OR
                (from_memory_id = ? AND to_memory_id = ?)
          )
        LIMIT 1
        """,
        (memory_a_id, memory_b_id, memory_b_id, memory_a_id),
    ).fetchone()
    return row is not None


def _sorted_pair(memory_a_id: int, memory_b_id: int) -> tuple[int, int]:
    left_id, right_id = sorted((int(memory_a_id), int(memory_b_id)))
    return left_id, right_id


def _fetch_memory_context_row(conn: sqlite3.Connection, memory_id: int) -> dict[str, Any]:
    row = conn.execute(
        f"""
        SELECT {', '.join(_MEMORY_CONTEXT_COLUMNS)}
        FROM memories
        WHERE id = ?
        """,
        (int(memory_id),),
    ).fetchone()
    if row is None:
        raise FileNotFoundError(f"Nie znaleziono wspomnienia o id={memory_id}")
    return row_to_dict(row)


def _fetch_direct_links(conn: sqlite3.Connection, memory_a_id: int, memory_b_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT {', '.join(_DIRECT_LINK_COLUMNS)}
        FROM memory_links
        WHERE (
            (from_memory_id = ? AND to_memory_id = ?)
            OR
            (from_memory_id = ? AND to_memory_id = ?)
        )
        ORDER BY id ASC
        """,
        (memory_a_id, memory_b_id, memory_b_id, memory_a_id),
    ).fetchall()
    return [row_to_dict(row) for row in rows]


def _shared_value(left: Any, right: Any) -> Any:
    return left if left == right else None


def _query_summary_related_memories(
    conn: sqlite3.Connection,
    base_ids: tuple[int, int],
    normalized_summary: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT {', '.join(_MEMORY_CONTEXT_COLUMNS)}
        FROM memories
        WHERE id NOT IN (?, ?)
          AND COALESCE(activity_state, 'active') <> 'archived'
          AND summary_short IS NOT NULL
          AND trim(summary_short) <> ''
        ORDER BY id ASC
        """,
        base_ids,
    ).fetchall()
    return [
        row_to_dict(row)
        for row in rows
        if normalize_summary_key(row["summary_short"]) == normalized_summary
    ]


def _query_project_related_memories(
    conn: sqlite3.Connection,
    base_ids: tuple[int, int],
    project_key: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT {', '.join(_MEMORY_CONTEXT_COLUMNS)}
        FROM memories
        WHERE id NOT IN (?, ?)
          AND COALESCE(activity_state, 'active') <> 'archived'
          AND project_key = ?
        ORDER BY id ASC
        """,
        (base_ids[0], base_ids[1], project_key),
    ).fetchall()
    return [row_to_dict(row) for row in rows]


def _merge_context_memory(
    target: dict[int, dict[str, Any]],
    memory: dict[str, Any],
    reason: str,
) -> None:
    memory_id = int(memory["id"])
    existing = target.get(memory_id)
    if existing is None:
        merged = dict(memory)
        merged["context_reasons"] = [reason]
        target[memory_id] = merged
        return
    if reason not in existing["context_reasons"]:
        existing["context_reasons"].append(reason)
        existing["context_reasons"].sort()


def build_minimal_conflict_context(
    conn: sqlite3.Connection,
    memory_a_id: int,
    memory_b_id: int,
) -> dict[str, Any]:
    left_id, right_id = _sorted_pair(memory_a_id, memory_b_id)
    left_memory = _fetch_memory_context_row(conn, left_id)
    right_memory = _fetch_memory_context_row(conn, right_id)
    direct_links = _fetch_direct_links(conn, left_id, right_id)

    return {
        "memory_a_id": left_id,
        "memory_b_id": right_id,
        "summary_short_shared": _shared_value(left_memory.get("summary_short"), right_memory.get("summary_short")),
        "memory_type_shared": _shared_value(left_memory.get("memory_type"), right_memory.get("memory_type")),
        "base_memories": [left_memory, right_memory],
        "direct_links": direct_links,
        "contradiction_link_exists": contradiction_link_exists(conn, left_id, right_id),
    }


def build_conflict_context_bundle(
    conn: sqlite3.Connection,
    memory_a_id: int,
    memory_b_id: int,
    *,
    related_limit: int = 5,
) -> dict[str, Any]:
    minimal_context = build_minimal_conflict_context(conn, memory_a_id, memory_b_id)
    left_memory, right_memory = minimal_context["base_memories"]
    base_ids = (int(minimal_context["memory_a_id"]), int(minimal_context["memory_b_id"]))
    shared_summary_raw = minimal_context.get("summary_short_shared")
    shared_summary = normalize_summary_key(shared_summary_raw)
    shared_project_key = None
    if normalize_project_key(left_memory.get("project_key")) and normalize_project_key(left_memory.get("project_key")) == normalize_project_key(right_memory.get("project_key")):
        shared_project_key = left_memory.get("project_key")

    related_by_id: dict[int, dict[str, Any]] = {}

    if shared_summary:
        for memory in _query_summary_related_memories(conn, base_ids, shared_summary):
            _merge_context_memory(related_by_id, memory, "shared_summary_short")

    if shared_project_key:
        for memory in _query_project_related_memories(conn, base_ids, str(shared_project_key)):
            _merge_context_memory(related_by_id, memory, "shared_project_key")

    limit = max(0, int(related_limit))
    context_memories = [related_by_id[memory_id] for memory_id in sorted(related_by_id)]
    if len(context_memories) > limit:
        context_memories = context_memories[:limit]

    return {
        **minimal_context,
        "project_key_shared": shared_project_key,
        "related_limit": limit,
        "context_memory_count": len(context_memories),
        "context_memories": context_memories,
    }


def has_conflict_signal(text_a: str, text_b: str) -> bool:
    norm_a = normalize_text_for_conflict(text_a)
    norm_b = normalize_text_for_conflict(text_b)

    if not norm_a or not norm_b or norm_a == norm_b:
        return False

    for positive, negative in CONTRADICTION_PHRASE_PAIRS:
        pos = normalize_text_for_conflict(positive)
        neg = normalize_text_for_conflict(negative)
        if (pos in norm_a and neg in norm_b) or (pos in norm_b and neg in norm_a):
            return True

    for prefix in ("nie ", "not ", "no "):
        a_neg = norm_a.startswith(prefix)
        b_neg = norm_b.startswith(prefix)
        if a_neg != b_neg:
            stripped_a = norm_a[len(prefix):] if a_neg else norm_a
            stripped_b = norm_b[len(prefix):] if b_neg else norm_b
            if stripped_a == stripped_b and stripped_a:
                return True

    return False


_CLUSTER_LINK_TYPES = {"contradicts", "supersedes"}


def build_conflict_clusters(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Finds connected components in the conflict graph.

    Builds clusters from memories linked by 'contradicts' or 'supersedes' relations.
    Each cluster has a central memory (highest degree, tie-break by quality) and
    a divergence source (memory that causes the most direct contradictions, or lowest quality).
    """
    link_rows = conn.execute(
        "SELECT from_memory_id, to_memory_id, relation_type FROM memory_links "
        "WHERE relation_type IN ('contradicts', 'supersedes')"
    ).fetchall()

    adj: dict[int, set[int]] = {}
    edge_types: dict[tuple[int, int], str] = {}
    for row in link_rows:
        a, b, rtype = int(row[0]), int(row[1]), str(row[2])
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)
        edge_types[(a, b)] = rtype
        edge_types[(b, a)] = rtype

    all_ids = set(adj.keys())
    visited: set[int] = set()
    raw_clusters: list[set[int]] = []

    for start in sorted(all_ids):
        if start in visited:
            continue
        component: set[int] = set()
        queue = [start]
        while queue:
            node = queue.pop()
            if node in visited:
                continue
            visited.add(node)
            component.add(node)
            queue.extend(adj[node] - visited)
        raw_clusters.append(component)

    memory_cache: dict[int, dict[str, Any]] = {}
    needed_ids = list(all_ids)
    if needed_ids:
        placeholders = ",".join("?" * len(needed_ids))
        rows = conn.execute(
            f"SELECT * FROM memories WHERE id IN ({placeholders})", needed_ids
        ).fetchall()
        for r in rows:
            d = row_to_dict(r)
            memory_cache[int(d["id"])] = d

    clusters: list[dict[str, Any]] = []
    for cluster_idx, member_ids in enumerate(sorted(raw_clusters, key=lambda s: min(s))):
        member_list = sorted(member_ids)
        link_count = sum(
            1 for (a, b) in edge_types
            if a in member_ids and b in member_ids and a < b
        )

        # degree in the conflict subgraph for each member
        degree: dict[int, int] = {mid: 0 for mid in member_list}
        contradicts_out: dict[int, int] = {mid: 0 for mid in member_list}
        for (a, b), rtype in edge_types.items():
            if a in member_ids and b in member_ids:
                degree[a] += 1
                if rtype == "contradicts":
                    contradicts_out[a] += 1

        # Central memory: highest degree, tie-break by evidence_count desc
        def _centrality_key(mid: int) -> tuple[int, int, float]:
            mem = memory_cache.get(mid, {})
            return (degree[mid], int(mem.get("evidence_count") or 1), float(mem.get("importance_score") or 0.5))

        central_id = max(member_list, key=_centrality_key)
        central_degree = degree[central_id]
        central_reason = "highest_degree" if central_degree > 1 else "only_member" if len(member_list) == 1 else "single_link"

        # Divergence source: most contradicts_out links, tie-break by lowest quality
        def _divergence_key(mid: int) -> tuple[int, float]:
            mem = memory_cache.get(mid, {})
            conf = float(mem.get("confidence_score") or 0.5)
            evid = int(mem.get("evidence_count") or 1)
            return (contradicts_out[mid], -(conf + evid * 0.1))

        divergence_id = max(member_list, key=_divergence_key)
        divergence_reason: str | None = None
        if contradicts_out[divergence_id] > 0:
            divergence_reason = "most_contradictions_out"
        elif len(member_list) > 1:
            divergence_reason = "lowest_quality_in_cluster"

        has_unresolved = any(
            bool(int((memory_cache.get(mid) or {}).get("contradiction_flag") or 0))
            for mid in member_list
        )

        clusters.append({
            "cluster_id": cluster_idx + 1,
            "size": len(member_list),
            "member_ids": member_list,
            "central_memory_id": central_id,
            "central_reason": central_reason,
            "divergence_source_id": divergence_id if divergence_reason else None,
            "divergence_reason": divergence_reason,
            "conflict_link_count": link_count,
            "has_unresolved": has_unresolved,
        })

    return clusters


def _memories_share_conflict_scope(left: dict, right: dict) -> bool:
    """
    Sprawdza czy dwie pamięci mogą tworzyć parę konfliktową.

    Reguły bezpieczeństwa multi-user (Stage 1):
    - private: tylko między pamięciami tego samego owner_user_id
    - project: tylko w tym samym workspace_id i project_key
    - workspace: tylko w tym samym workspace_id
    - jeśli brak workspace_id (legacy): brak ograniczeń (backward compat)
    """
    left_ws = left.get("workspace_id")
    right_ws = right.get("workspace_id")

    # Legacy rekordy bez workspace — brak ograniczeń (backward compat)
    if left_ws is None or right_ws is None:
        return True

    # Różne workspace — nigdy nie tworzą pary
    if left_ws != right_ws:
        return False

    left_scope = left.get("visibility_scope") or "private"
    right_scope = right.get("visibility_scope") or "private"

    # Prywatna pamięć może wchodzić w konflikt tylko z pamięcią tego samego właściciela
    if left_scope == "private" or right_scope == "private":
        left_owner = left.get("owner_user_id")
        right_owner = right.get("owner_user_id")
        if left_owner is None or right_owner is None:
            return True  # legacy fallback
        return left_owner == right_owner

    # Projektowe — tylko w tym samym projekcie
    if left_scope == "project" or right_scope == "project":
        left_proj = left.get("project_key")
        right_proj = right.get("project_key")
        if left_proj and right_proj:
            return left_proj == right_proj

    return True


def get_conflict_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            a.id          AS memory_a_id,
            b.id          AS memory_b_id,
            a.memory_type AS memory_type,
            a.summary_short,
            a.content     AS content_a,
            b.content     AS content_b,
            a.tags        AS tags_a,
            b.tags        AS tags_b
        FROM memories a
        JOIN memories b
          ON a.id < b.id
         AND a.memory_type = b.memory_type
         -- Same summary required (pre-filter; normalized check done in Python below)
         AND a.summary_short IS NOT NULL
         AND b.summary_short IS NOT NULL
         AND a.summary_short = b.summary_short
         -- Workspace scope isolation: same workspace OR either is legacy (NULL workspace_id)
         AND (a.workspace_id IS NULL OR b.workspace_id IS NULL OR a.workspace_id = b.workspace_id)
         -- Private scope isolation: if either memory is private, owner must match (NULL = legacy, allow)
         AND (
             (COALESCE(a.visibility_scope, 'private') != 'private'
              AND COALESCE(b.visibility_scope, 'private') != 'private')
             OR a.owner_user_id IS NULL
             OR b.owner_user_id IS NULL
             OR a.owner_user_id = b.owner_user_id
         )
         -- Project scope isolation: if either is project (and neither is private), project must match
         AND (
             COALESCE(a.visibility_scope, 'private') = 'private'
             OR COALESCE(b.visibility_scope, 'private') = 'private'
             OR (COALESCE(a.visibility_scope, 'private') != 'project'
                 AND COALESCE(b.visibility_scope, 'private') != 'project')
             OR a.project_key IS NULL
             OR b.project_key IS NULL
             OR a.project_key = b.project_key
         )
        WHERE COALESCE(a.activity_state, 'active') <> 'archived'
          AND COALESCE(b.activity_state, 'active') <> 'archived'
        ORDER BY a.id ASC, b.id ASC
        """
    ).fetchall()

    pairs: list[dict[str, Any]] = []

    for row in rows:
        item = row_to_dict(row)
        memory_a_id = int(item["memory_a_id"])
        memory_b_id = int(item["memory_b_id"])

        if duplicate_relation_exists_between(conn, memory_a_id, memory_b_id):
            continue

        content_a = str(item.get("content_a") or "")
        content_b = str(item.get("content_b") or "")
        if normalize_text_for_conflict(content_a) == normalize_text_for_conflict(content_b):
            continue
        if not has_conflict_signal(content_a, content_b):
            continue

        pairs.append(
            {
                "memory_a_id": memory_a_id,
                "memory_b_id": memory_b_id,
                "summary_short": item.get("summary_short"),
                "memory_type": item.get("memory_type"),
                "content_a": content_a,
                "content_b": content_b,
                "tags_shared": sorted(
                    normalize_tag_set(item.get("tags_a")) & normalize_tag_set(item.get("tags_b"))
                ),
                "contradiction_link_exists": contradiction_link_exists(conn, memory_a_id, memory_b_id),
            }
        )

    return pairs
