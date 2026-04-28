from __future__ import annotations

import sqlite3
from typing import Any

from app.conflict_logic import (
    contradiction_link_exists,
    has_conflict_signal,
    normalize_summary_key,
    normalize_tag_set,
    normalize_text_for_conflict,
)
from app.memory_store import row_to_dict

MAX_EVIDENCE_NORMALIZATION = 5.0
MAX_RECALL_NORMALIZATION = 5.0
MIN_PAIR_GRAVITY = 0.08
MIN_CONTENT_SIMILARITY = 0.45
MIN_SUMMARY_SIMILARITY = 0.60
SUMMARY_REUSE_MIN_OVERLAP = 0.60


def _token_set(text: str | None) -> set[str]:
    normalized = normalize_text_for_conflict(text)
    return {token for token in normalized.split() if len(token) >= 3}


def _jaccard_score(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def relation_link_exists(conn: sqlite3.Connection, from_memory_id: int, to_memory_id: int, relation_type: str) -> bool:
    row = conn.execute(
        """
        SELECT id
        FROM memory_links
        WHERE from_memory_id = ?
          AND to_memory_id = ?
          AND relation_type = ?
        LIMIT 1
        """,
        (from_memory_id, to_memory_id, relation_type),
    ).fetchone()
    return row is not None


def support_link_exists(conn: sqlite3.Connection, from_memory_id: int, to_memory_id: int) -> bool:
    return relation_link_exists(conn, from_memory_id, to_memory_id, "supports")


def summary_link_exists(conn: sqlite3.Connection, from_memory_id: int, to_memory_id: int, relation_type: str) -> bool:
    return relation_link_exists(conn, from_memory_id, to_memory_id, relation_type)


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


def memory_mass(memory: dict[str, Any]) -> float:
    importance = float(memory.get("importance_score") or 0.0)
    confidence = float(memory.get("confidence_score") or 0.0)
    evidence = min(float(memory.get("evidence_count") or 1), MAX_EVIDENCE_NORMALIZATION) / MAX_EVIDENCE_NORMALIZATION
    recall = min(float(memory.get("recall_count") or 0), MAX_RECALL_NORMALIZATION) / MAX_RECALL_NORMALIZATION
    mass = (0.35 * importance) + (0.25 * confidence) + (0.20 * evidence) + (0.20 * recall)
    return max(0.0, min(mass, 1.0))


def summary_similarity_score(summary_a: str | None, summary_b: str | None) -> float:
    normalized_a = normalize_summary_key(summary_a)
    normalized_b = normalize_summary_key(summary_b)
    if not normalized_a or not normalized_b:
        return 0.0
    if normalized_a == normalized_b:
        return 1.0
    return _jaccard_score(set(normalized_a.split()), set(normalized_b.split()))


def content_similarity_score(content_a: str | None, content_b: str | None) -> float:
    normalized_a = normalize_text_for_conflict(content_a)
    normalized_b = normalize_text_for_conflict(content_b)
    if not normalized_a or not normalized_b:
        return 0.0
    if normalized_a == normalized_b:
        return 1.0
    return _jaccard_score(_token_set(normalized_a), _token_set(normalized_b))


def tags_overlap_score(tags_a: str | None, tags_b: str | None) -> float:
    return _jaccard_score(normalize_tag_set(tags_a), normalize_tag_set(tags_b))


def pair_metrics(conn: sqlite3.Connection, left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any] | None:
    if int(left["id"]) == int(right["id"]):
        return None
    if left.get("memory_type") != right.get("memory_type"):
        return None
    if str(left.get("memory_type") or "") == "consolidated_summary":
        return None
    if int(left.get("contradiction_flag") or 0) == 1 or int(right.get("contradiction_flag") or 0) == 1:
        return None
    if contradiction_link_exists(conn, int(left["id"]), int(right["id"])):
        return None

    left_content = str(left.get("content") or "")
    right_content = str(right.get("content") or "")
    if has_conflict_signal(left_content, right_content):
        return None

    summary_similarity = summary_similarity_score(left.get("summary_short"), right.get("summary_short"))
    content_similarity = content_similarity_score(left_content, right_content)
    tags_similarity = tags_overlap_score(left.get("tags"), right.get("tags"))
    same_memory_type_bonus = 1.0 if left.get("memory_type") == right.get("memory_type") else 0.0
    duplicate_bonus = 1.0 if duplicate_relation_exists_between(conn, int(left["id"]), int(right["id"])) else 0.0

    attraction = (
        (0.35 * summary_similarity)
        + (0.35 * content_similarity)
        + (0.15 * tags_similarity)
        + (0.10 * same_memory_type_bonus)
        + (0.05 * duplicate_bonus)
    )
    gravity_score = memory_mass(left) * memory_mass(right) * attraction

    if gravity_score < MIN_PAIR_GRAVITY:
        return None
    if content_similarity < MIN_CONTENT_SIMILARITY and summary_similarity < MIN_SUMMARY_SIMILARITY and duplicate_bonus == 0.0:
        return None

    return {
        "memory_a_id": int(left["id"]),
        "memory_b_id": int(right["id"]),
        "gravity_score": round(gravity_score, 4),
        "summary_similarity": round(summary_similarity, 4),
        "content_similarity": round(content_similarity, 4),
        "tags_similarity": round(tags_similarity, 4),
        "duplicate_bonus": round(duplicate_bonus, 4),
        "mass_a": round(memory_mass(left), 4),
        "mass_b": round(memory_mass(right), 4),
    }


def get_eligible_memories(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM memories
        WHERE COALESCE(activity_state, 'active') = 'active'
          AND COALESCE(contradiction_flag, 0) = 0
          AND memory_type <> 'consolidated_summary'
        ORDER BY id ASC
        """
    ).fetchall()
    return [row_to_dict(row) for row in rows]


def _memories_share_consolidation_scope(left: dict, right: dict) -> bool:
    """
    Sprawdza czy dwie pamięci mogą być konsolidowane razem.

    Reguły bezpieczeństwa multi-user (Stage 1):
    - różne workspace → nigdy nie scalaj
    - private z różnymi ownerami → nie scalaj
    - legacy bez workspace_id → brak ograniczeń (backward compat)
    """
    left_ws = left.get("workspace_id")
    right_ws = right.get("workspace_id")

    if left_ws is None or right_ws is None:
        return True  # legacy fallback

    if left_ws != right_ws:
        return False

    left_scope = left.get("visibility_scope") or "private"
    right_scope = right.get("visibility_scope") or "private"

    if left_scope == "private" or right_scope == "private":
        left_owner = left.get("owner_user_id")
        right_owner = right.get("owner_user_id")
        if left_owner is None or right_owner is None:
            return True  # legacy fallback
        return left_owner == right_owner

    return True


def get_consolidation_pairs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    memories = get_eligible_memories(conn)
    pairs: list[dict[str, Any]] = []
    for index, left in enumerate(memories):
        for right in memories[index + 1:]:
            # Filtr bezpieczeństwa multi-user: nie scalaj pamięci między userami/workspace
            if not _memories_share_consolidation_scope(left, right):
                continue
            metrics = pair_metrics(conn, left, right)
            if metrics is not None:
                pairs.append(metrics)
    return pairs


def _build_clusters(memories: list[dict[str, Any]], pairs: list[dict[str, Any]]) -> list[list[int]]:
    adjacency: dict[int, set[int]] = {int(item["id"]): set() for item in memories}
    for pair in pairs:
        left_id = int(pair["memory_a_id"])
        right_id = int(pair["memory_b_id"])
        adjacency.setdefault(left_id, set()).add(right_id)
        adjacency.setdefault(right_id, set()).add(left_id)

    visited: set[int] = set()
    clusters: list[list[int]] = []
    for memory_id, neighbors in adjacency.items():
        if memory_id in visited or not neighbors:
            continue
        stack = [memory_id]
        cluster: list[int] = []
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            cluster.append(current)
            stack.extend(sorted(adjacency.get(current, set()) - visited))
        if len(cluster) >= 2:
            clusters.append(sorted(cluster))
    return clusters


def _dominant_summary(cluster_members: list[dict[str, Any]]) -> str:
    weighted_candidates: list[tuple[float, str]] = []
    for item in cluster_members:
        summary = str(item.get("summary_short") or "").strip()
        if not summary:
            continue
        weighted_candidates.append((memory_mass(item), summary))
    if not weighted_candidates:
        return f"Konsolidacja {cluster_members[0]['memory_type']}"
    weighted_candidates.sort(key=lambda entry: (-entry[0], entry[1].lower()))
    return weighted_candidates[0][1]


def _summary_content(cluster_members: list[dict[str, Any]], central_memory_id: int) -> str:
    lines = [f"Skonsolidowany rdzeń pamięci wokół wpisu {central_memory_id}."]
    for item in cluster_members:
        lines.append(f"- [{item['id']}] {str(item.get('content') or '').strip()}")
    return "\n".join(lines)


def _summary_tags(cluster_members: list[dict[str, Any]]) -> str | None:
    merged: set[str] = set()
    for item in cluster_members:
        merged.update(normalize_tag_set(item.get("tags")))
    if not merged:
        return None
    return ",".join(sorted(merged))


def _summary_confidence(cluster_members: list[dict[str, Any]]) -> float:
    if not cluster_members:
        return 0.5
    return round(sum(float(item.get("confidence_score") or 0.5) for item in cluster_members) / len(cluster_members), 3)


def _summary_importance(cluster_members: list[dict[str, Any]]) -> float:
    if not cluster_members:
        return 0.5
    return round(max(float(item.get("importance_score") or 0.5) for item in cluster_members), 3)


def _existing_support_count(conn: sqlite3.Connection, central_memory_id: int, member_ids: list[int]) -> int:
    if not member_ids:
        return 0
    placeholders = ",".join("?" for _ in member_ids)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM memory_links
        WHERE relation_type = 'supports'
          AND to_memory_id = ?
          AND from_memory_id IN ({placeholders})
        """,
        (central_memory_id, *member_ids),
    ).fetchone()
    return int(row["count"])


def _summary_source_ids(conn: sqlite3.Connection, summary_memory_id: int) -> set[int]:
    rows = conn.execute(
        """
        SELECT to_memory_id
        FROM memory_links
        WHERE from_memory_id = ?
          AND relation_type = 'consolidated_from'
        ORDER BY to_memory_id ASC
        """,
        (summary_memory_id,),
    ).fetchall()
    return {int(item["to_memory_id"]) for item in rows}


def _summary_overlap_score(source_ids: set[int], member_ids: set[int]) -> float:
    if not source_ids or not member_ids:
        return 0.0
    union = source_ids | member_ids
    if not union:
        return 0.0
    return len(source_ids & member_ids) / len(union)


def _summary_memory_payload(summary_memory: dict[str, Any]) -> dict[str, Any]:
    return {
        "summary_short": summary_memory.get("summary_short"),
        "content": summary_memory.get("content"),
        "source": summary_memory.get("source"),
        "importance_score": round(float(summary_memory.get("importance_score") or 0.5), 3),
        "confidence_score": round(float(summary_memory.get("confidence_score") or 0.5), 3),
        "tags": summary_memory.get("tags"),
    }


def _find_reusable_summary_memory_id(conn: sqlite3.Connection, central_memory_id: int, member_ids: list[int]) -> tuple[int | None, set[int], float]:
    rows = conn.execute(
        """
        SELECT DISTINCT m.*
        FROM memories m
        JOIN memory_links l
          ON l.from_memory_id = m.id
         AND l.relation_type = 'summarizes'
         AND l.to_memory_id = ?
        WHERE m.memory_type = 'consolidated_summary'
          AND COALESCE(m.activity_state, 'active') = 'active'
        ORDER BY m.id ASC
        """,
        (central_memory_id,),
    ).fetchall()
    member_id_set = {int(item) for item in member_ids}
    best_summary_id: int | None = None
    best_source_ids: set[int] = set()
    best_score = 0.0
    for row in rows:
        summary_id = int(row["id"])
        source_ids = _summary_source_ids(conn, summary_id)
        score = _summary_overlap_score(source_ids, member_id_set)
        if score >= SUMMARY_REUSE_MIN_OVERLAP and (score > best_score or (score == best_score and (best_summary_id is None or summary_id < best_summary_id))):
            best_summary_id = summary_id
            best_source_ids = source_ids
            best_score = score
    return best_summary_id, best_source_ids, round(best_score, 4)


def _missing_summary_links_count(conn: sqlite3.Connection, summary_memory_id: int, central_memory_id: int, member_ids: list[int]) -> int:
    missing = 0
    if not summary_link_exists(conn, summary_memory_id, central_memory_id, "summarizes"):
        missing += 1
    for member_id in member_ids:
        if not summary_link_exists(conn, summary_memory_id, int(member_id), "consolidated_from"):
            missing += 1
    return missing


def _stale_summary_links_count(conn: sqlite3.Connection, summary_memory_id: int, central_memory_id: int, member_ids: list[int]) -> int:
    stale = 0
    summarize_rows = conn.execute(
        """
        SELECT to_memory_id
        FROM memory_links
        WHERE from_memory_id = ?
          AND relation_type = 'summarizes'
        ORDER BY to_memory_id ASC
        """,
        (summary_memory_id,),
    ).fetchall()
    for row in summarize_rows:
        if int(row["to_memory_id"]) != int(central_memory_id):
            stale += 1

    existing_sources = _summary_source_ids(conn, summary_memory_id)
    stale += len(existing_sources - {int(item) for item in member_ids})
    return stale


def get_consolidation_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    memories = get_eligible_memories(conn)
    memory_map = {int(item["id"]): item for item in memories}
    pairs = get_consolidation_pairs(conn)
    pair_map: dict[tuple[int, int], dict[str, Any]] = {}
    for pair in pairs:
        key = tuple(sorted((int(pair["memory_a_id"]), int(pair["memory_b_id"]))))
        pair_map[key] = pair

    candidates: list[dict[str, Any]] = []
    for cluster_ids in _build_clusters(memories, pairs):
        cluster_members = [memory_map[memory_id] for memory_id in cluster_ids]
        cluster_members.sort(key=lambda item: (-memory_mass(item), int(item["id"])))
        central = cluster_members[0]
        central_id = int(central["id"])
        supporting_ids = [int(item["id"]) for item in cluster_members if int(item["id"]) != central_id]
        member_ids = [int(item["id"]) for item in cluster_members]
        relevant_pairs = [pair for key, pair in pair_map.items() if key[0] in cluster_ids and key[1] in cluster_ids]
        average_gravity = round(sum(float(item["gravity_score"]) for item in relevant_pairs) / len(relevant_pairs), 4) if relevant_pairs else 0.0
        existing_support_count = _existing_support_count(conn, central_id, supporting_ids)
        support_links_to_create_count = max(0, len(supporting_ids) - existing_support_count)

        proposed_summary_short = _dominant_summary(cluster_members)
        proposed_summary_content = _summary_content(cluster_members, central_id)
        summary_tags = _summary_tags(cluster_members)
        summary_importance = _summary_importance(cluster_members)
        summary_confidence = _summary_confidence(cluster_members)
        proposed_summary_memory = {
            "summary_short": proposed_summary_short,
            "content": proposed_summary_content,
            "memory_type": "consolidated_summary",
            "source": "consolidation_v1_auto",
            "importance_score": summary_importance,
            "confidence_score": summary_confidence,
            "tags": summary_tags,
        }

        existing_summary_memory_id, existing_summary_source_ids, reuse_overlap_score = _find_reusable_summary_memory_id(conn, central_id, member_ids)
        summary_links_to_create_count = len(member_ids) + 1
        summary_links_to_delete_count = 0
        summary_memory_to_update = False
        if existing_summary_memory_id is not None:
            summary_links_to_create_count = _missing_summary_links_count(conn, existing_summary_memory_id, central_id, member_ids)
            summary_links_to_delete_count = _stale_summary_links_count(conn, existing_summary_memory_id, central_id, member_ids)
            existing_summary = row_to_dict(require_row := conn.execute("SELECT * FROM memories WHERE id = ?", (existing_summary_memory_id,)).fetchone())
            summary_memory_to_update = _summary_memory_payload(existing_summary) != {
                "summary_short": proposed_summary_memory["summary_short"],
                "content": proposed_summary_memory["content"],
                "source": proposed_summary_memory["source"],
                "importance_score": round(float(proposed_summary_memory["importance_score"]), 3),
                "confidence_score": round(float(proposed_summary_memory["confidence_score"]), 3),
                "tags": proposed_summary_memory["tags"],
            }

        candidates.append(
            {
                "memory_type": central.get("memory_type"),
                "central_memory_id": central_id,
                "central_summary_short": central.get("summary_short"),
                "central_mass": round(memory_mass(central), 4),
                "member_ids": member_ids,
                "member_count": len(cluster_members),
                "supporting_memory_ids": supporting_ids,
                "existing_support_count": existing_support_count,
                "support_links_to_create_count": support_links_to_create_count,
                "average_gravity": average_gravity,
                "gravity_pairs": relevant_pairs,
                "proposed_summary_short": proposed_summary_short,
                "proposed_summary_content": proposed_summary_content,
                "proposed_summary_memory": proposed_summary_memory,
                "existing_summary_memory_id": existing_summary_memory_id,
                "existing_summary_source_ids": sorted(existing_summary_source_ids),
                "summary_reuse_overlap_score": reuse_overlap_score,
                "summary_memory_to_create": existing_summary_memory_id is None,
                "summary_memory_to_update": summary_memory_to_update,
                "summary_links_to_create_count": summary_links_to_create_count,
                "summary_links_to_delete_count": summary_links_to_delete_count,
                "total_links_to_create_count": support_links_to_create_count + summary_links_to_create_count,
            }
        )

    candidates.sort(key=lambda item: (-float(item["average_gravity"]), -int(item["member_count"]), int(item["central_memory_id"])))
    return candidates
