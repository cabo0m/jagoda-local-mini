from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from typing import Any

from app.schemas import derive_state_code

PROJECT_TYPE_CODES = {
    "project",
    "project_note",
    "project_context",
    "project_direction",
    "project_design",
    "project_architecture",
    "project_milestone",
    "task_context",
}

PREFERENCE_TYPE_CODES = {
    "preference",
    "interaction_preference",
    "workflow_preference",
    "interest",
}

KNOWN_PROJECT_KEYS = ("finexto", "morenatech", "jagoda")


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _join_text(*values: Any) -> str:
    return " ".join(_norm_text(value) for value in values if value is not None)


def _detect_project_key(*values: Any) -> str | None:
    haystack = _join_text(*values)
    for project_key in KNOWN_PROJECT_KEYS:
        if project_key in haystack:
            return project_key
    return None


def classify_memory(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    memory_type = _norm_text(item.get("memory_type"))
    content = _norm_text(item.get("content"))
    summary_short = _norm_text(item.get("summary_short"))
    source = _norm_text(item.get("source"))
    tags = _norm_text(item.get("tags"))
    joined = _join_text(memory_type, content, summary_short, source, tags)

    layer_code = "autobio"
    area_code = "knowledge"
    scope_code = "global"
    state_code = derive_state_code(item.get("state_code"), item.get("activity_state"), item.get("contradiction_flag"))
    confidence = 0.6
    reason = "default_memory_type_fallback"
    requires_review = False

    if memory_type == "profile":
        layer_code, area_code, scope_code, state_code, confidence, reason = "identity", "identity", "global", "validated", 0.97, "profile_identity_mapping"
    elif memory_type == "profile_note":
        layer_code, area_code, scope_code, state_code, confidence, reason = "autobio", "identity", "global", "validated", 0.9, "profile_note_identity_mapping"
    elif memory_type == "personal_note":
        layer_code, area_code, scope_code, confidence, reason = "autobio", "history", "user", 0.82, "personal_note_history_mapping"
    elif memory_type in PREFERENCE_TYPE_CODES:
        layer_code, area_code, scope_code, state_code, confidence, reason = "identity", "preferences", "user", "validated", 0.91, "preference_mapping"
    elif memory_type == "fact":
        layer_code, area_code, scope_code, state_code, confidence, reason = "autobio", "knowledge", "global", "validated", 0.88, "fact_mapping"
    elif memory_type == "consolidated_summary":
        layer_code, area_code, scope_code, state_code, confidence, reason = "autobio", "meta", "system", "validated", 0.9, "summary_meta_mapping"
    elif memory_type in PROJECT_TYPE_CODES:
        layer_code, area_code, scope_code, state_code, confidence, reason = "projects", "projects", "project", "active", 0.95, "project_mapping"
    elif memory_type == "working":
        layer_code = "working"
        scope_code = "conversation" if any(token in joined for token in ("chat", "conversation", "user", "michał", "michal")) else "system"
        if any(token in joined for token in ("rule", "prompt", "instruction", "system", "policy", "meta", "context")):
            area_code = "meta"
            reason = "working_meta_heuristic"
            confidence = 0.76
        elif any(token in joined for token in ("user", "chat", "conversation", "reply", "message", "michał", "michal")):
            area_code = "relation"
            reason = "working_relation_heuristic"
            confidence = 0.78
        else:
            area_code = "rumination"
            state_code = "candidate"
            confidence = 0.58
            reason = "working_uncertain_candidate"
            requires_review = True
    elif memory_type == "semantic":
        layer_code, area_code, scope_code, confidence, reason = "autobio", "meta", "system", 0.7, "semantic_meta_mapping"
    else:
        requires_review = True
        state_code = "candidate"
        confidence = 0.45

    if item.get("contradiction_flag"):
        state_code = "conflicted"
        reason = f"{reason}|contradiction_flag"
    elif _norm_text(item.get("activity_state")) == "archived" or item.get("archived_at"):
        state_code = "archived"

    project_key = _detect_project_key(memory_type, content, summary_short, source, tags)
    if scope_code == "project" and project_key is None:
        requires_review = True
        state_code = "candidate" if state_code == "active" else state_code
        reason = f"{reason}|project_key_missing"
        confidence = min(confidence, 0.72)

    if confidence < 0.75 and state_code not in {"archived", "conflicted"}:
        state_code = "candidate"
        requires_review = True

    return {
        "memory_id": int(item["id"]),
        "memory_type": memory_type,
        "layer_code": layer_code,
        "area_code": area_code,
        "scope_code": scope_code,
        "state_code": state_code,
        "project_key": project_key,
        "confidence": round(float(confidence), 3),
        "reason": reason,
        "requires_review": bool(requires_review),
    }


def build_backfill_plan(conn: sqlite3.Connection, *, only_missing: bool = True) -> dict[str, Any]:
    where_sql = "WHERE COALESCE(layer_code, '') = '' OR COALESCE(area_code, '') = '' OR COALESCE(state_code, '') = '' OR COALESCE(scope_code, '') = ''" if only_missing else ""
    rows = conn.execute(
        f"SELECT * FROM memories {where_sql} ORDER BY id ASC"
    ).fetchall()
    items = [classify_memory(row) for row in rows]
    by_layer = Counter(item["layer_code"] for item in items)
    by_area = Counter(item["area_code"] for item in items)
    by_state = Counter(item["state_code"] for item in items)
    review_items = [item for item in items if item["requires_review"]]
    return {
        "only_missing": only_missing,
        "count": len(items),
        "items": items,
        "summary": {
            "by_layer": dict(sorted(by_layer.items())),
            "by_area": dict(sorted(by_area.items())),
            "by_state": dict(sorted(by_state.items())),
            "review_count": len(review_items),
            "project_key_missing_count": sum(1 for item in items if "project_key_missing" in item["reason"]),
        },
        "review_items": review_items,
    }


def _record_event(conn: sqlite3.Connection, memory_id: int, event_type: str, payload: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO memory_events (memory_id, event_type, payload_json) VALUES (?, ?, ?)",
        (memory_id, event_type, json.dumps(payload, ensure_ascii=False)),
    )


def apply_backfill_plan(conn: sqlite3.Connection, *, only_missing: bool = True, dry_run: bool = False) -> dict[str, Any]:
    plan = build_backfill_plan(conn, only_missing=only_missing)
    if dry_run:
        return {"status": "preview_completed", **plan}

    changed_count = 0
    review_ids: list[int] = []
    for item in plan["items"]:
        memory_id = int(item["memory_id"])
        previous = conn.execute(
            "SELECT layer_code, area_code, state_code, scope_code, project_key FROM memories WHERE id = ?",
            (memory_id,),
        ).fetchone()
        old_values = dict(previous)
        conn.execute(
            """
            UPDATE memories
            SET layer_code = ?,
                area_code = ?,
                state_code = ?,
                scope_code = ?,
                project_key = COALESCE(project_key, ?)
            WHERE id = ?
            """,
            (
                item["layer_code"],
                item["area_code"],
                item["state_code"],
                item["scope_code"],
                item["project_key"],
                memory_id,
            ),
        )
        _record_event(
            conn,
            memory_id,
            "metadata_backfilled",
            {
                "old": old_values,
                "new": {
                    "layer_code": item["layer_code"],
                    "area_code": item["area_code"],
                    "state_code": item["state_code"],
                    "scope_code": item["scope_code"],
                    "project_key": item["project_key"],
                },
                "reason": item["reason"],
                "confidence": item["confidence"],
                "requires_review": item["requires_review"],
            },
        )
        changed_count += 1
        if item["requires_review"]:
            review_ids.append(memory_id)

    conn.commit()
    return {
        "status": "completed",
        **plan,
        "changed_count": changed_count,
        "review_ids": review_ids,
    }


def get_layer_report(conn: sqlite3.Connection, *, top_n: int = 5) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT layer_code, id, memory_type, summary_short, importance_score
        FROM memories
        WHERE COALESCE(layer_code, '') <> ''
        ORDER BY layer_code ASC, importance_score DESC, id DESC
        """
    ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        layer_code = str(row["layer_code"])
        if len(grouped[layer_code]) >= top_n:
            continue
        grouped[layer_code].append(
            {
                "memory_id": int(row["id"]),
                "memory_type": row["memory_type"],
                "summary_short": row["summary_short"],
                "importance_score": float(row["importance_score"] or 0.0),
            }
        )
    return {"top_n": top_n, "layers": dict(grouped)}
