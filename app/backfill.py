from __future__ import annotations

import json
import sqlite3
from collections import Counter
from typing import Any

from app.schemas import DEFAULT_SCOPE_CODE


PROJECT_TYPES = {
    "project",
    "project_note",
    "project_context",
    "project_direction",
    "project_design",
    "project_architecture",
    "project_milestone",
    "task",
    "goal",
}
PREFERENCE_TYPES = {"preference", "interaction_preference", "workflow_preference", "style", "interest"}
IDENTITY_TYPES = {"identity", "core_belief", "purpose", "profile"}
AUTOBIO_TYPES = {"profile_note", "personal_note", "fact", "history_note"}
WORKING_TYPES = {"working", "conversation", "context"}
SUMMARY_TYPES = {"consolidated_summary"}


def classify_memory_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, str | bool]:
    item = dict(row)
    memory_type = str(item.get("memory_type") or "").strip().lower()
    source = str(item.get("source") or "").strip().lower()
    summary_short = str(item.get("summary_short") or "").strip().lower()
    content = str(item.get("content") or "").strip().lower()
    haystack = " ".join(part for part in (memory_type, source, summary_short, content) if part)

    layer_code = "autobio"
    area_code = "knowledge"
    state_code = "active"
    scope_code = DEFAULT_SCOPE_CODE
    needs_review = False

    if memory_type in IDENTITY_TYPES:
        layer_code = "core" if memory_type in {"identity", "core_belief", "purpose"} else "identity"
        area_code = "identity"
    elif memory_type in PREFERENCE_TYPES:
        layer_code = "identity"
        area_code = "preferences"
    elif memory_type in PROJECT_TYPES:
        layer_code = "projects"
        area_code = "projects"
        scope_code = "project"
    elif memory_type in WORKING_TYPES:
        layer_code = "working"
        area_code = "relation" if "conversation" in haystack or "user" in haystack else "meta"
        scope_code = "conversation"
    elif memory_type in SUMMARY_TYPES:
        layer_code = "autobio"
        area_code = "meta"
    elif memory_type in AUTOBIO_TYPES:
        layer_code = "autobio"
        area_code = "identity" if memory_type == "profile_note" else "history"
    else:
        needs_review = True

    if "project" in haystack and layer_code != "projects":
        area_code = "projects"
        scope_code = "project"
        needs_review = True
    if "prefer" in haystack or "lubi" in haystack or "style" in haystack:
        area_code = "preferences" if layer_code != "projects" else area_code
    if "conflict" in haystack or "contradict" in haystack:
        state_code = "candidate"
        needs_review = True

    return {
        "layer_code": layer_code,
        "area_code": area_code,
        "state_code": state_code,
        "scope_code": scope_code,
        "needs_review": needs_review,
    }


def backfill_memory_metadata(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute("SELECT * FROM memories ORDER BY id ASC").fetchall()
    layer_counter: Counter[str] = Counter()
    area_counter: Counter[str] = Counter()
    review_ids: list[int] = []
    updated_count = 0

    for row in rows:
        current = dict(row)
        suggestion = classify_memory_row(row)
        layer_code = current.get("layer_code") or suggestion["layer_code"]
        area_code = current.get("area_code") or suggestion["area_code"]
        state_code = current.get("state_code") or suggestion["state_code"]
        scope_code = current.get("scope_code") or suggestion["scope_code"]

        changed = any(
            [
                current.get("layer_code") != layer_code,
                current.get("area_code") != area_code,
                current.get("state_code") != state_code,
                current.get("scope_code") != scope_code,
            ]
        )
        if not changed:
            layer_counter[str(layer_code)] += 1
            area_counter[str(area_code)] += 1
            if suggestion["needs_review"]:
                review_ids.append(int(current["id"]))
            continue

        conn.execute(
            """
            UPDATE memories
            SET layer_code = ?, area_code = ?, state_code = ?, scope_code = ?
            WHERE id = ?
            """,
            (layer_code, area_code, state_code, scope_code, int(current["id"])),
        )
        updated_count += 1
        layer_counter[str(layer_code)] += 1
        area_counter[str(area_code)] += 1
        if suggestion["needs_review"]:
            review_ids.append(int(current["id"]))
        conn.execute(
            """
            INSERT INTO memory_events (memory_id, event_type, payload_json)
            VALUES (?, ?, ?)
            """,
            (
                int(current["id"]),
                "backfill_classified",
                json.dumps(
                    {
                        "layer_code": layer_code,
                        "area_code": area_code,
                        "state_code": state_code,
                        "scope_code": scope_code,
                        "needs_review": suggestion["needs_review"],
                    },
                    ensure_ascii=False,
                ),
            ),
        )

    conn.commit()
    return {
        "scanned_count": len(rows),
        "updated_count": updated_count,
        "layer_counts": dict(layer_counter),
        "area_counts": dict(area_counter),
        "needs_review_count": len(review_ids),
        "needs_review_ids": review_ids,
    }


def top_memories_by_layer(conn: sqlite3.Connection, limit_per_layer: int = 5) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    layers = [row[0] for row in conn.execute("SELECT DISTINCT COALESCE(layer_code, '') FROM memories WHERE COALESCE(layer_code, '') <> '' ORDER BY layer_code ASC").fetchall()]
    for layer_code in layers:
        rows = conn.execute(
            """
            SELECT id, memory_type, summary_short, importance_score, layer_code, area_code
            FROM memories
            WHERE layer_code = ?
            ORDER BY importance_score DESC, recall_count DESC, id DESC
            LIMIT ?
            """,
            (layer_code, int(limit_per_layer)),
        ).fetchall()
        result[str(layer_code)] = [dict(row) for row in rows]
    return result
