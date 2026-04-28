from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

from app.conflict_logic import build_conflict_context_bundle, has_conflict_signal


_CONFLICT_KINDS = frozenset({
    "temporal_conflict",
    "scope_conflict",
    "source_conflict",
    "summary_detail_conflict",
    "definition_conflict",
    "unresolved_real_conflict",
})

_SUGGESTED_RELATIONS: dict[str, str] = {
    "temporal_conflict": "supersedes",
    "scope_conflict": "relates_to",
    "source_conflict": "relates_to",
    "summary_detail_conflict": "relates_to",
    "definition_conflict": "relates_to",
    "unresolved_real_conflict": "contradicts",
}

_SUGGESTED_ACTIONS: dict[str, str] = {
    "temporal_conflict": "set_valid_to_on_older_memory",
    "scope_conflict": "add_relates_to_link",
    "source_conflict": "review_source_quality",
    "summary_detail_conflict": "add_relates_to_link",
    "definition_conflict": "add_clarification_note",
    "unresolved_real_conflict": "needs_human_review",
}

_NEEDS_HUMAN_REVIEW: dict[str, bool] = {
    "temporal_conflict": False,
    "scope_conflict": False,
    "source_conflict": True,
    "summary_detail_conflict": False,
    "definition_conflict": True,
    "unresolved_real_conflict": True,
}

_EXPLANATION_TEMPLATES: dict[str, str] = {
    "temporal_conflict": (
        "Sprzeczność wynika najpewniej z różnicy czasu. "
        "Wpisy mogą opisywać ten sam stan systemu w różnych momentach, "
        "więc jeden z nich mógł zastąpić drugi po zmianie rzeczywistości."
    ),
    "scope_conflict": (
        "Wpisy mogą opisywać ten sam temat na różnym poziomie szczegółowości lub z różnego zakresu. "
        "To wygląda bardziej na różnicę perspektywy niż twardą sprzeczność."
    ),
    "source_conflict": (
        "Wpisy pochodzą z różnych źródeł lub różnią się jakością dowodów. "
        "Tu lepiej włączyć przegląd człowieka niż udawać automatyczną pewność."
    ),
    "summary_detail_conflict": (
        "Jeden wpis wygląda na skrótową notkę, a drugi na rozwinięty opis. "
        "Najpewniej to różnica poziomu detalu, nie twarda kolizja faktów."
    ),
    "definition_conflict": (
        "Wpisy używają tych samych pojęć, ale mogą opisywać je w różny sposób. "
        "To wygląda na różnicę definicyjną lub interpretacyjną, a nie twardą sprzeczność faktów. "
        "Zalecany przegląd człowieka w celu ujednolicenia znaczenia."
    ),
    "unresolved_real_conflict": (
        "Wykryto sygnały sprzeczności w treści wpisów. "
        "Nie udało się bezpiecznie ustalić dokładniejszej przyczyny, więc potrzebny jest review."
    ),
}


def _token_overlap_ratio(a: str, b: str) -> float:
    def tokens(text: str) -> set[str]:
        return {w.casefold().strip(".,;:!?()[]{}\"'") for w in text.split() if len(w) >= 3}
    t_a = tokens(a)
    t_b = tokens(b)
    if not t_a or not t_b:
        return 0.0
    return len(t_a & t_b) / min(len(t_a), len(t_b))


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _days_between(a: str | None, b: str | None) -> float | None:
    left = _parse_iso(a)
    right = _parse_iso(b)
    if left is None or right is None:
        return None
    return abs((left - right).total_seconds()) / 86400.0


# Source quality ranking -------------------------------------------------------

_SOURCE_TIER: dict[str, float] = {
    "manual": 1.0,
    "user": 0.9,
    "analyst": 0.85,
    "operator": 0.8,
    "system": 0.65,
    "auto": 0.55,
    "ai": 0.5,
    "ai_generated": 0.4,
    "unknown": 0.4,
}


def source_quality_score(memory: dict[str, Any], supports_count: int = 0) -> float:
    """Returns 0.0–1.0 composite quality score for a memory.

    Combines source tier (50%), confidence_score (30%), evidence_count (15%)
    and whether the memory has supporting links (5%).
    """
    raw_source = str(memory.get("source") or "").strip().casefold()
    source_tier = 0.5
    for key, weight in _SOURCE_TIER.items():
        if key in raw_source:
            source_tier = max(source_tier, weight)

    confidence = min(max(float(memory.get("confidence_score") or 0.5), 0.0), 1.0)
    evidence = min(int(memory.get("evidence_count") or 1), 10) / 10.0
    has_supports = 1.0 if supports_count > 0 else 0.0

    return round(
        source_tier * 0.50
        + confidence * 0.30
        + evidence * 0.15
        + has_supports * 0.05,
        3,
    )


def source_quality_breakdown(memory: dict[str, Any], supports_count: int = 0) -> dict[str, Any]:
    """Returns detailed quality breakdown for diagnostic use."""
    raw_source = str(memory.get("source") or "").strip().casefold()
    source_tier = 0.5
    matched_tier_key = "default"
    for key, weight in _SOURCE_TIER.items():
        if key in raw_source and weight > source_tier:
            source_tier = weight
            matched_tier_key = key

    confidence = min(max(float(memory.get("confidence_score") or 0.5), 0.0), 1.0)
    evidence = min(int(memory.get("evidence_count") or 1), 10) / 10.0
    has_supports = 1.0 if supports_count > 0 else 0.0

    total = round(
        source_tier * 0.50 + confidence * 0.30 + evidence * 0.15 + has_supports * 0.05,
        3,
    )
    return {
        "total_score": total,
        "source": raw_source or None,
        "source_tier": round(source_tier, 2),
        "source_tier_key": matched_tier_key,
        "confidence_score": round(confidence, 2),
        "evidence_count": int(memory.get("evidence_count") or 1),
        "evidence_score": round(evidence, 2),
        "supports_count": supports_count,
        "components": {
            "source_tier_weighted": round(source_tier * 0.50, 3),
            "confidence_weighted": round(confidence * 0.30, 3),
            "evidence_weighted": round(evidence * 0.15, 3),
            "supports_weighted": round(has_supports * 0.05, 3),
        },
    }


def _fetch_timeline_for_pair(
    conn: sqlite3.Connection,
    memory_a_id: int,
    memory_b_id: int,
    limit: int = 6,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, event_type, title, valid_at, created_at, origin
        FROM timeline_events
        WHERE memory_id IN (?, ?) OR related_memory_id IN (?, ?)
        ORDER BY COALESCE(valid_at, created_at) DESC, id DESC
        LIMIT ?
        """,
        (memory_a_id, memory_b_id, memory_a_id, memory_b_id, int(limit)),
    ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "event_type": row["event_type"],
            "title": row["title"],
            "valid_at": row["valid_at"],
            "created_at": row["created_at"],
            "origin": row["origin"],
        }
        for row in rows
    ]


def classify_conflict_kind(
    bundle: dict[str, Any],
    timeline_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    timeline_events = list(timeline_events or [])
    left = bundle["base_memories"][0]
    right = bundle["base_memories"][1]

    signals: dict[str, float] = {kind: 0.0 for kind in _CONFLICT_KINDS}
    debug_signals: list[str] = []

    if left.get("valid_to") or right.get("valid_to"):
        signals["temporal_conflict"] = max(signals["temporal_conflict"], 0.8)
        debug_signals.append("valid_to set on one or both memories")
    if left.get("valid_from") and right.get("valid_from") and left["valid_from"] != right["valid_from"]:
        signals["temporal_conflict"] = max(signals["temporal_conflict"], 0.65)
        debug_signals.append("different valid_from timestamps")
    days_apart = _days_between(left.get("created_at"), right.get("created_at"))
    if days_apart is not None and days_apart > 30:
        signals["temporal_conflict"] = max(signals["temporal_conflict"], 0.5)
        debug_signals.append(f"created_at differs by {days_apart:.0f} days")
    if timeline_events:
        debug_signals.append(f"timeline events available: {len(timeline_events)}")

    source_a = (left.get("source") or "").strip().casefold()
    source_b = (right.get("source") or "").strip().casefold()
    conf_a = float(left.get("confidence_score") or 0.5)
    conf_b = float(right.get("confidence_score") or 0.5)
    evid_a = int(left.get("evidence_count") or 1)
    evid_b = int(right.get("evidence_count") or 1)

    sq_a = source_quality_score(left)
    sq_b = source_quality_score(right)
    quality_gap = abs(sq_a - sq_b)

    if source_a and source_b and source_a != source_b:
        signals["source_conflict"] = max(signals["source_conflict"], 0.35)
        debug_signals.append(f"different sources: '{source_a}' vs '{source_b}'")
    if abs(conf_a - conf_b) > 0.25:
        signals["source_conflict"] = max(signals["source_conflict"], 0.55)
        debug_signals.append(f"confidence gap: {conf_a:.2f} vs {conf_b:.2f}")
    if evid_a > 0 and evid_b > 0:
        evidence_ratio = max(evid_a, evid_b) / min(evid_a, evid_b)
        if evidence_ratio >= 3:
            signals["source_conflict"] = max(signals["source_conflict"], 0.5)
            debug_signals.append(f"evidence_count asymmetry: {evid_a} vs {evid_b}")
    if quality_gap >= 0.35:
        score = min(0.60 + quality_gap * 0.5, 0.90)
        signals["source_conflict"] = max(signals["source_conflict"], round(score, 2))
        debug_signals.append(
            f"source quality gap: {sq_a:.2f} vs {sq_b:.2f} (gap={quality_gap:.2f})"
        )
    elif quality_gap >= 0.20:
        signals["source_conflict"] = max(signals["source_conflict"], 0.55)
        debug_signals.append(
            f"moderate source quality gap: {sq_a:.2f} vs {sq_b:.2f} (gap={quality_gap:.2f})"
        )

    type_a = (left.get("memory_type") or "").strip()
    type_b = (right.get("memory_type") or "").strip()
    len_a = len(left.get("content") or "")
    len_b = len(right.get("content") or "")
    min_len = min(len_a, len_b)

    if type_a and type_b and type_a != type_b:
        signals["scope_conflict"] = max(signals["scope_conflict"], 0.55)
        debug_signals.append(f"different memory_type: '{type_a}' vs '{type_b}'")
    if min_len > 0:
        length_ratio = max(len_a, len_b) / min_len
        if length_ratio >= 3:
            signals["scope_conflict"] = max(signals["scope_conflict"], 0.4)
            debug_signals.append(f"content length ratio: {length_ratio:.1f}x")

    has_same_summary = bundle.get("summary_short_shared") is not None
    length_diff = abs(len_a - len_b)
    summary_detail_ratio = (max(len_a, len_b) / min_len) if min_len > 0 else 1.0
    if has_same_summary and length_diff > 200 and summary_detail_ratio >= 3:
        signals["summary_detail_conflict"] = 0.65
        debug_signals.append(
            f"same summary_short, content length difference {length_diff} chars ({summary_detail_ratio:.1f}x)"
        )

    content_a = str(left.get("content") or "")
    content_b = str(right.get("content") or "")
    if has_conflict_signal(content_a, content_b):
        signals["unresolved_real_conflict"] = 0.7
        debug_signals.append("contradiction phrase detected in content")
    else:
        signals["unresolved_real_conflict"] = 0.2

    # definition_conflict: same topic, both substantive, high lexical overlap, no negation asymmetry
    def _has_negation(text: str) -> bool:
        lower = text.casefold()
        return " nie " in lower or lower.startswith("nie ") or " not " in lower or lower.startswith("not ")

    negation_asymmetry = _has_negation(content_a) != _has_negation(content_b)
    if (
        has_same_summary
        and not has_conflict_signal(content_a, content_b)
        and not negation_asymmetry
        and min_len > 30
        and (max(len_a, len_b) / min_len if min_len > 0 else 1.0) < 2.5
    ):
        overlap = _token_overlap_ratio(content_a, content_b)
        if overlap >= 0.5:
            signals["definition_conflict"] = 0.65
            debug_signals.append(f"high lexical overlap ({overlap:.2f}) with no negation signal")
        elif overlap >= 0.35:
            signals["definition_conflict"] = 0.55
            debug_signals.append(f"moderate lexical overlap ({overlap:.2f}) with no negation signal")

    priority = [
        "temporal_conflict",
        "summary_detail_conflict",
        "scope_conflict",
        "source_conflict",
        "definition_conflict",
        "unresolved_real_conflict",
    ]
    conflict_kind = "unresolved_real_conflict"
    confidence = round(signals["unresolved_real_conflict"], 2)
    for kind in priority:
        if signals[kind] >= 0.5:
            conflict_kind = kind
            confidence = round(signals[kind], 2)
            break

    return {
        "conflict_kind": conflict_kind,
        "confidence": confidence,
        "signals": debug_signals,
        "signal_scores": {kind: round(score, 2) for kind, score in signals.items()},
    }


def explain_conflict_pair(
    conn: sqlite3.Connection,
    memory_a_id: int,
    memory_b_id: int,
    *,
    related_limit: int = 5,
    timeline_limit: int = 6,
) -> dict[str, Any]:
    bundle = build_conflict_context_bundle(
        conn,
        int(memory_a_id),
        int(memory_b_id),
        related_limit=related_limit,
    )
    left = bundle["base_memories"][0]
    right = bundle["base_memories"][1]
    base_ids = [int(left["id"]), int(right["id"])]

    timeline_events = _fetch_timeline_for_pair(conn, base_ids[0], base_ids[1], limit=timeline_limit)
    classification = classify_conflict_kind(bundle, timeline_events)
    conflict_kind = str(classification["conflict_kind"])
    confidence = float(classification["confidence"])
    debug_signals = list(classification["signals"])

    context_memory_ids = [int(item["id"]) for item in bundle.get("context_memories", [])]
    supporting_link_ids = [int(link["id"]) for link in bundle.get("direct_links", [])]
    timeline_event_ids = [int(event["id"]) for event in timeline_events]

    template = _EXPLANATION_TEMPLATES[conflict_kind]
    top_signal = debug_signals[0] if debug_signals else ""
    explanation = f"{template} (Sygnał: {top_signal}.)" if top_signal else template
    conflict_reason = top_signal or "brak wyraźnego sygnału"

    return {
        "conflict_kind": conflict_kind,
        "conflict_reason": conflict_reason,
        "explanation": explanation,
        "confidence": confidence,
        "base_memory_ids": base_ids,
        "context_memory_ids": context_memory_ids,
        "supporting_link_ids": supporting_link_ids,
        "timeline_event_ids": timeline_event_ids,
        "suggested_relation": _SUGGESTED_RELATIONS[conflict_kind],
        "suggested_action": _SUGGESTED_ACTIONS[conflict_kind],
        "needs_human_review": _NEEDS_HUMAN_REVIEW[conflict_kind],
        "debug": {
            "signals": debug_signals,
            "signal_scores": classification["signal_scores"],
            "bundle_summary_shared": bundle.get("summary_short_shared"),
            "bundle_type_shared": bundle.get("memory_type_shared"),
            "context_memory_count": bundle.get("context_memory_count", 0),
            "timeline_event_count": len(timeline_events),
        },
    }


# ---------------------------------------------------------------------------
# AUTO-APPLY THRESHOLDS
# ---------------------------------------------------------------------------

_AUTO_APPLY_MIN_CONFIDENCE: dict[str, float] = {
    "temporal_conflict": 0.65,
    "scope_conflict": 0.5,
    "summary_detail_conflict": 0.5,
    "source_conflict": 999.0,        # never auto-apply
    "definition_conflict": 999.0,    # never auto-apply — requires human clarification
    "unresolved_real_conflict": 999.0,
}


def link_of_type_exists(
    conn: sqlite3.Connection,
    memory_a_id: int,
    memory_b_id: int,
    relation_type: str,
) -> bool:
    row = conn.execute(
        """
        SELECT id FROM memory_links
        WHERE relation_type = ?
          AND (
                (from_memory_id = ? AND to_memory_id = ?)
                OR
                (from_memory_id = ? AND to_memory_id = ?)
          )
        LIMIT 1
        """,
        (relation_type, memory_a_id, memory_b_id, memory_b_id, memory_a_id),
    ).fetchone()
    return row is not None


def find_older_newer(
    left: dict[str, Any],
    right: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (older, newer). Older = the one that was superseded."""
    if left.get("valid_to") and not right.get("valid_to"):
        return left, right
    if right.get("valid_to") and not left.get("valid_to"):
        return right, left
    left_dt = _parse_iso(left.get("created_at"))
    right_dt = _parse_iso(right.get("created_at"))
    if left_dt and right_dt and left_dt != right_dt:
        return (left, right) if left_dt < right_dt else (right, left)
    return (left, right) if int(left["id"]) < int(right["id"]) else (right, left)


def find_summary_detail(
    left: dict[str, Any],
    right: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (summary, detail) — shorter content = summary."""
    len_left = len(left.get("content") or "")
    len_right = len(right.get("content") or "")
    return (left, right) if len_left <= len_right else (right, left)


def build_proposed_changes(
    conn: sqlite3.Connection,
    conflict_kind: str,
    confidence: float,
    left: dict[str, Any],
    right: dict[str, Any],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    left_id = int(left["id"])
    right_id = int(right["id"])

    if conflict_kind == "temporal_conflict":
        older, newer = find_older_newer(left, right)
        older_id = int(older["id"])
        newer_id = int(newer["id"])
        changes.append({
            "action": "create_link",
            "from_memory_id": newer_id,
            "to_memory_id": older_id,
            "relation_type": "supersedes",
            "weight": 0.85,
            "reason": "newer memory supersedes the older one",
            "already_exists": link_of_type_exists(conn, newer_id, older_id, "supersedes"),
        })
        if not older.get("valid_to") and newer.get("created_at"):
            changes.append({
                "action": "set_valid_to",
                "memory_id": older_id,
                "proposed_valid_to": str(newer.get("created_at")),
                "reason": "mark older memory as expired at the time the newer one was created",
                "already_set": False,
            })

    elif conflict_kind in ("scope_conflict", "summary_detail_conflict"):
        if conflict_kind == "summary_detail_conflict":
            summary, detail = find_summary_detail(left, right)
            from_id, to_id = int(detail["id"]), int(summary["id"])
            reason = "detailed memory relates to its summary counterpart"
        else:
            from_id, to_id = left_id, right_id
            reason = "memories describe the same topic from different scopes"
        changes.append({
            "action": "create_link",
            "from_memory_id": from_id,
            "to_memory_id": to_id,
            "relation_type": "relates_to",
            "weight": 0.7,
            "reason": reason,
            "already_exists": link_of_type_exists(conn, from_id, to_id, "relates_to"),
        })

    elif conflict_kind == "source_conflict":
        changes.append({
            "action": "request_review",
            "memory_ids": [left_id, right_id],
            "reason": "source quality asymmetry — human review needed before linking",
        })

    elif conflict_kind == "definition_conflict":
        changes.append({
            "action": "request_review",
            "memory_ids": [left_id, right_id],
            "reason": "definition or interpretation divergence — clarification note needed",
        })

    else:  # unresolved_real_conflict
        changes.append({
            "action": "create_link",
            "from_memory_id": left_id,
            "to_memory_id": right_id,
            "relation_type": "contradicts",
            "weight": 0.9,
            "reason": "contradiction phrase detected — flagging the conflict",
            "already_exists": link_of_type_exists(conn, left_id, right_id, "contradicts"),
        })
        changes.append({
            "action": "request_review",
            "memory_ids": [left_id, right_id],
            "reason": "unresolved real conflict — human review needed",
        })

    return changes


def skip_reason(
    conflict_kind: str,
    confidence: float,
    proposed_changes: list[dict[str, Any]],
) -> str | None:
    if _NEEDS_HUMAN_REVIEW[conflict_kind]:
        return "needs_human_review"
    threshold = _AUTO_APPLY_MIN_CONFIDENCE[conflict_kind]
    if confidence < threshold:
        return f"low_confidence ({confidence:.2f} < {threshold:.2f})"
    link_changes = [c for c in proposed_changes if c["action"] == "create_link"]
    if link_changes and all(c.get("already_exists") for c in link_changes):
        return "link_already_exists"
    return None


def preview_resolution(
    conn: sqlite3.Connection,
    memory_a_id: int,
    memory_b_id: int,
) -> dict[str, Any]:
    explanation = explain_conflict_pair(conn, int(memory_a_id), int(memory_b_id))
    conflict_kind = str(explanation["conflict_kind"])
    confidence = float(explanation["confidence"])

    from app.conflict_logic import build_conflict_context_bundle
    bundle = build_conflict_context_bundle(conn, int(memory_a_id), int(memory_b_id), related_limit=0)
    left = bundle["base_memories"][0]
    right = bundle["base_memories"][1]

    proposed_changes = build_proposed_changes(conn, conflict_kind, confidence, left, right)
    reason = skip_reason(conflict_kind, confidence, proposed_changes)
    can_auto_apply = reason is None

    return {
        "memory_a_id": int(memory_a_id),
        "memory_b_id": int(memory_b_id),
        "conflict_kind": conflict_kind,
        "confidence": confidence,
        "proposed_changes": proposed_changes,
        "can_auto_apply": can_auto_apply,
        "skip_reason": reason,
        "explanation_summary": explanation["explanation"],
        "needs_human_review": explanation["needs_human_review"],
    }


def apply_resolution(
    conn: sqlite3.Connection,
    memory_a_id: int,
    memory_b_id: int,
) -> dict[str, Any]:
    """Apply proposed conflict resolution changes. Returns applied_changes list and metadata.

    Only applies if can_auto_apply is True. Does NOT commit — caller handles transaction.
    Returns status='skipped' with skip_reason when auto-apply is blocked.
    """
    preview = preview_resolution(conn, int(memory_a_id), int(memory_b_id))
    if not preview["can_auto_apply"]:
        return {
            "status": "skipped",
            "skip_reason": preview["skip_reason"],
            "conflict_kind": preview["conflict_kind"],
            "confidence": preview["confidence"],
            "memory_a_id": int(memory_a_id),
            "memory_b_id": int(memory_b_id),
            "applied_changes": [],
        }

    applied_changes: list[dict[str, Any]] = []
    for change in preview["proposed_changes"]:
        action = change["action"]

        if action == "create_link" and not change.get("already_exists"):
            cursor = conn.execute(
                """
                INSERT INTO memory_links (from_memory_id, to_memory_id, relation_type, weight, origin)
                VALUES (?, ?, ?, ?, 'conflict_explainer_auto')
                """,
                (change["from_memory_id"], change["to_memory_id"], change["relation_type"], change["weight"]),
            )
            applied_changes.append({
                "action": "create_link",
                "link_id": int(cursor.lastrowid),
                "from_memory_id": change["from_memory_id"],
                "to_memory_id": change["to_memory_id"],
                "relation_type": change["relation_type"],
                "weight": change["weight"],
            })

        elif action == "set_valid_to":
            memory_id = int(change["memory_id"])
            old_row = conn.execute("SELECT valid_to FROM memories WHERE id = ?", (memory_id,)).fetchone()
            old_valid_to = old_row["valid_to"] if old_row else None
            new_valid_to = str(change["proposed_valid_to"])
            conn.execute("UPDATE memories SET valid_to = ? WHERE id = ?", (new_valid_to, memory_id))
            applied_changes.append({
                "action": "set_valid_to",
                "memory_id": memory_id,
                "old_valid_to": old_valid_to,
                "new_valid_to": new_valid_to,
            })

    return {
        "status": "applied",
        "skip_reason": None,
        "conflict_kind": preview["conflict_kind"],
        "confidence": preview["confidence"],
        "memory_a_id": int(memory_a_id),
        "memory_b_id": int(memory_b_id),
        "applied_changes": applied_changes,
        "explanation_summary": preview["explanation_summary"],
    }
