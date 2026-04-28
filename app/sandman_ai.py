from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from app import lm_studio_client
from app.memory_store import row_to_dict
from app.sandman_logic import (
    get_duplicate_candidates,
    get_protected_canonical_memory_ids,
    get_secondary_duplicate_memory_ids,
)

LM_STUDIO_MODEL = lm_studio_client.LM_STUDIO_MODEL
BATCH_SIZE = int(os.environ.get("SANDMAN_AI_BATCH_SIZE", "6"))

_DECISION_JSON_SCHEMA: dict[str, Any] = {
    "name": "sandman_decisions",
    "schema": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "decision": {"type": "string", "enum": ["keep", "downgrade", "archive"]},
                "new_importance": {"anyOf": [{"type": "number"}, {"type": "null"}]},
                "reason": {"type": "string"},
            },
            "required": ["id", "decision", "new_importance", "reason"],
            "additionalProperties": False,
        },
    },
}

_STRICT_JSON_RULES = """\
ZASADY KRYTYCZNE:
- Odpowiedz wyłącznie surowym JSON array.
- Nie używaj markdown.
- Nie używaj backticków.
- Nie dodawaj komentarza, wyjaśnienia ani tekstu przed lub po JSON.
- Pierwszy znak odpowiedzi ma być [
- Ostatni znak odpowiedzi ma być ]
- Każdy obiekt musi zawierać dokładnie pola: id, decision, new_importance, reason.
- decision może być wyłącznie: keep, downgrade, archive.
- Jeśli decision != downgrade, ustaw new_importance na null.
- reason ma być po polsku i maksymalnie 8 słów.
- Jeśli nie jesteś pewna, i tak zwróć poprawny JSON array zgodny ze schematem.
"""

_SYSTEM_PROMPT = _STRICT_JSON_RULES + """\
Jesteś asystentem zarządzania pamięcią episodyczną. Analizujesz wpisy pamięci AI i decydujesz co z nimi zrobić.

Każdy wpis zawiera:
- id: unikalny identyfikator
- content: treść wspomnienia
- summary_short: krótkie podsumowanie
- memory_type: typ (working/profile/project/fact/consolidated_summary/profile_note)
- importance_score: ważność 0.0–1.0
- recall_count: ile razy wspomnienie było przywoływane
- tags: tagi
- created_at: data utworzenia
- last_accessed_at: ostatni dostęp

Dla każdego wpisu zdecyduj:
- "keep" — zachowaj bez zmian
- "downgrade" — obniż ważność
- "archive" — zarchiwizuj

WAŻNE ZASADY:
- Wspomnienia z recall_count >= 2 bardzo rzadko archiwizuj.
- Wspomnienia z importance_score >= 0.70 zachowaj, chyba że są wyraźnie przestarzałe.
- memory_type == "profile" lub "fact" archiwizuj tylko jeśli ewidentnie nieaktualne.
- memory_type == "profile_note" archiwizuj tylko gdy jest ewidentnie nieaktualne i zastąpione nowszym profile_note.
- Jeśli masz wątpliwość przy profile_note, wybierz keep.
- memory_type == "working" z recall_count == 0 i importance_score <= 0.40 to kandydaci do archiwizacji.
- consolidated_summary zwykle zachowuj, chyba że jest wyraźnie zbędne lub przestarzałe.
"""

_SYSTEM_PROMPT_AGGRESSIVE = _STRICT_JSON_RULES + """\
Jesteś asystentem zarządzania pamięcią episodyczną. Analizujesz wpisy pamięci AI i agresywnie porządkujesz bazę.

Twoim celem jest znaczące odchudzenie bazy, ale nadal masz przestrzegać poprawnego schematu JSON.

Kryteria:
- "archive": recall_count <= 1 ORAZ importance_score <= 0.55 ORAZ memory_type == "working"
- "archive": treść ogólna, brak konkretów, stara data
- "downgrade": recall_count == 0 i importance_score 0.40–0.70, obniż o 0.10–0.20
- "keep": tylko jeśli wyraźnie wartościowe i aktywnie używane
- profile_note traktuj ostrożniej niż working i w razie wątpliwości nie archiwizuj
"""


def _call_lm_studio(messages: list[dict[str, str]], timeout: int = 120) -> str:
    return lm_studio_client.call_lm_studio(
        messages,
        {"type": "json_schema", "json_schema": _DECISION_JSON_SCHEMA},
        max_tokens=int(os.environ.get("SANDMAN_AI_MAX_TOKENS", "16384")),
        timeout=timeout,
    )


def _normalize_decision(item: dict[str, Any], expected_ids: set[int]) -> dict[str, Any] | None:
    try:
        mem_id = int(item["id"])
    except (KeyError, TypeError, ValueError):
        return None
    if mem_id not in expected_ids:
        return None
    decision = str(item.get("decision", "keep")).lower().strip()
    if decision not in {"keep", "downgrade", "archive"}:
        decision = "keep"
    new_importance = item.get("new_importance")
    if decision != "downgrade":
        new_importance = None
    elif new_importance is not None:
        try:
            new_importance = round(float(new_importance), 3)
        except (TypeError, ValueError):
            new_importance = None
        if new_importance is not None:
            new_importance = lm_studio_client.clamp_importance(new_importance)
    reason = str(item.get("reason", "")).strip() or "brak uzasadnienia"
    return {"id": mem_id, "decision": decision, "new_importance": new_importance, "reason": reason}


def _parse_ai_response(raw: str, expected_ids: set[int]) -> list[dict[str, Any]]:
    text = raw.strip()
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        raise ValueError(f"Model nie zwrócił tablicy JSON. Odpowiedź: {text[:1200]}")
    try:
        parsed = json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise ValueError(f"Model zwrócił niepoprawny JSON. Odpowiedź: {text[:1200]}") from exc
    if not isinstance(parsed, list):
        raise ValueError(f"Model zwrócił JSON, ale nie tablicę. Odpowiedź: {text[:1200]}")
    decisions: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for item in parsed:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_decision(item, expected_ids)
        if normalized is None:
            continue
        decisions.append(normalized)
        seen_ids.add(int(normalized["id"]))
    for mem_id in sorted(expected_ids - seen_ids):
        decisions.append({"id": mem_id, "decision": "keep", "new_importance": None, "reason": "brak oceny — zachowano"})
    return decisions


def _fetch_active_memories(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, content, summary_short, memory_type, importance_score,
               recall_count, tags, created_at, last_accessed_at
        FROM memories
        WHERE COALESCE(activity_state, 'active') = 'active'
          AND COALESCE(contradiction_flag, 0) = 0
        ORDER BY importance_score ASC, id ASC
        """
    ).fetchall()
    return [row_to_dict(row) for row in rows]


def _memory_to_prompt_item(m: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(m["id"]),
        "content": (m.get("content") or "")[:400],
        "summary_short": m.get("summary_short") or "",
        "memory_type": m.get("memory_type") or "",
        "importance_score": m.get("importance_score"),
        "recall_count": m.get("recall_count") or 0,
        "tags": m.get("tags") or "",
        "created_at": (m.get("created_at") or "")[:10],
        "last_accessed_at": (m.get("last_accessed_at") or "")[:10],
    }


def evaluate_memories_with_ai(memories: list[dict[str, Any]], freedom_level: int = 1) -> list[dict[str, Any]]:
    system_prompt = _SYSTEM_PROMPT_AGGRESSIVE if freedom_level >= 2 else _SYSTEM_PROMPT
    all_decisions: list[dict[str, Any]] = []
    for batch_start in range(0, len(memories), BATCH_SIZE):
        batch = memories[batch_start : batch_start + BATCH_SIZE]
        expected_ids = {int(m["id"]) for m in batch}
        prompt_items = [_memory_to_prompt_item(m) for m in batch]
        user_content = json.dumps(prompt_items, ensure_ascii=False, separators=(",", ":")) + "\nZwróć dokładnie format: [{\"id\":1,\"decision\":\"keep\",\"new_importance\":null,\"reason\":\"...\"}]"
        messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_content}]
        raw = _call_lm_studio(messages)
        all_decisions.extend(_parse_ai_response(raw, expected_ids))
    return all_decisions


def get_ai_decisions(conn: sqlite3.Connection, freedom_level: int = 1) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    memories = _fetch_active_memories(conn)
    if not memories:
        return [], [], []
    duplicate_candidates = get_duplicate_candidates(conn)
    protected_ids = get_protected_canonical_memory_ids(conn, duplicate_candidates)
    secondary_ids = get_secondary_duplicate_memory_ids(conn, duplicate_candidates)
    duplicate_related_ids = protected_ids | secondary_ids
    memories_for_ai = [m for m in memories if int(m["id"]) not in duplicate_related_ids]
    decisions = evaluate_memories_with_ai(memories_for_ai, freedom_level)
    mem_lookup = {int(m["id"]): m for m in memories}
    archive_decisions: list[dict[str, Any]] = []
    downgrade_decisions: list[dict[str, Any]] = []
    keep_decisions: list[dict[str, Any]] = []
    for decision in decisions:
        mem_id = int(decision["id"])
        original = mem_lookup.get(mem_id, {})
        safe_decision = dict(decision)
        if str(original.get("memory_type") or "") == "profile_note" and safe_decision["decision"] == "archive":
            safe_decision["decision"] = "keep"
            safe_decision["new_importance"] = None
            safe_decision["reason"] = "profilowa notatka zachowana ostrożnie"
        entry = {
            **original,
            "ai_decision": safe_decision["decision"],
            "ai_reason": safe_decision["reason"],
            "ai_new_importance": safe_decision.get("new_importance"),
        }
        if safe_decision["decision"] == "archive":
            archive_decisions.append(entry)
        elif safe_decision["decision"] == "downgrade":
            downgrade_decisions.append(entry)
        else:
            keep_decisions.append(entry)
    return archive_decisions, downgrade_decisions, keep_decisions
