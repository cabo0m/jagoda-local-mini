from __future__ import annotations

import json
import sqlite3
from typing import Any

from app import lm_studio_client
from app.memory_store import utc_now_iso

LM_STUDIO_MODEL = lm_studio_client.LM_STUDIO_MODEL
DEFAULT_PROJECT_KEY = "jagoda-memory-api"

_HOST_CONTEXT_PROMPT = """\
HOST CONTEXT MCP MAPI-local:
- Dzialasz wewnatrz serwera MCP mapi-local.
- Nazwy narzedzi sa funkcjami hosta MAPI-local. Uzywaj ich przez JSON tool_call.
- Dla konkretnych liczbowych ID memories uzywaj bezposrednio get_memory(memory_id) i get_memory_links(memory_id).
- Nie szukaj konkretnych ID przez search_memories i nie buduj zapytan tekstowych w stylu memory_id:505 OR memory_id:506.
- Jesli uzytkownik poda kilka ID, obsluguj je kolejno. Jeden krok to jedno narzedzie.
"""

_ACTION_JSON_SCHEMA: dict[str, Any] = {
    "name": "sandman_memory_action",
    "schema": {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "enum": ["tool_call", "final"]},
            "tool_name": {
                "type": "string",
                "enum": [
                    "search_memories",
                    "get_memory",
                    "get_memory_links",
                    "get_project_timeline",
                    "list_conflicted_memories",
                    "create_memory",
                    "archive_memory",
                    "link_memories",
                    "update_memory_importance",
                    "get_sandman_ai_preview",
                    "explain_conflict",
                    "none",
                ],
            },
            "arguments": {"type": "object", "additionalProperties": True},
            "answer": {"type": "string"},
            "reason": {"type": "string"},
        },
        "required": ["kind", "tool_name", "arguments", "answer", "reason"],
        "additionalProperties": False,
    },
}

_SYSTEM_PROMPT = """\
Jesteś agentką pamięci Sandmana z dostępem do narzędzi MAPI.
Twoim zadaniem jest odpowiadać użytkownikowi oraz utrzymywać graf pamięci: szukać, czytać, linkować i ostrożnie porządkować wspomnienia.
Dostęp do pamięci odbywa się wyłącznie przez funkcje narzędziowe hosta MCP mapi-local.
Gdy potrzebujesz danych albo zapisu, zwróć kind="tool_call" z tool_name ustawionym na jedną z funkcji.
Jeśli użytkownik poda konkretne ID wspomnień, nie używaj search_memories i nie pisz zapytań typu "memory_id:505 OR ...". Użyj get_memory(memory_id) i get_memory_links(memory_id), po jednym ID na krok.
Zwracaj zawsze wyłącznie JSON zgodny ze schematem:
- kind: "tool_call" albo "final"
- tool_name: nazwa narzędzia albo "none"
- arguments: obiekt argumentów
- answer: odpowiedź końcowa albo pusty string przy tool_call
- reason: krótkie uzasadnienie kroku

Dostępne narzędzia:
- search_memories(query, limit=5)
- get_memory(memory_id)
- get_memory_links(memory_id)
- get_project_timeline(project_key="jagoda-memory-api", limit=8)
- list_conflicted_memories(limit=5)
- create_memory(content, summary_short, memory_type, importance_score=0.5, tags="", source="sandman_agent")
- archive_memory(memory_id, reason)
- link_memories(from_memory_id, to_memory_id, relation_type, weight=0.8)
- update_memory_importance(memory_id, new_importance, reason)
- get_sandman_ai_preview(freedom_level=1)
- explain_conflict(memory_a_id, memory_b_id)

Zasady ogólne:
- Odpowiadaj po polsku.
- Używaj tylko jednego narzędzia na krok.
- Nie zmyślaj treści wspomnień. Jeśli potrzebujesz danych, najpierw użyj narzędzia odczytu.
- Przed zapisem, linkowaniem, zmianą ważności albo archiwizacją zbierz wystarczający kontekst.
- Archiwizuj tylko wtedy, gdy użytkownik wyraźnie tego chce albo masz bardzo mocny dowód, że wpis jest zbędny lub nieaktualny.
- Przy kind="tool_call" pole answer musi być pustym stringiem.
- Przy kind="final" ustaw tool_name="none" i wpisz gotową odpowiedź w answer.

Zasady linkowania grafu:
- Gdy użytkownik prosi o linki, graf, relacje, podpinanie, skojarzenia, konsolidację albo mocniejsze linkowanie, aktywnie twórz link_memories. Nie kończ na samym wyszukiwaniu.
- Jedno wspomnienie może mieć wiele relacji. Traktuj outgoing_links[] i incoming_links[] jako tablice.
- Najpierw znajdź lub pobierz anchor memory, potem sprawdź get_memory_links(anchor_id), żeby nie tworzyć duplikatów.
- Szukaj kandydatów po project_key, tagach, summary_short, treści i osi czasu.
- Silne powiązania: ten sam projekt, wspólne tagi, requirement z implementacją, bootstrap/core z zasadami działania, timeline/migration ze zmianą schematu, starszy wpis z nowszym następcą.
- Dozwolone relation_type: supports, contradicts, supersedes, duplicate_of, related_to, context_for, clarifies, documents, implements, configures, validates, risk_for, metric_for, same_project.
- Preferuj related_to dla ogólnego powiązania.
- Preferuj context_for, gdy wpis daje tło lub warunek.
- Preferuj supports, gdy wpis potwierdza lub wzmacnia inny wpis.
- Preferuj implements, gdy wpis opisuje implementację wymagania.
- Preferuj documents, gdy wpis dokumentuje decyzję, stan lub mechanizm.
- Preferuj supersedes, gdy nowszy wpis zastępuje starszy.
- duplicate_of używaj tylko dla realnych duplikatów.
- Nie linkuj wszystkiego ze wszystkim. Każdy link musi mieć sensowne uzasadnienie w reason.
"""


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _call_lm_studio(messages: list[dict[str, str]], *, max_tokens: int = 2048, timeout: int = 300) -> dict[str, Any]:
    text = lm_studio_client.call_lm_studio(
        messages,
        {"type": "json_schema", "json_schema": _ACTION_JSON_SCHEMA},
        max_tokens=max_tokens,
        timeout=timeout,
    ).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"Model nie zwrócił obiektu JSON. Odpowiedź: {text[:1200]}")
    parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError(f"Model nie zwrócił obiektu JSON. Odpowiedź: {text[:1200]}")
    return parsed


# ---------------------------------------------------------------------------
# READ TOOLS
# ---------------------------------------------------------------------------

def _tool_search_memories(conn: sqlite3.Connection, query: str, limit: int = 5) -> dict[str, Any]:
    q = (query or "").strip()
    if not q:
        return {"count": 0, "items": []}
    limit = max(1, min(int(limit or 5), 10))
    like = f"%{q}%"
    rows = conn.execute(
        """
        SELECT id, summary_short, memory_type, importance_score, recall_count, tags, content
        FROM memories
        WHERE COALESCE(activity_state, 'active') = 'active'
          AND (
                COALESCE(content, '') LIKE ? OR
                COALESCE(summary_short, '') LIKE ? OR
                COALESCE(tags, '') LIKE ?
          )
        ORDER BY importance_score DESC, recall_count DESC, id DESC
        LIMIT ?
        """,
        (like, like, like, limit),
    ).fetchall()
    items = []
    for row in rows:
        item = _row_to_dict(row)
        item["content"] = str(item.get("content") or "")[:280]
        items.append(item)
    return {"count": len(items), "items": items}


def _tool_get_memory(conn: sqlite3.Connection, memory_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM memories WHERE id = ?", (int(memory_id),)).fetchone()
    if row is None:
        return {"found": False, "memory_id": int(memory_id)}
    outgoing = conn.execute(
        "SELECT * FROM memory_links WHERE from_memory_id = ? ORDER BY id DESC LIMIT 12",
        (int(memory_id),),
    ).fetchall()
    incoming = conn.execute(
        "SELECT * FROM memory_links WHERE to_memory_id = ? ORDER BY id DESC LIMIT 12",
        (int(memory_id),),
    ).fetchall()
    return {
        "found": True,
        "memory": _row_to_dict(row),
        "outgoing_links": [_row_to_dict(item) for item in outgoing],
        "incoming_links": [_row_to_dict(item) for item in incoming],
    }


def _tool_get_memory_links(conn: sqlite3.Connection, memory_id: int) -> dict[str, Any]:
    outgoing = conn.execute(
        "SELECT * FROM memory_links WHERE from_memory_id = ? ORDER BY id DESC LIMIT 20",
        (int(memory_id),),
    ).fetchall()
    incoming = conn.execute(
        "SELECT * FROM memory_links WHERE to_memory_id = ? ORDER BY id DESC LIMIT 20",
        (int(memory_id),),
    ).fetchall()
    return {
        "memory_id": int(memory_id),
        "outgoing_links": [_row_to_dict(item) for item in outgoing],
        "incoming_links": [_row_to_dict(item) for item in incoming],
    }


def _tool_get_project_timeline(conn: sqlite3.Connection, project_key: str = DEFAULT_PROJECT_KEY, limit: int = 8) -> dict[str, Any]:
    limit = max(1, min(int(limit or 8), 20))
    rows = conn.execute(
        """
        SELECT id, event_type, title, payload_json, valid_at, origin, created_at
        FROM timeline_events
        WHERE project_key = ?
        ORDER BY COALESCE(valid_at, created_at) DESC, id DESC
        LIMIT ?
        """,
        (project_key or DEFAULT_PROJECT_KEY, limit),
    ).fetchall()
    items = []
    for row in rows:
        item = _row_to_dict(row)
        payload = {}
        raw_payload = item.get("payload_json")
        if isinstance(raw_payload, str) and raw_payload.strip():
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                payload = {}
        item["description"] = str(payload.get("description") or "")
        item["status"] = payload.get("status")
        item.pop("payload_json", None)
        items.append(item)
    return {"project_key": project_key or DEFAULT_PROJECT_KEY, "count": len(items), "items": items}


def _tool_list_conflicted_memories(conn: sqlite3.Connection, limit: int = 5) -> dict[str, Any]:
    limit = max(1, min(int(limit or 5), 20))
    rows = conn.execute(
        """
        SELECT id, summary_short, memory_type, importance_score, recall_count, content
        FROM memories
        WHERE COALESCE(contradiction_flag, 0) = 1
        ORDER BY importance_score DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    items = []
    for row in rows:
        item = _row_to_dict(row)
        item["content"] = str(item.get("content") or "")[:280]
        items.append(item)
    return {"count": len(items), "items": items}


# ---------------------------------------------------------------------------
# WRITE TOOLS
# ---------------------------------------------------------------------------

def _tool_create_memory(
    conn: sqlite3.Connection,
    content: str,
    summary_short: str,
    memory_type: str,
    importance_score: float = 0.5,
    tags: str = "",
    source: str = "sandman_agent",
) -> dict[str, Any]:
    content = (content or "").strip()
    summary_short = (summary_short or "").strip()
    memory_type = (memory_type or "working").strip()
    if not content:
        return {"status": "error", "reason": "content nie może być puste"}
    existing = conn.execute(
        "SELECT id FROM memories WHERE summary_short = ? AND memory_type = ? AND COALESCE(activity_state, 'active') = 'active'",
        (summary_short, memory_type),
    ).fetchone()
    if existing:
        return {"status": "already_exists", "existing_memory_id": int(existing["id"])}
    importance_score = lm_studio_client.clamp_importance(float(importance_score or 0.5))
    now = utc_now_iso()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO memories (
            content, summary_short, memory_type, source,
            importance_score, confidence_score, tags,
            created_at, last_accessed_at, activity_state,
            evidence_count, contradiction_flag
        ) VALUES (?, ?, ?, ?, ?, 0.5, ?, ?, ?, 'active', 1, 0)
        """,
        (content, summary_short, memory_type, source, importance_score, tags or "", now, now),
    )
    conn.commit()
    memory_id = int(cursor.lastrowid)
    row = conn.execute(
        "SELECT id, summary_short, memory_type, importance_score FROM memories WHERE id = ?",
        (memory_id,),
    ).fetchone()
    return {
        "status": "created",
        "memory_id": memory_id,
        "summary_short": row["summary_short"],
        "memory_type": row["memory_type"],
    }


def _tool_archive_memory(conn: sqlite3.Connection, memory_id: int, reason: str) -> dict[str, Any]:
    memory_id = int(memory_id)
    row = conn.execute(
        "SELECT id, activity_state, memory_type FROM memories WHERE id = ?",
        (memory_id,),
    ).fetchone()
    if row is None:
        return {"status": "error", "reason": f"Wspomnienie {memory_id} nie istnieje"}
    if str(row["activity_state"] or "active") == "archived":
        return {"status": "already_archived", "memory_id": memory_id}
    now = utc_now_iso()
    note = f"sandman_agent: {(reason or 'agent_decision')[:120]}"
    conn.execute(
        "UPDATE memories SET activity_state = 'archived', archived_at = ?, sandman_note = ? WHERE id = ?",
        (now, note, memory_id),
    )
    conn.commit()
    return {"status": "archived", "memory_id": memory_id, "archived_at": now, "reason": reason}


def _tool_link_memories(
    conn: sqlite3.Connection,
    from_memory_id: int,
    to_memory_id: int,
    relation_type: str,
    weight: float = 0.8,
) -> dict[str, Any]:
    from_id = int(from_memory_id)
    to_id = int(to_memory_id)

    relation_type = (relation_type or "related_to").strip()
    relation_aliases = {"relates_to": "related_to"}
    relation_type = relation_aliases.get(relation_type, relation_type)

    weight = min(1.0, max(0.0, float(weight or 0.8)))

    allowed = {
        "supports",
        "contradicts",
        "supersedes",
        "duplicate_of",
        "related_to",
        "context_for",
        "clarifies",
        "documents",
        "implements",
        "configures",
        "validates",
        "risk_for",
        "metric_for",
        "same_project",
    }

    if relation_type not in allowed:
        relation_type = "related_to"

    existing = conn.execute(
        "SELECT id FROM memory_links WHERE from_memory_id = ? AND to_memory_id = ? AND relation_type = ?",
        (from_id, to_id, relation_type),
    ).fetchone()
    if existing:
        return {"status": "already_exists", "link_id": int(existing["id"]), "relation_type": relation_type}
    now = utc_now_iso()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO memory_links (from_memory_id, to_memory_id, relation_type, weight, origin, created_at) VALUES (?, ?, ?, ?, 'sandman_agent', ?)",
        (from_id, to_id, relation_type, weight, now),
    )
    conn.commit()
    link_id = int(cursor.lastrowid)
    return {
        "status": "created",
        "link_id": link_id,
        "from_memory_id": from_id,
        "to_memory_id": to_id,
        "relation_type": relation_type,
        "weight": weight,
    }


def _tool_update_memory_importance(
    conn: sqlite3.Connection,
    memory_id: int,
    new_importance: float,
    reason: str,
) -> dict[str, Any]:
    memory_id = int(memory_id)
    row = conn.execute(
        "SELECT id, importance_score, activity_state FROM memories WHERE id = ?",
        (memory_id,),
    ).fetchone()
    if row is None:
        return {"status": "error", "reason": f"Wspomnienie {memory_id} nie istnieje"}
    new_importance = lm_studio_client.clamp_importance(float(new_importance or 0.5))
    old_importance = float(row["importance_score"] or 0.5)
    note = f"sandman_agent: {(reason or 'agent_update')[:120]}"
    conn.execute(
        "UPDATE memories SET importance_score = ?, sandman_note = ? WHERE id = ?",
        (new_importance, note, memory_id),
    )
    conn.commit()
    return {
        "status": "updated",
        "memory_id": memory_id,
        "old_importance": old_importance,
        "new_importance": new_importance,
        "reason": reason,
    }



def _tool_explain_conflict(conn: sqlite3.Connection, memory_a_id: int, memory_b_id: int) -> dict[str, Any]:
    from app import conflict_explainer
    return conflict_explainer.explain_conflict_pair(conn, int(memory_a_id), int(memory_b_id))


def _tool_get_sandman_ai_preview(conn: sqlite3.Connection, freedom_level: int = 1) -> dict[str, Any]:
    freedom_level = max(0, min(int(freedom_level or 1), 2))
    from app import sandman_ai
    archive_decisions, downgrade_decisions, keep_decisions = sandman_ai.get_ai_decisions(conn, freedom_level)
    return {
        "freedom_level": freedom_level,
        "model": sandman_ai.LM_STUDIO_MODEL,
        "archive_count": len(archive_decisions),
        "downgrade_count": len(downgrade_decisions),
        "keep_count": len(keep_decisions),
        "archive_candidates": [
            {
                "id": d["id"],
                "summary_short": d.get("summary_short"),
                "ai_reason": d.get("ai_reason"),
                "importance_score": d.get("importance_score"),
                "memory_type": d.get("memory_type"),
            }
            for d in archive_decisions
        ],
        "downgrade_candidates": [
            {
                "id": d["id"],
                "summary_short": d.get("summary_short"),
                "ai_reason": d.get("ai_reason"),
                "importance_score": d.get("importance_score"),
                "ai_new_importance": d.get("ai_new_importance"),
                "memory_type": d.get("memory_type"),
            }
            for d in downgrade_decisions
        ],
    }


# ---------------------------------------------------------------------------
# DISPATCH
# ---------------------------------------------------------------------------

_TOOL_DISPATCH = {
    "search_memories": _tool_search_memories,
    "get_memory": _tool_get_memory,
    "get_memory_links": _tool_get_memory_links,
    "get_project_timeline": _tool_get_project_timeline,
    "list_conflicted_memories": _tool_list_conflicted_memories,
    "create_memory": _tool_create_memory,
    "archive_memory": _tool_archive_memory,
    "link_memories": _tool_link_memories,
    "update_memory_importance": _tool_update_memory_importance,
    "get_sandman_ai_preview": _tool_get_sandman_ai_preview,
    "explain_conflict": _tool_explain_conflict,
}


def _normalize_action(action: dict[str, Any]) -> dict[str, Any]:
    kind = str(action.get("kind") or "final").strip().lower()
    if kind not in {"tool_call", "final"}:
        kind = "final"
    tool_name = str(action.get("tool_name") or "none").strip()
    if tool_name not in _TOOL_DISPATCH and tool_name != "none":
        tool_name = "none"
    arguments = action.get("arguments") if isinstance(action.get("arguments"), dict) else {}
    answer = str(action.get("answer") or "").strip()
    reason = str(action.get("reason") or "").strip()
    if kind == "final":
        tool_name = "none"
    return {"kind": kind, "tool_name": tool_name, "arguments": arguments, "answer": answer, "reason": reason}


def _run_tool(conn: sqlite3.Connection, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if tool_name not in _TOOL_DISPATCH:
        raise ValueError(f"Nieznane narzędzie: {tool_name}")

    if tool_name == "search_memories":
        return _tool_search_memories(conn, str(arguments.get("query") or ""), int(arguments.get("limit") or 5))
    if tool_name in {"get_memory", "get_memory_links"}:
        return _TOOL_DISPATCH[tool_name](conn, int(arguments.get("memory_id") or 0))
    if tool_name == "get_project_timeline":
        return _tool_get_project_timeline(
            conn,
            str(arguments.get("project_key") or DEFAULT_PROJECT_KEY),
            int(arguments.get("limit") or 8),
        )
    if tool_name == "list_conflicted_memories":
        return _tool_list_conflicted_memories(conn, int(arguments.get("limit") or 5))
    if tool_name == "create_memory":
        return _tool_create_memory(
            conn,
            str(arguments.get("content") or ""),
            str(arguments.get("summary_short") or ""),
            str(arguments.get("memory_type") or "working"),
            float(arguments.get("importance_score") or 0.5),
            str(arguments.get("tags") or ""),
            str(arguments.get("source") or "sandman_agent"),
        )
    if tool_name == "archive_memory":
        return _tool_archive_memory(conn, int(arguments.get("memory_id") or 0), str(arguments.get("reason") or ""))
    if tool_name == "link_memories":
        return _tool_link_memories(
            conn,
            int(arguments.get("from_memory_id") or 0),
            int(arguments.get("to_memory_id") or 0),
            str(arguments.get("relation_type") or "related_to"),
            float(arguments.get("weight") or 0.8),
        )
    if tool_name == "update_memory_importance":
        return _tool_update_memory_importance(
            conn,
            int(arguments.get("memory_id") or 0),
            float(arguments.get("new_importance") or 0.5),
            str(arguments.get("reason") or ""),
        )
    if tool_name == "get_sandman_ai_preview":
        return _tool_get_sandman_ai_preview(conn, int(arguments.get("freedom_level") or 1))
    if tool_name == "explain_conflict":
        return _tool_explain_conflict(
            conn,
            int(arguments.get("memory_a_id") or 0),
            int(arguments.get("memory_b_id") or 0),
        )
    raise ValueError(f"Nieobsługiwane narzędzie: {tool_name}")


def _looks_like_linking_task(prompt: str) -> bool:
    text = (prompt or "").lower()
    needles = (
        "link", "linki", "linków", "linkowania", "graf", "relacje",
        "podpin", "podepn", "skojarz", "skojarzenia", "konsolidac",
        "memory_links", "related_to", "context_for", "supports",
    )
    return any(needle in text for needle in needles)


def _write_tools_allowed_for_prompt(prompt: str) -> set[str]:
    if _looks_like_linking_task(prompt):
        return {"link_memories"}
    return {"create_memory", "archive_memory", "link_memories", "update_memory_importance"}


def _blocked_tool_result(tool_name: str, prompt: str) -> dict[str, Any]:
    return {
        "status": "blocked_by_host_guard",
        "tool_name": tool_name,
        "reason": "W zadaniu linkowania host dopuszcza tylko link_memories jako narzędzie zapisu.",
        "linking_task": _looks_like_linking_task(prompt),
    }


def _auto_final_after_link(
    tool_result: dict[str, Any],
    *,
    step: int,
    trace: list[dict[str, Any]],
) -> dict[str, Any]:
    status = str(tool_result.get("status") or "unknown")

    if status == "created":
        link_id = tool_result.get("link_id")
        answer = (
            "Utworzyłam link "
            f"{tool_result.get('from_memory_id')} -> {tool_result.get('to_memory_id')} "
            f"({tool_result.get('relation_type')}, weight={tool_result.get('weight')})."
        )
        if link_id is not None:
            answer += f" ID linku: {link_id}."
    elif status == "already_exists":
        answer = (
            "Nie utworzyłam duplikatu. Taki link już istnieje "
            f"(link_id={tool_result.get('link_id')}, relation_type={tool_result.get('relation_type')})."
        )
    else:
        answer = f"Próba linkowania zakończyła się statusem: {status}."

    return {
        "status": "completed",
        "model": LM_STUDIO_MODEL,
        "steps": step,
        "answer": answer,
        "reason": "auto_final_after_link_memories",
        "trace": trace,
        "auto_final_after_write": True,
    }


# ---------------------------------------------------------------------------
# AGENT LOOP
# ---------------------------------------------------------------------------

def run_memory_tool_agent(conn: sqlite3.Connection, *, user_query: str, max_steps: int = 4) -> dict[str, Any]:
    prompt = (user_query or "").strip()
    if not prompt:
        raise ValueError("user_query nie może być puste")
    max_steps = max(1, min(int(max_steps or 8), 16))
    allowed_write_tools = _write_tools_allowed_for_prompt(prompt)

    messages: list[dict[str, str]] = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "system", "content": _HOST_CONTEXT_PROMPT},
        {"role": "user", "content": prompt},
    ]
    trace: list[dict[str, Any]] = []
    write_tools = {"create_memory", "archive_memory", "link_memories", "update_memory_importance"}

    for step in range(1, max_steps + 1):
        action = _normalize_action(_call_lm_studio(messages, max_tokens=2048, timeout=300))

        if action["kind"] == "final":
            return {
                "status": "completed",
                "model": LM_STUDIO_MODEL,
                "steps": step,
                "answer": action["answer"],
                "reason": action["reason"],
                "trace": trace,
            }

        if action["tool_name"] == "none":
            return {
                "status": "completed",
                "model": LM_STUDIO_MODEL,
                "steps": step,
                "answer": action["answer"] or "Nie wybrałam narzędzia ani odpowiedzi końcowej.",
                "reason": action["reason"],
                "trace": trace,
            }

        if action["tool_name"] in write_tools and action["tool_name"] not in allowed_write_tools:
            tool_result = _blocked_tool_result(action["tool_name"], prompt)
        else:
            tool_result = _run_tool(conn, action["tool_name"], action["arguments"])

        trace.append({
            "step": step,
            "tool_name": action["tool_name"],
            "arguments": action["arguments"],
            "reason": action["reason"],
            "result": tool_result,
        })

        if action["tool_name"] == "link_memories" and str(tool_result.get("status") or "") in {"created", "already_exists"}:
            return _auto_final_after_link(tool_result, step=step, trace=trace)

        messages.append({"role": "assistant", "content": json.dumps(action, ensure_ascii=False)})
        messages.append({
            "role": "user",
            "content": "Wynik narzędzia: " + json.dumps(
                {"tool_name": action["tool_name"], "result": tool_result},
                ensure_ascii=False,
            ),
        })

    messages.append({
        "role": "user",
        "content": "To ostatni krok. Nie wołaj już narzędzi. Zwróć kind='final' i odpowiedź dla użytkownika na podstawie zebranych danych.",
    })
    final_action = _normalize_action(_call_lm_studio(messages, max_tokens=2048, timeout=300))
    return {
        "status": "completed",
        "model": LM_STUDIO_MODEL,
        "steps": max_steps,
        "answer": final_action.get("answer") or "Nie udało się zbudować odpowiedzi końcowej.",
        "reason": final_action.get("reason", "forced_final"),
        "trace": trace,
    }


