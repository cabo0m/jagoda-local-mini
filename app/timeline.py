from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

UTC_ISO_8601_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

TIMELINE_REQUIRED_FIELDS = (
    "id",
    "event_time",
    "created_at",
    "event_type",
    "origin",
    "reconstructed",
)

TIMELINE_OPTIONAL_FIELDS = (
    "memory_id",
    "related_memory_id",
    "run_id",
    "operation_id",
    "timeline_scope",
    "semantic_kind",
    "title",
    "project_key",
    "valid_at",
    "source_table",
    "source_row_id",
    "payload_json",
)

TIMELINE_ALL_FIELDS = TIMELINE_REQUIRED_FIELDS + TIMELINE_OPTIONAL_FIELDS

TIMELINE_INDEX_NAMES = (
    "idx_timeline_events_time",
    "idx_timeline_events_memory",
    "idx_timeline_events_related_memory",
    "idx_timeline_events_run",
    "idx_timeline_events_type",
    "idx_timeline_events_operation",
    "idx_timeline_events_scope",
    "idx_timeline_events_semantic_kind",
    "idx_timeline_events_project_key",
    "idx_timeline_events_valid_at",
)

TIMELINE_LINK_TIMESTAMP_FIELDS = (
    "created_at",
    "archived_at",
)

TIMELINE_PAYLOAD_PREFERRED_KEYS = (
    "changed_fields",
    "old",
    "new",
    "reason",
    "relation_type",
    "weight",
    "recall_type",
    "strength",
    "snapshot_only",
    "description",
    "category",
    "status",
    "canonical",
    "derived_from_memory_ids",
    "derived_from_run_ids",
    "tags",
)

TIMELINE_EVENT_TYPES = {
    "memory.created",
    "memory.updated",
    "memory.archived",
    "memory.unarchived",
    "memory.recalled",
    "memory.accessed",
    "memory.accessed_last_snapshot",
    "memory.recalled_last_snapshot",
    "memory.scope_assigned",
    "memory.scope_changed",
    "memory.owner_assigned",
    "link.created",
    "link.archived",
    "sleep_run.started",
    "sleep_run.finished",
    "undo.applied",
    "project.milestone_recorded",
    "project.decision_recorded",
    "project.status_changed",
    "project.note_recorded",
    "project.phase_started",
    "project.phase_completed",
    "workspace.member_added",
}

TIMELINE_EVENT_PREFIXES = (
    "sleep_action.",
    "sandman_agent.",
    "conflict.",
)

TIMELINE_ORIGINS = {
    "api",
    "manual",
    "system",
    "undo",
    "backfill",
    "sandman_v1_auto",
    "conflict_v1_auto",
    "sandman_agent_auto",
    "consolidation_v1_auto",
    "conflict_explainer_auto",
    "multiuser_auto",
}

TIMELINE_SCOPES = {
    "system",
    "project",
    "memory",
    "run",
}

TIMELINE_SEMANTIC_KINDS = {
    "runtime_event",
    "milestone",
    "decision",
    "status_change",
    "note",
    "phase",
    "backfill_snapshot",
    "artifact_change",
}

PROJECT_EVENT_TYPE_TO_SEMANTIC_KIND = {
    "project.milestone_recorded": "milestone",
    "project.decision_recorded": "decision",
    "project.status_changed": "status_change",
    "project.note_recorded": "note",
    "project.phase_started": "phase",
    "project.phase_completed": "phase",
}


class TimelineValidationError(ValueError):
    """Raised when timeline data breaks the agreed contract."""



def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")



def ensure_utc_iso8601(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TimelineValidationError("Timestamp musi być niepustym stringiem UTC ISO 8601")

    candidate = value.strip()
    if not UTC_ISO_8601_PATTERN.fullmatch(candidate):
        raise TimelineValidationError(
            f"Nieprawidłowy format czasu '{value}'. Oczekiwano YYYY-MM-DDTHH:MM:SSZ"
        )

    try:
        datetime.strptime(candidate, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise TimelineValidationError(f"Nieprawidłowa data UTC ISO 8601: '{value}'") from exc

    return candidate



def validate_event_type(event_type: str) -> str:
    if not isinstance(event_type, str) or not event_type.strip():
        raise TimelineValidationError("event_type musi być niepustym stringiem")

    candidate = event_type.strip()
    if candidate in TIMELINE_EVENT_TYPES:
        return candidate

    if any(candidate.startswith(prefix) for prefix in TIMELINE_EVENT_PREFIXES):
        return candidate

    raise TimelineValidationError(f"Nieobsługiwany event_type: '{event_type}'")



def validate_origin(origin: str | None) -> str | None:
    if origin is None:
        return None

    if not isinstance(origin, str) or not origin.strip():
        raise TimelineValidationError("origin musi być None albo niepustym stringiem")

    candidate = origin.strip()
    if candidate not in TIMELINE_ORIGINS:
        raise TimelineValidationError(f"Nieobsługiwany origin: '{origin}'")

    return candidate



def validate_reconstructed(value: int | bool) -> int:
    if isinstance(value, bool):
        return int(value)

    try:
        candidate = int(value)
    except (TypeError, ValueError) as exc:
        raise TimelineValidationError("reconstructed musi mieć wartość 0 albo 1") from exc

    if candidate not in (0, 1):
        raise TimelineValidationError("reconstructed musi mieć wartość 0 albo 1")

    return candidate



def validate_operation_id(operation_id: str | None) -> str | None:
    if operation_id is None:
        return None

    if not isinstance(operation_id, str) or not operation_id.strip():
        raise TimelineValidationError("operation_id musi być None albo niepustym stringiem")

    return operation_id.strip()



def validate_timeline_scope(timeline_scope: str | None, *, event_type: str | None = None) -> str:
    if timeline_scope is None:
        if event_type and event_type.startswith("project."):
            return "project"
        return "system"

    if not isinstance(timeline_scope, str) or not timeline_scope.strip():
        raise TimelineValidationError("timeline_scope musi być niepustym stringiem")

    candidate = timeline_scope.strip()
    if candidate not in TIMELINE_SCOPES:
        raise TimelineValidationError(f"Nieobsługiwany timeline_scope: '{timeline_scope}'")
    return candidate



def validate_semantic_kind(semantic_kind: str | None, *, event_type: str | None = None) -> str:
    if semantic_kind is None:
        if event_type in PROJECT_EVENT_TYPE_TO_SEMANTIC_KIND:
            return PROJECT_EVENT_TYPE_TO_SEMANTIC_KIND[str(event_type)]
        return "runtime_event"

    if not isinstance(semantic_kind, str) or not semantic_kind.strip():
        raise TimelineValidationError("semantic_kind musi być niepustym stringiem")

    candidate = semantic_kind.strip()
    if candidate not in TIMELINE_SEMANTIC_KINDS:
        raise TimelineValidationError(f"Nieobsługiwany semantic_kind: '{semantic_kind}'")
    return candidate



def validate_title(title: str | None) -> str | None:
    if title is None:
        return None
    if not isinstance(title, str) or not title.strip():
        raise TimelineValidationError("title musi być None albo niepustym stringiem")
    return title.strip()



def validate_project_key(project_key: str | None) -> str | None:
    if project_key is None:
        return None
    if not isinstance(project_key, str) or not project_key.strip():
        raise TimelineValidationError("project_key musi być None albo niepustym stringiem")
    return project_key.strip()



def normalize_runtime_timestamp(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TimelineValidationError("Timestamp musi być niepustym stringiem UTC ISO 8601")

    candidate = value.strip()
    if UTC_ISO_8601_PATTERN.fullmatch(candidate):
        return candidate

    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TimelineValidationError(
            f"Nieprawidłowy format czasu '{value}'. Oczekiwano UTC ISO 8601"
        ) from exc

    if parsed.tzinfo is None:
        raise TimelineValidationError(
            f"Nieprawidłowy format czasu '{value}'. Oczekiwano daty z UTC"
        )

    parsed_utc = parsed.astimezone(timezone.utc).replace(microsecond=0)
    return parsed_utc.strftime("%Y-%m-%dT%H:%M:%SZ")



def coerce_runtime_origin(origin: str | None, default: str | None = None) -> str | None:
    candidate = origin if origin is not None else default
    if candidate is None:
        return None

    if not isinstance(candidate, str) or not candidate.strip():
        raise TimelineValidationError("origin musi być None albo niepustym stringiem")

    normalized = candidate.strip()
    aliases = {
        "conflicts_v1_auto": "conflict_v1_auto",
        "pytest": "manual",
        "unit_test": "manual",
        "manual_test_setup": "manual",
    }
    normalized = aliases.get(normalized, normalized)

    if normalized in TIMELINE_ORIGINS:
        return normalized

    return normalized



def timeline_payload_json(payload: Any) -> str | None:
    if payload is None:
        return None

    if not isinstance(payload, (dict, list)):
        raise TimelineValidationError("payload musi być dict, list albo None")

    return json.dumps(payload, ensure_ascii=False, sort_keys=True)



def timeline_contract() -> dict[str, Any]:
    return {
        "required_fields": list(TIMELINE_REQUIRED_FIELDS),
        "optional_fields": list(TIMELINE_OPTIONAL_FIELDS),
        "all_fields": list(TIMELINE_ALL_FIELDS),
        "index_names": list(TIMELINE_INDEX_NAMES),
        "event_types": sorted(TIMELINE_EVENT_TYPES),
        "event_prefixes": list(TIMELINE_EVENT_PREFIXES),
        "origins": sorted(TIMELINE_ORIGINS),
        "timeline_scopes": sorted(TIMELINE_SCOPES),
        "semantic_kinds": sorted(TIMELINE_SEMANTIC_KINDS),
        "link_timestamp_fields": list(TIMELINE_LINK_TIMESTAMP_FIELDS),
        "payload_rules": {
            "accepted_types": ["dict", "list", "null"],
            "preferred_keys": list(TIMELINE_PAYLOAD_PREFERRED_KEYS),
            "allow_full_snapshots": False,
        },
    }



def new_operation_id(prefix: str | None = None) -> str:
    suffix = uuid4().hex
    if prefix is None:
        return suffix

    cleaned_prefix = str(prefix).strip()
    if not cleaned_prefix:
        raise TimelineValidationError("prefix operation_id nie może być pusty")

    return f"{cleaned_prefix}:{suffix}"



def run_operation_id(run_id: int) -> str:
    if int(run_id) <= 0:
        raise TimelineValidationError("run_id musi być dodatnią liczbą całkowitą")
    return f"run:{int(run_id)}"



def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in existing:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")



def ensure_timeline_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS timeline_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT NOT NULL,
            event_type TEXT NOT NULL,
            memory_id INTEGER,
            related_memory_id INTEGER,
            run_id INTEGER,
            operation_id TEXT,
            timeline_scope TEXT NOT NULL DEFAULT 'system',
            semantic_kind TEXT NOT NULL DEFAULT 'runtime_event',
            title TEXT,
            project_key TEXT,
            valid_at TEXT,
            source_table TEXT,
            source_row_id INTEGER,
            origin TEXT,
            reconstructed INTEGER NOT NULL DEFAULT 0 CHECK (reconstructed IN (0, 1)),
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (memory_id) REFERENCES memories(id),
            FOREIGN KEY (related_memory_id) REFERENCES memories(id),
            FOREIGN KEY (run_id) REFERENCES sleep_runs(id)
        )
        """
    )
    ensure_column(conn, "timeline_events", "operation_id", "operation_id TEXT")
    ensure_column(conn, "timeline_events", "timeline_scope", "timeline_scope TEXT NOT NULL DEFAULT 'system'")
    ensure_column(conn, "timeline_events", "semantic_kind", "semantic_kind TEXT NOT NULL DEFAULT 'runtime_event'")
    ensure_column(conn, "timeline_events", "title", "title TEXT")
    ensure_column(conn, "timeline_events", "project_key", "project_key TEXT")
    ensure_column(conn, "timeline_events", "valid_at", "valid_at TEXT")
    ensure_column(conn, "memory_links", "created_at", "created_at TEXT")
    ensure_column(conn, "memory_links", "archived_at", "archived_at TEXT")
    # Multi-user columns (Stage 1) — dodawane idempotentnie
    ensure_column(conn, "timeline_events", "actor_user_id", "actor_user_id INTEGER")
    ensure_column(conn, "timeline_events", "workspace_id", "workspace_id INTEGER")
    ensure_column(conn, "timeline_events", "actor_type", "actor_type TEXT NOT NULL DEFAULT 'system'")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_time ON timeline_events(event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_memory ON timeline_events(memory_id, event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_related_memory ON timeline_events(related_memory_id, event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_run ON timeline_events(run_id, event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_type ON timeline_events(event_type, event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_operation ON timeline_events(operation_id, event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_scope ON timeline_events(timeline_scope, event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_semantic_kind ON timeline_events(semantic_kind, event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_project_key ON timeline_events(project_key, valid_at DESC, event_time DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_valid_at ON timeline_events(valid_at DESC, event_time DESC, id DESC)")



def record_timeline_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    event_time: str | None = None,
    memory_id: int | None = None,
    related_memory_id: int | None = None,
    run_id: int | None = None,
    operation_id: str | None = None,
    timeline_scope: str | None = None,
    semantic_kind: str | None = None,
    title: str | None = None,
    project_key: str | None = None,
    valid_at: str | None = None,
    source_table: str | None = None,
    source_row_id: int | None = None,
    origin: str | None = None,
    reconstructed: int = 0,
    payload: Any = None,
    actor_user_id: int | None = None,
    workspace_id: int | None = None,
    actor_type: str | None = None,
    now_fn: Callable[[], str] = utc_now_iso,
) -> int:
    ensure_timeline_schema(conn)

    event_type_value = validate_event_type(event_type)
    origin_value = coerce_runtime_origin(origin)
    reconstructed_value = validate_reconstructed(reconstructed)
    event_time_value = normalize_runtime_timestamp(event_time or now_fn())
    created_at_value = normalize_runtime_timestamp(now_fn())
    operation_id_value = validate_operation_id(operation_id)
    timeline_scope_value = validate_timeline_scope(timeline_scope, event_type=event_type_value)
    semantic_kind_value = validate_semantic_kind(semantic_kind, event_type=event_type_value)
    title_value = validate_title(title)
    project_key_value = validate_project_key(project_key)
    valid_at_value = normalize_runtime_timestamp(valid_at) if valid_at else event_time_value
    payload_json_value = timeline_payload_json(payload)
    actor_type_value = (actor_type or "system").strip() or "system"

    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO timeline_events (
            event_time,
            event_type,
            memory_id,
            related_memory_id,
            run_id,
            operation_id,
            timeline_scope,
            semantic_kind,
            title,
            project_key,
            valid_at,
            source_table,
            source_row_id,
            origin,
            reconstructed,
            payload_json,
            created_at,
            actor_user_id,
            workspace_id,
            actor_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_time_value,
            event_type_value,
            memory_id,
            related_memory_id,
            run_id,
            operation_id_value,
            timeline_scope_value,
            semantic_kind_value,
            title_value,
            project_key_value,
            valid_at_value,
            source_table,
            source_row_id,
            origin_value,
            reconstructed_value,
            payload_json_value,
            created_at_value,
            actor_user_id,
            workspace_id,
            actor_type_value,
        ),
    )
    return int(cursor.lastrowid)



def record_project_event(
    conn: sqlite3.Connection,
    *,
    project_key: str,
    event_type: str,
    title: str,
    description: str | None = None,
    valid_at: str | None = None,
    origin: str | None = "manual",
    memory_ids: list[int] | None = None,
    run_ids: list[int] | None = None,
    tags: list[str] | None = None,
    status: str | None = None,
    canonical: bool = True,
    category: str | None = None,
    operation_id: str | None = None,
    now_fn: Callable[[], str] = utc_now_iso,
) -> int:
    event_type_value = validate_event_type(event_type)
    if not event_type_value.startswith("project."):
        raise TimelineValidationError("record_project_event obsługuje tylko eventy project.*")

    memory_ids_value = [int(item) for item in (memory_ids or [])]
    run_ids_value = [int(item) for item in (run_ids or [])]
    payload = {
        "description": description,
        "category": category or PROJECT_EVENT_TYPE_TO_SEMANTIC_KIND.get(event_type_value),
        "status": status,
        "canonical": bool(canonical),
        "derived_from_memory_ids": memory_ids_value,
        "derived_from_run_ids": run_ids_value,
        "tags": list(tags or []),
    }
    payload = {key: value for key, value in payload.items() if value not in (None, [], "")}

    event_time_value = valid_at or now_fn()
    primary_memory_id = memory_ids_value[0] if memory_ids_value else None
    primary_run_id = run_ids_value[0] if run_ids_value else None
    operation_id_value = operation_id or new_operation_id("proj")

    return record_timeline_event(
        conn,
        event_type=event_type_value,
        event_time=event_time_value,
        valid_at=valid_at or event_time_value,
        memory_id=primary_memory_id,
        run_id=primary_run_id,
        operation_id=operation_id_value,
        timeline_scope="project",
        semantic_kind=PROJECT_EVENT_TYPE_TO_SEMANTIC_KIND.get(event_type_value),
        title=title,
        project_key=project_key,
        source_table="timeline_events",
        source_row_id=None,
        origin=origin,
        payload=payload,
        now_fn=now_fn,
    )



def backfill_timeline(conn: sqlite3.Connection) -> int:
    ensure_timeline_schema(conn)
    inserted = 0

    def counting_execute(sql: str) -> None:
        nonlocal inserted
        cursor = conn.execute(sql)
        inserted += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    counting_execute(
        """
        INSERT INTO timeline_events (
            event_time, event_type, memory_id, related_memory_id, run_id,
            operation_id, timeline_scope, semantic_kind, title, project_key, valid_at,
            source_table, source_row_id, origin, reconstructed, payload_json, created_at
        )
        SELECT
            m.created_at,
            'memory.created',
            m.id,
            NULL,
            NULL,
            NULL,
            'system',
            'backfill_snapshot',
            NULL,
            NULL,
            m.created_at,
            'memories',
            m.id,
            COALESCE(m.source, 'backfill'),
            1,
            NULL,
            CURRENT_TIMESTAMP
        FROM memories m
        WHERE m.created_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM timeline_events t
              WHERE t.event_type = 'memory.created'
                AND t.source_table = 'memories'
                AND t.source_row_id = m.id
                AND t.reconstructed = 1
          )
        """
    )

    counting_execute(
        """
        INSERT INTO timeline_events (
            event_time, event_type, memory_id, related_memory_id, run_id,
            operation_id, timeline_scope, semantic_kind, title, project_key, valid_at,
            source_table, source_row_id, origin, reconstructed, payload_json, created_at
        )
        SELECT
            m.last_accessed_at,
            'memory.accessed_last_snapshot',
            m.id,
            NULL,
            NULL,
            NULL,
            'system',
            'backfill_snapshot',
            NULL,
            NULL,
            m.last_accessed_at,
            'memories',
            m.id,
            'backfill',
            1,
            NULL,
            CURRENT_TIMESTAMP
        FROM memories m
        WHERE m.last_accessed_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM timeline_events t
              WHERE t.event_type = 'memory.accessed_last_snapshot'
                AND t.source_table = 'memories'
                AND t.source_row_id = m.id
                AND t.reconstructed = 1
          )
        """
    )

    counting_execute(
        """
        INSERT INTO timeline_events (
            event_time, event_type, memory_id, related_memory_id, run_id,
            operation_id, timeline_scope, semantic_kind, title, project_key, valid_at,
            source_table, source_row_id, origin, reconstructed, payload_json, created_at
        )
        SELECT
            m.last_recalled_at,
            'memory.recalled_last_snapshot',
            m.id,
            NULL,
            NULL,
            NULL,
            'system',
            'backfill_snapshot',
            NULL,
            NULL,
            m.last_recalled_at,
            'memories',
            m.id,
            'backfill',
            1,
            NULL,
            CURRENT_TIMESTAMP
        FROM memories m
        WHERE m.last_recalled_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM timeline_events t
              WHERE t.event_type = 'memory.recalled_last_snapshot'
                AND t.source_table = 'memories'
                AND t.source_row_id = m.id
                AND t.reconstructed = 1
          )
        """
    )

    counting_execute(
        """
        INSERT INTO timeline_events (
            event_time, event_type, memory_id, related_memory_id, run_id,
            operation_id, timeline_scope, semantic_kind, title, project_key, valid_at,
            source_table, source_row_id, origin, reconstructed, payload_json, created_at
        )
        SELECT
            m.archived_at,
            'memory.archived',
            m.id,
            NULL,
            NULL,
            NULL,
            'system',
            'backfill_snapshot',
            NULL,
            NULL,
            m.archived_at,
            'memories',
            m.id,
            'backfill',
            1,
            NULL,
            CURRENT_TIMESTAMP
        FROM memories m
        WHERE m.archived_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM timeline_events t
              WHERE t.event_type = 'memory.archived'
                AND t.source_table = 'memories'
                AND t.source_row_id = m.id
                AND t.reconstructed = 1
          )
        """
    )

    counting_execute(
        """
        INSERT INTO timeline_events (
            event_time, event_type, memory_id, related_memory_id, run_id,
            operation_id, timeline_scope, semantic_kind, title, project_key, valid_at,
            source_table, source_row_id, origin, reconstructed, payload_json, created_at
        )
        SELECT
            r.started_at,
            'sleep_run.started',
            NULL,
            NULL,
            r.id,
            'run:' || r.id,
            'run',
            'backfill_snapshot',
            NULL,
            NULL,
            r.started_at,
            'sleep_runs',
            r.id,
            'backfill',
            1,
            NULL,
            CURRENT_TIMESTAMP
        FROM sleep_runs r
        WHERE r.started_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM timeline_events t
              WHERE t.event_type = 'sleep_run.started'
                AND t.source_table = 'sleep_runs'
                AND t.source_row_id = r.id
                AND t.reconstructed = 1
          )
        """
    )

    counting_execute(
        """
        INSERT INTO timeline_events (
            event_time, event_type, memory_id, related_memory_id, run_id,
            operation_id, timeline_scope, semantic_kind, title, project_key, valid_at,
            source_table, source_row_id, origin, reconstructed, payload_json, created_at
        )
        SELECT
            r.finished_at,
            'sleep_run.finished',
            NULL,
            NULL,
            r.id,
            'run:' || r.id,
            'run',
            'backfill_snapshot',
            NULL,
            NULL,
            r.finished_at,
            'sleep_runs',
            r.id,
            'backfill',
            1,
            json_object(
                'status', r.status,
                'mode', r.mode,
                'freedom_level', r.freedom_level,
                'scanned_count', r.scanned_count,
                'changed_count', r.changed_count,
                'archived_count', r.archived_count,
                'downgraded_count', r.downgraded_count,
                'duplicate_count', r.duplicate_count,
                'conflict_count', r.conflict_count,
                'created_summary_count', r.created_summary_count,
                'rollback_of_run_id', r.rollback_of_run_id
            ),
            CURRENT_TIMESTAMP
        FROM sleep_runs r
        WHERE r.finished_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM timeline_events t
              WHERE t.event_type = 'sleep_run.finished'
                AND t.source_table = 'sleep_runs'
                AND t.source_row_id = r.id
                AND t.reconstructed = 1
          )
        """
    )

    counting_execute(
        """
        INSERT INTO timeline_events (
            event_time, event_type, memory_id, related_memory_id, run_id,
            operation_id, timeline_scope, semantic_kind, title, project_key, valid_at,
            source_table, source_row_id, origin, reconstructed, payload_json, created_at
        )
        SELECT
            a.created_at,
            'sleep_action.' || a.action_type,
            CASE
                WHEN a.memory_id IS NOT NULL
                 AND EXISTS (SELECT 1 FROM memories m WHERE m.id = a.memory_id)
                THEN a.memory_id
                ELSE NULL
            END,
            NULL,
            a.run_id,
            'run:' || a.run_id,
            'run',
            'backfill_snapshot',
            NULL,
            NULL,
            a.created_at,
            'sleep_run_actions',
            a.id,
            'backfill',
            1,
            json_object('reason', a.reason, 'old_value', a.old_value, 'new_value', a.new_value),
            CURRENT_TIMESTAMP
        FROM sleep_run_actions a
        WHERE a.created_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM timeline_events t
              WHERE t.source_table = 'sleep_run_actions'
                AND t.source_row_id = a.id
                AND t.reconstructed = 1
          )
        """
    )

    return inserted



def initialize_timeline_connection(conn: sqlite3.Connection, *, auto_backfill: bool = True) -> int:
    ensure_timeline_schema(conn)
    if not auto_backfill:
        return 0
    return backfill_timeline(conn)



def timeline_rows_to_dicts(
    rows: list[sqlite3.Row],
    *,
    row_to_dict: Callable[[sqlite3.Row], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        item = row_to_dict(row) if row_to_dict is not None else dict(row)
        payload_raw = item.get("payload_json")
        if isinstance(payload_raw, str) and payload_raw.strip():
            try:
                item["payload"] = json.loads(payload_raw)
            except json.JSONDecodeError:
                item["payload"] = payload_raw
        else:
            item["payload"] = None
        items.append(item)
    return items



def timeline_query(
    conn: sqlite3.Connection,
    *,
    limit: int,
    offset: int = 0,
    memory_id: int | None = None,
    run_id: int | None = None,
    operation_id: str | None = None,
    event_type: str | None = None,
    timeline_scope: str | None = None,
    semantic_kind: str | None = None,
    project_key: str | None = None,
    from_time: str | None = None,
    to_time: str | None = None,
    row_to_dict: Callable[[sqlite3.Row], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM timeline_events WHERE 1 = 1"
    params: list[Any] = []
    if memory_id is not None:
        sql += " AND (memory_id = ? OR related_memory_id = ?)"
        params.extend([memory_id, memory_id])
    if run_id is not None:
        sql += " AND run_id = ?"
        params.append(run_id)
    if operation_id:
        sql += " AND operation_id = ?"
        params.append(operation_id)
    if event_type:
        sql += " AND event_type = ?"
        params.append(event_type)
    if timeline_scope:
        sql += " AND timeline_scope = ?"
        params.append(timeline_scope)
    if semantic_kind:
        sql += " AND semantic_kind = ?"
        params.append(semantic_kind)
    if project_key:
        sql += " AND project_key = ?"
        params.append(project_key)
    if from_time:
        sql += " AND COALESCE(valid_at, event_time) >= ?"
        params.append(from_time)
    if to_time:
        sql += " AND COALESCE(valid_at, event_time) <= ?"
        params.append(to_time)
    sql += " ORDER BY COALESCE(valid_at, event_time) DESC, event_time DESC, id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = conn.execute(sql, params).fetchall()
    return timeline_rows_to_dicts(rows, row_to_dict=row_to_dict)
