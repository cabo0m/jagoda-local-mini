from __future__ import annotations

import sqlite3
from typing import Callable

from app import timeline

MigrationFn = Callable[[sqlite3.Connection], None]

MIGRATION_SEQUENCE: list[tuple[str, MigrationFn]] = []


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in existing:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def ensure_schema_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def applied_migration_versions(conn: sqlite3.Connection) -> set[str]:
    ensure_schema_migrations_table(conn)
    rows = conn.execute("SELECT version FROM schema_migrations ORDER BY version ASC").fetchall()
    return {str(row["version"]) for row in rows}


def _migration_0001_memory_core(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            summary_short TEXT,
            memory_type TEXT NOT NULL,
            source TEXT,
            importance_score REAL DEFAULT 0.5,
            confidence_score REAL DEFAULT 0.5,
            tags TEXT,
            recall_count INTEGER DEFAULT 0,
            last_recalled_at TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_memory_id INTEGER NOT NULL,
            to_memory_id INTEGER NOT NULL,
            relation_type TEXT NOT NULL,
            weight REAL NOT NULL,
            origin TEXT,
            FOREIGN KEY (from_memory_id) REFERENCES memories(id),
            FOREIGN KEY (to_memory_id) REFERENCES memories(id)
        )
        """
    )
    _ensure_column(conn, "memories", "created_at", "created_at TEXT")
    _ensure_column(conn, "memories", "last_accessed_at", "last_accessed_at TEXT")
    _ensure_column(conn, "memories", "activity_state", "activity_state TEXT NOT NULL DEFAULT 'active'")
    _ensure_column(conn, "memories", "evidence_count", "evidence_count INTEGER NOT NULL DEFAULT 1")
    _ensure_column(conn, "memories", "contradiction_flag", "contradiction_flag INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "memories", "archived_at", "archived_at TEXT")
    _ensure_column(conn, "memories", "sandman_note", "sandman_note TEXT")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sleep_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'started',
            mode TEXT NOT NULL DEFAULT 'preview',
            freedom_level INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            scanned_count INTEGER NOT NULL DEFAULT 0,
            changed_count INTEGER NOT NULL DEFAULT 0,
            archived_count INTEGER NOT NULL DEFAULT 0,
            downgraded_count INTEGER NOT NULL DEFAULT 0,
            duplicate_count INTEGER NOT NULL DEFAULT 0,
            conflict_count INTEGER NOT NULL DEFAULT 0,
            created_summary_count INTEGER NOT NULL DEFAULT 0,
            rollback_of_run_id INTEGER,
            FOREIGN KEY (rollback_of_run_id) REFERENCES sleep_runs(id)
        )
        """
    )
    _ensure_column(conn, "sleep_runs", "rollback_of_run_id", "rollback_of_run_id INTEGER")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sleep_run_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            memory_id INTEGER,
            action_type TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            reason TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES sleep_runs(id)
        )
        """
    )

    cursor.execute("UPDATE memories SET created_at = COALESCE(created_at, last_recalled_at, CURRENT_TIMESTAMP) WHERE created_at IS NULL")
    cursor.execute("UPDATE memories SET last_accessed_at = COALESCE(last_accessed_at, last_recalled_at, created_at, CURRENT_TIMESTAMP) WHERE last_accessed_at IS NULL")
    cursor.execute("UPDATE memories SET activity_state = 'active' WHERE activity_state IS NULL OR trim(activity_state) = ''")
    cursor.execute("UPDATE memories SET evidence_count = 1 WHERE evidence_count IS NULL OR evidence_count < 1")
    cursor.execute("UPDATE memories SET contradiction_flag = 0 WHERE contradiction_flag IS NULL")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_activity_state ON memories(activity_state)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_memory_type ON memories(memory_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_importance_score ON memories(importance_score)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_created_at ON memories(created_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_last_accessed_at ON memories(last_accessed_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memory_links_from_memory_id ON memory_links(from_memory_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memory_links_to_memory_id ON memory_links(to_memory_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memory_links_relation_type ON memory_links(relation_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sleep_runs_started_at ON sleep_runs(started_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sleep_runs_status ON sleep_runs(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sleep_runs_rollback_of_run_id ON sleep_runs(rollback_of_run_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sleep_run_actions_run_id ON sleep_run_actions(run_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sleep_run_actions_memory_id ON sleep_run_actions(memory_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sleep_run_actions_action_type ON sleep_run_actions(action_type)")


def _migration_0002_timeline_schema(conn: sqlite3.Connection) -> None:
    timeline.ensure_timeline_schema(conn)


def _migration_0003_timeline_schema_hardening(conn: sqlite3.Connection) -> None:
    timeline.ensure_timeline_schema(conn)


def _migration_0004_project_timeline_semantics(conn: sqlite3.Connection) -> None:
    timeline.ensure_timeline_schema(conn)


def _migration_0005_memory_layer_area_metadata(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for column_name, column_sql in (
        ("layer_code", "layer_code TEXT"),
        ("area_code", "area_code TEXT"),
        ("state_code", "state_code TEXT"),
        ("scope_code", "scope_code TEXT"),
        ("parent_memory_id", "parent_memory_id INTEGER"),
        ("version", "version INTEGER NOT NULL DEFAULT 1"),
        ("promoted_from_id", "promoted_from_id INTEGER"),
        ("demoted_from_id", "demoted_from_id INTEGER"),
        ("supersedes_memory_id", "supersedes_memory_id INTEGER"),
        ("valid_from", "valid_from TEXT"),
        ("valid_to", "valid_to TEXT"),
        ("decay_score", "decay_score REAL NOT NULL DEFAULT 0.0"),
        ("emotional_weight", "emotional_weight REAL NOT NULL DEFAULT 0.0"),
        ("identity_weight", "identity_weight REAL NOT NULL DEFAULT 0.0"),
        ("project_key", "project_key TEXT"),
        ("conversation_key", "conversation_key TEXT"),
        ("last_validated_at", "last_validated_at TEXT"),
        ("validation_source", "validation_source TEXT"),
    ):
        _ensure_column(conn, "memories", column_name, column_sql)


    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_layer_rules (
            layer_code TEXT PRIMARY KEY,
            description TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_area_rules (
            area_code TEXT PRIMARY KEY,
            description TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id INTEGER,
            event_type TEXT NOT NULL,
            payload_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (memory_id) REFERENCES memories(id)
        )
        """
    )

    cursor.executemany(
        "INSERT OR IGNORE INTO memory_layer_rules (layer_code, description) VALUES (?, ?)",
        [
            ("core", "Most protected memory layer."),
            ("identity", "Identity and stable traits."),
            ("autobio", "Autobiographic knowledge and durable facts."),
            ("projects", "Active projects and project decisions."),
            ("working", "Current working context."),
            ("buffer", "Temporary buffer and draft memories."),
        ],
    )
    cursor.executemany(
        "INSERT OR IGNORE INTO memory_area_rules (area_code, description) VALUES (?, ?)",
        [
            ("identity", "Who the system is."),
            ("relation", "User relationship."),
            ("projects", "Project context."),
            ("knowledge", "Facts and knowledge."),
            ("preferences", "Preferences and work style."),
            ("history", "History and milestones."),
            ("rumination", "Hypotheses and drafts."),
            ("meta", "Memory system rules."),
        ],
    )
    cursor.execute("UPDATE memories SET version = 1 WHERE version IS NULL OR version < 1")
    cursor.execute("UPDATE memories SET decay_score = 0.0 WHERE decay_score IS NULL")
    cursor.execute("UPDATE memories SET emotional_weight = 0.0 WHERE emotional_weight IS NULL")
    cursor.execute("UPDATE memories SET identity_weight = 0.0 WHERE identity_weight IS NULL")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_layer_code ON memories(layer_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_area_code ON memories(area_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_state_code ON memories(state_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_scope_code ON memories(scope_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_project_key ON memories(project_key)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_conversation_key ON memories(conversation_key)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memory_events_memory_id ON memory_events(memory_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memory_events_event_type ON memory_events(event_type)")


def _migration_0006_feature_flags(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_flags (
            flag_key TEXT PRIMARY KEY,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            rollout_mode TEXT NOT NULL DEFAULT 'all',
            allowed_project_keys TEXT,
            allowed_scope_codes TEXT,
            read_only_mode INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO feature_flags (
            flag_key,
            is_enabled,
            rollout_mode,
            allowed_project_keys,
            allowed_scope_codes,
            read_only_mode,
            notes
        )
        VALUES (?, 1, 'all', NULL, NULL, 0, ?)
        """,
        (
            "cross_project_knowledge_layer",
            "Default rollout for Cross-Project Knowledge Layer",
        ),
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_flags_rollout_mode ON feature_flags(rollout_mode)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_flags_is_enabled ON feature_flags(is_enabled)")



def _migration_0007_ownership_sla(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    for column_name, column_sql in (
        ("owner_role", "owner_role TEXT"),
        ("owner_id", "owner_id TEXT"),
        ("review_due_at", "review_due_at TEXT"),
        ("revalidation_due_at", "revalidation_due_at TEXT"),
    ):
        _ensure_column(conn, "memories", column_name, column_sql)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_owner_role ON memories(owner_role)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_owner_id ON memories(owner_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_review_due_at ON memories(review_due_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_revalidation_due_at ON memories(revalidation_due_at)")


def _migration_0008_expired_duplicate_sla(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    _ensure_column(conn, "memories", "expired_due_at", "expired_due_at TEXT")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS duplicate_review_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_memory_id INTEGER NOT NULL,
            duplicate_memory_id INTEGER NOT NULL,
            owner_role TEXT,
            owner_id TEXT,
            duplicate_due_at TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(canonical_memory_id, duplicate_memory_id),
            FOREIGN KEY (canonical_memory_id) REFERENCES memories(id),
            FOREIGN KEY (duplicate_memory_id) REFERENCES memories(id)
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_duplicate_review_items_due_at ON duplicate_review_items(duplicate_due_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_duplicate_review_items_owner_role ON duplicate_review_items(owner_role)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_duplicate_review_items_status ON duplicate_review_items(status)")


def _migration_0009_owner_resolution_layer(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS owner_directory_items (
            owner_key TEXT PRIMARY KEY,
            owner_type TEXT NOT NULL,
            display_name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            routing_metadata_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS owner_role_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_role TEXT NOT NULL,
            owner_key TEXT NOT NULL,
            project_key TEXT,
            scope_code TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(owner_role, project_key, scope_code),
            FOREIGN KEY (owner_key) REFERENCES owner_directory_items(owner_key)
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_owner_directory_items_owner_type ON owner_directory_items(owner_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_owner_directory_items_is_active ON owner_directory_items(is_active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_owner_role_mappings_owner_role ON owner_role_mappings(owner_role)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_owner_role_mappings_project_key ON owner_role_mappings(project_key)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_owner_role_mappings_scope_code ON owner_role_mappings(scope_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_owner_role_mappings_is_active ON owner_role_mappings(is_active)")

    directory_seed = [
        ("memory_maintainer", "team", "Memory Maintainer"),
        ("knowledge_curator", "team", "Knowledge Curator"),
        ("review_team", "team", "Review Team"),
        ("project_maintainer", "team", "Project Maintainer"),
    ]
    cursor.executemany(
        "INSERT OR IGNORE INTO owner_directory_items (owner_key, owner_type, display_name, is_active) VALUES (?, ?, ?, 1)",
        directory_seed,
    )
    mapping_seed = [
        ("memory_maintainer", "memory_maintainer", None, None, "Bootstrap global mapping"),
        ("knowledge_curator", "knowledge_curator", None, None, "Bootstrap global mapping"),
        ("review_team", "review_team", None, None, "Bootstrap global mapping"),
        ("project_maintainer", "project_maintainer", None, None, "Bootstrap global mapping"),
    ]
    cursor.executemany(
        "INSERT OR IGNORE INTO owner_role_mappings (owner_role, owner_key, project_key, scope_code, is_active, notes) VALUES (?, ?, ?, ?, 1, ?)",
        mapping_seed,
    )


def _migration_0010_multiuser_identity_foundation(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    # 1. Tabela users
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_user_key TEXT NOT NULL UNIQUE,
            display_name TEXT,
            email TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TEXT
        )
        """
    )

    # 2. Tabela workspaces
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS workspaces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_key TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # 3. Tabela workspace_memberships
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS workspace_memberships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role_code TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            invited_by_user_id INTEGER,
            FOREIGN KEY (workspace_id) REFERENCES workspaces(id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (invited_by_user_id) REFERENCES users(id),
            UNIQUE (workspace_id, user_id)
        )
        """
    )

    # 4. Nowe kolumny w memories
    for col, col_sql in [
        ("owner_user_id", "owner_user_id INTEGER"),
        ("workspace_id", "workspace_id INTEGER"),
        ("visibility_scope", "visibility_scope TEXT NOT NULL DEFAULT 'private'"),
        ("access_role_min", "access_role_min TEXT"),
        ("created_by_user_id", "created_by_user_id INTEGER"),
        ("last_modified_by_user_id", "last_modified_by_user_id INTEGER"),
        ("subject_user_id", "subject_user_id INTEGER"),
        ("sharing_policy", "sharing_policy TEXT NOT NULL DEFAULT 'explicit'"),
    ]:
        _ensure_column(conn, "memories", col, col_sql)

    # 5. Nowe kolumny w memory_links
    for col, col_sql in [
        ("workspace_id", "workspace_id INTEGER"),
        ("visibility_scope", "visibility_scope TEXT NOT NULL DEFAULT 'inherited'"),
        ("created_by_user_id", "created_by_user_id INTEGER"),
    ]:
        _ensure_column(conn, "memory_links", col, col_sql)

    # 6. Nowe kolumny w timeline_events
    for col, col_sql in [
        ("actor_user_id", "actor_user_id INTEGER"),
        ("workspace_id", "workspace_id INTEGER"),
        ("actor_type", "actor_type TEXT NOT NULL DEFAULT 'system'"),
    ]:
        _ensure_column(conn, "timeline_events", col, col_sql)

    # 7. Seed: default workspace
    cursor.execute(
        """
        INSERT INTO workspaces (workspace_key, name)
        SELECT 'default', 'Default Workspace'
        WHERE NOT EXISTS (SELECT 1 FROM workspaces WHERE workspace_key = 'default')
        """
    )

    # 8. Seed: system:legacy user
    cursor.execute(
        """
        INSERT INTO users (external_user_key, display_name, status)
        SELECT 'system:legacy', 'Legacy System User', 'active'
        WHERE NOT EXISTS (SELECT 1 FROM users WHERE external_user_key = 'system:legacy')
        """
    )

    # 9. Seed: użytkownicy legacy z owner_id
    cursor.execute(
        """
        INSERT INTO users (external_user_key, display_name, status)
        SELECT DISTINCT
            'legacy:' || owner_id,
            owner_id,
            'active'
        FROM memories
        WHERE owner_id IS NOT NULL
          AND TRIM(owner_id) <> ''
          AND owner_role = 'user'
          AND NOT EXISTS (
              SELECT 1 FROM users u WHERE u.external_user_key = 'legacy:' || memories.owner_id
          )
        """
    )

    # 10. Membership: system:legacy jako owner default workspace
    cursor.execute(
        """
        INSERT INTO workspace_memberships (workspace_id, user_id, role_code, status)
        SELECT w.id, u.id, 'owner', 'active'
        FROM workspaces w
        JOIN users u ON u.external_user_key = 'system:legacy'
        WHERE w.workspace_key = 'default'
          AND NOT EXISTS (
              SELECT 1 FROM workspace_memberships wm
              WHERE wm.workspace_id = w.id AND wm.user_id = u.id
          )
        """
    )

    # 11. Membership: legacy userzy jako editor default workspace
    cursor.execute(
        """
        INSERT INTO workspace_memberships (workspace_id, user_id, role_code, status)
        SELECT w.id, u.id, 'editor', 'active'
        FROM workspaces w
        JOIN users u ON u.external_user_key LIKE 'legacy:%'
        WHERE w.workspace_key = 'default'
          AND NOT EXISTS (
              SELECT 1 FROM workspace_memberships wm
              WHERE wm.workspace_id = w.id AND wm.user_id = u.id
          )
        """
    )

    # 12. Backfill: workspace_id w memories
    cursor.execute(
        """
        UPDATE memories
        SET workspace_id = (SELECT id FROM workspaces WHERE workspace_key = 'default')
        WHERE workspace_id IS NULL
        """
    )

    # 13A. Backfill: owner_user_id z owner_id
    cursor.execute(
        """
        UPDATE memories
        SET owner_user_id = (
            SELECT u.id FROM users u
            WHERE u.external_user_key = 'legacy:' || memories.owner_id
        )
        WHERE owner_user_id IS NULL
          AND owner_role = 'user'
          AND owner_id IS NOT NULL
          AND TRIM(owner_id) <> ''
        """
    )

    # 13B. Fallback owner_user_id dla typów osobistych
    cursor.execute(
        """
        UPDATE memories
        SET owner_user_id = (SELECT id FROM users WHERE external_user_key = 'system:legacy')
        WHERE owner_user_id IS NULL
          AND memory_type IN (
              'preference', 'interaction_preference', 'workflow_preference',
              'profile', 'profile_note', 'personal_note', 'working'
          )
        """
    )

    # 14A. Backfill visibility_scope: project dla typów projektowych z project_key
    cursor.execute(
        """
        UPDATE memories
        SET visibility_scope = 'project'
        WHERE project_key IS NOT NULL
          AND TRIM(project_key) <> ''
          AND memory_type IN (
              'project', 'project_note', 'project_context', 'project_direction',
              'project_design', 'project_architecture', 'project_milestone'
          )
        """
    )

    # 14B. Backfill visibility_scope: workspace dla fact/summary bez project_key
    cursor.execute(
        """
        UPDATE memories
        SET visibility_scope = 'workspace'
        WHERE visibility_scope = 'private'
          AND memory_type IN ('fact', 'consolidated_summary')
          AND (project_key IS NULL OR TRIM(project_key) = '')
        """
    )

    # 14C. Backfill visibility_scope: private dla typów osobistych
    cursor.execute(
        """
        UPDATE memories
        SET visibility_scope = 'private'
        WHERE memory_type IN (
              'preference', 'interaction_preference', 'workflow_preference',
              'profile', 'profile_note', 'personal_note', 'interest', 'working'
          )
        """
    )

    # 14D. Fallback: project dla rekordów z project_key
    cursor.execute(
        """
        UPDATE memories
        SET visibility_scope = 'project'
        WHERE visibility_scope = 'private'
          AND project_key IS NOT NULL
          AND TRIM(project_key) <> ''
        """
    )

    # 15. Backfill created_by_user_id i last_modified_by_user_id
    cursor.execute(
        """
        UPDATE memories
        SET created_by_user_id = COALESCE(
            owner_user_id,
            (SELECT id FROM users WHERE external_user_key = 'system:legacy')
        )
        WHERE created_by_user_id IS NULL
        """
    )
    cursor.execute(
        """
        UPDATE memories
        SET last_modified_by_user_id = COALESCE(
            owner_user_id,
            created_by_user_id,
            (SELECT id FROM users WHERE external_user_key = 'system:legacy')
        )
        WHERE last_modified_by_user_id IS NULL
        """
    )

    # 16. Backfill memory_links: workspace_id i visibility_scope
    cursor.execute(
        """
        UPDATE memory_links
        SET workspace_id = (
            SELECT m.workspace_id FROM memories m WHERE m.id = memory_links.from_memory_id
        )
        WHERE workspace_id IS NULL
        """
    )
    cursor.execute(
        """
        UPDATE memory_links
        SET visibility_scope = (
            SELECT CASE
                WHEN m1.visibility_scope = m2.visibility_scope THEN m1.visibility_scope
                ELSE 'inherited'
            END
            FROM memories m1
            JOIN memories m2 ON m2.id = memory_links.to_memory_id
            WHERE m1.id = memory_links.from_memory_id
        )
        WHERE visibility_scope = 'inherited'
        """
    )

    # 17. Backfill timeline_events: workspace_id i actor_user_id
    cursor.execute(
        """
        UPDATE timeline_events
        SET workspace_id = (SELECT id FROM workspaces WHERE workspace_key = 'default')
        WHERE workspace_id IS NULL
        """
    )
    cursor.execute(
        """
        UPDATE timeline_events
        SET actor_user_id = (SELECT id FROM users WHERE external_user_key = 'system:legacy')
        WHERE actor_user_id IS NULL
        """
    )

    # 18. Indeksy
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_workspace_memberships_workspace ON workspace_memberships(workspace_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_workspace_memberships_user ON workspace_memberships(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_workspace_memberships_role ON workspace_memberships(role_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_workspace_id ON memories(workspace_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_owner_user_id ON memories(owner_user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_visibility_scope ON memories(visibility_scope)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_workspace_scope ON memories(workspace_id, visibility_scope)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_workspace_project_scope ON memories(workspace_id, project_key, visibility_scope)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_subject_user_id ON memories(subject_user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memories_created_by_user_id ON memories(created_by_user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memory_links_workspace_id ON memory_links(workspace_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_memory_links_visibility_scope ON memory_links(visibility_scope)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_workspace_id ON timeline_events(workspace_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timeline_events_actor_user_id ON timeline_events(actor_user_id)")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_timeline_events_workspace_event_time ON timeline_events(workspace_id, event_time)"
    )

    # 19. Feature flags dla multiuser
    for flag_key, notes in [
        ("multiuser_identity_enabled", "Controls multi-user identity foundation (workspace, visibility_scope)"),
        ("multiuser_scope_retrieval_enabled", "Controls scope-aware memory retrieval filtering"),
        ("multiuser_timeline_actor_enabled", "Controls actor/workspace logging in timeline events"),
    ]:
        cursor.execute(
            "INSERT OR IGNORE INTO feature_flags (flag_key, is_enabled, rollout_mode, notes) VALUES (?, 1, 'all', ?)",
            (flag_key, notes),
        )


def _migration_0011_scope_aware_maintenance(conn: sqlite3.Connection) -> None:
    """Faza 3 + Faza 4: scope-aware maintenance i scope promotion governance."""
    cursor = conn.cursor()

    # --- Faza 3: sleep_runs scope context ---
    _ensure_column(conn, "sleep_runs", "workspace_id", "workspace_id INTEGER")
    _ensure_column(conn, "sleep_runs", "project_key", "project_key TEXT")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sleep_runs_workspace_id ON sleep_runs(workspace_id)")

    # --- Faza 4: scope promotion governance ---
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scope_promotion_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id INTEGER NOT NULL,
            proposed_by_user_id INTEGER,
            current_scope TEXT NOT NULL,
            target_scope TEXT NOT NULL,
            reason TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            workspace_id INTEGER,
            project_key TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TEXT,
            reviewed_by_user_id INTEGER,
            review_note TEXT,
            FOREIGN KEY (memory_id) REFERENCES memories(id),
            FOREIGN KEY (proposed_by_user_id) REFERENCES users(id),
            FOREIGN KEY (reviewed_by_user_id) REFERENCES users(id)
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_scope_proposals_memory_id ON scope_promotion_proposals(memory_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_scope_proposals_status ON scope_promotion_proposals(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_scope_proposals_workspace_id ON scope_promotion_proposals(workspace_id)")

    # Feature flags
    for flag_key, notes in [
        ("multiuser_scope_maintenance_enabled", "Controls workspace-scoped Sandman runs (Faza 3)"),
        ("multiuser_scope_promotion_enabled", "Controls scope promotion governance workflow (Faza 4)"),
    ]:
        cursor.execute(
            "INSERT OR IGNORE INTO feature_flags (flag_key, is_enabled, rollout_mode, notes) VALUES (?, 1, 'all', ?)",
            (flag_key, notes),
        )


def _migration_0012_priority_and_sla_policies(conn: sqlite3.Connection) -> None:
    """Epic 4: priority model na memories/duplicate_review_items + tabela polityk SLA."""
    cursor = conn.cursor()
    _ensure_column(conn, "memories", "priority", "priority TEXT NOT NULL DEFAULT 'normal'")
    _ensure_column(conn, "duplicate_review_items", "priority", "priority TEXT NOT NULL DEFAULT 'normal'")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sla_policies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_type TEXT NOT NULL,
            sla_days INTEGER NOT NULL,
            priority TEXT,
            memory_type TEXT,
            scope_code TEXT,
            project_key TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sla_policies_queue_type ON sla_policies(queue_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sla_policies_is_active ON sla_policies(is_active)")


def _migration_0013_escalation_history(conn: sqlite3.Connection) -> None:
    """Epic 3: tabela historii eskalacji dla overdue items."""
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS escalation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            escalation_level INTEGER NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            owner_role TEXT,
            project_key TEXT,
            scope_code TEXT,
            reason TEXT NOT NULL,
            days_overdue INTEGER,
            priority TEXT,
            escalated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at TEXT,
            resolved_by TEXT,
            UNIQUE(entity_type, entity_id, escalation_level, reason)
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_escalation_history_entity ON escalation_history(entity_type, entity_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_escalation_history_level ON escalation_history(escalation_level)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_escalation_history_resolved ON escalation_history(resolved_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_escalation_history_escalated_at ON escalation_history(escalated_at)")


MIGRATION_SEQUENCE = [
    ("0001_memory_core", _migration_0001_memory_core),
    ("0002_timeline_schema", _migration_0002_timeline_schema),
    ("0003_timeline_schema_hardening", _migration_0003_timeline_schema_hardening),
    ("0004_project_timeline_semantics", _migration_0004_project_timeline_semantics),
    ("0005_memory_layer_area_metadata", _migration_0005_memory_layer_area_metadata),
    ("0006_feature_flags", _migration_0006_feature_flags),
    ("0007_ownership_sla", _migration_0007_ownership_sla),
    ("0008_expired_duplicate_sla", _migration_0008_expired_duplicate_sla),
    ("0009_owner_resolution_layer", _migration_0009_owner_resolution_layer),
    ("0010_multiuser_identity_foundation", _migration_0010_multiuser_identity_foundation),
    ("0011_scope_aware_maintenance", _migration_0011_scope_aware_maintenance),
    ("0012_priority_and_sla_policies", _migration_0012_priority_and_sla_policies),
    ("0013_escalation_history", _migration_0013_escalation_history),
]


def apply_all_migrations(conn: sqlite3.Connection) -> list[str]:
    ensure_schema_migrations_table(conn)
    applied = applied_migration_versions(conn)
    ran: list[str] = []
    for version, migration_fn in MIGRATION_SEQUENCE:
        if version in applied:
            continue
        migration_fn(conn)
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (version,))
        ran.append(version)
    return ran
