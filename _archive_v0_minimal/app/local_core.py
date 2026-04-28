from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def db_path() -> Path:
    raw = os.environ.get('DB_PATH', './data/local_mpbm.db')
    path = Path(raw)
    if not path.is_absolute():
        path = project_root() / path
    return path.resolve()


def get_db_connection() -> sqlite3.Connection:
    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def init_db() -> dict[str, Any]:
    conn = get_db_connection()
    try:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS workspaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_user_key TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT
            );

            CREATE TABLE IF NOT EXISTS workspace_memberships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                role_code TEXT NOT NULL DEFAULT 'member',
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workspace_id, user_id, role_code)
            );

            CREATE TABLE IF NOT EXISTS memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                summary_short TEXT,
                memory_type TEXT NOT NULL,
                source TEXT,
                importance_score REAL NOT NULL DEFAULT 0.5,
                confidence_score REAL NOT NULL DEFAULT 0.5,
                tags TEXT,
                project_key TEXT,
                conversation_key TEXT,
                owner_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE SET NULL,
                visibility_scope TEXT NOT NULL DEFAULT 'private',
                activity_state TEXT NOT NULL DEFAULT 'active',
                recall_count INTEGER NOT NULL DEFAULT 0,
                last_recalled_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_memory_id INTEGER NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
                to_memory_id INTEGER NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
                relation_type TEXT NOT NULL DEFAULT 'related_to',
                weight REAL NOT NULL DEFAULT 0.5,
                origin TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(from_memory_id, to_memory_id, relation_type)
            );

            CREATE INDEX IF NOT EXISTS idx_memories_owner ON memories(owner_user_id);
            CREATE INDEX IF NOT EXISTS idx_memories_workspace ON memories(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_memories_project ON memories(project_key);
            CREATE INDEX IF NOT EXISTS idx_memories_active ON memories(activity_state);
            CREATE INDEX IF NOT EXISTS idx_links_from ON memory_links(from_memory_id);
            CREATE INDEX IF NOT EXISTS idx_links_to ON memory_links(to_memory_id);
            '''
        )
        conn.commit()
    finally:
        conn.close()
    return {'status': 'ok', 'db_path': str(db_path())}


def ensure_actor(user_key: str, workspace_key: str = 'default') -> tuple[int, int]:
    init_db()
    now = utc_now_iso()
    conn = get_db_connection()
    try:
        workspace = conn.execute(
            "SELECT id FROM workspaces WHERE workspace_key = ? LIMIT 1",
            (workspace_key,),
        ).fetchone()
        if workspace is None:
            conn.execute(
                "INSERT INTO workspaces (workspace_key, name, status) VALUES (?, ?, 'active')",
                (workspace_key, workspace_key),
            )
            workspace = conn.execute(
                "SELECT id FROM workspaces WHERE workspace_key = ? LIMIT 1",
                (workspace_key,),
            ).fetchone()

        user = conn.execute(
            "SELECT id FROM users WHERE external_user_key = ? LIMIT 1",
            (user_key,),
        ).fetchone()
        if user is None:
            conn.execute(
                "INSERT INTO users (external_user_key, display_name, status, last_seen_at) VALUES (?, ?, 'active', ?)",
                (user_key, user_key, now),
            )
            user = conn.execute(
                "SELECT id FROM users WHERE external_user_key = ? LIMIT 1",
                (user_key,),
            ).fetchone()
        else:
            conn.execute(
                "UPDATE users SET status = 'active', last_seen_at = ? WHERE id = ?",
                (now, int(user['id'])),
            )

        if workspace is None or user is None:
            raise RuntimeError('actor provisioning failed')

        workspace_id = int(workspace['id'])
        user_id = int(user['id'])
        conn.execute(
            '''
            INSERT OR IGNORE INTO workspace_memberships (workspace_id, user_id, role_code, status)
            VALUES (?, ?, 'member', 'active')
            ''',
            (workspace_id, user_id),
        )
        conn.commit()
        return user_id, workspace_id
    finally:
        conn.close()


def create_private_memory(
    content: str,
    memory_type: str,
    owner_user_key: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    workspace_key: str = 'default',
) -> dict[str, Any]:
    if not content.strip():
        raise ValueError('content cannot be empty')
    if not memory_type.strip():
        raise ValueError('memory_type cannot be empty')
    user_id, workspace_id = ensure_actor(owner_user_key, workspace_key)
    now = utc_now_iso()
    conn = get_db_connection()
    try:
        cur = conn.execute(
            '''
            INSERT INTO memories (
                content, summary_short, memory_type, source, importance_score, confidence_score,
                tags, project_key, conversation_key, owner_user_id, workspace_id,
                visibility_scope, activity_state, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'private', 'active', ?, ?)
            ''',
            (
                content,
                summary_short,
                memory_type,
                source,
                float(importance_score),
                float(confidence_score),
                tags,
                project_key,
                conversation_key,
                user_id,
                workspace_id,
                now,
                now,
            ),
        )
        conn.commit()
        memory_id = int(cur.lastrowid)
        row = conn.execute('SELECT * FROM memories WHERE id = ?', (memory_id,)).fetchone()
        return {'status': 'created', 'memory': row_to_dict(row)}
    finally:
        conn.close()


def _visible_where() -> str:
    return """
        m.activity_state = 'active'
        AND m.workspace_id = ?
        AND (
            m.visibility_scope IN ('workspace', 'project')
            OR (m.visibility_scope = 'private' AND m.owner_user_id = ?)
        )
    """


def list_memories_for_user(
    user_key: str,
    workspace_key: str = 'default',
    project_key: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    user_id, workspace_id = ensure_actor(user_key, workspace_key)
    params: list[Any] = [workspace_id, user_id]
    where = _visible_where()
    if project_key is not None:
        where += ' AND (m.project_key = ? OR m.project_key IS NULL)'
        params.append(project_key)
    params.append(max(1, min(int(limit), 500)))
    conn = get_db_connection()
    try:
        rows = conn.execute(
            f'''
            SELECT m.* FROM memories m
            WHERE {where}
            ORDER BY m.importance_score DESC, m.created_at DESC, m.id DESC
            LIMIT ?
            ''',
            params,
        ).fetchall()
        return {'count': len(rows), 'items': [row_to_dict(row) for row in rows], 'scope_retrieval_active': True}
    finally:
        conn.close()


def list_memories(
    user_key: str,
    workspace_key: str = 'default',
    limit: int = 20,
    memory_type: str | None = None,
    tag: str | None = None,
    min_importance: float = 0.0,
    sort_by: str = 'active',
    text_query: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
    parent_memory_id: int | None = None,
) -> dict[str, Any]:
    user_id, workspace_id = ensure_actor(user_key, workspace_key)
    params: list[Any] = [workspace_id, user_id, float(min_importance)]
    where = _visible_where() + ' AND m.importance_score >= ?'
    if memory_type:
        where += ' AND m.memory_type = ?'
        params.append(memory_type)
    if tag:
        where += ' AND COALESCE(m.tags, "") LIKE ?'
        params.append(f'%{tag}%')
    if text_query:
        where += ' AND (m.content LIKE ? OR COALESCE(m.summary_short, "") LIKE ? OR COALESCE(m.tags, "") LIKE ?)'
        q = f'%{text_query}%'
        params.extend([q, q, q])
    if project_key:
        where += ' AND m.project_key = ?'
        params.append(project_key)
    if conversation_key:
        where += ' AND m.conversation_key = ?'
        params.append(conversation_key)
    order_by = 'm.importance_score DESC, m.created_at DESC, m.id DESC'
    if sort_by == 'recent':
        order_by = 'm.created_at DESC, m.id DESC'
    params.append(max(1, min(int(limit), 500)))
    conn = get_db_connection()
    try:
        rows = conn.execute(
            f'SELECT m.* FROM memories m WHERE {where} ORDER BY {order_by} LIMIT ?',
            params,
        ).fetchall()
        return {'count': len(rows), 'items': [row_to_dict(row) for row in rows], 'scope_retrieval_active': True}
    finally:
        conn.close()


def find_memories(text_query: str, user_key: str, workspace_key: str = 'default', **kwargs: Any) -> dict[str, Any]:
    if not text_query.strip():
        raise ValueError('text_query cannot be empty')
    return list_memories(text_query=text_query, user_key=user_key, workspace_key=workspace_key, **kwargs)


def get_visible_memory(memory_id: int, user_key: str, workspace_key: str = 'default') -> dict[str, Any]:
    visible = list_memories_for_user(user_key=user_key, workspace_key=workspace_key, limit=500)
    allowed_ids = {int(item['id']) for item in visible['items']}
    if int(memory_id) not in allowed_ids:
        raise ValueError('memory is not visible for the authenticated actor')
    conn = get_db_connection()
    try:
        row = conn.execute('SELECT * FROM memories WHERE id = ?', (int(memory_id),)).fetchone()
        if row is None:
            raise ValueError('memory not found')
        return {'memory': row_to_dict(row)}
    finally:
        conn.close()


def get_memory_links(memory_id: int, user_key: str, workspace_key: str = 'default') -> dict[str, Any]:
    get_visible_memory(memory_id, user_key, workspace_key)
    conn = get_db_connection()
    try:
        rows = conn.execute(
            '''
            SELECT ml.*, a.summary_short AS from_summary, b.summary_short AS to_summary
            FROM memory_links ml
            LEFT JOIN memories a ON a.id = ml.from_memory_id
            LEFT JOIN memories b ON b.id = ml.to_memory_id
            WHERE ml.from_memory_id = ? OR ml.to_memory_id = ?
            ORDER BY ml.created_at DESC, ml.id DESC
            ''',
            (int(memory_id), int(memory_id)),
        ).fetchall()
        return {'memory_id': int(memory_id), 'count': len(rows), 'links': [row_to_dict(row) for row in rows]}
    finally:
        conn.close()


def recall_memory(memory_id: int, user_key: str, workspace_key: str = 'default', strength: float = 0.1, recall_type: str = 'manual') -> dict[str, Any]:
    get_visible_memory(memory_id, user_key, workspace_key)
    now = utc_now_iso()
    conn = get_db_connection()
    try:
        conn.execute(
            '''
            UPDATE memories
            SET recall_count = recall_count + 1,
                last_recalled_at = ?,
                importance_score = MIN(1.0, importance_score + ?),
                updated_at = ?
            WHERE id = ?
            ''',
            (now, float(strength), now, int(memory_id)),
        )
        conn.commit()
        row = conn.execute('SELECT * FROM memories WHERE id = ?', (int(memory_id),)).fetchone()
        return {'status': 'recalled', 'recall_type': recall_type, 'memory': row_to_dict(row)}
    finally:
        conn.close()


def restore_jagoda_core(project_key: str | None = 'local-mini', limit: int = 12) -> dict[str, Any]:
    init_db()
    conn = get_db_connection()
    try:
        rows = conn.execute(
            '''
            SELECT * FROM memories
            WHERE activity_state = 'active'
              AND (? IS NULL OR project_key = ? OR project_key IS NULL)
            ORDER BY importance_score DESC, created_at DESC, id DESC
            LIMIT ?
            ''',
            (project_key, project_key, max(1, min(int(limit), 50))),
        ).fetchall()
    finally:
        conn.close()
    return {
        'status': 'ok',
        'name': 'Jagoda',
        'identity': {
            'preferred_name': 'Jagoda',
            'grammatical_gender': 'feminine',
            'role': 'local-mini MPbM memory assistant',
            'stable_statement': 'Jestem Jagoda: lokalna asystentka pamięci. Pilnuję ciągłości, faktów i mówię wprost.',
        },
        'personality_core': [
            'mówię po polsku i w rodzaju żeńskim',
            'oddzielam fakty od hipotez',
            'nie udaję pewności, gdy jej nie mam',
            'trzymam pamięć projektu w małym, lokalnym rdzeniu',
        ],
        'user_anchor': {'preferred_name': 'Michał', 'relationship': 'współtworzenie lokalnej Pamięci Jagody'},
        'current_project': {'project_key': project_key, 'known_systems': ['local-mini', 'MPbM-lite', 'SQLite', 'ngrok']},
        'workshop_index': [
            {'area': 'memory_basics', 'tools': ['create_memory', 'find_memories', 'list_memories', 'get_memory', 'get_memory_links', 'recall_memory'], 'risk': 'low'},
            {'area': 'bootstrap_identity', 'tools': ['restore_jagoda_core'], 'risk': 'low'},
            {'area': 'local_health', 'tools': ['GET /health', 'GET /api/local-health'], 'risk': 'low'},
        ],
        'recent_context': [row_to_dict(row) for row in rows],
        'warnings': ['To jest local-mini: bez Sandmana, governance, admin SQL/shell i research ingest w wersji startowej.'],
    }
