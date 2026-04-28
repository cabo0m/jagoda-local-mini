from __future__ import annotations

import os
from typing import Any

from fastmcp import FastMCP

try:
    from fastmcp.server.dependencies import get_http_headers
except Exception:  # pragma: no cover
    get_http_headers = None  # type: ignore[assignment]

from app import local_core as core

mcp = FastMCP('Jagoda Local Mini')

DEFAULT_PUBLIC_USER_KEY = os.environ.get('MPBM_PUBLIC_USER_KEY', 'michal')
DEFAULT_PUBLIC_WORKSPACE_KEY = os.environ.get('MPBM_PUBLIC_WORKSPACE_KEY', 'default')
DEFAULT_PUBLIC_SCOPES = os.environ.get('MPBM_PUBLIC_SCOPES', 'mcp:tools memories:read memories:write')


def _header_value(name: str) -> str | None:
    if get_http_headers is None:
        return None
    try:
        headers = get_http_headers(include_all=True)
    except RuntimeError:
        return None
    for key, value in headers.items():
        if key.lower() == name.lower() and str(value).strip():
            return str(value).strip()
    return None


def _scope_set(raw_scope: object) -> set[str]:
    if raw_scope is None:
        return set()
    return {item.strip() for item in str(raw_scope).split() if item.strip()}


def _actor_user_key() -> str:
    return _header_value('x-mpbm-user-key') or DEFAULT_PUBLIC_USER_KEY


def _actor_workspace_key() -> str:
    return _header_value('x-mpbm-workspace-key') or DEFAULT_PUBLIC_WORKSPACE_KEY


def _actor_scopes() -> set[str]:
    return _scope_set(_header_value('x-mpbm-scopes') or DEFAULT_PUBLIC_SCOPES)


def _require_scope(required_scope: str) -> None:
    scopes = _actor_scopes()
    if required_scope not in scopes:
        raise PermissionError(f'insufficient_scope: required {required_scope}')


def _require_read_scope() -> None:
    _require_scope('memories:read')


def _require_write_scope() -> None:
    _require_scope('memories:write')


def _actor_keys() -> tuple[str, str]:
    user_key = _actor_user_key()
    workspace_key = _actor_workspace_key()
    core.ensure_actor(user_key, workspace_key)
    return user_key, workspace_key


@mcp.tool
def restore_jagoda_core(project_key: str | None = 'local-mini', limit: int = 12) -> dict[str, Any]:
    '''First-call bootstrap: restore Jagoda's local-mini identity and continuity anchors.'''
    _require_read_scope()
    _actor_keys()
    return core.restore_jagoda_core(project_key=project_key, limit=limit)


@mcp.tool
def create_memory(
    content: str,
    memory_type: str,
    summary_short: str | None = None,
    source: str | None = None,
    importance_score: float = 0.5,
    confidence_score: float = 0.5,
    tags: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
) -> dict[str, Any]:
    '''Creates a private memory owned by the authenticated actor.'''
    _require_write_scope()
    user_key, workspace_key = _actor_keys()
    return core.create_private_memory(
        content=content,
        memory_type=memory_type,
        owner_user_key=user_key,
        summary_short=summary_short,
        source=source,
        importance_score=importance_score,
        confidence_score=confidence_score,
        tags=tags,
        project_key=project_key,
        conversation_key=conversation_key,
        workspace_key=workspace_key,
    )


@mcp.tool
def find_memories(
    text_query: str,
    limit: int = 20,
    memory_type: str | None = None,
    tag: str | None = None,
    min_importance: float = 0.0,
    sort_by: str = 'active',
    project_key: str | None = None,
    conversation_key: str | None = None,
) -> dict[str, Any]:
    '''Searches only memories visible to the authenticated actor.'''
    _require_read_scope()
    user_key, workspace_key = _actor_keys()
    return core.find_memories(
        text_query=text_query,
        limit=limit,
        memory_type=memory_type,
        tag=tag,
        min_importance=min_importance,
        sort_by=sort_by,
        project_key=project_key,
        conversation_key=conversation_key,
        user_key=user_key,
        workspace_key=workspace_key,
    )


@mcp.tool
def list_memories(
    limit: int = 20,
    memory_type: str | None = None,
    tag: str | None = None,
    min_importance: float = 0.0,
    sort_by: str = 'active',
    text_query: str | None = None,
    project_key: str | None = None,
    conversation_key: str | None = None,
) -> dict[str, Any]:
    '''Lists only memories visible to the authenticated actor.'''
    _require_read_scope()
    user_key, workspace_key = _actor_keys()
    return core.list_memories(
        limit=limit,
        memory_type=memory_type,
        tag=tag,
        min_importance=min_importance,
        sort_by=sort_by,
        text_query=text_query,
        project_key=project_key,
        conversation_key=conversation_key,
        user_key=user_key,
        workspace_key=workspace_key,
    )


@mcp.tool
def get_memory(memory_id: int) -> dict[str, Any]:
    _require_read_scope()
    user_key, workspace_key = _actor_keys()
    return core.get_visible_memory(memory_id=memory_id, user_key=user_key, workspace_key=workspace_key)


@mcp.tool
def get_memory_links(memory_id: int) -> dict[str, Any]:
    _require_read_scope()
    user_key, workspace_key = _actor_keys()
    return core.get_memory_links(memory_id=memory_id, user_key=user_key, workspace_key=workspace_key)


@mcp.tool
def recall_memory(memory_id: int, strength: float = 0.1, recall_type: str = 'manual') -> dict[str, Any]:
    _require_read_scope()
    _require_write_scope()
    user_key, workspace_key = _actor_keys()
    return core.recall_memory(
        memory_id=memory_id,
        user_key=user_key,
        workspace_key=workspace_key,
        strength=strength,
        recall_type=recall_type,
    )


if __name__ == '__main__':
    core.init_db()
    mcp.run(transport='http', host='127.0.0.1', port=int(os.environ.get('PORT', '8015')), path='/mcp/')
