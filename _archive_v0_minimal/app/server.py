from __future__ import annotations

import hmac
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from starlette.applications import Starlette
from starlette.datastructures import Headers
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from app import local_core
from app.mcp_server import mcp

load_dotenv()

STARTED_AT = datetime.now(timezone.utc)
PUBLIC_BASE_URL = os.environ.get('PUBLIC_BASE_URL', 'http://127.0.0.1:8015').rstrip('/')
MCP_BEARER_TOKEN = os.environ.get('MCP_BEARER_TOKEN', '')
MCP_STATIC_SUB = os.environ.get('MPBM_PUBLIC_USER_KEY', 'michal')
MPBM_DEFAULT_WORKSPACE_KEY = os.environ.get('MPBM_PUBLIC_WORKSPACE_KEY', 'default')
SUPPORTED_SCOPES = ('mcp:tools', 'memories:read', 'memories:write')
DEFAULT_SCOPE = ' '.join(SUPPORTED_SCOPES)
AUDIT_LOG_PATH = local_core.project_root() / 'data' / 'local_security_audit.jsonl'


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def _scope_set(raw_scope: object) -> set[str]:
    if raw_scope is None:
        return set()
    return {item.strip() for item in str(raw_scope).split() if item.strip()}


def _safe_header_value(value: object, fallback: str) -> str:
    raw = str(value or '').strip() or fallback
    safe = ''.join(ch if ch.isalnum() or ch in {':', '-', '_', '.', '@'} else '_' for ch in raw)
    return safe[:160] or fallback


def _append_header(scope: Scope, name: str, value: str) -> None:
    trusted_name = name.lower().encode('latin-1')
    headers = [item for item in list(scope.get('headers') or []) if item[0].lower() != trusted_name]
    headers.append((trusted_name, value.encode('latin-1')))
    scope['headers'] = headers


def _oauth_metadata() -> dict[str, object]:
    return {
        'issuer': PUBLIC_BASE_URL,
        'authorization_endpoint': f'{PUBLIC_BASE_URL}/oauth/authorize',
        'token_endpoint': f'{PUBLIC_BASE_URL}/oauth/token',
        'response_types_supported': ['code'],
        'grant_types_supported': ['authorization_code'],
        'token_endpoint_auth_methods_supported': ['none'],
        'scopes_supported': list(SUPPORTED_SCOPES),
    }


def _protected_resource_metadata() -> dict[str, object]:
    return {
        'resource': f'{PUBLIC_BASE_URL}/mcp/',
        'authorization_servers': [PUBLIC_BASE_URL],
        'bearer_methods_supported': ['header'],
        'scopes_supported': list(SUPPORTED_SCOPES),
    }


def _www_authenticate_header(error: str | None = None) -> str:
    metadata_url = f'{PUBLIC_BASE_URL}/.well-known/oauth-protected-resource'
    parts = ['Bearer', 'realm="mcp"', f'resource_metadata="{metadata_url}"']
    if error:
        parts.append(f'error="{error}"')
    return ', '.join(parts)


def _audit_security_event(event_type: str, scope: Scope, details: dict[str, object] | None = None) -> None:
    headers = Headers(scope=scope)
    auth_context = scope.get('auth_context') if isinstance(scope.get('auth_context'), dict) else {}
    event = {
        'ts': _utc_now(),
        'event_type': event_type,
        'path': str(scope.get('path') or ''),
        'method': str(scope.get('method') or ''),
        'client': str((scope.get('client') or ['unknown'])[0]),
        'user_key': auth_context.get('user_key'),
        'workspace_key': auth_context.get('workspace_key'),
        'client_id': auth_context.get('client_id'),
        'scopes': auth_context.get('scope'),
        'user_agent': headers.get('user-agent'),
        'details': details or {},
    }
    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with AUDIT_LOG_PATH.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + '\n')


async def _json_response(scope: Scope, receive: Receive, send: Send, payload: dict[str, object], status_code: int, headers: dict[str, str] | None = None) -> None:
    response = JSONResponse(payload, status_code=status_code, headers=headers)
    await response(scope, receive, send)


class BearerAuthMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get('type') != 'http':
            await self.app(scope, receive, send)
            return

        path = str(scope.get('path') or '')
        public_paths = {
            '/health',
            '/api/local-health',
            '/.well-known/oauth-authorization-server',
            '/.well-known/openid-configuration',
            '/.well-known/oauth-protected-resource',
            '/mcp/.well-known/oauth-authorization-server',
            '/mcp/.well-known/openid-configuration',
            '/mcp/.well-known/oauth-protected-resource',
            '/mcp/.well-known/oauth-protected-resource/',
        }
        if not path.startswith('/mcp/') or path in public_paths:
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        authorization = headers.get('authorization', '')
        if not authorization.startswith('Bearer '):
            _audit_security_event('mcp.auth.missing_token', scope, {'reason': 'missing_bearer'})
            await _json_response(
                scope,
                receive,
                send,
                {'error': 'missing_token', 'error_description': 'Bearer token is required for MCP access.'},
                401,
                {'WWW-Authenticate': _www_authenticate_header('invalid_token')},
            )
            return

        token = authorization.removeprefix('Bearer ').strip()
        if not MCP_BEARER_TOKEN or not hmac.compare_digest(token, MCP_BEARER_TOKEN):
            _audit_security_event('mcp.auth.invalid_token', scope, {'reason': 'invalid_token'})
            await _json_response(
                scope,
                receive,
                send,
                {'error': 'invalid_token'},
                401,
                {'WWW-Authenticate': _www_authenticate_header('invalid_token')},
            )
            return

        scopes = _scope_set(DEFAULT_SCOPE)
        actor_user_key = _safe_header_value(MCP_STATIC_SUB, 'michal')
        actor_workspace_key = _safe_header_value(MPBM_DEFAULT_WORKSPACE_KEY, 'default')
        _append_header(scope, 'x-mpbm-user-key', actor_user_key)
        _append_header(scope, 'x-mpbm-workspace-key', actor_workspace_key)
        _append_header(scope, 'x-mpbm-scopes', ' '.join(sorted(scopes)))
        scope['auth_context'] = {
            'sub': actor_user_key,
            'scope': sorted(scopes),
            'client_id': 'local-static-token',
            'user_key': actor_user_key,
            'workspace_key': actor_workspace_key,
        }
        _audit_security_event('mcp.auth.accepted', scope, {'granted_scopes': sorted(scopes)})
        await self.app(scope, receive, send)


async def health(request: Request):
    local_core.init_db()
    return JSONResponse(
        {
            'status': 'ok',
            'service': 'Jagoda Local Mini',
            'db_path': str(local_core.db_path()),
            'db_exists': local_core.db_path().exists(),
            'mcp_path': '/mcp/',
            'public_base_url': PUBLIC_BASE_URL,
            'started_at': STARTED_AT.isoformat().replace('+00:00', 'Z'),
            'checked_at': _utc_now(),
            'uptime_seconds': round(time.time() - STARTED_AT.timestamp(), 3),
            'auth_mode': 'static_bearer',
        }
    )


async def oauth_authorization_server(request: Request):
    return JSONResponse(_oauth_metadata())


async def oauth_protected_resource(request: Request):
    return JSONResponse(_protected_resource_metadata())


routes = [
    Route('/health', health, methods=['GET']),
    Route('/api/local-health', health, methods=['GET']),
    Route('/.well-known/oauth-authorization-server', oauth_authorization_server, methods=['GET']),
    Route('/.well-known/openid-configuration', oauth_authorization_server, methods=['GET']),
    Route('/.well-known/oauth-protected-resource', oauth_protected_resource, methods=['GET']),
    Route('/mcp/.well-known/oauth-protected-resource', oauth_protected_resource, methods=['GET']),
    Route('/mcp/.well-known/oauth-protected-resource/', oauth_protected_resource, methods=['GET']),
    Route('/mcp/.well-known/oauth-authorization-server', oauth_authorization_server, methods=['GET']),
    Route('/mcp/.well-known/openid-configuration', oauth_authorization_server, methods=['GET']),
]

middleware = [
    Middleware(BearerAuthMiddleware),
    Middleware(CORSMiddleware, allow_origins=[], allow_methods=['GET', 'POST'], allow_headers=['Authorization', 'Content-Type']),
]


def _build_mcp_app() -> ASGIApp:
    http_app_factory = getattr(mcp, 'http_app', None)
    if callable(http_app_factory):
        return http_app_factory(path='/mcp/')
    return Starlette(routes=[])


mcp_app = _build_mcp_app()
app = Starlette(routes=routes, middleware=middleware, lifespan=getattr(mcp_app, 'lifespan', None))
app.mount('/', mcp_app)


if __name__ == '__main__':
    import uvicorn

    local_core.init_db()
    uvicorn.run(app, host='127.0.0.1', port=int(os.environ.get('PORT', '8015')))
