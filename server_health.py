from __future__ import annotations

import base64
import hashlib
import hmac
import html
import json
import os
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from starlette.applications import Starlette
from starlette.datastructures import Headers
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from server_mpbm_core import mcp
from oauth_token_store import OAuthTokenStore
from invite_store import InviteStore
from mpbm_public_health import build_public_health_payload, render_public_health_html

STARTED_AT = datetime.now(timezone.utc)
APP_DIR = Path(os.environ.get("ASSISTANT_ROOT", "/srv/Firma_morenatech.work_Jagoda")).resolve()
DB_PATH = Path(os.environ.get("DB_PATH", str(APP_DIR / "data" / "assistant_memory.db"))).resolve()
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://jagoda.morenatech.pl").rstrip("/")
MCP_BEARER_TOKEN = os.environ.get("MCP_BEARER_TOKEN", "")
MCP_STATIC_SUB = os.environ.get("MCP_STATIC_SUB", "system:legacy")
MPBM_DEFAULT_WORKSPACE_KEY = os.environ.get("MPBM_PUBLIC_WORKSPACE_KEY", "default")
MCP_TOKEN_TTL_SECONDS = int(os.environ.get("MCP_TOKEN_TTL_SECONDS", "3600"))
SECURITY_AUDIT_LOG_PATH = Path(os.environ.get("MPBM_SECURITY_AUDIT_LOG", str(APP_DIR / "data" / "mpbm_security_audit.jsonl"))).resolve()
MPBM_INVITE_CODES = os.environ.get("MPBM_INVITE_CODES", "")
MPBM_ALLOW_UNINVITED_OAUTH = os.environ.get("MPBM_ALLOW_UNINVITED_OAUTH", "true").lower() in {"1", "true", "yes", "on"}

SUPPORTED_SCOPES = ("mcp:tools", "memories:read", "memories:write")
DEFAULT_SCOPE = " ".join(SUPPORTED_SCOPES)
OAUTH_AUTH_CODES: dict[str, dict[str, object]] = {}
OAUTH_ACCESS_TOKENS: dict[str, dict[str, object]] = {}
OAUTH_TOKEN_STORE = OAuthTokenStore(DB_PATH)
MPBM_INVITE_STORE = InviteStore(DB_PATH)



def _parse_invite_codes(raw_codes: str) -> dict[str, str]:
    """Parse invite mapping from env: CODE:user_key,CODE2:user_key2."""
    result: dict[str, str] = {}
    for chunk in (raw_codes or "").split(","):
        item = chunk.strip()
        if not item:
            continue
        if ":" not in item:
            continue
        code, user_key = item.split(":", 1)
        code = code.strip()
        user_key = _safe_header_value(user_key.strip(), "")
        if code and user_key:
            result[code] = user_key
    return result


def _invite_map() -> dict[str, str]:
    return _parse_invite_codes(MPBM_INVITE_CODES)


def _db_invites_enabled() -> bool:
    try:
        return MPBM_INVITE_STORE.has_any_invites()
    except Exception:
        return False


def _invite_required() -> bool:
    return (bool(_invite_map()) or _db_invites_enabled()) and not MPBM_ALLOW_UNINVITED_OAUTH


def _oauth_invite_form(query_items: list[tuple[str, str]], error: str | None = None) -> HTMLResponse:
    hidden_fields = []
    for key, value in query_items:
        if key == "invite_code":
            continue
        hidden_fields.append(
            f'<input type="hidden" name="{html.escape(key, quote=True)}" value="{html.escape(value, quote=True)}">'
        )
    error_html = f'<p style="color:#b00020">{html.escape(error)}</p>' if error else ""
    body = f"""
<!doctype html>
<html lang="pl">
  <head><meta charset="utf-8"><title>MPbM invite</title></head>
  <body style="font-family: system-ui, sans-serif; max-width: 560px; margin: 4rem auto; line-height: 1.45">
    <h1>MPbM connector</h1>
    <p>Wpisz kod zaproszenia, żeby dokończyć logowanie do serwera pamięci.</p>
    {error_html}
    <form method="get" action="/oauth/authorize">
      {''.join(hidden_fields)}
      <label>Kod zaproszenia<br><input name="invite_code" required autofocus style="font-size: 1rem; padding: .5rem; width: 100%"></label>
      <p><button type="submit" style="font-size: 1rem; padding: .5rem 1rem">Połącz</button></p>
    </form>
  </body>
</html>
"""
    return HTMLResponse(body, status_code=401 if error else 200)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _now_epoch() -> int:
    return int(time.time())


def _scope_set(raw_scope: object) -> set[str]:
    if raw_scope is None:
        return set()
    return {item.strip() for item in str(raw_scope).split() if item.strip()}



def _safe_header_value(value: object, fallback: str) -> str:
    raw = str(value or "").strip() or fallback
    safe = "".join(ch if ch.isalnum() or ch in {":", "-", "_", ".", "@"} else "_" for ch in raw)
    return safe[:160] or fallback


def _append_header(scope: Scope, name: str, value: str) -> None:
    trusted_name = name.lower().encode("latin-1")
    headers = [item for item in list(scope.get("headers") or []) if item[0].lower() != trusted_name]
    headers.append((trusted_name, value.encode("latin-1")))
    scope["headers"] = headers

def _validate_requested_scope(raw_scope: str | None) -> tuple[bool, str, str | None]:
    scope = (raw_scope or DEFAULT_SCOPE).strip()
    requested = _scope_set(scope)
    unsupported = requested - set(SUPPORTED_SCOPES)
    if unsupported:
        return False, scope, f"unsupported scopes: {', '.join(sorted(unsupported))}"
    if not requested:
        return False, scope, "empty scope"
    return True, scope, None


def _pkce_s256(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _verify_pkce(verifier: str, challenge: str, method: str) -> bool:
    if not verifier or not challenge:
        return False
    normalized_method = (method or "plain").upper()
    if normalized_method == "S256":
        expected = _pkce_s256(verifier)
    elif normalized_method == "PLAIN":
        expected = verifier
    else:
        return False
    return hmac.compare_digest(expected, challenge)


def _oauth_metadata() -> dict[str, object]:
    return {
        "issuer": PUBLIC_BASE_URL,
        "authorization_endpoint": f"{PUBLIC_BASE_URL}/oauth/authorize",
        "token_endpoint": f"{PUBLIC_BASE_URL}/oauth/token",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256", "plain"],
        "token_endpoint_auth_methods_supported": ["none"],
        "scopes_supported": list(SUPPORTED_SCOPES),
        "registration_endpoint": f"{PUBLIC_BASE_URL}/oauth/register",
    }


def _protected_resource_metadata() -> dict[str, object]:
    return {
        "resource": f"{PUBLIC_BASE_URL}/mcp/",
        "authorization_servers": [PUBLIC_BASE_URL],
        "bearer_methods_supported": ["header"],
        "scopes_supported": list(SUPPORTED_SCOPES),
    }


def _validate_bearer_token(token: str) -> tuple[bool, dict[str, object]]:
    if not token:
        return False, {"error": "missing_token"}

    if MCP_BEARER_TOKEN and hmac.compare_digest(token, MCP_BEARER_TOKEN):
        return True, {
            "iss": PUBLIC_BASE_URL,
            "sub": MCP_STATIC_SUB,
            "aud": f"{PUBLIC_BASE_URL}/mcp/",
            "scope": DEFAULT_SCOPE,
            "client_id": "static-token",
            "exp": None,
        }

    token_record = OAUTH_ACCESS_TOKENS.get(token)
    if token_record is None:
        load_status, persisted_record = OAUTH_TOKEN_STORE.load(token)
        if load_status == "missing":
            return False, {"error": "invalid_token"}
        if load_status == "expired":
            return False, {"error": "expired_token"}
        if load_status == "revoked":
            return False, {"error": "invalid_token"}
        if persisted_record is None:
            return False, {"error": "invalid_token"}
        token_record = persisted_record
        OAUTH_ACCESS_TOKENS[token] = dict(token_record)

    expires_at = int(token_record.get("expires_at") or 0)
    if expires_at <= _now_epoch():
        OAUTH_ACCESS_TOKENS.pop(token, None)
        OAUTH_TOKEN_STORE.delete(token)
        return False, {"error": "expired_token"}

    return True, dict(token_record)


def _www_authenticate_header(error: str | None = None) -> str:
    metadata_url = f"{PUBLIC_BASE_URL}/.well-known/oauth-protected-resource"
    parts = ["Bearer", 'realm="mcp"', f'resource_metadata="{metadata_url}"']
    if error:
        parts.append(f'error="{error}"')
    return ", ".join(parts)



def _audit_security_event(event_type: str, scope: Scope, details: dict[str, object] | None = None) -> None:
    """Append a redacted security event to JSONL audit log."""
    headers = Headers(scope=scope)
    auth_context = scope.get("auth_context") if isinstance(scope.get("auth_context"), dict) else {}
    event = {
        "ts": _utc_now(),
        "event_type": event_type,
        "path": str(scope.get("path") or ""),
        "method": str(scope.get("method") or ""),
        "client": str((scope.get("client") or ["unknown"])[0]),
        "user_key": auth_context.get("user_key"),
        "workspace_key": auth_context.get("workspace_key"),
        "client_id": auth_context.get("client_id"),
        "scopes": auth_context.get("scope"),
        "user_agent": headers.get("user-agent"),
        "details": details or {},
    }
    SECURITY_AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SECURITY_AUDIT_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")

async def _json_response(scope: Scope, receive: Receive, send: Send, payload: dict[str, object], status_code: int, headers: dict[str, str] | None = None) -> None:
    response = JSONResponse(payload, status_code=status_code, headers=headers)
    await response(scope, receive, send)


class MCPBearerAuthMiddleware:
    """Protects the public MCP endpoint with a Bearer token gate.

    This middleware intentionally does not trust user identifiers from MCP tool
    arguments. It only authenticates the HTTP request and attaches token claims
    to the ASGI scope for the next layer.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path") or "")

        public_mcp_metadata_paths = {
            "/mcp/.well-known/oauth-protected-resource",
            "/mcp/.well-known/oauth-protected-resource/",
            "/mcp/.well-known/oauth-authorization-server",
            "/mcp/.well-known/oauth-authorization-server/",
            "/mcp/.well-known/openid-configuration",
            "/mcp/.well-known/openid-configuration/",
        }

        if path in public_mcp_metadata_paths:
            await self.app(scope, receive, send)
            return

        if not path.startswith("/mcp/"):
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        authorization = headers.get("authorization", "")
        if not authorization.startswith("Bearer "):
            _audit_security_event("mcp.auth.missing_token", scope, {"reason": "missing_bearer"})
            await _json_response(
                scope,
                receive,
                send,
                {"error": "missing_token", "error_description": "Bearer token is required for MCP access."},
                401,
                {"WWW-Authenticate": _www_authenticate_header("invalid_token")},
            )
            return

        token = authorization.removeprefix("Bearer ").strip()
        is_valid, claims = _validate_bearer_token(token)
        if not is_valid:
            _audit_security_event("mcp.auth.invalid_token", scope, {"reason": str(claims.get("error", "invalid_token"))})
            await _json_response(
                scope,
                receive,
                send,
                {"error": claims.get("error", "invalid_token")},
                401,
                {"WWW-Authenticate": _www_authenticate_header("invalid_token")},
            )
            return

        scopes = _scope_set(claims.get("scope"))
        if "mcp:tools" not in scopes:
            _audit_security_event("mcp.auth.insufficient_scope", scope, {"required_scope": "mcp:tools", "granted_scopes": sorted(scopes)})
            await _json_response(
                scope,
                receive,
                send,
                {"error": "insufficient_scope", "required_scope": "mcp:tools"},
                403,
                {"WWW-Authenticate": _www_authenticate_header("insufficient_scope")},
            )
            return

        actor_user_key = _safe_header_value(claims.get("sub"), MCP_STATIC_SUB)
        actor_workspace_key = _safe_header_value(claims.get("workspace_key"), MPBM_DEFAULT_WORKSPACE_KEY)
        _append_header(scope, "x-mpbm-user-key", actor_user_key)
        _append_header(scope, "x-mpbm-workspace-key", actor_workspace_key)
        _append_header(scope, "x-mpbm-scopes", " ".join(sorted(scopes)))
        scope["auth_context"] = {
            "iss": claims.get("iss"),
            "sub": claims.get("sub"),
            "aud": claims.get("aud"),
            "scope": sorted(scopes),
            "client_id": claims.get("client_id"),
            "user_key": actor_user_key,
            "workspace_key": actor_workspace_key,
        }
        _audit_security_event("mcp.auth.accepted", scope, {"granted_scopes": sorted(scopes)})
        await self.app(scope, receive, send)


async def health(request: Request):
    uptime_seconds = round(time.time() - STARTED_AT.timestamp(), 3)
    return JSONResponse(
        {
            "status": "ok",
            "service": "MPbM",
            "app_dir": str(APP_DIR),
            "db_path": str(DB_PATH),
            "db_exists": DB_PATH.exists(),
            "mcp_path": "/mcp/",
            "oauth_metadata": f"{PUBLIC_BASE_URL}/.well-known/oauth-authorization-server",
            "protected_resource_metadata": f"{PUBLIC_BASE_URL}/.well-known/oauth-protected-resource",
            "started_at": STARTED_AT.isoformat().replace("+00:00", "Z"),
            "checked_at": _utc_now(),
            "uptime_seconds": uptime_seconds,
        }
    )


async def mpbm_health(request: Request):
    payload = build_public_health_payload(
        app_dir=APP_DIR,
        db_path=DB_PATH,
        public_base_url=PUBLIC_BASE_URL,
        started_at=STARTED_AT,
        security_audit_log_path=SECURITY_AUDIT_LOG_PATH,
        invite_codes_configured=bool(_invite_map()),
        allow_uninvited_oauth=MPBM_ALLOW_UNINVITED_OAUTH,
        oauth_ram_cache_count=len(OAUTH_ACCESS_TOKENS),
    )
    accept = request.headers.get("accept", "")
    if "text/html" in accept and "application/json" not in accept:
        return HTMLResponse(render_public_health_html(payload), status_code=200)
    return JSONResponse(payload)


async def oauth_authorization_server(request: Request):
    return JSONResponse(_oauth_metadata())


async def oauth_protected_resource(request: Request):
    return JSONResponse(_protected_resource_metadata())


async def oauth_register(request: Request):
    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    client_id = "mpbm-" + secrets.token_urlsafe(24)
    return JSONResponse(
        {
            "client_id": client_id,
            "client_id_issued_at": _now_epoch(),
            "token_endpoint_auth_method": "none",
            "grant_types": payload.get("grant_types", ["authorization_code"]),
            "response_types": payload.get("response_types", ["code"]),
            "redirect_uris": payload.get("redirect_uris", []),
            "client_name": payload.get("client_name", "MPbM MCP Client"),
        },
        status_code=201,
    )


async def oauth_authorize(request: Request):
    query = request.query_params
    redirect_uri = query.get("redirect_uri")
    state = query.get("state")
    client_id = query.get("client_id", "dynamic-client")
    code_challenge = query.get("code_challenge", "")
    code_challenge_method = query.get("code_challenge_method", "S256")
    scope = query.get("scope")
    invite_code = query.get("invite_code", "")

    if not redirect_uri:
        return JSONResponse({"error": "invalid_request", "error_description": "missing redirect_uri"}, status_code=400)
    if not code_challenge:
        return JSONResponse({"error": "invalid_request", "error_description": "PKCE code_challenge is required"}, status_code=400)
    if code_challenge_method.upper() not in {"S256", "PLAIN"}:
        return JSONResponse({"error": "invalid_request", "error_description": "unsupported code_challenge_method"}, status_code=400)

    scope_ok, normalized_scope, scope_error = _validate_requested_scope(scope)
    if not scope_ok:
        return JSONResponse({"error": "invalid_scope", "error_description": scope_error}, status_code=400)

    env_invites = _invite_map()
    db_invites_enabled = _db_invites_enabled()
    subject_user_key = None
    subject_workspace_key = MPBM_DEFAULT_WORKSPACE_KEY
    token_scope = normalized_scope

    if db_invites_enabled:
        if not invite_code:
            return _oauth_invite_form(list(query.multi_items()))
        invite_validation = MPBM_INVITE_STORE.validate_code(invite_code.strip())
        if invite_validation.status == "ok" and invite_validation.record:
            subject_user_key = _safe_header_value(invite_validation.record.get("user_key"), "dynamic-client")
            subject_workspace_key = _safe_header_value(invite_validation.record.get("workspace_key"), MPBM_DEFAULT_WORKSPACE_KEY)
            token_scope = str(invite_validation.record.get("scopes") or normalized_scope)
        elif env_invites:
            subject_user_key = env_invites.get(invite_code.strip())
            if not subject_user_key:
                return _oauth_invite_form(list(query.multi_items()), "Nieprawidłowy kod zaproszenia.")
        else:
            return _oauth_invite_form(list(query.multi_items()), "Nieprawidłowy, wygasły albo cofnięty kod zaproszenia.")
    elif env_invites:
        if not invite_code:
            return _oauth_invite_form(list(query.multi_items()))
        subject_user_key = env_invites.get(invite_code.strip())
        if not subject_user_key:
            return _oauth_invite_form(list(query.multi_items()), "Nieprawidłowy kod zaproszenia.")
    elif MPBM_ALLOW_UNINVITED_OAUTH:
        subject_user_key = _safe_header_value(client_id, "dynamic-client")
    else:
        return JSONResponse(
            {"error": "access_denied", "error_description": "OAuth invites are not configured."},
            status_code=403,
        )

    code = secrets.token_urlsafe(32)
    OAUTH_AUTH_CODES[code] = {
        "client_id": client_id,
        "subject_user_key": subject_user_key,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method,
        "scope": token_scope,
        "workspace_key": subject_workspace_key,
        "created_at": _now_epoch(),
    }

    params = {"code": code}
    if state:
        params["state"] = state
    separator = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(redirect_uri + separator + urlencode(params), status_code=302)


async def oauth_token(request: Request):
    form = await request.form()
    grant_type = form.get("grant_type")
    code = form.get("code")
    redirect_uri = form.get("redirect_uri")
    code_verifier = str(form.get("code_verifier") or "")

    if grant_type != "authorization_code" or not code:
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

    auth_code = OAUTH_AUTH_CODES.pop(str(code), None)
    if auth_code is None:
        return JSONResponse({"error": "invalid_grant"}, status_code=400)

    if redirect_uri and str(redirect_uri) != auth_code.get("redirect_uri"):
        return JSONResponse({"error": "invalid_grant", "error_description": "redirect_uri mismatch"}, status_code=400)

    if not _verify_pkce(
        code_verifier,
        str(auth_code.get("code_challenge") or ""),
        str(auth_code.get("code_challenge_method") or "S256"),
    ):
        return JSONResponse({"error": "invalid_grant", "error_description": "PKCE verification failed"}, status_code=400)

    access_token = secrets.token_urlsafe(48)
    expires_at = _now_epoch() + MCP_TOKEN_TTL_SECONDS
    token_claims = {
        "iss": PUBLIC_BASE_URL,
        "sub": str(auth_code.get("subject_user_key") or auth_code.get("client_id") or "dynamic-client"),
        "aud": f"{PUBLIC_BASE_URL}/mcp/",
        "scope": str(auth_code.get("scope") or DEFAULT_SCOPE),
        "client_id": str(auth_code.get("client_id") or "dynamic-client"),
        "expires_at": expires_at,
        "workspace_key": str(auth_code.get("workspace_key") or MPBM_DEFAULT_WORKSPACE_KEY),
    }
    OAUTH_ACCESS_TOKENS[access_token] = token_claims
    OAUTH_TOKEN_STORE.store(access_token, token_claims)
    return JSONResponse(
        {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": MCP_TOKEN_TTL_SECONDS,
            "scope": token_claims["scope"],
        }
    )


routes = [
    Route("/health", health, methods=["GET"]),
    Route("/mpbm-health", mpbm_health, methods=["GET"]),
    Route("/api/mpbm-health", mpbm_health, methods=["GET"]),
    Route("/.well-known/oauth-authorization-server", oauth_authorization_server, methods=["GET"]),
    Route("/.well-known/openid-configuration", oauth_authorization_server, methods=["GET"]),
    Route("/.well-known/oauth-protected-resource", oauth_protected_resource, methods=["GET"]),
    Route("/oauth/register", oauth_register, methods=["POST"]),
    Route("/oauth/authorize", oauth_authorize, methods=["GET"]),
    Route("/oauth/token", oauth_token, methods=["POST"]),
    Route("/.well-known/oauth-protected-resource/mcp", oauth_protected_resource, methods=["GET"]),
    Route("/.well-known/oauth-protected-resource/mcp/", oauth_protected_resource, methods=["GET"]),
    Route("/mcp/.well-known/oauth-protected-resource", oauth_protected_resource, methods=["GET"]),
    Route("/mcp/.well-known/oauth-protected-resource/", oauth_protected_resource, methods=["GET"]),
    Route("/mcp/.well-known/oauth-authorization-server", oauth_authorization_server, methods=["GET"]),
    Route("/mcp/.well-known/openid-configuration", oauth_authorization_server, methods=["GET"]),
]

middleware = [
    Middleware(MCPBearerAuthMiddleware),
    Middleware(CORSMiddleware, allow_origins=[], allow_methods=["GET", "POST"], allow_headers=["Authorization", "Content-Type"]),
]

def _build_mcp_app() -> ASGIApp:
    http_app_factory = getattr(mcp, "http_app", None)
    if callable(http_app_factory):
        return http_app_factory(path="/mcp/")
    return Starlette(routes=[])


mcp_app = _build_mcp_app()

app = Starlette(
    routes=routes,
    middleware=middleware,
    lifespan=getattr(mcp_app, "lifespan", None),
)

app.mount("/", mcp_app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8015,
    )



