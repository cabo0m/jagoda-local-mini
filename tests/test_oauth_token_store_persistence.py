from __future__ import annotations

import base64
import hashlib
import sqlite3
from urllib.parse import parse_qs, urlparse

from starlette.testclient import TestClient

import server_health
from oauth_token_store import OAuthTokenStore


def _pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _issue_oauth_token(client: TestClient, client_id: str = "persistent-client") -> str:
    verifier = "persistent-verifier-1234567890"
    authorize = client.get(
        "/oauth/authorize",
        params={
            "client_id": client_id,
            "redirect_uri": "https://client.example/callback",
            "response_type": "code",
            "scope": "mcp:tools memories:read memories:write",
            "code_challenge": _pkce_challenge(verifier),
            "code_challenge_method": "S256",
            "state": "persist-state",
        },
        follow_redirects=False,
    )
    assert authorize.status_code == 302
    code = parse_qs(urlparse(authorize.headers["location"]).query)["code"][0]

    token = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": verifier,
        },
    )
    assert token.status_code == 200
    return str(token.json()["access_token"])


def test_oauth_token_survives_process_cache_clear(monkeypatch, tmp_path) -> None:
    store = OAuthTokenStore(tmp_path / "mpbm_tokens.db")
    monkeypatch.setattr(server_health, "OAUTH_TOKEN_STORE", store)
    server_health.OAUTH_ACCESS_TOKENS.clear()
    client = TestClient(server_health.app)

    access_token = _issue_oauth_token(client)
    assert access_token in server_health.OAUTH_ACCESS_TOKENS

    server_health.OAUTH_ACCESS_TOKENS.clear()
    is_valid, claims = server_health._validate_bearer_token(access_token)

    assert is_valid is True
    assert claims["sub"] == "persistent-client"
    assert claims["client_id"] == "persistent-client"
    assert "mcp:tools" in str(claims["scope"])
    assert access_token in server_health.OAUTH_ACCESS_TOKENS


def test_oauth_token_store_does_not_persist_raw_bearer_token(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "mpbm_tokens.db"
    store = OAuthTokenStore(db_path)
    monkeypatch.setattr(server_health, "OAUTH_TOKEN_STORE", store)
    server_health.OAUTH_ACCESS_TOKENS.clear()
    client = TestClient(server_health.app)

    access_token = _issue_oauth_token(client, client_id="no-raw-token-client")
    raw_db_text = db_path.read_bytes().decode("latin-1", errors="ignore")

    assert access_token not in raw_db_text

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT token_hash, claims_json FROM mpbm_oauth_access_tokens").fetchone()
    assert row is not None
    assert row[0] == hashlib.sha256(access_token.encode("utf-8")).hexdigest()
    assert access_token not in row[1]


def test_persisted_revoked_oauth_token_is_rejected(monkeypatch, tmp_path) -> None:
    store = OAuthTokenStore(tmp_path / "mpbm_tokens.db")
    monkeypatch.setattr(server_health, "OAUTH_TOKEN_STORE", store)
    server_health.OAUTH_ACCESS_TOKENS.clear()
    client = TestClient(server_health.app)

    access_token = _issue_oauth_token(client, client_id="revoked-client")
    server_health.OAUTH_ACCESS_TOKENS.clear()
    store.revoke(access_token)

    is_valid, claims = server_health._validate_bearer_token(access_token)

    assert is_valid is False
    assert claims["error"] == "invalid_token"
