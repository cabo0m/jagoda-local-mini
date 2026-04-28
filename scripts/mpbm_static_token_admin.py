from __future__ import annotations

"""Operator helper for local/admin static Bearer token rotation.

This script is intentionally conservative:
- it generates or syncs only the owner/admin static token used by MCP_BEARER_TOKEN;
- it does not create per-user public OAuth tokens;
- it never stores raw tokens in the database.

For normal external users, create an invite code and let OAuth/PKCE issue tokens.
"""

import argparse
import hashlib
import json
import secrets
from pathlib import Path
from typing import Any

DEFAULT_TOKEN_FILE = Path(".static_token.local")
DEFAULT_ENV_FILE = Path("/etc/jagoda-mcp.env")


def sha256_prefix(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def generate_token(prefix: str = "MPBM_STATIC") -> str:
    safe_prefix = "".join(ch for ch in prefix.upper() if ch.isalnum() or ch in {"_", "-"}) or "MPBM_STATIC"
    return f"{safe_prefix}_{secrets.token_urlsafe(48)}"


def print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8").strip()


def write_private(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.strip() + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except PermissionError:
        pass


def parse_env(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def update_env_value(path: Path, key: str, value: str) -> bool:
    lines = parse_env(path)
    out: list[str] = []
    found = False
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            out.append(line)
            continue
        current_key = line.split("=", 1)[0].strip()
        if current_key == key:
            out.append(f"{key}={value}")
            found = True
        else:
            out.append(line)
    if not found:
        out.append(f"{key}={value}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")
    return found


def env_value_hash(path: Path, key: str) -> dict[str, Any]:
    for line in parse_env(path):
        if "=" not in line or line.strip().startswith("#"):
            continue
        current_key, value = line.split("=", 1)
        if current_key.strip() == key:
            clean = value.strip()
            return {"present": True, "length": len(clean), "sha256_prefix": sha256_prefix(clean)}
    return {"present": False, "length": 0, "sha256_prefix": None}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage the owner/admin MPbM static Bearer token file/env.")
    parser.add_argument("--token-file", default=str(DEFAULT_TOKEN_FILE))
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--env-key", default="MCP_BEARER_TOKEN")
    sub = parser.add_subparsers(dest="command", required=True)

    rotate = sub.add_parser("rotate", help="Generate a new static token and write it to token file.")
    rotate.add_argument("--prefix", default="MPBM_STATIC")
    rotate.add_argument("--sync-env", action="store_true", help="Also update the env file with MCP_BEARER_TOKEN.")
    rotate.add_argument("--show-once", action="store_true", help="Print the raw token once. Avoid in shared terminals/logs.")

    sync_env = sub.add_parser("sync-env", help="Copy token-file value into env file key.")

    status = sub.add_parser("status", help="Show token-file/env presence, length and hash prefixes only.")

    args = parser.parse_args(argv)
    token_file = Path(args.token_file)
    env_file = Path(args.env_file)

    if args.command == "rotate":
        token = generate_token(args.prefix)
        write_private(token_file, token)
        env_updated = False
        if args.sync_env:
            update_env_value(env_file, args.env_key, token)
            env_updated = True
        payload: dict[str, Any] = {
            "status": "rotated",
            "token_file": str(token_file),
            "token_length": len(token),
            "token_sha256_prefix": sha256_prefix(token),
            "env_file": str(env_file),
            "env_updated": env_updated,
            "restart_required": env_updated,
            "warning": "Raw static token is for owner/admin only. Do not share it with public users.",
        }
        if args.show_once:
            payload["token_SHOW_ONCE"] = token
        print_json(payload)
        return 0

    if args.command == "sync-env":
        token = read_text(token_file)
        if not token:
            print_json({"status": "missing_token_file", "token_file": str(token_file)})
            return 1
        existed = update_env_value(env_file, args.env_key, token)
        print_json(
            {
                "status": "synced",
                "token_file": str(token_file),
                "env_file": str(env_file),
                "env_key": args.env_key,
                "env_key_previously_present": existed,
                "token_length": len(token),
                "token_sha256_prefix": sha256_prefix(token),
                "restart_required": True,
            }
        )
        return 0

    if args.command == "status":
        token = read_text(token_file)
        print_json(
            {
                "token_file": {
                    "path": str(token_file),
                    "present": token is not None,
                    "length": len(token) if token else 0,
                    "sha256_prefix": sha256_prefix(token) if token else None,
                },
                "env_file": {
                    "path": str(env_file),
                    args.env_key: env_value_hash(env_file, args.env_key),
                },
            }
        )
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
