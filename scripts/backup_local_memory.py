from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_INCLUDE_FILES = [
    "data/mpbm_security_audit.jsonl",
    "server_health.py",
    "server_mpbm_core.py",
    "oauth_token_store.py",
    "docs/MPBM_PUBLIC_CONNECTOR_RUNBOOK.md",
    "docs/MPBM_BACKUP_RUNBOOK.md",
    "docs/README.md",
]

SECRET_NAME_FRAGMENTS = (
    "static_token",
    "bearer",
    ".env",
    "token.local",
    "secret",
)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def is_probable_secret(path: Path) -> bool:
    lowered = str(path).replace("\\", "/").lower()
    return any(fragment in lowered for fragment in SECRET_NAME_FRAGMENTS)


def copy_file(src: Path, dst: Path) -> dict[str, Any]:
    if is_probable_secret(src):
        raise ValueError(f"Refusing to backup probable secret: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return {
        "path": str(dst),
        "source": str(src),
        "size": dst.stat().st_size,
        "sha256": sha256_file(dst),
    }


def backup_sqlite_db(src: Path, dst: Path) -> dict[str, Any]:
    if is_probable_secret(src):
        raise ValueError(f"Refusing to backup probable secret: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(str(src))
    try:
        target = sqlite3.connect(str(dst))
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
    return {
        "path": str(dst),
        "source": str(src),
        "size": dst.stat().st_size,
        "sha256": sha256_file(dst),
        "method": "sqlite_backup_api",
    }


def write_manifest(backup_dir: Path, manifest: dict[str, Any]) -> None:
    (backup_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    lines = [
        f"Backup created: {manifest['backup_dir']}",
        f"Created at UTC: {manifest['created_at_utc']}",
        f"Project root: {manifest['project_root']}",
        "",
        "Files:",
    ]
    for item in manifest["files"]:
        rel = Path(item["path"]).relative_to(backup_dir)
        lines.append(f"- {rel} | {item['size']} bytes | {item['sha256']}")
    lines.append("")
    lines.append("Secrets policy: .env/static token/bearer/secret-looking files are intentionally excluded.")
    (backup_dir / "MANIFEST.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def zip_backup(backup_dir: Path) -> Path:
    zip_path = backup_dir.with_suffix(".zip")
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(backup_dir.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(backup_dir.parent))
    return zip_path


def prune_old_backups(base_dir: Path, keep: int) -> list[str]:
    if keep <= 0:
        return []
    dirs = sorted(
        [p for p in base_dir.glob("local_memory_daily_*") if p.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )
    removed: list[str] = []
    for old_dir in dirs[keep:]:
        zip_path = old_dir.with_suffix(".zip")
        shutil.rmtree(old_dir, ignore_errors=True)
        if zip_path.exists():
            zip_path.unlink()
        removed.append(str(old_dir))
    return removed


def run_backup(project_root: Path, keep: int, backup_base: Path | None = None) -> dict[str, Any]:
    project_root = project_root.resolve()
    backup_base = (backup_base or (project_root / "backups" / "local_memory_daily")).resolve()
    backup_dir = backup_base / f"local_memory_daily_{utc_stamp()}"
    backup_dir.mkdir(parents=True, exist_ok=False)

    files: list[dict[str, Any]] = []
    db_src = project_root / "data" / "jagoda_memory.db"
    if not db_src.exists():
        raise FileNotFoundError(f"Missing primary local memory database: {db_src}")
    files.append(backup_sqlite_db(db_src, backup_dir / "data" / "jagoda_memory.db"))

    for rel in DEFAULT_INCLUDE_FILES:
        src = project_root / rel
        if src.exists() and src.is_file():
            files.append(copy_file(src, backup_dir / rel))

    manifest = {
        "created_at_utc": iso_utc(),
        "project_root": str(project_root),
        "backup_dir": str(backup_dir),
        "backup_type": "local_memory_daily",
        "primary_memory": str(db_src),
        "files": files,
        "retention_keep": keep,
    }
    write_manifest(backup_dir, manifest)
    zip_path = zip_backup(backup_dir)
    removed = prune_old_backups(backup_base, keep)

    result = {
        "status": "ok",
        "backup_dir": str(backup_dir),
        "backup_zip": str(zip_path),
        "files_count": len(files),
        "primary_db_sha256": files[0]["sha256"],
        "removed_old_backups": removed,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return result


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Create a rotating local backup of Jagoda's primary memory database.")
    parser.add_argument("--project-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--backup-base", default=None)
    parser.add_argument("--keep", type=int, default=30)
    args = parser.parse_args(argv)

    run_backup(
        project_root=Path(args.project_root),
        keep=args.keep,
        backup_base=Path(args.backup_base).resolve() if args.backup_base else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
