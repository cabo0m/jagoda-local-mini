# MPbM backup runbook

Status: aktualne po wykonaniu backupu przed dalszym wpuszczaniem użytkowników, dodaniu rotacyjnego backupu lokalnej pamięci oraz dodaniu skryptu backupu runtime VPS.

## Priorytet

Lokalna baza jest primary memory source Jagody:

```text
C:\jagoda-memory-api\data\jagoda_memory.db
```

VPS jest ważny operacyjnie jako publiczny endpoint MPbM, ale nie zastępuje lokalnej pamięci. Lokalny backup jest ważniejszy od backupu VPS.

## Cel

Przed zapraszaniem kolejnych użytkowników do publicznego MPbM trzeba mieć snapshot najważniejszych plików runtime, bazy i dokumentacji.

## Zakres minimalnego backupu

Minimalny backup powinien zawierać:

```text
data/jagoda_memory.db
data/mpbm_security_audit.jsonl
server_health.py
server_mpbm_core.py
oauth_token_store.py
invite_store.py
docs/MPBM_PUBLIC_CONNECTOR_RUNBOOK.md
docs/MPBM_INVITE_DB_RUNBOOK.md
docs/MPBM_BACKUP_RUNBOOK.md
docs/README.md
```

Nie backupować do repo sekretów typu:

```text
.static_token.local
/etc/jagoda-mcp.env
Bearer tokeny
invite codes w jawnej postaci
```

## Automatyczny lokalny backup rotacyjny

Dodano skrypt:

```text
scripts/backup_local_memory.py
```

Wykonuje backup lokalnej bazy przez SQLite backup API, więc jest bezpieczniejszy niż zwykłe kopiowanie aktywnego pliku `.db`.

Domyślny katalog:

```text
C:\jagoda-memory-api\backups\local_memory_daily\
```

Ręczne uruchomienie:

```powershell
cd C:\jagoda-memory-api
.\.venv\Scripts\python.exe scripts\backup_local_memory.py --project-root C:\jagoda-memory-api --keep 30
```

Ostatni test skryptu:

```text
C:\jagoda-memory-api\backups\local_memory_daily\local_memory_daily_20260426_165213
C:\jagoda-memory-api\backups\local_memory_daily\local_memory_daily_20260426_165213.zip
```

Wynik testu:

```text
status=ok
files_count=8
primary_db_sha256=E772E306686B120396450A35EEF3ED221B10B9667B823B0E9D0AA559074E5EAA
```

## Rejestracja zadania Windows Task Scheduler

Dodano skrypt:

```text
scripts/register_local_memory_backup_task.ps1
```

Rejestracja codziennego backupu o 03:15 z retencją 30 backupów:

```powershell
cd C:\jagoda-memory-api
powershell -ExecutionPolicy Bypass -File scripts\register_local_memory_backup_task.ps1 `
  -ProjectRoot C:\jagoda-memory-api `
  -TaskName "Jagoda Local Memory Daily Backup" `
  -Time "03:15" `
  -Keep 30
```

Ręczne odpalenie zarejestrowanego zadania:

```powershell
Start-ScheduledTask -TaskName "Jagoda Local Memory Daily Backup"
```

Sprawdzenie zadania:

```powershell
Get-ScheduledTask -TaskName "Jagoda Local Memory Daily Backup"
```

## Jednorazowy backup przed onboardingiem

Ostatni wykonany backup lokalny przed onboardingiem:

```text
C:\jagoda-memory-api\backups\pre_user_onboarding_20260426_175858
C:\jagoda-memory-api\backups\pre_user_onboarding_20260426_175858.zip
```

Backup zawiera 7 plików i manifest z SHA256.

## Automatyzowalny backup runtime VPS

Dodano skrypt:

```text
scripts/backup_vps_runtime.py
```

Skrypt używa SQLite backup API dla `jagoda_memory.db`, kopiuje runtime files i dokumentację, tworzy `manifest.json`, `MANIFEST.txt` oraz archiwum `.tar.gz`. Nie kopiuje plików wyglądających jak sekrety.

Ręczne uruchomienie na VPS:

```bash
cd /srv/Firma_morenatech.work_Jagoda

/srv/Firma_morenatech.work_Jagoda/.venv/bin/python scripts/backup_vps_runtime.py \
  --project-root /srv/Firma_morenatech.work_Jagoda \
  --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db \
  --backup-base /srv/Firma_morenatech.work_Jagoda/backups/vps_runtime \
  --keep 14
```

Oczekiwany wynik:

```text
status=ok
backup_dir=.../backups/vps_runtime/vps_runtime_YYYYMMDD_HHMMSS
backup_archive=.../backups/vps_runtime/vps_runtime_YYYYMMDD_HHMMSS.tar.gz
files_count >= 8
primary_db_sha256=<SHA256>
```

Lokalny smoke test skryptu wykonany na kopii lokalnej:

```text
backup_dir=C:\jagoda-memory-api\.pytest_tmp\vps_backup_smoke\vps_runtime_20260427_074615
backup_archive=C:\jagoda-memory-api\.pytest_tmp\vps_backup_smoke\vps_runtime_20260427_074615.tar.gz
files_count=10
primary_db_sha256=2693EA4E0269651BE72F69EB08B07B60F4C0BF8D93F18618AC3BD552EF5BA602
```

Na VPS po wykonaniu backupu sprawdzić:

```bash
ls -lah /srv/Firma_morenatech.work_Jagoda/backups/vps_runtime | tail
cat /srv/Firma_morenatech.work_Jagoda/backups/vps_runtime/vps_runtime_*/MANIFEST.txt | tail -40
```

Nie kopiować `.static_token.local` ani `/etc/jagoda-mcp.env` do zwykłego backupu projektowego. Sekrety backupować osobnym kanałem, poza repo i poza czatem.

## Starszy ręczny wariant VPS

Jeśli skrypt nie jest dostępny, awaryjnie można wykonać ręczny snapshot:

```bash
cd /srv/Firma_morenatech.work_Jagoda
TS="$(date -u +%Y%m%d_%H%M%S)"
BACKUP_DIR="backups/pre_user_onboarding_$TS"
mkdir -p "$BACKUP_DIR/data" "$BACKUP_DIR/docs"

cp data/jagoda_memory.db "$BACKUP_DIR/data/"
cp data/mpbm_security_audit.jsonl "$BACKUP_DIR/data/"
cp server_health.py server_mpbm_core.py oauth_token_store.py invite_store.py "$BACKUP_DIR/"
cp docs/MPBM_PUBLIC_CONNECTOR_RUNBOOK.md docs/MPBM_INVITE_DB_RUNBOOK.md docs/MPBM_BACKUP_RUNBOOK.md docs/README.md "$BACKUP_DIR/docs/"

find "$BACKUP_DIR" -type f -print0 | xargs -0 sha256sum > "$BACKUP_DIR/MANIFEST.sha256"
tar -czf "$BACKUP_DIR.tar.gz" "$BACKUP_DIR"
```

Nie kopiować `.static_token.local` ani `/etc/jagoda-mcp.env` do zwykłego backupu projektowego. Sekrety backupować osobnym kanałem, poza repo i poza czatem.
