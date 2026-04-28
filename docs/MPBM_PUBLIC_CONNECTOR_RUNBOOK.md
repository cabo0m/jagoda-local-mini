# MPbM public connector runbook

Status: aktualne po wdrożeniu publicznego MPbM na `https://jagoda.morenatech.pl/mcp/`, invite OAuth/PKCE, izolacji użytkowników, trwałego OAuth token store oraz DB-backed invite store.

## TL;DR

Publiczny MPbM działa przez:

```text
https://jagoda.morenatech.pl/mcp/
```

Na VPS jedyną prawdziwą ścieżką projektu jest:

```text
/srv/Firma_morenatech.work_Jagoda
```

Proces publiczny to:

```text
server_health.py + server_mpbm_core.py + oauth_token_store.py
```

Nie wystawiać publicznie starego `server.py`.

## MPbM vs MAPI

MPbM to publiczny, ograniczony connector dla zwykłych użytkowników. Powinien pokazywać tylko:

```text
create_memory
find_memories
list_memories
get_memory
get_memory_links
recall_memory
```

MAPI to lokalny/adminowy connector operatorski. Jeżeli connector pokazuje narzędzia typu:

```text
query_sql
write_file_text
run_powershell
undo_run
run_sandman_v1
link_memories
```

to jest MAPI, nie publiczny MPbM.

## Bezpieczeństwo

Nie rozdawać `MCP_BEARER_TOKEN`. Static Bearer token jest tylko dla właściciela/admina i działa jako `MCP_STATIC_SUB`, obecnie `michal`.

Zwykli użytkownicy logują się przez OAuth/PKCE i invite code. User nie wpisuje ręcznie Bearer tokena ani identyfikatora. To invite code mapuje go na stały `user_key`, np. `basia`. Aktualna ścieżka operatorska dla zaproszeń to tabela SQLite `mpbm_invites` zarządzana przez `scripts/mpbm_invites.py`; szczegóły są w `docs/MPBM_INVITE_DB_RUNBOOK.md`.

## Konfiguracja VPS

`/etc/jagoda-mcp.env` powinien zawierać co najmniej:

```bash
ASSISTANT_ROOT=/srv/Firma_morenatech.work_Jagoda
DB_PATH=/srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db
PUBLIC_BASE_URL=https://jagoda.morenatech.pl
MPBM_PUBLIC_WORKSPACE_KEY=default
MCP_STATIC_SUB=michal
MCP_TOKEN_TTL_SECONDS=3600
MPBM_SECURITY_AUDIT_LOG=/srv/Firma_morenatech.work_Jagoda/data/mpbm_security_audit.jsonl
MPBM_ALLOW_UNINVITED_OAUTH=false\n# Legacy fallback only, not primary path:\n# MPBM_INVITE_CODES=KOD_BASI:basia,KOD_ADAMA:adam
```

`MCP_BEARER_TOKEN` ma pochodzić z sekretu na VPS, np. `.static_token.local`, i nie może trafić do repo, dokumentacji ani czatu.

## Systemd

Aktualnie używana usługa produkcyjna:

```text
jagoda-mcp.service
```

Powinna uruchamiać:

```text
/srv/Firma_morenatech.work_Jagoda/.venv/bin/python /srv/Firma_morenatech.work_Jagoda/server_health.py
```

z working directory:

```text
/srv/Firma_morenatech.work_Jagoda
```

Po zmianach:

```bash
systemctl restart jagoda-mcp
systemctl status jagoda-mcp --no-pager
```

## Caddy

Na VPS porty 80/443 trzyma Caddy, więc nie uruchamiać nginx równolegle.

Caddy dla `jagoda.morenatech.pl` proxy'uje:

```text
/.well-known/* -> 127.0.0.1:8015
/oauth/*       -> 127.0.0.1:8015
/mcp/*         -> 127.0.0.1:8015
```

`/health` może być chronione basicauth.

Po zmianie Caddyfile:

```bash
caddy validate --config /etc/caddy/Caddyfile
systemctl reload caddy
```

## Invite codes DB\n\nZaproszenia dla zwykłych użytkowników powinny być tworzone w bazie, nie w ENV jako główna ścieżka. Operator używa:\n\n```bash\npython scripts/mpbm_invites.py --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db add --user-key basia --workspace-key default --created-by michal --ttl-days 14\npython scripts/mpbm_invites.py --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db list\npython scripts/mpbm_invites.py --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db revoke 123\npython scripts/mpbm_invites.py --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db renew 123 --ttl-days 14\n```\n\nRaw invite code jest pokazany tylko raz przy `add`. Baza przechowuje `code_hash`, nie surowy kod. Pełny runbook: `docs/MPBM_INVITE_DB_RUNBOOK.md`.\n\n## OAuth token persistence

`oauth_token_store.py` zapisuje access tokeny w SQLite w tabeli:

```text
mpbm_oauth_access_tokens
```

Przechowywane pola:

```text
token_hash
claims_json
expires_at
created_at
last_seen_at
revoked_at
```

Nie zapisujemy surowego Bearer tokena. Zapisujemy `sha256(token)` oraz claims. RAM-owe `OAUTH_ACCESS_TOKENS` działa jako cache. Po restarcie `jagoda-mcp` ważny token może zostać odczytany z SQLite.

## Smoke testy

Health lokalnie:

```bash
curl http://127.0.0.1:8015/health
```

Oczekiwane: `status=ok`, `app_dir=/srv/Firma_morenatech.work_Jagoda`, `db_exists=true`.

Metadata publicznie:

```bash
curl https://jagoda.morenatech.pl/.well-known/oauth-protected-resource
curl https://jagoda.morenatech.pl/.well-known/oauth-authorization-server
```

`/mcp/` bez tokena:

```bash
curl -i \
  -X POST https://jagoda.morenatech.pl/mcp/ \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{}'
```

Oczekiwane: `401 missing_token`.

`/mcp/` ze static Bearer tokenem:

```bash
TOKEN="$(cat /srv/Firma_morenatech.work_Jagoda/.static_token.local | tr -d '\n')"

curl -i \
  -X POST https://jagoda.morenatech.pl/mcp/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{}'
```

Oczekiwane: nie `401`, zwykle `400` JSON-RPC validation error dla pustego `{}`.

## Test izolacji użytkowników

Potwierdzone produkcyjnie:

```text
Basia nie widzi prywatnych memories Michała.
Michał nie widzi prywatnych memories Basi.
Oboje widzą swoje prywatne memories.
scope_retrieval_active=true.
```

Znaczniki smoke testów:

```text
BASIA_ISOLATION_2026_04_26_A
MICHAL_ISOLATION_2026_04_26_A
```

## Testy lokalne

Aktualny pakiet regresyjny OAuth/MPbM i invite DB:

```bash
python -m pytest \\\n  tests/test_invite_store.py \\\n  tests/test_mpbm_invites_cli.py \\\n  tests/test_oauth_invite_store_flow.py \\\n  tests/test_oauth_token_store_persistence.py \
  tests/test_oauth_mcp_security.py \
  tests/test_oauth_actor_context.py \
  tests/test_oauth_audit_log.py \
  -q
```

Ostatni znany wynik dla samego pakietu invite DB: `11 passed`; dla token persistence/security wcześniej: `15 passed`.

## Diagnostyka

Logi usługi:

```bash
journalctl -fu jagoda-mcp
```

Audit security:

```bash
tail -f /srv/Firma_morenatech.work_Jagoda/data/mpbm_security_audit.jsonl
```

Porty:

```bash
ss -ltnp | egrep ':80|:443|:8015'
```

Typowe błędy:

```text
401 missing_token  -> klient nie wysłał Authorization Bearer
401 invalid_token  -> token nie pasuje do static ani persisted token store
401 expired_token  -> token był w store, ale wygasł
403 access_denied  -> invite wymagane, ale nie skonfigurowane lub flow zablokowany
400 JSON-RPC       -> auth działa, ale request MCP jest pusty/niepoprawny
```

## Następne kroki

1. Dodać dashboard zdrowia publicznego MPbM.\n2. Dodać regularny backup `jagoda_memory.db` i `mpbm_security_audit.jsonl`.\n3. Oznaczyć smoke-test memories tagiem `smoke-test` i okresowo archiwizować.\n4. Opcjonalnie dodać helper importu legacy `MPBM_INVITE_CODES` do DB bez drukowania raw kodów.

