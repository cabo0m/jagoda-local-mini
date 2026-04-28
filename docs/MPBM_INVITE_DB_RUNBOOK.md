# MPbM invite codes DB runbook

Status: aktualna ścieżka operatorska dla zaproszeń MPbM. Invite codes mają żyć w SQLite, nie w `MPBM_INVITE_CODES` jako głównym mechanizmie.

## Zasada

Raw invite code jest sekretem jednorazowym. Pokazujemy go operatorowi tylko przy tworzeniu kodu. W bazie zapisujemy wyłącznie hash:

```text
mpbm_invites.code_hash
```

Nie zapisujemy surowego kodu w dokumentacji, logach, pamięciach ani repo.

## Pliki

```text
invite_store.py
scripts/mpbm_invites.py
server_health.py
tests/test_invite_store.py
tests/test_mpbm_invites_cli.py
tests/test_oauth_invite_store_flow.py
```

`server_health.py` używa `InviteStore(DB_PATH)` i podczas `/oauth/authorize` sprawdza najpierw tabelę `mpbm_invites`. Stare `MPBM_INVITE_CODES` zostaje tylko jako awaryjny/legacy fallback.

## Minimalny env na VPS

W `/etc/jagoda-mcp.env` zostawiamy:

```bash
MPBM_ALLOW_UNINVITED_OAUTH=false
```

Nie dodajemy nowych użytkowników przez `MPBM_INVITE_CODES` jako standard. Dopuszczalne tylko tymczasowo przy awarii DB invite store.

## Utworzenie zaproszenia

Na VPS:

```bash
cd /srv/Firma_morenatech.work_Jagoda

/srv/Firma_morenatech.work_Jagoda/.venv/bin/python scripts/mpbm_invites.py \
  --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db \
  add \
  --user-key basia \
  --workspace-key default \
  --created-by michal \
  --ttl-days 14 \
  --note "pierwszy test connectora"
```

Wynik zawiera pole:

```text
invite_code_SHOW_ONCE
```

Ten kod przekazujemy użytkownikowi bezpiecznym kanałem. Po zamknięciu terminala nie da się go odzyskać, bo baza ma tylko hash.

## Listowanie zaproszeń

```bash
/srv/Firma_morenatech.work_Jagoda/.venv/bin/python scripts/mpbm_invites.py \
  --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db \
  list
```

Lista pokazuje metadane, status i `code_hash_prefix`, ale nie pokazuje raw invite code.

Statusy:

```text
active   - kod nieużyty i ważny
used     - kod był użyty co najmniej raz
expired  - minął expires_at
revoked  - operator cofnął kod
```

Uwaga: aktualny mechanizm pozwala na ponowne użycie tego samego invite code, dopóki nie wygaśnie lub nie zostanie cofnięty. Jeśli chcemy zaproszenia jednorazowe, trzeba dodać politykę `single_use` i blokadę po `used_at`.

## Podgląd jednego wpisu

```bash
/srv/Firma_morenatech.work_Jagoda/.venv/bin/python scripts/mpbm_invites.py \
  --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db \
  show 123
```

## Cofnięcie zaproszenia

```bash
/srv/Firma_morenatech.work_Jagoda/.venv/bin/python scripts/mpbm_invites.py \
  --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db \
  revoke 123
```

Po cofnięciu `/oauth/authorize` odrzuci ten kod.

## Odnowienie zaproszenia

```bash
/srv/Firma_morenatech.work_Jagoda/.venv/bin/python scripts/mpbm_invites.py \
  --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db \
  renew 123 \
  --ttl-days 14
```

`renew` czyści `revoked_at` i ustawia nowy termin ważności.

## Usunięcie zaproszenia

```bash
/srv/Firma_morenatech.work_Jagoda/.venv/bin/python scripts/mpbm_invites.py \
  --db-path /srv/Firma_morenatech.work_Jagoda/data/jagoda_memory.db \
  delete 123
```

Preferuj `revoke` zamiast `delete`, bo `revoke` zostawia ślad operatorski. `delete` używać głównie do pomyłek przy tworzeniu.

## Migracja ze starego MPBM_INVITE_CODES

Aktualnie nie ma jeszcze bezpiecznego helpera `import-env`. Migracja ręczna:

1. Otwórz `/etc/jagoda-mcp.env` na VPS.
2. Dla każdego starego wpisu `KOD:user_key` utwórz nowe zaproszenie przez `scripts/mpbm_invites.py add`.
3. Przekaż użytkownikowi nowy kod, nie stary.
4. Usuń `MPBM_INVITE_CODES` z env albo zostaw puste.
5. Upewnij się, że `MPBM_ALLOW_UNINVITED_OAUTH=false`.
6. Zrestartuj usługę:

```bash
systemctl restart jagoda-mcp
```

Nie kopiuj starych raw kodów do dokumentacji ani do pamięci. Jeśli helper `import-env` zostanie dopisany później, ma importować tylko hash i nie drukować raw kodów.

## Test OAuth po utworzeniu zaproszenia

1. Użytkownik dodaje connector `https://jagoda.morenatech.pl/mcp/`.
2. Flow OAuth pokazuje formularz invite.
3. Użytkownik wpisuje kod.
4. Connector dostaje token OAuth.
5. Token claims powinny mieć `sub=user_key` z zaproszenia.
6. `list_memories` powinno mieć `scope_retrieval_active=true`.

## Testy lokalne

```bash
python -m pytest \
  tests/test_invite_store.py \
  tests/test_mpbm_invites_cli.py \
  tests/test_oauth_invite_store_flow.py \
  -q
```

Ostatni znany wynik po dodaniu testów CLI: `11 passed`.

## Checklist przed szerszym onboardingiem

```text
[ ] MPBM_ALLOW_UNINVITED_OAUTH=false
[ ] nowe zaproszenia tworzone przez scripts/mpbm_invites.py
[ ] MPBM_INVITE_CODES nie jest główną ścieżką
[ ] raw invite code nie jest w repo/logach/dokumentacji
[ ] test connectora nowego usera przeszedł
[ ] test izolacji: user A nie widzi private usera B
[ ] backup DB wykonany przed masowym onboardingiem
```
