# MultiUserMemory Stage 1 — Rollout Checklist

Checklist operacyjny dla wdrożenia MultiUserMemory Stage 1
(`0010_multiuser_identity_foundation`).

> **Stan implementacji:** ✅ Stage 1 zakończony — 377 testów green.
> Ten dokument służy jako przewodnik wdrożeniowy dla operatora.
> Checkboxy w sekcjach deployment celowo pozostają puste — odhacza je operator przy wdrożeniu na konkretnej instancji.

---

## Przed wdrożeniem

### Środowisko
- [ ] Wykonano backup bazy `data/jagoda_memory.db`
- [ ] Potwierdzono że serwer można zrestartować bez utraty danych

### Weryfikacja kodu

```bash
# Pełna suita regresyjna
pytest -q -m regression

# Migracje
pytest tests/test_db_migrations.py -v

# Testy multiuser
pytest tests/test_multiuser_memory.py -v          # 57 testów
pytest tests/test_sandman_scope_isolation.py -v   # 11 testów — izolacja scope w duplikatach
pytest tests/test_conflict_scope_isolation.py -v  # 11 testów — izolacja scope w konfliktach

# Wszystkie testy łącznie — oczekiwane: 377 passed
pytest tests/ -q
```

- [ ] `pytest -q -m regression` — wszystkie zielone
- [ ] `pytest tests/test_db_migrations.py` — wszystkie zielone, w tym `0010_multiuser_identity_foundation`
- [ ] `pytest tests/test_multiuser_memory.py` — 57 passed
- [ ] `pytest tests/test_sandman_scope_isolation.py` — 11 passed
- [ ] `pytest tests/test_conflict_scope_isolation.py` — 11 passed

---

## Wdrożenie (kolejność)

### Krok 1 — Migracja schematu

```bash
# Migracja uruchamia się automatycznie przy pierwszym połączeniu.
# Można wywołać ją ręcznie przez MCP:
apply_schema_migrations()
```

Migracja `0010_multiuser_identity_foundation` tworzy:

| Tabela / zmiana | Opis |
|---|---|
| `users` | Użytkownicy; seed: `system:legacy` |
| `workspaces` | Workspace'y; seed: `default` |
| `workspace_memberships` | Członkostwo `system:legacy → default` (owner) |
| `memories` +8 kolumn | `owner_user_id`, `workspace_id`, `visibility_scope`, `access_role_min`, `created_by_user_id`, `last_modified_by_user_id`, `sharing_policy`, `subject_user_id` |
| `memory_links` +3 kolumny | `workspace_id`, `visibility_scope`, `created_by_user_id` |
| `timeline_events` +4 kolumny | `actor_user_id`, `workspace_id`, `actor_type`, `subject_user_id` |

> **Uwaga:** Po migracji 0010 serwer automatycznie zastosuje też 0011–0013
> (`scope_aware_maintenance`, `priority_and_sla_policies`, `escalation_history`).
> Nie wymagają one osobnej akcji — są addytywne i backward-compatible.

### Krok 2 — Walidacja post-migracyjna

```
validate_migration_0010()
```

Sprawdź:
- [ ] `status == "clean"`
- [ ] `memories_missing_workspace == 0`
- [ ] `memories_missing_scope == 0`
- [ ] `private_without_owner == 0`
- [ ] `links_missing_workspace == 0`
- [ ] `project_scope_without_project_key == 0`
- [ ] `workspace_scope_without_workspace_id == 0`
- [ ] `multiuser_flags.multiuser_identity_enabled.is_enabled == true`
- [ ] `multiuser_flags.multiuser_scope_retrieval_enabled.is_enabled == true`
- [ ] `multiuser_flags.multiuser_timeline_actor_enabled.is_enabled == true`

Jeśli `status == "needs_attention"` — sprawdź `red_flags` i przejdź do sekcji
[Troubleshooting](#troubleshooting).

### Krok 3 — Smoke test

```
# Sprawdź get_workspace_info
get_workspace_info(workspace_key="default")
# Oczekiwano: member_count >= 1, members zawiera system:legacy

# Utwórz testową prywatną pamięć
create_private_memory(
    content="[ROLLOUT TEST] Prywatna pamięć testowa",
    memory_type="personal_note",
    owner_user_key="system:legacy",
    summary_short="rollout test private"
)
# Oczekiwano: status == "created", visibility_scope == "private", owner_user_id != null

# Utwórz testową pamięć workspace
create_workspace_memory(
    content="[ROLLOUT TEST] Pamięć workspace testowa",
    memory_type="fact",
    summary_short="rollout test workspace"
)
# Oczekiwano: status == "created", visibility_scope == "workspace"

# Sprawdź że list_memories_for_user działa
list_memories_for_user(user_key="system:legacy", limit=5)
# Oczekiwano: scope_retrieval_active == true, count >= 1
```

- [ ] `get_workspace_info` zwraca `member_count >= 1`
- [ ] `create_private_memory` zwraca `visibility_scope == "private"`
- [ ] `create_workspace_memory` zwraca `visibility_scope == "workspace"`
- [ ] `list_memories_for_user` zwraca `scope_retrieval_active == true`

### Krok 4 — Weryfikacja izolacji scope

```
# Utwórz dwóch testowych userów
# (lub użyj istniejących kluczy — system:legacy i drugiego usera)

create_private_memory(
    content="[ROLLOUT TEST] Prywatna pamięć usera A",
    memory_type="personal_note",
    owner_user_key="system:legacy",
    summary_short="rollout test isolation"
)
# Zapisz zwrócone memory.id jako ID_A

create_private_memory(
    content="[ROLLOUT TEST] Prywatna pamięć usera B",
    memory_type="personal_note",
    owner_user_key="<inny_user_key>",
    summary_short="rollout test isolation"
)
# Zapisz zwrócone memory.id jako ID_B

# Sprawdź że user A NIE widzi pamięci usera B
list_memories_for_user(user_key="system:legacy", limit=50)
# Oczekiwano: ID_B nie pojawia się w items
```

- [ ] Pamięć usera B nie pojawia się w retrievalu usera A

### Krok 5 — Weryfikacja starych narzędzi (backward compat)

```
# Legacy list_memories nadal działa globalnie
list_memories(limit=5)
# Oczekiwano: count >= 1, brak klucza scope_retrieval_active

# Legacy create_memory nadal działa
create_memory(content="[ROLLOUT TEST] Legacy test", memory_type="fact")
# Oczekiwano: status == "created", workspace_id != null, visibility_scope != null
```

- [ ] `list_memories` działa bez zmian
- [ ] `create_memory` zwraca `workspace_id != null` i `visibility_scope != null`

### Krok 6 — Sprzątanie po smoke testach

```sql
-- Usuń rekordy testowe (jeśli baza produkcyjna)
DELETE FROM memories
WHERE content LIKE '[ROLLOUT TEST]%';
```

- [ ] Testowe rekordy usunięte z bazy

---

## Feature Flags

Wszystkie flagi domyślnie włączone po migracji (`is_enabled=1, rollout_mode='all'`).

| Flag key | Domyślnie | Steruje |
|---|---|---|
| `multiuser_identity_enabled` | on | `create_private/project/workspace_memory` |
| `multiuser_scope_retrieval_enabled` | on | Filtr scope w `list_memories`, `find_memories`, `list_memories_for_user` |
| `multiuser_timeline_actor_enabled` | on | `actor_user_id` / `workspace_id` w `timeline_events` |

### Wyłączanie (rollback runtime, bez migracji)

```python
# Przez MCP
set_feature_flag(name="multiuser_scope_retrieval_enabled", enabled=False)
set_feature_flag(name="multiuser_identity_enabled", enabled=False)
```

```sql
-- Alternatywnie: bezpośrednio SQL
UPDATE feature_flags SET is_enabled = 0 WHERE flag_key = 'multiuser_scope_retrieval_enabled';
UPDATE feature_flags SET is_enabled = 0 WHERE flag_key = 'multiuser_identity_enabled';
```

---

## Rollback (pełny)

Stage 1 nie usuwa żadnych kolumn — rollback schematu nie jest potrzebny.

Jeśli chcesz wyłączyć MultiUser funkcjonalność bez cofania migracji:

1. Wyłącz wszystkie trzy flagi (patrz wyżej)
2. Zrestartuj serwer
3. Nowe narzędzia (`create_private_memory` itp.) zwrócą `status: disabled`
4. Stare narzędzia (`list_memories`, `create_memory`) działają jak przed Stage 1

---

## Troubleshooting

### `memories_missing_workspace > 0`

Były rekordy wstawione PRZED migracją bez `workspace_id`. Napraw:

```sql
UPDATE memories
SET workspace_id = (SELECT id FROM workspaces WHERE workspace_key = 'default')
WHERE workspace_id IS NULL;
```

### `private_without_owner > 0`

Prywatne rekordy bez `owner_user_id`. Napraw:

```sql
UPDATE memories
SET owner_user_id = (SELECT id FROM users WHERE external_user_key = 'system:legacy')
WHERE visibility_scope = 'private' AND owner_user_id IS NULL;
```

### `project_scope_without_project_key > 0`

Rekordy z `visibility_scope='project'` ale bez `project_key`. Napraw (zmień scope na workspace):

```sql
UPDATE memories
SET visibility_scope = 'workspace'
WHERE visibility_scope = 'project'
  AND (project_key IS NULL OR TRIM(project_key) = '');
```

### `links_missing_workspace > 0`

Linki bez `workspace_id`. Napraw (backfill z from_memory):

```sql
UPDATE memory_links
SET workspace_id = (
    SELECT m.workspace_id FROM memories m WHERE m.id = memory_links.from_memory_id
)
WHERE workspace_id IS NULL;
```

### `create_private_memory` zwraca `status: disabled`

Flaga `multiuser_identity_enabled` jest wyłączona. Włącz:

```python
set_feature_flag(name="multiuser_identity_enabled", enabled=True)
```

### `list_memories_for_user` nie filtruje po scope

Flaga `multiuser_scope_retrieval_enabled` jest wyłączona. Włącz:

```python
set_feature_flag(name="multiuser_scope_retrieval_enabled", enabled=True)
```

---

## Nowe narzędzia MCP (Stage 1)

### Zarządzanie pamięcią

| Narzędzie | Opis |
|---|---|
| `create_private_memory` | Tworzy prywatne wspomnienie przypisane do użytkownika |
| `create_project_memory` | Tworzy wspomnienie projektowe (widoczne w workspace i projekcie) |
| `create_workspace_memory` | Tworzy wspomnienie workspace-level (widoczne wszystkim w workspace) |
| `list_memories_for_user` | Scope-aware listing dla konkretnego użytkownika |

### Diagnostyka i administracja

| Narzędzie | Opis |
|---|---|
| `get_workspace_info` | Info o workspace: członkowie, role, statystyki scope |
| `validate_migration_0010` | Raport walidacyjny post-migracyjny (scope coverage, red flags) |

### Legacy narzędzia (rozszerzone o scope)

| Narzędzie | Nowe parametry | Opis |
|---|---|---|
| `list_memories` | `user_key`, `workspace_key` | Globalny listing z opcjonalnym filtrem scope-aware |
| `find_memories` | `user_key`, `workspace_key` | Wyszukiwanie z opcjonalnym filtrem scope-aware |
| `create_memory` | — | Automatycznie ustawia `workspace_id` i `visibility_scope` |

---

## Zakres testów pokrywających Stage 1

| Plik testowy | Liczba testów | Zakres |
|---|---|---|
| `tests/test_multiuser_memory.py` | 57 | Pełne scenariusze multiuser: create, retrieval, scope isolation |
| `tests/test_sandman_scope_isolation.py` | 11 | SQL-level scope isolation w wykrywaniu duplikatów |
| `tests/test_conflict_scope_isolation.py` | 11 | SQL-level scope isolation w wykrywaniu konfliktów |
| `tests/test_db_migrations.py` | ~8 | Migracja 0010 w `EXPECTED_VERSIONS`, struktura tabel |
| `tests/test_memory_logic.py` | 354+ | Regresja single-user flow po migracji |

**Łącznie: 377 testów green** (stan: 2026-04-25)
