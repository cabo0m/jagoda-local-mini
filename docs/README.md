# Dokumentacja projektu

Status: uporządkowane po wdrożeniu publicznego MPbM, invite OAuth/PKCE, izolacji użytkowników i trwałego OAuth token store.

## Najważniejsze wejścia

### Publiczny MPbM connector

Aktualny runbook produkcyjnego connectora:

- `docs/MPBM_PUBLIC_CONNECTOR_RUNBOOK.md`\n- `docs/MPBM_INVITE_DB_RUNBOOK.md`

Czytaj go, gdy pracujesz z:

- publicznym endpointem `https://jagoda.morenatech.pl/mcp/`,
- OAuth/PKCE,
- invite codes DB,
- persistent OAuth token store,\n- DB-backed invite store,
- Caddy / systemd / VPS,
- testami izolacji użytkowników `michal` / `basia`.

Starszy, węższy runbook invite OAuth został zarchiwizowany jako materiał historyczny:

- `docs/_archive/mpbm_legacy/MPBM_INVITE_OAUTH_RUNBOOK.md`

### MultiUser / scope-aware memory

Aktualna checklista operacyjna Stage 1:

- `docs/MULTIUSER_STAGE1_ROLLOUT_CHECKLIST.md`

Starsze plany i tasklisty MultiUserMemory zostały przeniesione do archiwum:

- `docs/_archive/multiuser_legacy/`

### MPbM: materiały produktowe

- `docs/mpbm/01_landing_page.md`
- `docs/mpbm/02_technical_one_pager.md`
- `docs/mpbm/03_product_document.md`

### MPbM: instalacja VPS

Aktualne kroki instalacyjne:

- `docs/mpbm/installation/01_ssh_access_bootstrap.md`
- `docs/mpbm/installation/02_app_directory_and_permissions.md`
- `docs/mpbm/installation/05_systemd_service.md`

Uwaga: aktualna produkcja używa `server_health.py`, `server_mpbm_core.py`, `oauth_token_store.py`, `Caddy`, usługi `jagoda-mcp` i env file `/etc/jagoda-mcp.env`. Nie wystawiać publicznie starego `server.py`.

## Cross-Project Knowledge Layer

Pakiet dokumentacji dla Cross-Project Knowledge Layer:

- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_INDEX.md`
- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_RUNBOOK.md`
- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OPERATIONS_CHECKLIST.md`
- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_ADMIN_GUIDE.md`
- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_FEATURE_FLAGS_RUNBOOK.md`

Co otworzyć najpierw:

- pełny przegląd: `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_INDEX.md`
- proces operatorski: `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_RUNBOOK.md`
- codzienna checklista: `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OPERATIONS_CHECKLIST.md`
- onboarding i decyzje operatorskie: `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_ADMIN_GUIDE.md`
- rollout, read-only i rollback flag: `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_FEATURE_FLAGS_RUNBOOK.md`

## Ownership i SLA

Główne dokumenty:

- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OWNERSHIP_SLA.md`
- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OVERDUE_ESCALATION_RUNBOOK.md`
- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_BULK_ACTIONS_RUNBOOK.md`
- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OWNER_CATALOG_GOVERNANCE.md`
- `docs/CROSS_PROJECT_KNOWLEDGE_LAYER_OWNER_CATALOG_HEALTH_REPAIR_RUNBOOK.md`

System ma już:

- ownership fields w `memories`,
- due dates dla review, rewalidacji, expired i duplicate queue,
- overdue queue,
- owner summary w dashboardzie,
- alerty jakości i eskalacji,
- recommended bulk actions.

## Zrozumienie sprzeczności

Dokumentacja dla Conflict Explainer:

- `docs/ZROZUMIENIE_SPRZECZNOSCI_STATUS_WDROZENIA.md`
- `docs/Zrozumienie_Sprzecznosci_Plan.md`

Czytaj najpierw status wdrożenia, jeśli chcesz zobaczyć, co już zostało dowiezione.

## Timeline

Dokumentacja timeline:

- `docs/TIMELINE_DATA_CONTRACT.md`
- `docs/TIMELINE_INVENTORY.md`
- `docs/TIMELINE_IMPLEMENTATION_PLAN.md`
- `docs/TIMELINE_V1_PLAN.md`

Część dokumentów timeline nadal opisuje historię migracji z dużego `server.py`. Traktować je jako dokumenty techniczno-historyczne, nie jako instrukcję publicznego MPbM.

## AI worker / Sandman / materiały poboczne

- `docs/AI_WORKER_PIPELINE.md`
- `docs/AI_WORKER_RUNNER.md`
- `docs/AI_PRESETS.md`
- `docs/SANDMAN_AGENT_V2.md`
- `docs/SANDMAN_V1_PLAN.md`

## Archiwum

Przestarzałe albo zdublowane dokumenty trafiają do:

- `docs/_archive/obsolete_installation_docs/`
- `docs/_archive/mpbm_legacy/`
- `docs/_archive/multiuser_legacy/`

Nie usuwać archiwum bez potrzeby. To nie są aktualne instrukcje operatorskie, ale mogą pomóc odtworzyć decyzje historyczne.

