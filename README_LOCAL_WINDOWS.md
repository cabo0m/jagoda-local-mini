# Jagoda Local Mini jako lusterko MPbM

To jest lokalne lusterko publicznego MPbM, przygotowane do uruchamiania na Windowsie i wystawiania przez ngrok.

Ta instrukcja prowadzi krok po kroku od pustego laptopa do działającego endpointu MCP. Idziemy jak wycieczka szkolna po muzeum: najpierw sprawdzamy bilety, potem sale po kolei, a do piwnicy z tokenami nikt nie schodzi bez opiekuna.

---

# 1. Co masz dostać na laptopie

Na laptop przenosisz cały katalog:

```text
jagoda-local-mini
```

Najlepiej, żeby po przeniesieniu leżał tutaj:

```bat
%USERPROFILE%\Documents\jagoda-local-mini
```

Przykład pełnej ścieżki:

```text
C:\Users\micha\Documents\jagoda-local-mini
```

W środku powinny być między innymi takie pliki:

```text
server_health.py
server_mpbm_core.py
server_core.py
run_local_mpbm.bat
run_ngrok.bat
.env.example
requirements.txt
```

Jeśli widzisz tylko pojedynczy plik README, to skopiowany został zły poziom katalogu. To tak, jakby na wycieczkę do muzeum zabrać sam bilet, ale bez muzeum.

---

# 2. Co musi być zainstalowane na Windowsie

Na laptopie muszą działać dwie komendy:

```bat
py --version
ngrok version
```

Otwórz `cmd` albo PowerShell i wpisz:

```bat
py --version
```

Oczekiwany wynik to coś w rodzaju:

```text
Python 3.11.x
```

Potem sprawdź ngrok:

```bat
ngrok version
```

Oczekiwany wynik to coś w rodzaju:

```text
ngrok version 3.x.x
```

Jeśli `py` nie działa, zainstaluj Python dla Windows i zaznacz Python Launcher. Jeśli `ngrok` nie działa, zainstaluj ngrok albo dodaj go do `PATH`.

Nie idziemy dalej, dopóki te dwie komendy nie działają. Bez nich drzwi do muzeum są namalowane na ścianie.

---

# 3. Wejście do katalogu projektu

Otwórz terminal i wpisz:

```bat
cd /d %USERPROFILE%\Documents\jagoda-local-mini
```

Sprawdź zawartość:

```bat
dir
```

Powinieneś zobaczyć między innymi:

```text
server_health.py
server_mpbm_core.py
run_local_mpbm.bat
run_ngrok.bat
.env.example
```

Jeśli nie widzisz tych plików, zatrzymaj się i sprawdź lokalizację katalogu.

---

# 4. Przygotowanie pliku `.env`

Plik `.env` trzyma konfigurację lokalnego serwera. Jeśli go jeszcze nie ma, utwórz go z przykładu:

```bat
copy .env.example .env
```

Otwórz go w Notatniku:

```bat
notepad .env
```

Na początku powinien wyglądać mniej więcej tak:

```text
ASSISTANT_ROOT=.
DB_PATH=./data/jagoda_memory.db
PUBLIC_BASE_URL=http://127.0.0.1:8015
MPBM_SECURITY_AUDIT_LOG=./data/mpbm_security_audit.jsonl
MPBM_PUBLIC_WORKSPACE_KEY=default
MPBM_PUBLIC_USER_KEY=michal
MPBM_PUBLIC_SCOPES=mcp:tools memories:read memories:write
MCP_STATIC_SUB=michal
MCP_BEARER_TOKEN=change-me-local-static-token
MCP_TOKEN_TTL_SECONDS=3600
MPBM_ALLOW_UNINVITED_OAUTH=false
PORT=8015
```

Na tym etapie zmień tylko jedną rzecz: `MCP_BEARER_TOKEN`.

W terminalu wygeneruj token:

```bat
py -c "import secrets; print('LOCAL_' + secrets.token_urlsafe(48))"
```

Dostaniesz długi tekst zaczynający się od `LOCAL_`. Wklej go do `.env` tak:

```text
MCP_BEARER_TOKEN=LOCAL_tutaj_wklej_wygenerowany_token
```

Zapisz plik.

Nie wklejaj prawdziwego tokena do czatu, maila, README ani commita. Token jest kluczem do drzwi, a nie magnesem na lodówkę.

---

# 5. Pierwsze uruchomienie lokalnego serwera

W terminalu, nadal w katalogu `jagoda-local-mini`, wpisz:

```bat
run_local_mpbm.bat
```

Co powinno się wydarzyć:

1. Skrypt sprawdzi, czy istnieje `.env`.
2. Utworzy środowisko `.venv`, jeśli go nie ma.
3. Zainstaluje paczki z `requirements.txt`.
4. Uruchomi `server_health.py`.
5. Serwer zacznie słuchać na `127.0.0.1:8015`.

Pierwsze uruchomienie może potrwać, bo Windows będzie budował małą maszynownię z paczek Pythona. Nie zamykaj tego okna. To okno jest teraz działającym serwerem.

Oczekujesz czegoś w stylu:

```text
Uvicorn running on http://127.0.0.1:8015
```

Jeśli pojawi się błąd, zatrzymaj się. Nie uruchamiaj ngroka, dopóki lokalny serwer nie działa.

---

# 6. Sprawdzenie, czy serwer żyje lokalnie

Otwórz drugie okno terminala. Wpisz:

```bat
curl http://127.0.0.1:8015/health
```

Dobry znak to odpowiedź JSON zawierająca:

```text
"status":"ok"
```

albo podobnie sformatowane:

```json
{
  "status": "ok",
  "service": "MPbM"
}
```

Potem sprawdź publiczny health MPbM:

```bat
curl http://127.0.0.1:8015/api/mpbm-health
```

Tutaj też szukamy:

```text
"status":"ok"
```

Jeśli oba health checki działają, lokalna sala główna jest otwarta.

---

# 7. Uruchomienie ngroka

W drugim oknie terminala przejdź do katalogu projektu:

```bat
cd /d %USERPROFILE%\Documents\jagoda-local-mini
```

Uruchom ngrok:

```bat
run_ngrok.bat
```

To odpala:

```bat
ngrok http 8015
```

Na ekranie ngroka znajdź adres `Forwarding`, na przykład:

```text
https://abc-123.ngrok-free.app
```

Skopiuj ten adres. To będzie publiczny adres lokalnego MPbM.

---

# 8. Wpisanie adresu ngroka do `.env`

Wróć do okna z lokalnym serwerem i zatrzymaj go:

```text
Ctrl+C
```

Otwórz `.env`:

```bat
notepad .env
```

Znajdź linię:

```text
PUBLIC_BASE_URL=http://127.0.0.1:8015
```

Zmień ją na adres z ngroka:

```text
PUBLIC_BASE_URL=https://abc-123.ngrok-free.app
```

Zapisz `.env`.

Uruchom serwer ponownie:

```bat
run_local_mpbm.bat
```

Ten restart jest ważny. Serwer czyta `.env` przy starcie. Bez restartu będzie dalej wierzył, że publiczny adres to localhost, czyli będzie rozdawał zwiedzającym mapę do własnej kieszeni.

---

# 9. Sprawdzenie endpointów przez ngrok

W drugim terminalu sprawdź metadata protected resource:

```bat
curl https://abc-123.ngrok-free.app/.well-known/oauth-protected-resource
```

W odpowiedzi powinno być coś w tym stylu:

```json
{
  "resource": "https://abc-123.ngrok-free.app/mcp/",
  "authorization_servers": [
    "https://abc-123.ngrok-free.app"
  ]
}
```

Potem sprawdź metadata authorization server:

```bat
curl https://abc-123.ngrok-free.app/.well-known/oauth-authorization-server
```

Jeśli oba endpointy odpowiadają JSON-em, ngrok i metadata działają.

---

# 10. Sprawdzenie ochrony `/mcp/`

Najpierw test bez tokena:

```bat
curl -i -X POST https://abc-123.ngrok-free.app/mcp/
```

Dobry wynik to odmowa, na przykład:

```text
HTTP/2 401
missing_token
```

To jest poprawne. Ochroniarz przy wejściu zauważył brak biletu.

Teraz test z tokenem z `.env`:

```bat
curl -i -X POST https://abc-123.ngrok-free.app/mcp/ ^
  -H "Authorization: Bearer TU_WKLEJ_TOKEN_Z_ENV" ^
  -H "Content-Type: application/json" ^
  -d "{}"
```

Interpretacja:

```text
401 missing_token       token nie został wysłany
401 invalid_token       token jest zły albo serwer nie został zrestartowany po zmianie .env
400 validation error    auth przepuścił, ale pusty JSON nie jest poprawnym requestem MCP
```

Dla pustego `{}` wynik `400 validation error` jest akceptowalny. To znaczy, że bramka Bearer działa, a narzeka już sam protokół MCP.

---

# 11. Podłączenie connectora

W konfiguracji connectora ustaw:

```text
Server URL:
https://abc-123.ngrok-free.app/mcp/

Authorization:
Bearer TU_WKLEJ_TOKEN_Z_ENV
```

Jeśli connector pyta o OAuth zamiast ręcznego tokena, powinien czytać metadata z endpointów `.well-known`. Na start najprostszy jest jednak statyczny Bearer token.

Po podłączeniu sprawdź listę narzędzi. Powinny być publiczne narzędzia MPbM:

```text
whoami
get_onboarding_status
save_initialization_profile
skip_initialization
restore_core
create_memory
find_memories
list_memories
get_memory
get_memory_links
recall_memory
```

Nie powinno być narzędzi administracyjnych typu:

```text
query_sql
run_powershell
write_file_text
delete_path
```

Jeśli je widzisz, podpięty jest zły serwer. To nie jest lokalny publiczny MPbM, tylko administracyjna piwnica z bezpiecznikami.

---

# 12. Pierwsza kolejność wywołań w connectorze

Po podłączeniu nie zaczynaj od `create_memory`. Najpierw sprawdzamy, kto wszedł do muzeum.

Kolejność:

```text
whoami
```

Potem:

```text
get_onboarding_status
```

Jeśli onboarding jest wymagany, wypełnij profil:

```text
save_initialization_profile
```

Jeśli świadomie chcesz pominąć onboarding:

```text
skip_initialization
```

Dopiero potem przywróć rdzeń:

```text
restore_core
```

I dopiero teraz używaj pamięci:

```text
create_memory
find_memories
list_memories
get_memory
get_memory_links
recall_memory
```

To jest docelowa kolejność. Najpierw tożsamość i mapa, potem szuflady ze wspomnieniami.

---

# 13. Typowe problemy

## Problem: `py` nie działa

Sprawdź:

```bat
py --version
```

Jeśli nie działa, zainstaluj Python dla Windows i zaznacz Python Launcher.

## Problem: `ngrok` nie działa

Sprawdź:

```bat
ngrok version
```

Jeśli nie działa, zainstaluj ngrok albo dodaj go do zmiennej `PATH`.

## Problem: `curl http://127.0.0.1:8015/health` nie działa

Sprawdź, czy okno z `run_local_mpbm.bat` nadal działa. Jeśli serwer się wysypał, błąd będzie widoczny właśnie tam.

## Problem: `401 invalid_token`

Najczęstsze przyczyny:

1. Token w connectorze nie jest taki sam jak `MCP_BEARER_TOKEN` w `.env`.
2. Po zmianie tokena nie zrestartowałeś `run_local_mpbm.bat`.
3. Przy kopiowaniu tokena złapała się spacja albo nowa linia.

## Problem: metadata pokazuje `127.0.0.1` zamiast ngroka

W `.env` nadal masz:

```text
PUBLIC_BASE_URL=http://127.0.0.1:8015
```

Zmień na adres ngroka i zrestartuj serwer.

## Problem: baza jest pusta

To normalne, jeśli nie przeniosłeś pliku bazy:

```text
data\jagoda_memory.db
```

Czysta baza jest OK do testów. Jeśli chcesz przenieść pamięć, przenieś też `data\jagoda_memory.db`.

## Problem: chcesz zacząć od zera

Zatrzymaj serwer i usuń bazę:

```bat
del data\jagoda_memory.db
```

Potem uruchom ponownie:

```bat
run_local_mpbm.bat
```

---

# 14. Czego nie wysyłać i nie commitować

Nie wrzucaj publicznie:

```text
.env
.static_token.local
data\*.db
data\*.jsonl
.venv\
__pycache__\
```

Kod można przenosić. Tokeny i prywatna baza to eksponaty za szkłem pancernym.

---

# 15. Najkrótsza ścieżka testowa

Dla szybkiego testu na nowym laptopie:

```bat
cd /d %USERPROFILE%\Documents\jagoda-local-mini
copy .env.example .env
notepad .env
run_local_mpbm.bat
```

W drugim oknie:

```bat
curl http://127.0.0.1:8015/health
curl http://127.0.0.1:8015/api/mpbm-health
run_ngrok.bat
```

Potem:

1. Skopiuj adres ngroka.
2. Wpisz go do `.env` jako `PUBLIC_BASE_URL`.
3. Zrestartuj `run_local_mpbm.bat`.
4. Sprawdź:

```bat
curl https://twoj-ngrok.ngrok-free.app/.well-known/oauth-protected-resource
```

Jeśli ten endpoint odpowiada JSON-em, lokalne lusterko MPbM jest gotowe do podłączenia connectora.
