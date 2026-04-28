# MPbM operator helper functions for PowerShell.
# Load from project root:
#   . .\scripts\mpbm-setup-aliases.ps1
#
# Raw static tokens are printed only by Invoke-MpbmTokenRotate -ShowOnce.

if (-not $env:MPBM_APP_DIR) { $env:MPBM_APP_DIR = (Get-Location).Path }
if (-not $env:MPBM_DB_PATH) {
  if ($env:DB_PATH) { $env:MPBM_DB_PATH = $env:DB_PATH }
  else { $env:MPBM_DB_PATH = Join-Path $env:MPBM_APP_DIR 'data\jagoda_memory.db' }
}
if (-not $env:MPBM_ENV_FILE) { $env:MPBM_ENV_FILE = '/etc/jagoda-mcp.env' }
if (-not $env:MPBM_TOKEN_FILE) { $env:MPBM_TOKEN_FILE = Join-Path $env:MPBM_APP_DIR '.static_token.local' }
if (-not $env:MPBM_PYTHON) { $env:MPBM_PYTHON = 'python' }

function Set-MpbmLocation { Set-Location $env:MPBM_APP_DIR }
function Get-MpbmHealth { curl.exe -s https://jagoda.morenatech.pl/api/mpbm-health }
function Test-MpbmDeploy { Push-Location $env:MPBM_APP_DIR; try { & $env:MPBM_PYTHON -m py_compile server_health.py server_mpbm_core.py server_core.py scripts/mpbm_user_admin.py scripts/mpbm_static_token_admin.py scripts/mpbm_invites.py } finally { Pop-Location } }

function Get-MpbmUsers { & $env:MPBM_PYTHON scripts/mpbm_user_admin.py --db-path $env:MPBM_DB_PATH list @args }
function Get-MpbmUser { param([Parameter(Mandatory=$true)][string]$UserKey) & $env:MPBM_PYTHON scripts/mpbm_user_admin.py --db-path $env:MPBM_DB_PATH show --user-key $UserKey }
function New-MpbmUser { param([Parameter(Mandatory=$true)][string]$UserKey, [string]$DisplayName, [string]$WorkspaceKey='default') & $env:MPBM_PYTHON scripts/mpbm_user_admin.py --db-path $env:MPBM_DB_PATH ensure --user-key $UserKey --display-name $DisplayName --workspace-key $WorkspaceKey }
function Disable-MpbmUser { param([Parameter(Mandatory=$true)][string]$UserKey) & $env:MPBM_PYTHON scripts/mpbm_user_admin.py --db-path $env:MPBM_DB_PATH deactivate --user-key $UserKey }

function Get-MpbmInvites { & $env:MPBM_PYTHON scripts/mpbm_invites.py --db-path $env:MPBM_DB_PATH list @args }
function New-MpbmInvite { param([Parameter(Mandatory=$true)][string]$UserKey, [string]$WorkspaceKey='default', [int]$TtlDays=14) & $env:MPBM_PYTHON scripts/mpbm_invites.py --db-path $env:MPBM_DB_PATH create --user-key $UserKey --workspace-key $WorkspaceKey --ttl-days $TtlDays --created-by operator }
function Revoke-MpbmInvite { param([Parameter(Mandatory=$true)][int]$Id) & $env:MPBM_PYTHON scripts/mpbm_invites.py --db-path $env:MPBM_DB_PATH revoke $Id }
function Renew-MpbmInvite { param([Parameter(Mandatory=$true)][int]$Id, [int]$TtlDays=14) & $env:MPBM_PYTHON scripts/mpbm_invites.py --db-path $env:MPBM_DB_PATH renew $Id --ttl-days $TtlDays }

function Get-MpbmTokenStatus { & $env:MPBM_PYTHON scripts/mpbm_static_token_admin.py --token-file $env:MPBM_TOKEN_FILE --env-file $env:MPBM_ENV_FILE status }
function Sync-MpbmTokenEnv { & $env:MPBM_PYTHON scripts/mpbm_static_token_admin.py --token-file $env:MPBM_TOKEN_FILE --env-file $env:MPBM_ENV_FILE sync-env }
function Invoke-MpbmTokenRotate { param([switch]$ShowOnce) if ($ShowOnce) { & $env:MPBM_PYTHON scripts/mpbm_static_token_admin.py --token-file $env:MPBM_TOKEN_FILE --env-file $env:MPBM_ENV_FILE rotate --sync-env --show-once } else { & $env:MPBM_PYTHON scripts/mpbm_static_token_admin.py --token-file $env:MPBM_TOKEN_FILE --env-file $env:MPBM_ENV_FILE rotate --sync-env } }

function New-MpbmUserInvite {
  param(
    [Parameter(Mandatory=$true)][string]$UserKey,
    [string]$DisplayName,
    [string]$WorkspaceKey='default',
    [int]$TtlDays=14
  )
  if (-not $DisplayName) { $DisplayName = $UserKey }
  & $env:MPBM_PYTHON scripts/mpbm_user_admin.py --db-path $env:MPBM_DB_PATH ensure --user-key $UserKey --display-name $DisplayName --workspace-key $WorkspaceKey | Out-Null
  & $env:MPBM_PYTHON scripts/mpbm_invites.py --db-path $env:MPBM_DB_PATH create --user-key $UserKey --workspace-key $WorkspaceKey --ttl-days $TtlDays --created-by operator
}
