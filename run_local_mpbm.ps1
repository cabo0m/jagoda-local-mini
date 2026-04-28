$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

if (-not (Test-Path '.env')) {
    Copy-Item '.env.example' '.env'
}

Get-Content '.env' | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith('#') -or -not $line.Contains('=')) { return }
    $key, $value = $line.Split('=', 2)
    [Environment]::SetEnvironmentVariable($key.Trim(), $value.Trim(), 'Process')
}

if (-not (Test-Path '.venv')) {
    py -m venv .venv
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
& .\.venv\Scripts\python.exe server_health.py
