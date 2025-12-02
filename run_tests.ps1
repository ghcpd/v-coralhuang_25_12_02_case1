$ErrorActionPreference = "Stop"

# Create a venv if missing
if (-not (Test-Path -Path .venv)) {
    python -m venv .venv
}

# Activate and install requirements
Start-Process -FilePath ".\.venv\Scripts\python.exe" -ArgumentList '-m','pip','install','-U','pip' -WorkingDirectory (Get-Location) -Wait -NoNewWindow
Start-Process -FilePath ".\.venv\Scripts\python.exe" -ArgumentList '-m','pip','install','-r','requirements.txt' -WorkingDirectory (Get-Location) -Wait -NoNewWindow

# Configure env for faster test run (small backoff, short timeouts)
$env:OUTBOX_BASE_BACKOFF_SECONDS = "1"
$env:OUTBOX_MAX_BACKOFF_SECONDS = "8"
$env:OUTBOX_JITTER_SECONDS = "1"
$env:OUTBOX_MAX_ATTEMPTS = "3"
$env:OUTBOX_BATCH_SIZE = "200"
$env:OUTBOX_WORKERS = "6"

Write-Host "Seeding and running forwarder test (this may take ~10-30s depending on counts)"
Start-Process -FilePath ".\.venv\Scripts\python.exe" -ArgumentList '.\scripts\test_runner.py','--count','800','--fail-rate','0.03','--workers','3' -WorkingDirectory (Get-Location) -Wait -NoNewWindow
