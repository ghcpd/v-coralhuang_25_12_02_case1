$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not (Test-Path ".venv")) {
    Write-Host "Creating venv..."
    python -m venv .venv
}
. .\.venv\Scripts\Activate.ps1
Write-Host "Installing requirements..."
pip install -r requirements.txt | Out-Null

Write-Host "Seeding dataset and running forwarder (single instance)..."
python -m outbox.test_runner --seed 1000 --instances 1

Write-Host "Running forwarder with 2 instances to validate locking..."
python -m outbox.test_runner --seed 1000 --instances 2
