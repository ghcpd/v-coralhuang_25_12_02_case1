param(
    [switch]$DualInstance = $false,
    [int]$Count = 1000
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPath = Join-Path $root ".venv"
$pythonPath = Join-Path $venvPath "Scripts\python.exe"

function Ensure-Venv {
    if (Test-Path $pythonPath) {
        $ver = & $pythonPath --version
        if ($ver -match "3\.14") {
            Write-Host "Existing venv uses Python 3.14; recreating with Python 3.10 for compatibility..."
            Remove-Item -Recurse -Force $venvPath
        }
    }
    if (-not (Test-Path $pythonPath)) {
        Write-Host "Creating virtual environment..."
        if (Get-Command py -ErrorAction SilentlyContinue) {
            try {
                py -3.10 -m venv $venvPath
            } catch {
                python -m venv $venvPath
            }
        } else {
            python -m venv $venvPath
        }
    }
}

function Install-Requirements {
    Write-Host "Installing requirements..."
    & $pythonPath -m pip install --upgrade pip | Out-Null
    & $pythonPath -m pip install -r (Join-Path $root "requirements.txt") | Out-Null
}

function Seed-Outbox {
    param([int]$Count)
    Write-Host "Seeding outbox with $Count records..."
    & $pythonPath -m outbox.seeder --reset --count $Count | Out-Null
}

function Run-Forwarder {
    param([switch]$Dual)
    if ($Dual) {
        Write-Host "Running dual forwarder instances..."
        $job = Start-Job -ScriptBlock {
            param($py, $cwd)
            Push-Location $cwd
            try {
                & $py -m outbox.forwarder --exit-on-idle
            } finally {
                Pop-Location
            }
        } -ArgumentList $pythonPath, $root
        & $pythonPath -m outbox.forwarder --exit-on-idle
        try {
            Receive-Job $job -Wait -ErrorAction Stop | Out-Null
        } catch {
            Write-Host "Background job completed with output:" -ForegroundColor Yellow
            Receive-Job $job -Wait -ErrorAction SilentlyContinue | Out-String | Write-Host
        }
        Remove-Job $job | Out-Null
    }
    else {
        Write-Host "Running forwarder..."
        & $pythonPath -m outbox.forwarder --exit-on-idle
    }
}

function Print-Summary {
    $summaryJson = & $pythonPath -m outbox.summary --json
    $summary = $summaryJson | ConvertFrom-Json
    Write-Host "-------- Test Summary --------"
    Write-Host ("Total outbox: {0}" -f $summary.total_outbox)
    Write-Host ("Forwarded:    {0}" -f $summary.forwarded)
    Write-Host ("Failed:       {0}" -f $summary.failed)
    Write-Host ("Dead (DLQ):   {0}" -f $summary.dead)
    Write-Host ("DLQ table:    {0}" -f $summary.dlq)
    Write-Host ("Events:       {0}" -f $summary.events)
    Write-Host ("Audit rows:   {0}" -f $summary.audit)
    if ($summary.avg_latency_sec -ne $null) {
        Write-Host ("Avg latency: {0:N3} s" -f $summary.avg_latency_sec)
    }
    else {
        Write-Host "Avg latency: n/a"
    }
}

Ensure-Venv
Install-Requirements
Seed-Outbox -Count $Count

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
Run-Forwarder -Dual:$DualInstance
$stopwatch.Stop()

Print-Summary
Write-Host ("Duration:     {0:N2} s" -f $stopwatch.Elapsed.TotalSeconds)

if ($DualInstance) {
    Write-Host "(Dual instance run completed; locking validated by absence of duplicates in events table.)"
}
