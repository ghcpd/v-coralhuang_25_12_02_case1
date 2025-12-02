# Enhanced Outbox Forwarder Test Runner
# This script sets up the environment, seeds data, runs the forwarder, and reports results

param(
    [int]$SeedCount = 1000,
    [int]$TimeoutSeconds = 60,
    [switch]$MultiInstance,
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

Write-Host "=== Enhanced Outbox Forwarder Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Get script directory
$ScriptDir = $PSScriptRoot
$VenvDir = Join-Path $ScriptDir ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$VenvPip = Join-Path $VenvDir "Scripts\pip.exe"

# Step 1: Create virtual environment if it doesn't exist
if (-not (Test-Path $VenvDir)) {
    Write-Host "[1/6] Creating virtual environment..." -ForegroundColor Yellow
    python -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create virtual environment"
        exit 1
    }
    Write-Host "Virtual environment created successfully" -ForegroundColor Green
} else {
    Write-Host "[1/6] Virtual environment already exists" -ForegroundColor Green
}

# Step 2: Install dependencies
Write-Host "[2/6] Installing dependencies..." -ForegroundColor Yellow
& $VenvPip install --quiet --upgrade pip
& $VenvPip install --quiet -r (Join-Path $ScriptDir "requirements.txt")
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to install dependencies"
    exit 1
}
Write-Host "Dependencies installed successfully" -ForegroundColor Green

# Step 3: Clean data if requested
if ($Clean) {
    Write-Host "[3/6] Cleaning existing data..." -ForegroundColor Yellow
    & $VenvPython -m outbox.seed_data clear
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to clear data"
        exit 1
    }
    Write-Host "Data cleaned successfully" -ForegroundColor Green
} else {
    Write-Host "[3/6] Skipping data cleanup (use -Clean to clear)" -ForegroundColor Gray
}

# Step 4: Seed test data
Write-Host "[4/6] Seeding $SeedCount test records..." -ForegroundColor Yellow
& $VenvPython -m outbox.seed_data seed $SeedCount
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to seed data"
    exit 1
}
Write-Host "Data seeded successfully" -ForegroundColor Green

# Step 5: Run forwarder
Write-Host "[5/6] Running forwarder (timeout: ${TimeoutSeconds}s)..." -ForegroundColor Yellow
$StartTime = Get-Date

if ($MultiInstance) {
    Write-Host "Starting 2 forwarder instances concurrently..." -ForegroundColor Cyan
    
    # Start first instance in background
    $Job1 = Start-Job -ScriptBlock {
        param($VenvPython, $ScriptDir)
        Set-Location $ScriptDir
        & $VenvPython -m outbox.forwarder
    } -ArgumentList $VenvPython, $ScriptDir
    
    # Give first instance a head start
    Start-Sleep -Seconds 1
    
    # Start second instance in background
    $Job2 = Start-Job -ScriptBlock {
        param($VenvPython, $ScriptDir)
        Set-Location $ScriptDir
        & $VenvPython -m outbox.forwarder
    } -ArgumentList $VenvPython, $ScriptDir
    
    # Monitor progress
    $Elapsed = 0
    while ($Elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds 2
        $Elapsed = (Get-Date) - $StartTime | Select-Object -ExpandProperty TotalSeconds
        
        # Check if all messages are processed using simple status checker
        try {
            $StatusOutput = & $VenvPython -m outbox.check_status 2>$null
            if ($StatusOutput -match "^(\d+),(\d+),(\d+)$") {
                $Pending = [int]$Matches[1]
                $Locked = [int]$Matches[2]
                $Forwarded = [int]$Matches[3]
                
                if ($Pending -eq 0 -and $Locked -eq 0) {
                    Write-Host "All messages processed! (Forwarded: $Forwarded)" -ForegroundColor Green
                    break
                }
                Write-Host "Progress: Forwarded=$Forwarded, Pending=$Pending, Locked=$Locked | Elapsed: $([int]$Elapsed)s / ${TimeoutSeconds}s" -ForegroundColor Gray
            }
        } catch {
            Write-Host "Elapsed: $([int]$Elapsed)s / ${TimeoutSeconds}s" -ForegroundColor Gray
        }
    }
    
    # Stop both instances
    Write-Host "Stopping forwarder instances..." -ForegroundColor Yellow
    Stop-Job $Job1, $Job2 -ErrorAction SilentlyContinue
    Remove-Job $Job1, $Job2 -ErrorAction SilentlyContinue
    
} else {
    # Start single instance in background
    $Job = Start-Job -ScriptBlock {
        param($VenvPython, $ScriptDir)
        Set-Location $ScriptDir
        & $VenvPython -m outbox.forwarder
    } -ArgumentList $VenvPython, $ScriptDir
    
    # Monitor progress
    $Elapsed = 0
    $LastReport = 0
    while ($Elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds 2
        $Elapsed = (Get-Date) - $StartTime | Select-Object -ExpandProperty TotalSeconds
        
        # Check if all messages are processed using simple status checker
        try {
            $StatusOutput = & $VenvPython -m outbox.check_status 2>$null
            if ($StatusOutput -match "^(\d+),(\d+),(\d+)$") {
                $Pending = [int]$Matches[1]
                $Locked = [int]$Matches[2]
                $Forwarded = [int]$Matches[3]
                
                if ($Pending -eq 0 -and $Locked -eq 0) {
                    Write-Host "All messages processed! (Forwarded: $Forwarded)" -ForegroundColor Green
                    break
                }
                
                # Report progress every 5 seconds
                if ($Elapsed - $LastReport -ge 5) {
                    Write-Host "Progress: Forwarded=$Forwarded, Pending=$Pending, Locked=$Locked | Elapsed: $([int]$Elapsed)s / ${TimeoutSeconds}s" -ForegroundColor Gray
                    $LastReport = $Elapsed
                }
            }
        } catch {
            if ($Elapsed - $LastReport -ge 5) {
                Write-Host "Elapsed: $([int]$Elapsed)s / ${TimeoutSeconds}s" -ForegroundColor Gray
                $LastReport = $Elapsed
            }
        }
    }
    
    # Stop the job
    Write-Host "Stopping forwarder..." -ForegroundColor Yellow
    Stop-Job $Job -ErrorAction SilentlyContinue
    Remove-Job $Job -ErrorAction SilentlyContinue
}

$EndTime = Get-Date
$Duration = ($EndTime - $StartTime).TotalSeconds

Write-Host "Forwarder completed in $([math]::Round($Duration, 2))s" -ForegroundColor Green

# Step 6: Print summary
Write-Host ""
Write-Host "[6/6] Test Summary" -ForegroundColor Yellow
Write-Host "==================" -ForegroundColor Yellow

& $VenvPython -m outbox.seed_data stats

# Calculate performance metrics
$AvgLatency = if ($SeedCount -gt 0) { [math]::Round(($Duration * 1000) / $SeedCount, 2) } else { 0 }
Write-Host ""
Write-Host "Performance Metrics:" -ForegroundColor Cyan
Write-Host "  Total Duration: $([math]::Round($Duration, 2))s" -ForegroundColor White
Write-Host "  Avg Per-Record Latency: ${AvgLatency}ms" -ForegroundColor White
Write-Host "  Throughput: $([math]::Round($SeedCount / $Duration, 2)) records/sec" -ForegroundColor White

Write-Host ""
Write-Host "=== Test Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  - Check DLQ: python -m outbox.replay_dlq list"
Write-Host "  - Replay DLQ: python -m outbox.replay_dlq replay"
Write-Host "  - Run multi-instance test: .\run_tests.ps1 -MultiInstance"
Write-Host "  - Clean and retest: .\run_tests.ps1 -Clean -SeedCount 10000"
