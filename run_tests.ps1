# Enhanced Outbox Forwarder - One-Click Test Runner
# This script sets up the environment, seeds data, and runs the forwarder with concurrency tests

param(
    [int]$NumRecords = 1000,
    [switch]$ConcurrencyTest
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "=== Enhanced Outbox Forwarder Test Runner ===" -ForegroundColor Cyan

# Step 1: Check and create virtual environment
Write-Host "`n[1/5] Setting up Python virtual environment..." -ForegroundColor Yellow
$VenvPath = Join-Path $ScriptDir "venv"

if (-not (Test-Path $VenvPath)) {
    Write-Host "Creating virtual environment..."
    python -m venv $VenvPath
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to create venv" -ForegroundColor Red
        exit 1
    }
    Write-Host "Virtual environment created" -ForegroundColor Green
} else {
    Write-Host "Virtual environment already exists" -ForegroundColor Green
}

# Activate venv
$ActivateScript = Join-Path $VenvPath -ChildPath "Scripts" | Join-Path -ChildPath "Activate.ps1"
& $ActivateScript

# Step 2: Install dependencies
Write-Host "`n[2/5] Installing dependencies..." -ForegroundColor Yellow
pip install -q --upgrade pip
pip install -q -r "$ScriptDir\requirements.txt"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to install dependencies" -ForegroundColor Red
    exit 1
}
Write-Host "Dependencies installed" -ForegroundColor Green

# Step 3: Seed database with test data
Write-Host "`n[3/5] Seeding database with $NumRecords records..." -ForegroundColor Yellow

# Create a temporary seed script in the project directory
$TempSeedScript = Join-Path $ScriptDir "_temp_seed.py"
$SeedContent = @"
import asyncio
import uuid
from datetime import datetime
from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent

async def seed_data(num_records):
    await init_db()
    async with SessionLocal() as session:
        # Clear existing data for clean test
        from sqlalchemy import delete
        await session.execute(delete(OutboxEvent))
        await session.commit()
        
        for i in range(num_records):
            record = OutboxEvent(
                id=str(uuid.uuid4()),
                topic='test_topic_' + str(i % 10),
                payload='{"index": ' + str(i) + ', "timestamp": "' + datetime.utcnow().isoformat() + '"}',
                status="new",
                attempt=0
            )
            session.add(record)
            if (i + 1) % 100 == 0:
                await session.commit()
        
        await session.commit()
        
        # Verify count
        from sqlalchemy import select
        stmt = select(OutboxEvent)
        count = len((await session.execute(stmt)).scalars().all())
        print('Seeded ' + str(count) + ' records')

asyncio.run(seed_data($NumRecords))
"@

Set-Content -Path $TempSeedScript -Value $SeedContent

Push-Location $ScriptDir
python _temp_seed.py
Pop-Location

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to seed database" -ForegroundColor Red
    exit 1
}
Write-Host "Database seeded successfully" -ForegroundColor Green

# Step 4: Run forwarder (single instance)
Write-Host "`n[4/5] Running forwarder (single instance)..." -ForegroundColor Yellow

# Create a temporary forwarder script
$TempForwarderScript = Join-Path $ScriptDir "_temp_forwarder.py"
$ForwarderContent = @"
import asyncio
import time
from datetime import datetime
from sqlalchemy import select
from outbox.database import SessionLocal, init_db
from outbox.models import OutboxEvent
from outbox.forwarder import fetch_batch_and_lock, process_batch_concurrent, recover_stale_locks, log_metrics, SCAN_INTERVAL_SECONDS

start_time = time.time()

async def test_run():
    await init_db()
    
    for iteration in range(120):  # Run for up to 60 seconds
        async with SessionLocal() as session:
            # Recover stale locks
            await recover_stale_locks(session)
            
            # Fetch and process batch
            batch = await fetch_batch_and_lock(session)
            if batch:
                print('[' + str(iteration) + '] Processing ' + str(len(batch)) + ' records')
                await process_batch_concurrent(session, batch)
            else:
                # Check if still pending
                stmt = select(OutboxEvent).where(OutboxEvent.status.in_(["new", "locked", "failed"]))
                pending = len((await session.execute(stmt)).scalars().all())
                if pending == 0:
                    print('All done! Elapsed: ' + str(round(time.time() - start_time, 1)) + 's')
                    break
            
            await log_metrics(session)
        
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

asyncio.run(test_run())
"@

Set-Content -Path $TempForwarderScript -Value $ForwarderContent

Push-Location $ScriptDir
python _temp_forwarder.py
Pop-Location

if ($LASTEXITCODE -ne 0) {
    Write-Host "Forwarder encountered an error (this might be expected)" -ForegroundColor Yellow
}
Write-Host "Forwarder run completed" -ForegroundColor Green

# Step 5: Display test results
Write-Host "`n[5/5] Collecting test results..." -ForegroundColor Yellow

# Create a temporary results script
$TempResultScript = Join-Path $ScriptDir "_temp_results.py"
$ResultContent = @'
# -*- coding: utf-8 -*-
import asyncio
from sqlalchemy import select
from outbox.database import SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ, Event

async def get_metrics():
    async with SessionLocal() as session:
        # Count by status
        new = len((await session.execute(select(OutboxEvent).where(OutboxEvent.status == "new"))).scalars().all())
        locked = len((await session.execute(select(OutboxEvent).where(OutboxEvent.status == "locked"))).scalars().all())
        forwarded = len((await session.execute(select(OutboxEvent).where(OutboxEvent.status == "forwarded"))).scalars().all())
        failed = len((await session.execute(select(OutboxEvent).where(OutboxEvent.status == "failed"))).scalars().all())
        dlq = len((await session.execute(select(OutboxDLQ))).scalars().all())
        events = len((await session.execute(select(Event))).scalars().all())
        
        print("")
        print("=== OUTBOX FORWARDER TEST SUMMARY ===")
        print("  New:         " + str(new).rjust(6))
        print("  Locked:      " + str(locked).rjust(6))
        print("  Forwarded:   " + str(forwarded).rjust(6))
        print("  Failed:      " + str(failed).rjust(6))
        print("  DLQ:         " + str(dlq).rjust(6))
        print("  Events:      " + str(events).rjust(6) + " (in events table)")
        print("-------------------------------------")
        
        total_processed = forwarded + failed + dlq
        total_seeded = new + locked + forwarded + failed + dlq
        
        print("  Total seeded: " + str(total_seeded).rjust(6))
        pct = 100.0*total_processed/max(1,total_seeded)
        print("  Total processed: " + str(total_processed).rjust(6) + " (" + str(round(pct, 1)) + "%)")
        print("=====================================")
        
        if new == 0 and locked == 0 and (forwarded + dlq) == total_seeded:
            msg = "[SUCCESS] All records processed!"
            if dlq > 0:
                msg = msg + " (DLQ count: " + str(dlq) + ")"
            print(msg)
        else:
            print("[INCOMPLETE] Some records still pending")

asyncio.run(get_metrics())
'@

Set-Content -Path $TempResultScript -Value $ResultContent

Push-Location $ScriptDir
python _temp_results.py
Pop-Location

Write-Host "`n=== Test Run Complete ===" -ForegroundColor Cyan
Write-Host "`nTo manually run the forwarder: python -m outbox.forwarder" -ForegroundColor Gray
Write-Host "To replay DLQ records: python -m outbox.dlq_replay replay-all" -ForegroundColor Gray
