# Quick Reference Guide

## 🚀 Quick Start (TL;DR)

```powershell
# One command to test everything:
.\run_tests.ps1
```

## 📋 Common Commands

### Testing
```powershell
# Basic test (1000 records)
.\run_tests.ps1

# High volume (10,000 records)
.\run_tests.ps1 -SeedCount 10000

# Multi-instance test
.\run_tests.ps1 -MultiInstance

# Clean and retest
.\run_tests.ps1 -Clean -SeedCount 5000
```

### Manual Setup
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Data Management
```powershell
# Seed data
python -m outbox.seed_data seed 1000

# View stats
python -m outbox.seed_data stats

# Clear all data
python -m outbox.seed_data clear

# Validate results
python -m outbox.validate
```

### Run Forwarder
```powershell
# Single instance
python -m outbox.forwarder

# Multiple instances (separate terminals)
python -m outbox.forwarder  # Terminal 1
python -m outbox.forwarder  # Terminal 2
```

### DLQ Operations
```powershell
# List DLQ messages
python -m outbox.replay_dlq list

# Replay all
python -m outbox.replay_dlq replay

# Replay 10 messages
python -m outbox.replay_dlq replay 10

# Clear DLQ
python -m outbox.replay_dlq clear
```

## 📊 Key Files

| File | Purpose |
|------|---------|
| `run_tests.ps1` | One-click test runner |
| `requirements.txt` | Python dependencies |
| `README.md` | Full documentation |
| `outbox/forwarder.py` | Main forwarder logic |
| `outbox/models.py` | Database models |
| `outbox/seed_data.py` | Test data seeder |
| `outbox/replay_dlq.py` | DLQ management |
| `outbox/validate.py` | Acceptance criteria validator |

## ⚙️ Configuration

Edit `outbox/forwarder.py` to tune:

```python
BATCH_SIZE = 100              # Records per scan
CONCURRENCY_WORKERS = 10      # Parallel workers
MAX_ATTEMPTS = 5              # Before DLQ
SCAN_INTERVAL_SECONDS = 0.5   # Scan frequency
```

## 🎯 Success Indicators

After running tests, you should see:
- ✅ All outbox records forwarded (status='forwarded')
- ✅ Equal number of events created
- ✅ Average latency < 3s
- ✅ No duplicate events
- ✅ Audit entries for all transitions
- ✅ DLQ entries for max-attempt failures

## 🐛 Troubleshooting

### "Module not found"
```powershell
# Ensure venv is activated
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### "Database locked"
```powershell
# Stop all forwarder instances
# Delete outbox.db and restart
```

### Forwarder not processing
```powershell
# Check if records are in 'new' status
python -m outbox.seed_data stats

# Verify forwarder is running
# Check logs for errors
```

## 📈 Performance Tuning

### For Higher Throughput
- ↑ `CONCURRENCY_WORKERS` (10 → 20)
- ↑ `BATCH_SIZE` (100 → 500)
- ↓ `SCAN_INTERVAL_SECONDS` (0.5 → 0.1)

### For Lower Latency
- ↓ `SCAN_INTERVAL_SECONDS` (0.5 → 0.1)
- ↓ `BATCH_SIZE` (100 → 50)

### For Better Reliability
- ↑ `MAX_ATTEMPTS` (5 → 10)
- ↑ `LOCK_TIMEOUT_SECONDS` (30 → 60)
- ↓ `BASE_RETRY_DELAY_SECONDS` (2 → 1)

## 🔍 Monitoring Queries

```sql
-- Current status distribution
SELECT status, COUNT(*) FROM event_outbox GROUP BY status;

-- Recent forwards
SELECT * FROM event_outbox_audit WHERE event_type='forwarded' ORDER BY created_at DESC LIMIT 10;

-- Retry history
SELECT outbox_id, event_type, attempt, created_at FROM event_outbox_audit WHERE outbox_id='YOUR_ID' ORDER BY created_at;

-- DLQ entries
SELECT * FROM outbox_dlq;

-- Performance check
SELECT AVG(julianday(updated_at) - julianday(created_at)) * 86400 as avg_latency_seconds FROM event_outbox WHERE status='forwarded';
```

## 🎓 Architecture Overview

```
Input → Outbox (new) → Lock (locked) → Process → Success → Events (forwarded)
                                               ↓
                                           Failure → Retry (failed+next_retry_at)
                                               ↓
                                         Max Attempts → DLQ (dead)
```

## 🆘 Support

For issues or questions:
1. Check `README.md` for detailed documentation
2. Run `python -m outbox.validate` to check system state
3. Check forwarder logs for error details
4. Review `IMPLEMENTATION_SUMMARY.md` for design details

---

**Happy Event Forwarding! 🚀**
