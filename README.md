# Health Vault

A passive, self-healing pipeline that automatically ingests Apple Health data from iCloud Drive into a local PostgreSQL database. No Tailscale, no port forwarding, no manual exports.

## Architecture

```
iPhone (Health Auto Export) → iCloud Drive → Mac Mini (watchdog) → PostgreSQL 17
```

1. **Health Auto Export** (iOS app) runs on a schedule, exporting health metrics as JSON to an iCloud Drive folder
2. **iCloud Drive** syncs the files to your Mac Mini automatically via macOS's native daemon
3. **Health Vault** (this project) watches the iCloud folder, parses new JSON files, inserts the data into PostgreSQL, and archives processed files
4. **launchd** keeps the watcher running persistently — auto-starts at boot, auto-restarts on crash

## Prerequisites

- macOS with iCloud Drive enabled (same Apple ID as your iPhone)
- PostgreSQL 17 running locally (`brew services start postgresql@17`)
- Python 3.13 (`/opt/homebrew/bin/python3.13`)
- [Health Auto Export](https://apps.apple.com/us/app/health-auto-export-json-csv/id1115567069) installed on your iPhone

## Quick Start

### 1. Install dependencies

```bash
/opt/homebrew/bin/python3.13 -m pip install -r requirements.txt
```

### 2. Create the database

```bash
/opt/homebrew/bin/python3.13 setup_db.py
```

### 3. Run manually (to test)

```bash
/opt/homebrew/bin/python3.13 main.py
```

### 4. Install as a persistent service

```bash
# Copy the plist to LaunchAgents
cp com.ashtoncoghlan.health-vault.plist ~/Library/LaunchAgents/

# Load the agent
launchctl load ~/Library/LaunchAgents/com.ashtoncoghlan.health-vault.plist

# Verify it's running
launchctl list | grep health-vault
```

### 5. Configure Health Auto Export (iOS)

1. Open **Health Auto Export** on your iPhone
2. Grant HealthKit read access for your desired metrics:
   - Heart rate, resting heart rate, HRV
   - Steps, distance, active energy, basal energy
   - Sleep stages (REM, deep, core, awake)
   - Blood oxygen (SpO2), respiratory rate
   - Workouts (type, duration, calories)
3. Enable **Background App Refresh** in iOS Settings → General
4. Create an **Automation**:
   - Frequency: Every 1–6 hours
   - Format: **JSON**
   - Destination: **iCloud Drive** → `health_vault/inbox/`

## File Flow

```
iCloud Drive/health_vault/inbox/         ← New exports land here
                    ↓
              Health Vault              ← Parses, deduplicates, inserts
                    ↓
iCloud Drive/health_vault/archive/       ← Processed files moved here
```

## Database Schema

### `health_metrics` — Primary data table

| Column | Type | Description |
|--------|------|-------------|
| `id` | `BIGSERIAL` | Auto-incrementing primary key |
| `metric_type` | `TEXT` | Normalized name (e.g. `heart_rate`, `steps`) |
| `recorded_at` | `TIMESTAMPTZ` | When the measurement was taken |
| `ingested_at` | `TIMESTAMPTZ` | When it was inserted into the DB |
| `source_device` | `TEXT` | `"Apple Watch"`, `"iPhone"`, etc. |
| `value` | `DOUBLE PRECISION` | Extracted numeric value |
| `unit` | `TEXT` | `"bpm"`, `"count"`, `"kcal"`, etc. |
| `raw_payload` | `JSONB` | Full original data point (lossless) |

### `ingested_files` — File tracking table

Prevents re-processing of the same export file.

## Querying Your Data

```sql
-- Latest 10 heart rate readings
SELECT recorded_at, value, unit, source_device
FROM health_metrics
WHERE metric_type = 'heart_rate'
ORDER BY recorded_at DESC
LIMIT 10;

-- Daily average resting heart rate
SELECT date_trunc('day', recorded_at) AS day,
       ROUND(AVG(value)::numeric, 1) AS avg_rhr
FROM health_metrics
WHERE metric_type = 'resting_heart_rate'
GROUP BY 1 ORDER BY 1 DESC;

-- Sleep duration per night
SELECT date_trunc('day', recorded_at) AS night,
       SUM(value) / 60.0 AS hours
FROM health_metrics
WHERE metric_type LIKE 'sleep_%'
GROUP BY 1 ORDER BY 1 DESC;

-- Total steps per day
SELECT date_trunc('day', recorded_at) AS day,
       SUM(value)::integer AS total_steps
FROM health_metrics
WHERE metric_type = 'steps'
GROUP BY 1 ORDER BY 1 DESC;

-- Correlate sleep vs. activity
WITH sleep AS (
    SELECT date_trunc('day', recorded_at) AS day,
           SUM(value) / 60.0 AS sleep_hours
    FROM health_metrics WHERE metric_type LIKE 'sleep_%'
    GROUP BY 1
),
activity AS (
    SELECT date_trunc('day', recorded_at) AS day,
           SUM(value)::numeric AS active_cal
    FROM health_metrics WHERE metric_type = 'active_energy_burned'
    GROUP BY 1
)
SELECT s.day, s.sleep_hours, a.active_cal
FROM sleep s JOIN activity a ON s.day = a.day
ORDER BY s.day DESC;
```

## Logs

```bash
# Live log stream
tail -f ~/Library/Logs/health_vault.log

# Error log
tail -f ~/Library/Logs/health_vault.error.log

# macOS Console.app also indexes these automatically
```

## Managing the Service

```bash
# Stop
launchctl unload ~/Library/LaunchAgents/com.ashtoncoghlan.health-vault.plist

# Start
launchctl load ~/Library/LaunchAgents/com.ashtoncoghlan.health-vault.plist

# Check status
launchctl list | grep health-vault

# View all ingested files
psql health_vault -c "SELECT filename, record_count, ingested_at FROM ingested_files ORDER BY ingested_at DESC;"

# Total records by metric type
psql health_vault -c "SELECT metric_type, COUNT(*) FROM health_metrics GROUP BY 1 ORDER BY 2 DESC;"
```

## Design Decisions

- **JSONB + structured columns**: Hybrid approach gives you fast typed queries via indexed columns AND lossless archival via the raw JSONB payload. No schema migrations needed as new metric types appear.
- **ON CONFLICT DO NOTHING**: Row-level deduplication at the database layer ensures idempotency even if the same data appears in multiple export files.
- **iCloud-aware settling**: 5-second delay after file detection accounts for iCloud's progressive download behavior where files may initially appear as stubs.
- **Periodic sweep**: Catches files that arrived while the watcher was stopped (reboot, crash, etc.).
- **No network exposure**: Zero ports opened, zero VPN tunnels. Entire data flow runs through Apple's native iCloud sync infrastructure.
# health-vault
