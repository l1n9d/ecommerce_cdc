# Incident: 2026-04-29 — RDS Storage-full caused by replication slot WAL retention

## Symptoms
- Debezium task FAILED with `Connection refused` to RDS
- RDS instance status: `Storage-full`
- AWS Console showed disk had been climbing all night while connector was offline

## Root cause
- Stopped Kafka Connect overnight via `docker compose stop`
- Replication slot `debezium_slot` remained in Postgres (slot persists across consumer disconnects by design)
- Postgres retained ~19GB of WAL waiting for the slot to advance
- 20GB free-tier disk filled, RDS auto-protected by refusing connections

## Diagnosis
Key query that confirmed the cause:
-- How big is database?
SELECT pg_size_pretty(pg_database_size('ecommerce'));

-- What's the WAL size and slot status? This is the diagnostic.
SELECT 
    slot_name,
    active,
    wal_status,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconsumed
FROM pg_replication_slots;

-- Free space remaining
SELECT pg_size_pretty(pg_database_size('ecommerce')) AS db_size;

Database size: 12 MB
Total WAL written since project start: 71 GB
WAL retained by inactive slot: 19 GB

## Resolution
1. Modified RDS allocated storage from 20GB → 30GB to bring instance back online
2. Dropped the inactive replication slot: `SELECT pg_drop_replication_slot('debezium_slot');`
3. Waited ~5 minutes for Postgres to reclaim WAL files
4. Deleted and recreated the Debezium connector (fresh slot, fresh snapshot)

## Prevention
- Enabled RDS storage autoscaling (max 50GB)
- Added CloudWatch alarm: FreeStorageSpace < 5GB → email alert
- Documented in README: do not stop Connect long-term without first dropping the slot
- (Future) Add monitoring on pg_replication_slots.confirmed_flush_lsn lag

## Lessons
- Replication slots persist when consumers disconnect — that's a feature, not a bug, but it has operational implications
- A 1MB-of-data simulator can easily generate 20GB of WAL in normal runtime
- pg_replication_slots is the diagnostic to check first when RDS Postgres fills disk unexpectedly