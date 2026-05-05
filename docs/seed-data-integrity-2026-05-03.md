# Bug fix: synthetic seed data violated real-world temporal invariants

## Symptom
After fixing the SCD2 valid_from bug, the same singular test still failed —
this time with 1175 unmatched orders (down from 5534).

## Root cause
The seed script generated `customer.created_at` and `order.placed_at` using
independent random distributions:

- customers: `created_at = NOW() - random(0, 730 days)`
- orders: `placed_at = NOW() - random(0, 365 days)`

This allowed orders to be placed before their own customer existed —
about 20% of the dataset. The dimensional model rejects this correctly:
no version of the customer covers the order's placed_at because the
customer literally didn't exist yet.

## Fix
Constrained order placed_at to never precede the customer's created_at:

```python
earliest_possible = max(customer_created, datetime.now() - timedelta(days=365))
placed_at = earliest_possible + timedelta(seconds=random(0, window))
```

## Lesson
Synthetic test data must respect real-world invariants. In production, this
kind of corruption would either indicate genuine data quality problems
upstream OR a flawed event-time logic in the application — both worth
investigating immediately. The same singular test that catches "invalid
temporal joins" will catch this in any production CDC pipeline.

## Recovery procedure
Fixing the seed required rewinding the entire pipeline:

1. Stop Sink Connector and Debezium connector
2. Drop Postgres replication slot
3. Re-run seed script (TRUNCATEs and re-inserts)
4. Drop Snowflake RAW tables (Sink will recreate)
5. Delete Kafka data topics (`ecommerce.public.*` plus heartbeat)
6. Recreate Debezium with a NEW slot.name — Connect persists offset state
   keyed by connector name, so reusing the same name skips the snapshot
7. Recreate Sink Connector
8. Wait for snapshot + buffer flush (~60s)
9. Run dbt build

Each step matters. Skipping any leaves stale state somewhere.