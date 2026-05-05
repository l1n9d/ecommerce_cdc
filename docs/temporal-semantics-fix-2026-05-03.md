# Bug fix: SCD2 snapshot valid_from temporal semantics

## Symptom
Singular test `fct_orders_no_unmatched_customer` failed with 5534/5755 orders
missing customer_sk after point-in-time join.

## Root cause
In the original dim_customers, snapshot events (op_type = 'r') used the snapshot's
source_ts as valid_from. This is when CDC observed the row, not when the row 
actually started existing in the source system. 

Orders placed before CDC initialization had placed_at timestamps earlier than any
dimension version's valid_from, so the [valid_from, valid_to) interval check failed.

## Fix
For snapshot rows, set valid_from = source_created_at (operational table's
created_at column). For real change events (op_type in 'c', 'u'),
`source_ts` remains correct because that IS when the change happened.

```sql
CASE
    WHEN op_type = 'r' THEN source_created_at
    ELSE source_ts
END AS valid_from
```

## Lesson
SCD2 valid_from semantics matter: it should represent when the row's state began
in the SOURCE system, not when our pipeline observed it. This is non-obvious for
snapshot events where there is no "moment of change" — only a "moment of observation."

## Detection
The singular relational integrity test (fct_orders_no_unmatched_customer) caught
this immediately. Schema tests (unique, not_null) would not have — the join just
silently produced NULL customer_sk values, which schema tests treat as valid.