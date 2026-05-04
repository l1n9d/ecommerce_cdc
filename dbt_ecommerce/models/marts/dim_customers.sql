{{ config(materialized='table') }}

-- ============================================================
-- dim_customers
--
-- SCD Type 2 dimension built from customers CDC change stream.
--
-- VALID_FROM SEMANTICS (important):
-- For snapshot events (op_type = 'r'), valid_from uses source_created_at
-- from the operational customers table. This represents when the row
-- ACTUALLY started existing in the source system — not when CDC observed it.
-- Without this fix, all snapshot rows would have valid_from = the moment
-- CDC took its initial snapshot, breaking point-in-time joins for any
-- fact rows older than CDC initialization.
--
-- For real change events (op_type in 'c', 'u'), valid_from = source_ts,
-- which IS the moment the change happened in the source system.
-- ============================================================

WITH change_events AS (
    SELECT
        customer_id,
        op_type,
        lsn,
        source_ts,
        source_created_at,
        email,
        first_name,
        last_name,
        phone,
        street_address,
        city,
        state,
        zip_code,
        country
    FROM {{ ref('stg_customers_changes') }}
    WHERE op_type IN ('r', 'c', 'u')
),

versioned AS (
    SELECT
        *,
        -- valid_from: when this row's state started in the source system
        CASE
            WHEN op_type = 'r' THEN source_created_at
            ELSE source_ts
        END AS effective_valid_from,
        LEAD(source_ts) OVER (
            PARTITION BY customer_id
            ORDER BY lsn
        ) AS valid_to_raw
    FROM change_events
),

deleted_customers AS (
    SELECT DISTINCT customer_id
    FROM {{ ref('stg_customers_changes') }}
    WHERE op_type = 'd'
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['v.customer_id', 'v.lsn']) }} AS customer_sk,
    v.customer_id,
    v.email,
    v.first_name,
    v.last_name,
    v.phone,
    v.street_address,
    v.city,
    v.state,
    v.zip_code,
    v.country,
    v.effective_valid_from AS valid_from,
    v.valid_to_raw AS valid_to,
    CASE
        WHEN v.valid_to_raw IS NULL AND d.customer_id IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current,
    v.lsn AS source_lsn,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM versioned v
LEFT JOIN deleted_customers d
    ON v.customer_id = d.customer_id
