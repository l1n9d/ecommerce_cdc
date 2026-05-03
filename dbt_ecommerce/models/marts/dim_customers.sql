{{ config(materialized='table') }}

-- ============================================================
-- dim_customers
--
-- SCD Type 2 dimension built from the customers CDC change stream.
-- One row per version of each customer. valid_from/valid_to/is_current
-- enable point-in-time correct joins from fact tables.
--
-- DESIGN NOTES
-- - Built manually from CDC change stream (not dbt snapshot) because the
--   source is an event stream with explicit LSN ordering, not a polled
--   current-state table.
-- - LEAD over LSN gives deterministic ordering even for same-millisecond
--   updates -- LSN is globally unique and monotonic.
-- - Excludes 'd' (delete) op_type from versions; deletes mark the entity
--   as no-longer-current, handled via is_current flag on the prior version.
-- - Surrogate key is hash(customer_id, lsn) -- deterministic, joinable from facts.
-- ============================================================

WITH change_events AS (
    -- Every change event for every customer, ordered chronologically
    SELECT
        customer_id,
        op_type,
        lsn,
        source_ts,
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
    -- Keep r/c/u (existence + updates). Deletes are handled below.
    WHERE op_type IN ('r', 'c', 'u')
),

versioned AS (
    -- For each customer, compute when each version was superseded
    SELECT
        *,
        LEAD(source_ts) OVER (
            PARTITION BY customer_id
            ORDER BY lsn
        ) AS valid_to_raw
    FROM change_events
),

-- Identify customers who have been deleted -- their last non-delete version 
-- should not be marked is_current
deleted_customers AS (
    SELECT DISTINCT customer_id
    FROM {{ ref('stg_customers_changes') }}
    WHERE op_type = 'd'
)

SELECT
    -- Surrogate key: unique per version of each customer
    {{ dbt_utils.generate_surrogate_key(['v.customer_id', 'v.lsn']) }} AS customer_sk,
    
    -- Natural key: stable across versions
    v.customer_id,
    
    -- Business attributes (current state at this version's lifetime)
    v.email,
    v.first_name,
    v.last_name,
    v.phone,
    v.street_address,
    v.city,
    v.state,
    v.zip_code,
    v.country,
    
    -- SCD2 contract columns
    v.source_ts AS valid_from,
    v.valid_to_raw AS valid_to,
    -- A version is current if (a) it's the latest (no LEAD value) AND
    -- (b) the customer hasn't been deleted entirely.
    CASE 
        WHEN v.valid_to_raw IS NULL AND d.customer_id IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current,
    
    -- Audit fields
    v.lsn AS source_lsn,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
    
FROM versioned v
LEFT JOIN deleted_customers d
    ON v.customer_id = d.customer_id
