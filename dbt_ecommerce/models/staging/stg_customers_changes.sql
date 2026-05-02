{{ config(materialized='view') }}

-- ============================================================
-- stg_customers_changes
--
-- Staging model for customers CDC events. One row per Debezium change
-- event, with VARIANT JSON parsed into typed columns.
--
-- DESIGN NOTES
-- - One row = one change event (not one row per current customer).
-- - Preserves all op codes (r=read/snapshot, c=create, u=update, d=delete).
-- - Dedup via QUALIFY on (customer_id, lsn) handles at-least-once delivery.
-- - Source-aligned: column names mirror the Postgres customers table.
-- - PARSE_JSON once in payload CTE; downstream refs use parsed VARIANT.
-- ============================================================

WITH source AS (
    SELECT
        RECORD_METADATA,
        RECORD_CONTENT
    FROM {{ source('raw_cdc', 'customers_raw') }}
),

-- Parse JSON once. Every other reference uses this CTE so we don't re-parse.
payload AS (
    SELECT
        RECORD_METADATA,
        RECORD_CONTENT,
        PARSE_JSON(RECORD_CONTENT) AS event
    FROM source
),

extracted AS (
    SELECT
        -- CDC metadata: what kind of change, when, where in WAL
        event:op::STRING                                       AS op_type,
        event:source:lsn::NUMBER                               AS lsn,
        event:source:txid::NUMBER                              AS tx_id,
        TO_TIMESTAMP_LTZ(event:source:ts_ms::NUMBER / 1000)    AS source_ts,
        TO_TIMESTAMP_LTZ(event:ts_ms::NUMBER / 1000)           AS debezium_ts,
        TO_TIMESTAMP_LTZ(RECORD_METADATA:CreateTime::NUMBER / 1000) AS kafka_ts,

        -- After-image: row state at the point of this change.
        -- For deletes, after is null; the primary key still comes via the message key.
        -- For our purposes, pulling fields from after is enough.
        event:after:customer_id::NUMBER       AS customer_id,
        event:after:email::STRING             AS email,
        event:after:first_name::STRING        AS first_name,
        event:after:last_name::STRING         AS last_name,
        event:after:phone::STRING             AS phone,
        event:after:street_address::STRING    AS street_address,
        event:after:city::STRING              AS city,
        event:after:state::STRING             AS state,
        event:after:zip_code::STRING          AS zip_code,
        event:after:country::STRING           AS country,
        TO_TIMESTAMP_LTZ(event:after:created_at::STRING)  AS source_created_at,
        TO_TIMESTAMP_LTZ(event:after:updated_at::STRING)  AS source_updated_at,

        -- For deletes, primary key comes from RECORD_METADATA.key
        -- We coalesce to make customer_id available even when 'after' is null
        COALESCE(
            event:after:customer_id::NUMBER,
            PARSE_JSON(RECORD_METADATA:key::STRING):customer_id::NUMBER
        ) AS customer_natural_key
    FROM payload
)

SELECT
    -- Surrogate key for change events: deterministic, useful for dedup downstream
    {{ dbt_utils.generate_surrogate_key(['customer_natural_key', 'lsn']) }} AS change_event_id,
    customer_natural_key AS customer_id,
    op_type,
    lsn,
    tx_id,
    source_ts,
    debezium_ts,
    kafka_ts,
    -- After-image columns
    email,
    first_name,
    last_name,
    phone,
    street_address,
    city,
    state,
    zip_code,
    country,
    source_created_at,
    source_updated_at
FROM extracted
-- Dedup: same (customer_id, lsn) shouldn't appear twice. If it does (Kafka
-- redelivered after a crash), keep the first one we saw.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_natural_key, lsn
    ORDER BY kafka_ts
) = 1
