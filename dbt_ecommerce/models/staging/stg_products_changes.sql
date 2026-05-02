{{ config(materialized='view') }}

-- Staging model for products CDC events.
-- One row per Debezium change event. SCD2 candy: current_price changes
-- generate the most interesting price-history dimension downstream.

WITH source AS (
    SELECT RECORD_METADATA, RECORD_CONTENT
    FROM {{ source('raw_cdc', 'products_raw') }}
),

payload AS (
    SELECT
        RECORD_METADATA,
        RECORD_CONTENT,
        PARSE_JSON(RECORD_CONTENT) AS event
    FROM source
),

extracted AS (
    SELECT
        event:op::STRING                                            AS op_type,
        event:source:lsn::NUMBER                                    AS lsn,
        event:source:txid::NUMBER                                   AS tx_id,
        TO_TIMESTAMP_LTZ(event:source:ts_ms::NUMBER / 1000)         AS source_ts,
        TO_TIMESTAMP_LTZ(event:ts_ms::NUMBER / 1000)                AS debezium_ts,
        TO_TIMESTAMP_LTZ(RECORD_METADATA:CreateTime::NUMBER / 1000) AS kafka_ts,

        event:after:product_id::NUMBER       AS product_id,
        event:after:sku::STRING              AS sku,
        event:after:name::STRING             AS name,
        event:after:category::STRING         AS category,
        event:after:description::STRING      AS description,
        event:after:current_price::FLOAT     AS current_price,
        event:after:weight_kg::FLOAT         AS weight_kg,
        event:after:is_active::BOOLEAN       AS is_active,
        TO_TIMESTAMP_LTZ(event:after:created_at::STRING)  AS source_created_at,
        TO_TIMESTAMP_LTZ(event:after:updated_at::STRING)  AS source_updated_at,

        COALESCE(
            event:after:product_id::NUMBER,
            PARSE_JSON(RECORD_METADATA:key::STRING):product_id::NUMBER
        ) AS product_natural_key
    FROM payload
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_natural_key', 'lsn']) }} AS change_event_id,
    product_natural_key AS product_id,
    op_type, lsn, tx_id, source_ts, debezium_ts, kafka_ts,
    sku, name, category, description, current_price, weight_kg, is_active,
    source_created_at, source_updated_at
FROM extracted
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_natural_key, lsn
    ORDER BY kafka_ts
) = 1
