{{ config(materialized='view') }}

-- Staging model for inventory CDC events.
-- High-frequency: every order decrements inventory; restocks add stock.
-- Note: inventory.product_id is BOTH primary key AND foreign key to products.
-- We don't have a created_at on inventory (only updated_at).

WITH source AS (
    SELECT RECORD_METADATA, RECORD_CONTENT
    FROM {{ source('raw_cdc', 'inventory_raw') }}
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

        event:after:product_id::NUMBER          AS product_id,
        event:after:quantity_on_hand::NUMBER    AS quantity_on_hand,
        event:after:reorder_threshold::NUMBER   AS reorder_threshold,
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
    quantity_on_hand, reorder_threshold,
    source_updated_at
FROM extracted
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_natural_key, lsn
    ORDER BY kafka_ts
) = 1
