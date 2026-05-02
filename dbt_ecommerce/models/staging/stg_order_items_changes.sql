{{ config(materialized='view') }}

-- Staging model for order_items CDC events.
-- Note: unit_price is snapshot at time of order (point-in-time correctness for
-- price history) — see Day 3 design discussion.

WITH source AS (
    SELECT RECORD_METADATA, RECORD_CONTENT
    FROM {{ source('raw_cdc', 'order_items_raw') }}
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

        event:after:order_item_id::NUMBER       AS order_item_id,
        event:after:order_id::NUMBER            AS order_id,
        event:after:product_id::NUMBER          AS product_id,
        event:after:quantity::NUMBER            AS quantity,
        event:after:unit_price::FLOAT           AS unit_price,
        event:after:line_total::FLOAT           AS line_total,
        TO_TIMESTAMP_LTZ(event:after:created_at::STRING)  AS source_created_at,

        COALESCE(
            event:after:order_item_id::NUMBER,
            PARSE_JSON(RECORD_METADATA:key::STRING):order_item_id::NUMBER
        ) AS order_item_natural_key
    FROM payload
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_item_natural_key', 'lsn']) }} AS change_event_id,
    order_item_natural_key AS order_item_id,
    op_type, lsn, tx_id, source_ts, debezium_ts, kafka_ts,
    order_id, product_id, quantity, unit_price, line_total,
    source_created_at
FROM extracted
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_item_natural_key, lsn
    ORDER BY kafka_ts
) = 1
