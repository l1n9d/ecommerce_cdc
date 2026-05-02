{{ config(materialized='view') }}

-- Staging model for orders CDC events.
-- Heaviest volume table: every new order = 1 event, every status transition = 1 event.
-- Note shipping_address fields snapshot at time of order — preserves point-in-time
-- correctness even if customer's address changes later.

WITH source AS (
    SELECT RECORD_METADATA, RECORD_CONTENT
    FROM {{ source('raw_cdc', 'orders_raw') }}
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

        event:after:order_id::NUMBER            AS order_id,
        event:after:customer_id::NUMBER         AS customer_id,
        event:after:order_status::STRING        AS order_status,
        event:after:order_total::FLOAT          AS order_total,
        event:after:payment_method::STRING      AS payment_method,
        event:after:shipping_address::STRING    AS shipping_address,
        event:after:shipping_city::STRING       AS shipping_city,
        event:after:shipping_state::STRING      AS shipping_state,
        event:after:shipping_zip::STRING        AS shipping_zip,
        TO_TIMESTAMP_LTZ(event:after:placed_at::STRING)   AS placed_at,
        TO_TIMESTAMP_LTZ(event:after:updated_at::STRING)  AS source_updated_at,

        COALESCE(
            event:after:order_id::NUMBER,
            PARSE_JSON(RECORD_METADATA:key::STRING):order_id::NUMBER
        ) AS order_natural_key
    FROM payload
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_natural_key', 'lsn']) }} AS change_event_id,
    order_natural_key AS order_id,
    op_type, lsn, tx_id, source_ts, debezium_ts, kafka_ts,
    customer_id, order_status, order_total, payment_method,
    shipping_address, shipping_city, shipping_state, shipping_zip,
    placed_at, source_updated_at
FROM extracted
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_natural_key, lsn
    ORDER BY kafka_ts
) = 1
