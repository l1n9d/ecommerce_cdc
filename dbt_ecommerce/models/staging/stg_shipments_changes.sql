{{ config(materialized='view') }}

-- Staging model for shipments CDC events.
-- Lifecycle: pending -> in_transit -> delivered (status transitions = updates).

WITH source AS (
    SELECT RECORD_METADATA, RECORD_CONTENT
    FROM {{ source('raw_cdc', 'shipments_raw') }}
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

        event:after:shipment_id::NUMBER         AS shipment_id,
        event:after:order_id::NUMBER            AS order_id,
        event:after:carrier::STRING             AS carrier,
        event:after:tracking_number::STRING     AS tracking_number,
        event:after:status::STRING              AS shipment_status,
        TO_TIMESTAMP_LTZ(event:after:shipped_at::STRING)    AS shipped_at,
        TO_TIMESTAMP_LTZ(event:after:delivered_at::STRING)  AS delivered_at,
        TO_TIMESTAMP_LTZ(event:after:updated_at::STRING)    AS source_updated_at,

        COALESCE(
            event:after:shipment_id::NUMBER,
            PARSE_JSON(RECORD_METADATA:key::STRING):shipment_id::NUMBER
        ) AS shipment_natural_key
    FROM payload
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['shipment_natural_key', 'lsn']) }} AS change_event_id,
    shipment_natural_key AS shipment_id,
    op_type, lsn, tx_id, source_ts, debezium_ts, kafka_ts,
    order_id, carrier, tracking_number, shipment_status,
    shipped_at, delivered_at, source_updated_at
FROM extracted
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY shipment_natural_key, lsn
    ORDER BY kafka_ts
) = 1
