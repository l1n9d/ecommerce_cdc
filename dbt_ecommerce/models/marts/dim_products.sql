{{ config(materialized='table') }}

-- Same valid_from fix as dim_customers — see comments there.

WITH change_events AS (
    SELECT
        product_id,
        op_type,
        lsn,
        source_ts,
        source_created_at,
        sku,
        name,
        category,
        description,
        current_price,
        weight_kg,
        is_active
    FROM {{ ref('stg_products_changes') }}
    WHERE op_type IN ('r', 'c', 'u')
),

versioned AS (
    SELECT
        *,
        CASE
            WHEN op_type = 'r' THEN source_created_at
            ELSE source_ts
        END AS effective_valid_from,
        LEAD(source_ts) OVER (
            PARTITION BY product_id
            ORDER BY lsn
        ) AS valid_to_raw
    FROM change_events
),

deleted_products AS (
    SELECT DISTINCT product_id
    FROM {{ ref('stg_products_changes') }}
    WHERE op_type = 'd'
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['v.product_id', 'v.lsn']) }} AS product_sk,
    v.product_id,
    v.sku,
    v.name,
    v.category,
    v.description,
    v.current_price,
    v.weight_kg,
    v.is_active,
    v.effective_valid_from AS valid_from,
    v.valid_to_raw AS valid_to,
    CASE
        WHEN v.valid_to_raw IS NULL AND d.product_id IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current,
    v.lsn AS source_lsn,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM versioned v
LEFT JOIN deleted_products d
    ON v.product_id = d.product_id
