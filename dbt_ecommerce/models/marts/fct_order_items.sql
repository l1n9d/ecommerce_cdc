{{ config(materialized='table') }}

-- ============================================================
-- fct_order_items
--
-- Line-item fact table. One row per (order, product) line.
-- Point-in-time joins to BOTH dim_customers (via the parent order's
-- placed_at) and dim_products (via the parent order's placed_at).
--
-- DESIGN NOTES
-- - The grain is (order_id, product_id) — one row per line item.
-- - unit_price comes from the order_items source (already point-in-time
--   correct from the operational schema design — see Day 3).
-- - Joining to dim_products gives us product attributes (category, name,
--   weight) at order time, NOT current product state.
-- - We re-join to dim_customers at order time as well, so this fact can
--   stand alone for queries like "revenue by customer state by product
--   category at time of purchase."
-- ============================================================

WITH order_item_changes AS (
    SELECT *
    FROM {{ ref('stg_order_items_changes') }}
    WHERE op_type IN ('r', 'c', 'u')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_item_id
        ORDER BY lsn DESC
    ) = 1
),

orders_for_pit AS (
    -- Need each order's placed_at and customer_id to do PIT joins on the line items.
    -- Use the same latest-event-per-order logic as fct_orders.
    SELECT 
        order_id,
        customer_id,
        placed_at
    FROM {{ ref('stg_orders_changes') }}
    WHERE op_type IN ('r', 'c', 'u')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY lsn DESC
    ) = 1
)

SELECT
    -- Natural key
    oi.order_item_id,
    
    -- Foreign keys to other facts/dimensions
    oi.order_id,
    oi.product_id,
    o.customer_id,
    
    -- Surrogate keys for point-in-time joins
    c.customer_sk,
    p.product_sk,
    
    -- Product attributes AT TIME OF ORDER (price history demo!)
    p.name AS product_name_at_order,
    p.category AS product_category_at_order,
    p.current_price AS product_list_price_at_order,
    p.weight_kg AS product_weight_kg,
    
    -- Customer attributes at time of order
    c.city AS customer_city_at_order,
    c.state AS customer_state_at_order,
    
    -- Line item measures
    oi.quantity,
    oi.unit_price,                  -- price actually charged (snapshot from operational)
    oi.line_total,
    
    -- Derived: did the customer pay above or below the catalog price at the time?
    -- This is interesting because our seed introduced ±5% price variance.
    -- A non-zero discount/markup is real signal in the data.
    oi.unit_price - p.current_price AS price_variance_vs_catalog,
    CASE 
        WHEN p.current_price > 0 
        THEN (oi.unit_price - p.current_price) / p.current_price
        ELSE NULL
    END AS price_variance_pct,
    
    -- Time dimension
    o.placed_at AS order_placed_at,
    
    -- Audit
    oi.lsn AS source_lsn,
    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM order_item_changes oi
INNER JOIN orders_for_pit o
    ON oi.order_id = o.order_id
LEFT JOIN {{ ref('dim_customers') }} c
    ON o.customer_id = c.customer_id
    AND o.placed_at >= c.valid_from
    AND o.placed_at < COALESCE(c.valid_to, '9999-12-31'::timestamp)
LEFT JOIN {{ ref('dim_products') }} p
    ON oi.product_id = p.product_id
    AND o.placed_at >= p.valid_from
    AND o.placed_at < COALESCE(p.valid_to, '9999-12-31'::timestamp)
