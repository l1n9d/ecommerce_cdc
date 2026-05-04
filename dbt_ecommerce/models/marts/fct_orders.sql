{{ config(materialized='table') }}

-- ============================================================
-- fct_orders
--
-- Order fact table. One row per order, joined to the customer
-- dimension version that was valid AT THE TIME OF THE ORDER.
--
-- DESIGN NOTES
-- - Built from stg_orders_changes, taking the LATEST event per order
--   to represent its final state (or current state if still in progress).
-- - Point-in-time join to dim_customers via [valid_from, valid_to) interval.
-- - Stores BOTH customer_sk (point-in-time) AND customer_id (natural key)
--   so analysts can query both ways:
--     "all orders ever from customer #42" -> JOIN ON customer_id
--     "what was customer's address at time of order" -> JOIN ON customer_sk
-- - Measures: order_total, plus derived metrics like days_to_ship.
-- ============================================================

WITH order_changes AS (
    -- Get the LATEST state of each order. For orders that haven't been
    -- updated since creation, this is the create event. For orders that
    -- progressed through statuses, this is the most recent status change.
    SELECT *
    FROM {{ ref('stg_orders_changes') }}
    WHERE op_type IN ('r', 'c', 'u')  -- exclude deletes
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id 
        ORDER BY lsn DESC
    ) = 1
),

shipments_aggregated AS (
    -- Most recent shipment state per order
    SELECT *
    FROM {{ ref('stg_shipments_changes') }}
    WHERE op_type IN ('r', 'c', 'u')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY lsn DESC
    ) = 1
)

SELECT
    -- Natural key
    o.order_id,
    
    -- Customer keys: BOTH for analytical flexibility
    o.customer_id,                          -- natural: "all orders by this customer"
    c.customer_sk,                          -- point-in-time: "customer state at order time"
    
    -- Customer attributes at TIME OF ORDER (denormalized for query convenience)
    c.email AS customer_email_at_order,
    c.city AS customer_city_at_order,
    c.state AS customer_state_at_order,
    
    -- Order attributes
    o.order_status,
    o.payment_method,
    o.shipping_address,
    o.shipping_city,
    o.shipping_state,
    o.shipping_zip,
    
    -- Measures
    o.order_total,
    
    -- Time dimensions
    o.placed_at,
    s.shipped_at,
    s.delivered_at,
    
    -- Derived metrics
    DATEDIFF('day', o.placed_at, s.shipped_at)    AS days_to_ship,
    DATEDIFF('day', s.shipped_at, s.delivered_at) AS days_in_transit,
    DATEDIFF('day', o.placed_at, s.delivered_at)  AS days_to_deliver,
    
    -- Audit
    o.lsn AS source_lsn,
    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM order_changes o
-- Point-in-time join to customer dimension
LEFT JOIN {{ ref('dim_customers') }} c
    ON o.customer_id = c.customer_id
    AND o.placed_at >= c.valid_from
    AND o.placed_at < COALESCE(c.valid_to, '9999-12-31'::timestamp)
-- Most recent shipment per order
LEFT JOIN shipments_aggregated s
    ON o.order_id = s.order_id
