-- Every order should match exactly one customer dimension version.
-- Returns rows where the point-in-time join failed (no matching customer
-- version found for the order's placed_at). 
-- Could indicate: customer_id missing from dim_customers, or placed_at
-- falls outside any version's [valid_from, valid_to) range.
SELECT
    order_id,
    customer_id,
    placed_at
FROM {{ ref('fct_orders') }}
WHERE customer_id IS NOT NULL
  AND customer_sk IS NULL
