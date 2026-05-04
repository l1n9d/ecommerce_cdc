-- Sanity: line_total should equal quantity * unit_price (within float precision).
-- Returns rows where the operational calculation is inconsistent.
SELECT
    order_item_id,
    quantity,
    unit_price,
    line_total,
    quantity * unit_price AS expected_line_total,
    ABS(line_total - quantity * unit_price) AS variance
FROM {{ ref('fct_order_items') }}
WHERE ABS(line_total - quantity * unit_price) > 0.01
