-- SCD2 correctness check: every customer must have exactly one current version.
-- Returns rows for customers that violate this invariant.
-- A passing test returns zero rows.
SELECT 
    customer_id,
    COUNT(*) AS current_count
FROM {{ ref('dim_customers') }}
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) <> 1
