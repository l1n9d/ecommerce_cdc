SELECT 
    product_id,
    COUNT(*) AS current_count
FROM {{ ref('dim_products') }}
WHERE is_current = TRUE
GROUP BY product_id
HAVING COUNT(*) <> 1
