SELECT 
    a.product_id,
    a.product_sk AS sk_earlier,
    b.product_sk AS sk_later,
    a.valid_from AS a_from,
    a.valid_to   AS a_to,
    b.valid_from AS b_from,
    b.valid_to   AS b_to
FROM {{ ref('dim_products') }} a
JOIN {{ ref('dim_products') }} b
    ON a.product_id = b.product_id
    AND a.product_sk <> b.product_sk
WHERE a.valid_from < COALESCE(b.valid_to, '9999-12-31'::timestamp)
  AND COALESCE(a.valid_to, '9999-12-31'::timestamp) > b.valid_from
