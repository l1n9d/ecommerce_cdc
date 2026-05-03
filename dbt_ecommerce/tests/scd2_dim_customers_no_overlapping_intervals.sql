-- SCD2 correctness check: no two versions of the same customer should have
-- overlapping [valid_from, valid_to) intervals.
-- Returns rows for any customer with overlapping versions.
-- A passing test returns zero rows.
SELECT 
    a.customer_id,
    a.customer_sk AS sk_earlier,
    b.customer_sk AS sk_later,
    a.valid_from AS a_from,
    a.valid_to   AS a_to,
    b.valid_from AS b_from,
    b.valid_to   AS b_to
FROM {{ ref('dim_customers') }} a
JOIN {{ ref('dim_customers') }} b
    ON a.customer_id = b.customer_id
    AND a.customer_sk <> b.customer_sk
WHERE a.valid_from < COALESCE(b.valid_to, '9999-12-31'::timestamp)
  AND COALESCE(a.valid_to, '9999-12-31'::timestamp) > b.valid_from
