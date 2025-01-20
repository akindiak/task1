INSERT INTO {{ params.target_table }}
    (order_id, customer_email, order_date, amount, currency, ingested_at)

SELECT order_id, customer_email, order_date, amount, currency, ingested_at
FROM {{ params.staging_table }}

ON CONFLICT (order_id) DO UPDATE SET
    customer_email = EXCLUDED.customer_email,
    order_date = EXCLUDED.order_date,
    amount = EXCLUDED.amount,
    currency = EXCLUDED.currency,
    ingested_at = EXCLUDED.ingested_at