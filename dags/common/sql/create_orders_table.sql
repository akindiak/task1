CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    order_id uuid PRIMARY KEY,
    customer_email varchar(255),
    order_date TIMESTAMP,
    amount numeric(15, 2),
    currency char(3),
    ingested_at TIMESTAMP
)
