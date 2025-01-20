Prerequisites:
- create token(app_id) using https://openexchangerates.org and past it to .env file key OPEN_EXCHANGE_API_TOKEN

Run in Docker:
- docker compose up --build

You can later navigate to Airflow UI and check dags.
Credentials:
- login: airflow
- password: airflow

Dags should be unpaused.

To verify data in databases you can connect using the following specs:
- source_db(postgres1) - host: localhost port: 5434 user: airflow password: airflow db: airflow table: orders
- target_db(postgres2) - host: localhost port: 5436 user: airflow password: airflow db: airflow table: orders_eur

