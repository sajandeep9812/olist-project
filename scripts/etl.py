# scripts/etl.py
"""
Simple ETL starter for Olist dataset.
- Extracts tables from MySQL
- Runs a small transform (example: join orders + payments)
- Loads a derived table back to MySQL as `fact_orders`

Run: python scripts/etl.py
"""
import logging
from config.db import get_engine
import pandas as pd

logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

CHUNK_SIZE = 100_000


def extract_table(table_name, engine, limit=None):
    sql = f"SELECT * FROM {table_name}"
    if limit is not None:
        sql += f" LIMIT {limit}"
    logging.info(f"Extracting {table_name}")
    return pd.read_sql(sql, engine)


def transform_orders_payments(orders, payments):
    """Return joined orders + payments with basic cleaning."""
    logging.info("Transform: merging orders and payments")
    df = orders.merge(payments, on='order_id', how='left')

    # Convert date columns to datetime if present
    for col in ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Example derived metric
    if 'payment_value' in df.columns:
        df['revenue'] = df['payment_value']
    else:
        df['revenue'] = 0.0

    return df


def load_dataframe_to_sql(df, table_name, engine, if_exists='replace'):
    logging.info(f"Loading DataFrame -> {table_name} (rows={len(df)})")
    df.to_sql(table_name, engine, if_exists=if_exists,
              index=False, method='multi', chunksize=CHUNK_SIZE)
    logging.info("Load finished")


def main():
    engine = get_engine()

    # Extract (careful with memory — for full dataset consider chunked processing)
    orders = extract_table('olist_orders_dataset', engine)
    payments = extract_table('olist_order_payments_dataset', engine)

    # Transform
    fact = transform_orders_payments(orders, payments)

    # Load
    load_dataframe_to_sql(fact, 'fact_orders', engine)

    logging.info('ETL job completed')


if __name__ == '__main__':
    main()
