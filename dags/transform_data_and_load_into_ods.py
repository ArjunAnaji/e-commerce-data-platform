# DAG 2: Transform Landing Zone to ODS Layer with Cleansing
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_pg_engines():
    pg_conn_obj = BaseHook.get_connection("postgres_conn")
    pg_uri = f"postgresql://{pg_conn_obj.login}:{pg_conn_obj.password}@{pg_conn_obj.host}:{pg_conn_obj.port}/" \
             f"{pg_conn_obj.schema}"
    pg_landing = sqlalchemy.create_engine(pg_uri, connect_args={"options": "-csearch_path=landing_zone"})
    pg_ods = sqlalchemy.create_engine(pg_uri, connect_args={"options": "-csearch_path=operational_data_store"})
    return pg_landing, pg_ods


def drop_nulls(df, columns):
    return df.dropna(subset=columns)


def ensure_uniqueness(df, columns):
    return df.drop_duplicates(subset=columns)


def transform_customer(df, **kwargs):
    # Drop required nulls
    df = drop_nulls(df, ['id', 'first_name', 'last_name', 'email', 'shipping_address'])

    # Standardize gender and filter valid values
    df['gender'] = df['gender'].str.strip().str.capitalize()
    df = df[df['gender'].isin(['Male', 'Female'])]

    # Validate email pattern
    df = df[df['email'].str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", na=False)]
    
    # Ensure ID uniqueness
    df = ensure_uniqueness(df, ['id'])
    logger.info(f"Transformed {len(df)} new records from customer table")

    return df


def transform_salesorder(df, pg_ods):
    # Check for valid customer IDs in the salesorder dataset
    existing_customers = pd.read_sql("SELECT DISTINCT id FROM customer", pg_ods)
    df = df[df['customer_id'].isin(existing_customers['id'])]
    logger.info(f"{len(df)} new records after checking for valid customer ids")

    # Drop required nulls
    df = drop_nulls(df, ['id', 'order_number', 'created_at', 'modified_at', 'order_total', 'total_qty_ordered'])

    # Ensure datetime fields are properly formatted
    # Make an explicit copy to prevent warning of setting values on a copy of a slice from a DataFrame
    df = df.copy()
    df.loc[:, 'created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df.loc[:, 'modified_at'] = pd.to_datetime(df['modified_at'], errors='coerce')
    df = drop_nulls(df, ['created_at', 'modified_at'])

    # Ensure uniqueness of id and order_number
    df = ensure_uniqueness(df, ['id', 'order_number'])
    logger.info(f"Transformed {len(df)} new records from salesorder table")

    return df


def transform_salesorderitem(df, pg_ods):
    existing_orders = pd.read_sql("SELECT DISTINCT id FROM salesorder", pg_ods)
    df = df[df['order_id'].isin(existing_orders['id'])]

    df = drop_nulls(df, ['order_id'])
    df = ensure_uniqueness(df, ['item_id', 'order_id'])

    # Make an explicit copy to prevent warning of setting values on a copy of a slice from a DataFrame
    df = df.copy()
    df.loc[:, 'created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df.loc[:, 'modified_at'] = pd.to_datetime(df['modified_at'], errors='coerce')
    df = drop_nulls(df, ['created_at', 'modified_at', 'price', 'qty_ordered', 'line_total'])

    # Extract and deduplicate product table entries
    product_df = df[['product_id', 'product_sku', 'product_name']].drop_duplicates(subset=['product_id'])

    # Remove already existing product_ids in ODS to prevent duplicates
    existing_products = pd.read_sql("SELECT DISTINCT product_id FROM product", pg_ods)
    new_products = product_df[~product_df['product_id'].isin(existing_products['product_id'])]

    new_products.to_sql('product', pg_ods, if_exists='append', index=False)
    logger.info(f"Loaded {len(new_products)} new products into ODS")

    # Drop product columns from salesorderitem
    df = df.drop(columns=['product_sku', 'product_name'])
    logger.info(f"Transformed {len(df)} new records from salesorderitem table")

    return df


# NOTE: This logic assumes append-only data since it's an e-commerce use case data that flows in an automated manner
# with no manual updates. If updates are possible, consider using modified_at field and compare row hashes.
def get_new_records(pg_engine, table, key_column, target_schema):
    try:
        # Get the latest value of the key column that was loaded in the last job run
        last_value_query = f"SELECT MAX({key_column}) FROM {target_schema}.{table}"
        last_value = pd.read_sql(last_value_query, pg_engine).iloc[0, 0]
        logger.info(f"Latest value of {key_column} column in the {table} table: {last_value}")

        # Construct the condition and query for incremental pull
        last_value = last_value or 0  # default to 0 if None
        condition = f"{key_column} > {last_value}"

        query = f"SELECT * FROM {table} WHERE {condition}"
        new_records_df = pd.read_sql(query, pg_engine)
        logger.info(f"Extracted {len(new_records_df)} new records from {table} table")
        return new_records_df
    except Exception as e:
        logger.error(f"Error extracting data from {table}: {e}")
        return pd.DataFrame()


def load_to_ods(df, table_name, pg_engine):
    if not df.empty:
        df['extracted_at'] = pd.Timestamp.utcnow()
        df.to_sql(table_name, pg_engine, if_exists="append", index=False)
        logger.info(f"Loaded {len(df)} records into {table_name} table in ODS")
    else:
        logger.info(f"No new records to load into {table_name} table")


def transform_and_load_to_ods():
    try:
        pg_landing, pg_ods = get_pg_engines()

        # Data transformation and load
        # Customer transformation
        customer = get_new_records(pg_landing, 'customer', 'id', 'operational_data_store')
        customer_cleaned = transform_customer(customer)
        load_to_ods(customer_cleaned, "customer", pg_ods)
        logger.info("Transformed customer table loaded to ODS")

        # Salesorder transformation
        salesorder = get_new_records(pg_landing, 'salesorder', 'id', 'operational_data_store')
        salesorder_cleaned = transform_salesorder(salesorder, pg_ods)
        load_to_ods(salesorder_cleaned, "salesorder", pg_ods)
        logger.info("Transformed salesorder table loaded to ODS")

        # Salesorderitem transformation
        salesorderitem = get_new_records(pg_landing, 'salesorderitem', 'item_id', 'operational_data_store')
        salesorderitem_cleaned = transform_salesorderitem(salesorderitem, pg_ods)
        load_to_ods(salesorderitem_cleaned, "salesorderitem", pg_ods)
        logger.info("Transformed salesorderitem table loaded to ODS")

    except Exception as e:
        logger.error(f"Transformation DAG failed: {e}")
        raise


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# Define DAG
dag2 = DAG(
    dag_id="transform_landing_data_and_load_to_ods",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# Define tasks

# Sensor to wait for DAG 1 completion
wait_for_dag1 = ExternalTaskSensor(
    task_id='wait_for_extract_dag',
    external_dag_id='extract_mysql_to_pg_landing',
    external_task_id='extract_data',
    timeout=600,
    mode='poke',
    dag=dag2
)

transform_and_load_task = PythonOperator(
    task_id="transform_and_load",
    python_callable=transform_and_load_to_ods,
    provide_context=False,
    dag=dag2,
)

wait_for_dag1 >> transform_and_load_task
