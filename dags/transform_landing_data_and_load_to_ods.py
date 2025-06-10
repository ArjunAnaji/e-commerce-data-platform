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


def transform_salesorder(df, **kwargs):
    # Check for valid customer IDs in the salesorder dataset
    pg_ods = kwargs['pg_engine']
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


def transform_salesorderitem(df, **kwargs):
    # Check for valid order IDs in the salesorderitem dataset
    pg_ods = kwargs['pg_engine']
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

        # Define table configs with a flexibility to add customer parameters
        etl_config = [
            {
                'table': 'customer',
                'key_column': 'id',
                'transform_func': transform_customer,
                'kwargs': {}
            },
            {
                'table': 'salesorder',
                'key_column': 'id',
                'transform_func': transform_salesorder,
                'kwargs': {'pg_engine': pg_ods}
            },
            {
                'table': 'salesorderitem',
                'key_column': 'item_id',
                'transform_func': transform_salesorderitem,
                'kwargs': {'pg_engine': pg_ods}
            }
        ]

        for config in etl_config:
            # Data transformation and load
            table, key_column, transform_func, kwargs = config['table'], config['key_column'], \
                config['transform_func'], config['kwargs']
            new_records_df = get_new_records(pg_landing, table, config['key_column'], 'operational_data_store')

            if new_records_df is not None and not new_records_df.empty:
                logger.info(f"Extracted {len(new_records_df)} new records from {table} table")
                transformed_df = config['transform_func'](new_records_df, **config['kwargs'])
                logger.info(f"Transformed {len(transformed_df)} new records from {table} table")
                load_to_ods(transformed_df, table, pg_ods)
                logger.info(f"Loaded the cleaned and transformed data of {table} table into Operational Data Store.")
            else:
                logger.info(f"No new data found for {table} table, skipping transformation")

    except Exception as e:
        logger.error(f"Transformation DAG failed: {e}")
        raise


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define DAG
dag2 = DAG(
    dag_id="transform_landing_data_and_load_to_ods",
    default_args=default_args,
    schedule_interval='30 0 * * *',
    catchup=False,
    concurrency=5,
    max_active_runs=1
)

# Define tasks

# Sensor to wait for DAG 1 completion
wait_for_dag1 = ExternalTaskSensor(
    task_id='wait_for_extract_dag',
    external_dag_id='extract_source_data_to_landing_zone',
    external_task_id='extract_and_load_source_data',
    timeout=600,
    mode='poke',
    poke_interval=10,
    dag=dag2
)

transform_and_load_task = PythonOperator(
    task_id="transform_and_load",
    python_callable=transform_and_load_to_ods,
    provide_context=False,
    dag=dag2,
)

wait_for_dag1 >> transform_and_load_task
