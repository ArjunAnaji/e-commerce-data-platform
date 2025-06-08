from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime
import pandas as pd
import sqlalchemy
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Get Postgres engine connections
def get_pg_engines():
    pg_conn_obj = BaseHook.get_connection("postgres_conn")
    pg_uri = f"postgresql://{pg_conn_obj.login}:{pg_conn_obj.password}@{pg_conn_obj.host}:{pg_conn_obj.port}/" \
             f"{pg_conn_obj.schema}"
    pg_ods = sqlalchemy.create_engine(pg_uri, connect_args={"options": "-csearch_path=operational_data_store"})
    pg_datamart = sqlalchemy.create_engine(pg_uri, connect_args={"options": "-csearch_path=data_mart"})
    return pg_ods, pg_datamart


# Load table from operational_data_store with selected columns
def load_table(table_name, columns, engine_src):
    logger.info(f"Reading table: {table_name}")
    column_str = ", ".join(columns)
    query = f"SELECT {column_str} FROM {table_name}"
    df = pd.read_sql(query, engine_src)
    logger.info(f"Retrieved {len(df)} records from {table_name}")
    return df


def apply_data_quality_checks(final_df):
    # Data validation
    before = len(final_df)
    final_df.dropna(subset=[
        "item_id", "order_id", "order_total", "total_qty_ordered", "item_price", "item_qty_order", "item_unit_total"
    ], inplace=True)

    final_df.drop_duplicates(subset=["order_id", "item_id"], inplace=True)

    logger.info(f"Dropped {before - len(final_df)} rows due to nulls and duplicate rows.")

    # Ensure order_created_at is not in the future, which usually indicates bad data or timezone errors.
    before = len(final_df)
    final_df = final_df[final_df["order_created_at"] <= datetime.now()]
    logger.info(f"Dropped {before - len(final_df)} rows with future order_created_at")

    # Quantities and prices should not be negative or zero
    numeric_fields = ["order_total", "total_qty_ordered", "item_price", "item_qty_order", "item_unit_total"]
    for col in numeric_fields:
        before = len(final_df)
        final_df = final_df[final_df[col] > 0]
        logger.info(f"Dropped {before - len(final_df)} rows with negative values in column '{col}'")

    # Remove records with suspicious or disposable email domains (e.g., tempmail.com, example.com, etc.)
    before = len(final_df)
    disposable_domains = ["tempmail.com", "example.com", "test.com", "dummy.com"]
    final_df = final_df[~final_df["customer_email"].str.split("@").str[1].isin(disposable_domains)]
    logger.info(f"Dropped {before - len(final_df)} rows with dummy/test email domains")

    # Even if a field isn’t null, a blank string can still be bad data (especially in names or IDs). Remove them, if any
    before = len(final_df)
    final_df = final_df[final_df["customer_name"].str.strip() != ""]
    logger.info(f"Dropped {before - len(final_df)} rows with blank customer names.")

    return final_df


# Main callable to transform and load the flat table
def load_data_mart(**kwargs):
    pg_ods, pg_datamart = get_pg_engines()
    logger.info("Connected to PostgreSQL ODS and Data Mart schemas")

    # Load required columns from source tables
    customer_df = load_table("customer", ["id", "first_name", "last_name", "gender", "email"], pg_ods)
    order_df = load_table("salesorder", ["id", "customer_id", "order_number", "created_at", "order_total",
                                         "total_qty_ordered"],
                          pg_ods)
    item_df = load_table("salesorderitem", ["item_id", "order_id", "product_id", "price", "qty_ordered", "line_total"],
                         pg_ods)
    product_df = load_table("product", ["product_id", "product_sku", "product_name"], pg_ods)

    # Merge to form flat table
    logger.info("Merging all datasets to create a final flat set...")
    merged_df = item_df.merge(order_df, left_on="order_id", right_on="id", suffixes=("_item", "_order"))
    merged_df = merged_df.merge(customer_df, left_on="customer_id", right_on="id", suffixes=("", "_customer"))
    merged_df = merged_df.merge(product_df, on="product_id")

    # Rename columns
    final_df = merged_df.rename(columns={
        "item_id": "item_id",
        "order_id": "order_id",
        "order_number": "order_number",
        "created_at": "order_created_at",
        "order_total": "order_total",
        "total_qty_ordered": "total_qty_ordered",
        "customer_id": "customer_id",
        "first_name": "customer_first_name",
        "last_name": "customer_last_name",
        "gender": "customer_gender",
        "email": "customer_email",
        "product_id": "product_id",
        "product_sku": "product_sku",
        "product_name": "product_name",
        "price": "item_price",
        "qty_ordered": "item_qty_order",
        "line_total": "item_unit_total"
    })

    # Add customer_name column
    final_df["customer_name"] = final_df["customer_first_name"] + " " + final_df["customer_last_name"]

    # Select and validate final columns
    final_df = final_df[[
        "item_id", "order_id", "order_number", "order_created_at", "order_total", "total_qty_ordered",
        "customer_id", "customer_name", "customer_gender", "customer_email",
        "product_id", "product_sku", "product_name", "item_price", "item_qty_order", "item_unit_total"
    ]]

    logger.info(f"Final DataFrame prepared with {len(final_df)} records")

    final_df = apply_data_quality_checks(final_df)

    logger.info(f"Final record count after applying all validations and quality checks: {len(final_df)}")

    # Write to flat table in data mart
    final_df.to_sql("sales_order_item_flat", pg_datamart, if_exists="replace", index=False)
    logger.info(f"Loaded the dataset into data mart table: sales_order_item_flat")


# Airflow DAG definition
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag3 = DAG(
    dag_id="load_data_mart",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["data_mart"]
)

# Sensor to wait for upstream DAG completion
wait_for_dag2 = ExternalTaskSensor(
    task_id='wait_for_transform_dag',
    external_dag_id='transform_landing_data_and_load_to_ods',
    external_task_id='transform_and_load',
    timeout=600,
    mode='poke',
    dag=dag3
)

# Task to load and transform flat table
load_data_mart_task = PythonOperator(
    task_id="load_sales_order_item_flat_table",
    python_callable=load_data_mart,
    provide_context=True,
    dag=dag3
)

wait_for_dag2 >> load_data_mart_task
