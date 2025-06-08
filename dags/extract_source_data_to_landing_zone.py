# DAG 1: Extract from MySQL and Load to PostgreSQL Landing Zone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import pymysql
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Establish SQLAlchemy engine connections to MySQL and PostgreSQL using Airflow connection IDs
def get_engines():
    mysql_conn = BaseHook.get_connection("mysql_conn")
    pg_conn = BaseHook.get_connection("postgres_conn")

    mysql_uri = f"mysql+pymysql://{mysql_conn.login}:{mysql_conn.password}@{mysql_conn.host}:{mysql_conn.port}/" \
                f"{mysql_conn.schema}"
    pg_uri = f"postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"

    mysql_engine = sqlalchemy.create_engine(
        mysql_uri,
        connect_args={"connect_timeout": 10},
        pool_size=5,
        max_overflow=0
    )
    pg_engine = sqlalchemy.create_engine(pg_uri, connect_args={"options": "-csearch_path=landing_zone"})
    return mysql_engine, pg_engine


# Extracts only new data from the MySQL source based on the last value in PostgreSQL landing zone
# Uses `id` for customer, and `created_at` for salesorder/salesorderitem. Retains rows with invalid timestamps.
def extract_incremental_data(mysql_engine, pg_engine, table, key_column):
    try:
        last_value_query = f"SELECT MAX({key_column}) FROM {table}"
        last_value = pd.read_sql(last_value_query, pg_engine).iloc[0, 0]
        logger.info(f"Latest value of {key_column} column in the landing zone: {last_value}")
        # Construct condition for incremental pull
        if key_column == "id":
            last_value = last_value or 0  # default to 0 if None
            condition = f"{key_column} > {last_value}"
        else:
            if pd.isna(last_value):
                last_value = "1970-01-01 00:00:00"  # default timestamp
            condition = f"({key_column} > '{last_value}' AND DATE({key_column}) < CURDATE()) OR {key_column} IS NULL"
        query = f"SELECT * FROM {table} WHERE {condition}"
        df = pd.read_sql(query, mysql_engine)
        # Attempt to parse `created_at`; invalid formats will become NaT but retained
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        logger.info(f"Extracted {len(df)} records from {table}")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from {table}: {e}")
        return pd.DataFrame()


# Loads extracted DataFrame to PostgreSQL landing zone under the specified table
def load_to_landing(df, table, pg_engine):
    if not df.empty:
        # Add timestamp to record the time of target table load
        df['extracted_at'] = pd.Timestamp.utcnow()

        df.to_sql(table, pg_engine, if_exists='append', index=False)
        logger.info(f"Loaded {len(df)} records into {table} in landing zone")
    else:
        logger.info(f"No new data to load into {table}")


# Coordinates the extraction and loading of new records for each table from MySQL to PostgreSQL landing zone
def extract_data():
    mysql_engine, pg_engine = get_engines()
    logger.info('Connections established with source and target database systems.')
    tables = {
        'customer': 'id',
        'salesorder': 'created_at',
        'salesorderitem': 'created_at'
    }

    for table, key_col in tables.items():
        logger.info(f"Processing table: {table}")
        df = extract_incremental_data(mysql_engine, pg_engine, table, key_col)
        load_to_landing(df, table, pg_engine)


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag1 = DAG(
    dag_id='extract_source_data_to_landing_zone',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_and_load_source_data',
    python_callable=extract_data,
    dag=dag1
)
