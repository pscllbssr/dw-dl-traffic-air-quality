from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

def min_max_scale(x):
    mn, mx = x.min(), x.max()
    x_scaled = (x - mn) / (mx - mn)
    return x_scaled

# Function to fetch data from MariaDB using MySqlHook
def fetch_data_from_mariadb(connection_id, query):
    hook = MySqlHook(mysql_conn_id=connection_id)
    df = hook.get_pandas_df(sql=query)
    return df


# Function to merge dataframes
def transform_data(ti):
    df = ti.xcom_pull(task_ids='fetch_data_from_lake')

    # rename and drop
    df.rename(columns={'value': 'original_value', 'unit': 'original_value_unit'}, inplace=True)
    df.drop('state', axis=1, inplace=True)

    # min max scale
    df['value'] = df.groupby('param')['original_value'].transform(min_max_scale)

    # round down to hour
    df['observed'] = df['observed'].dt.floor('h')

    # drop duplicates
    df = df.drop_duplicates(subset=['route_id', 'observed', 'param'])

    print(df.info())

    return df


# Function to store dataframe to MariaDB using MySqlHook
def store_data_to_warehouse(connection_id, table_name, **context):
    merged_df = context['ti'].xcom_pull(task_ids='transform_data')

    print('storing info:')
    print(merged_df.info())

    hook = MySqlHook(mysql_conn_id=connection_id)
    engine = hook.get_sqlalchemy_engine()
    merged_df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

query_air_quality = """
SELECT id, observed, route_id, param, unit, value, state
FROM `traffic-air-quality`.air_quality;
"""

dag = DAG(
    'unify_air-quality_data',
    default_args=default_args,
    description='transform air-quality data from data lake and store into data warehouse',
    schedule_interval=None,
    catchup=False,
    tags=["data-warehouse"]
)

fetch_data = PythonOperator(
    task_id='fetch_data_from_lake',
    python_callable=fetch_data_from_mariadb,
    op_kwargs={'connection_id': 'datalake-db', 'query': query_air_quality},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

store_data = PythonOperator(
    task_id='store_data_to_warehouse',
    python_callable=store_data_to_warehouse,
    op_kwargs={'connection_id': 'datawarehouse-db', 'table_name': 'air_quality'},
    provide_context=True,
    dag=dag,
)

# Setting up the task dependencies
fetch_data >> transform_task
transform_task >> store_data
