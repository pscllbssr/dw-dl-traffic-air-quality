from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_data',
    description='A DAG to fetch data from an API and store it to a local file',
    schedule_interval='@daily',
    start_date=datetime(2024, 4, 12),
    tags=["development"]
)


def fetch_data():
    # Fetch data from the API
    data = {'hello': 'bar'}
    return data


def store_data(**kwargs):
    # Retrieve the data passed by the previous task
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    # Store received JSON data to a local file
    print(f"write {data} to file")


fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

fetch_task >> store_task
