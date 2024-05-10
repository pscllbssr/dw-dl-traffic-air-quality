from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

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
    'fetch_and_store_data_in_db',
    default_args=default_args,
    description='A DAG to fetch data from an API and store it in a SQL database',
    schedule_interval='@daily',
    start_date=datetime(2024, 4, 12)
)

def fetch_data():
    # Simulated API data fetching
    return {
        'location_id': 'Location123',
        'vehicle_flow_rate': 100,
        'measurement_time': '2024-04-12 12:00:00',
        'num_input_values': 5,
        'speed': 45.5
    }

def store_data(**kwargs):
    # Retrieve the data passed by the previous task
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    # Connect to the database
    mysql_hook = MySqlHook(mysql_conn_id='datalake-db')
    conn = mysql_hook.get_conn()
    # Define the SQL query
    sql_query = """
        INSERT INTO traffic_flow_data (location_id, vehicle_flow_rate, measurement_time, num_input_values, speed) 
        VALUES (%s, %s, %s, %s, %s)
    """
    # Execute the SQL query
    with conn.cursor() as cursor:
        cursor.execute(sql_query, (data['location_id'], data['vehicle_flow_rate'], data['measurement_time'], data['num_input_values'], data['speed']))
        conn.commit()
    conn.close()

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
