from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'write_to_route_request',
    default_args=default_args,
    description='A DAG to write a record into the route-request table',
    schedule_interval='@once',
    tags=["development"]
)


# Function to write record into route-request table
def write_to_route_request():
    # Connect to the database
    mysql_hook = MySqlHook(mysql_conn_id='datalake-db')
    conn = mysql_hook.get_conn()

    # Define the SQL query
    sql_query = """
        INSERT INTO route_request (distance, duration, static_duration) 
        VALUES (%s, %s, %s)
    """
    # Example data to insert
    data = (2000, 100, 100)

    # Execute the SQL query
    with conn.cursor() as cursor:
        cursor.execute(sql_query, data)
        conn.commit()

    # Close the connection
    conn.close()


# Define the task to execute the Python function
write_to_route_request_task = PythonOperator(
    task_id='write_to_route_request_task',
    python_callable=write_to_route_request,
    dag=dag
)

# Set task dependencies
write_to_route_request_task
