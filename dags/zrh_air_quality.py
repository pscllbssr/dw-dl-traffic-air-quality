from datetime import datetime, timedelta, date

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook


def fetch_data_from_api(execution_date, **kwargs):
    # load the growing file
    df = pd.read_csv(
        'https://data.stadt-zuerich.ch/dataset/ugz_luftschadstoffmessung_stundenwerte/download/ugz_ogd_air_h1_2024.csv',
        parse_dates=['Datum'])

    # Calculate the date of yesterday
    one_day_before_execution_date = execution_date.date() - timedelta(days=1)

    print(type(one_day_before_execution_date))

    print(f"Air Quality ZRH: getting entries for {one_day_before_execution_date.strftime('%Y-%m-%d')}")

    # Filter rows where 'Datum' equals to yesterday
    filtered_df = df[df['Datum'].dt.date == one_day_before_execution_date]

    filtered_df.rename(columns={
        'Datum': 'observed',
        'Standort': 'route_id',
        'Parameter': 'param',
        'Einheit': 'unit',
        'Wert': 'value',
        'Status': 'state'
    }, inplace=True)

    filtered_df.drop(columns=['Intervall'], inplace=True)

    return filtered_df


def save_to_database(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='fetch_data')

    print(df.info())

    # Connect to the database
    mysql_hook = MySqlHook(mysql_conn_id='datalake-db')
    conn = mysql_hook.get_conn()

    # SQL query to insert DataFrame data into the database table
    sql = f"INSERT INTO air_quality (observed, route_id, param, unit, value, state) VALUES (%s, %s, %s, %s, %s, %s)"

    # Execute the SQL query
    with conn.cursor() as cursor:

        # Iterate over each row in the DataFrame and execute the SQL query
        for index, row in df.iterrows():
            cursor.execute(sql, (row['observed'].to_pydatetime(), row['route_id'], row['param'], row['unit'], row['value'], row['state']))

        # Commit your changes in the database
        conn.commit()

    # Close the cursor and database connection
    cursor.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 25),
}

dag = DAG(
    'ingest_air_quality_zrh',
    default_args=default_args,
    description='Fetch data from API and save to MySQL database',
    schedule_interval='@daily',
    catchup=True,
    tags=["data-lake"]
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_from_api,
    provide_context=True,
    dag=dag,
)

save_to_database_task = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> save_to_database_task
