from datetime import datetime, timedelta, date

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook


def fetch_data_from_api(execution_date, **kwargs):

    # load the growing file
    df = pd.read_csv(
        'https://data.stadt-zuerich.ch/dataset/sid_dav_verkehrszaehlung_miv_od2031/download/sid_dav_verkehrszaehlung_miv_OD2031_2024.csv',
        usecols=['MSID', 'MessungDatZeit', 'LieferDat', 'AnzFahrzeuge',
                 'AnzFahrzeugeStatus'],
        parse_dates=['MessungDatZeit', 'LieferDat']
    )

    # Calculate the date of yesterday
    two_days_before_today = execution_date.date() - timedelta(days=2)

    print(f"Traffic flow ZRH: getting entries for {two_days_before_today.strftime('%Y-%m-%d')}")

    # Filter rows where 'Datum' equals to yesterday
    filtered_df = df[df['MessungDatZeit'].dt.date == two_days_before_today]

    return filtered_df


def save_to_database(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='fetch_data')

    # replace nan with -1 for MySQL
    df['AnzFahrzeuge'] = df['AnzFahrzeuge'].apply(lambda x: -1 if pd.isna(x) else x)

    print(df.info())

    # Create a list of tuples containing the data to be inserted
    data_to_insert = [
        (row['MSID'], row['MessungDatZeit'], row['LieferDat'], row['AnzFahrzeuge'], row['AnzFahrzeugeStatus']) for
        index, row in df.iterrows()]

    # Connect to the database
    mysql_hook = MySqlHook(mysql_conn_id='datalake-db')
    conn = mysql_hook.get_conn()

    # SQL query to insert DataFrame data into the database table
    sql = "INSERT INTO zrh_traffic_flow (station_id, observed, data_published, vehicle_count, vehicle_count_status) VALUES (%s, %s, %s, %s, %s)"

    # Execute the bulk insert operation
    with conn.cursor() as cursor:
        cursor.executemany(sql, data_to_insert)
        # Commit changes
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
    'ingest_zrh_traffic_flow',
    default_args=default_args,
    description='Fetch data from Open Data Zurich and save to MySQL database',
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
