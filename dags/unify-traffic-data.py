from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
# from sklearn import preprocessing
import pandas as pd

ZURICH_TRAFFIC_STATIONS = [
    'Z038M001', 'Z038M002', # Rosengarten*
    'Z033M001', 'Z033M002', 'Z028M001', 'Z028M002', # Stampfenbachstrasse
    'Z106M001', 'Z106M002', # Heubeeribüel
    'Z068M001', 'Z068M002' # Schimmelstrasse
]

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
def merge_dataframes(ti):
    df1 = ti.xcom_pull(task_ids='fetch_data_from_source1')
    df2 = ti.xcom_pull(task_ids='fetch_data_from_source2')
    df3 = ti.xcom_pull(task_ids='fetch_data_from_source3')

    # min_max_scaler = preprocessing.MinMaxScaler()

    # transfrom google routes
    df1['time_per_distance'] = df1.duration / df1.distance
    #df1['value'] = min_max_scaler.fit_transform(df1[['time_per_distance']].values)
    df1['value'] = min_max_scale(df1.time_per_distance)
    df1 = df1[['route_id', 'value', 'observed', 'time_per_distance']]
    df1.rename(columns={'route_id': 'station_id', 'time_per_distance': 'original_value'}, inplace=True)
    df1.loc[:, 'origin'] = 'google-routes'
    df1.loc[:, 'original_value_unit'] = 'travel time [s/m]'

    # transform zurich traffic data
    #df2['value'] = min_max_scaler.fit_transform(df2[['vehicle_flow_rate']].values)
    df2['value'] = min_max_scale(df2['vehicle_count'])
    df2 = df2[['station_id', 'value', 'observed', 'vehicle_count']]
    df2.rename(columns={'vehicle_count': 'original_value'}, inplace=True)
    df2['origin'] = 'zurich-traffic-counters'
    df2['original_value_unit'] = 'vehicles per hour'

    # transform federal traffic data
    df3['value'] = min_max_scale(df3.vehicle_flow_rate)
    df3 = df3[['location_id', 'measurement_time', 'value', 'vehicle_flow_rate']]
    df3.rename(
        columns={'location_id': 'station_id', 'measurement_time': 'observed', 'vehicle_flow_rate': 'original_value'},
        inplace=True)
    df3.loc[:, 'origin'] = 'federal-traffic-counters'
    df3['original_value_unit'] = 'vehicles per hour'

    print(df1.info())
    print(df2.info())
    print(df3.info())

    # merge the sources
    merged_df = pd.concat([df1, df2, df3], ignore_index=True)

    # round down to hour
    merged_df['observed'] = merged_df['observed'].dt.floor('h')

    # drop duplicates
    merged_df = merged_df.drop_duplicates(subset=['station_id', 'observed'])

    return merged_df


# Function to store dataframe to MariaDB using MySqlHook
def store_data_to_mariadb(connection_id, table_name, **context):
    merged_df = context['ti'].xcom_pull(task_ids='merge_dataframes')

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

dag = DAG(
    'unify_traffic_data',
    default_args=default_args,
    description='Unify different traffic sources from data lake and store into data warehouse',
    schedule_interval=None,
    catchup=False
)

query_routes = """SELECT rr.distance, rr.duration, rr.static_duration, rr.observed, rr.route_id
FROM route_request rr
WHERE observed IS NOT NULL
"""

query_zrh_traffic = f"""SELECT station_id, observed, data_published, vehicle_count, vehicle_count_status
FROM `traffic-air-quality`.zrh_traffic_flow
WHERE vehicle_count_status != 'Fehlend' 
AND station_id IN ('{"','".join(ZURICH_TRAFFIC_STATIONS)}');
"""

query_federal_traffic = """SELECT id, location_id, vehicle_flow_rate, measurement_time, num_input_values, speed
FROM `traffic-air-quality`.traffic_flow_data;
"""

fetch_data1 = PythonOperator(
    task_id='fetch_data_from_source1',
    python_callable=fetch_data_from_mariadb,
    op_kwargs={'connection_id': 'datalake-db', 'query': query_routes},
    dag=dag,
)

fetch_data2 = PythonOperator(
    task_id='fetch_data_from_source2',
    python_callable=fetch_data_from_mariadb,
    op_kwargs={'connection_id': 'datalake-db', 'query': query_zrh_traffic},
    dag=dag,
)

fetch_data3 = PythonOperator(
    task_id='fetch_data_from_source3',
    python_callable=fetch_data_from_mariadb,
    op_kwargs={'connection_id': 'datalake-db', 'query': query_federal_traffic},
    dag=dag,
)

merge_task = PythonOperator(
    task_id='merge_dataframes',
    python_callable=merge_dataframes,
    provide_context=True,
    dag=dag,
)

store_data = PythonOperator(
    task_id='store_data_to_mariadb',
    python_callable=store_data_to_mariadb,
    op_kwargs={'connection_id': 'datawarehouse-db', 'table_name': 'traffic'},
    provide_context=True,
    dag=dag,
)

# Setting up the task dependencies
fetch_data1 >> merge_task
fetch_data2 >> merge_task
fetch_data3 >> merge_task
merge_task >> store_data
