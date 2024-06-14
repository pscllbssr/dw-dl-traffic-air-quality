# Step 1: Importing Modules
# To initiate the DAG Object
# Importing datetime and timedelta modules for scheduling the DAGs
import ast
import json
import os
from datetime import datetime

import pytz
import requests
from airflow import DAG
from airflow.models import Variable
# Importing operators
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# constants used latter in code
API_KEY = Variable.get("GMAPS_API_KEY")
ROUTES_API_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
LOCAL_TZ = pytz.timezone("Europe/Zurich")

# Define routes
routes = [{
    "start": "Stampfenbachstrasse 52, 8006 Zürich",
    "stop": "Schaffhauserstrasse 40, 8006 Zürich",
    "via_lat": 47.3868,
    "via_lon": 8.5398,
    "station_id": "Zch_Stampfenbachstrasse"
}, {
    "start": "Splügenstrasse 14, 8002 Zürich",
    "stop": "Seebahnstrasse 110, 8003 ",
    "via_lat": 47.371,
    "via_lon": 8.5235,
    "station_id": "Zch_Schimmelstrasse"
}, {
    "start": "Heinrichstrasse 269, 8005 Zürich",
    "stop": "Bucheggstrasse 64, 8057 Zürich",
    "via_lat": 47.3952,
    "via_lon": 8.5261,
    "station_id": "Zch_Rosengartenstrasse"
}, {
    "start": "Susenbergstrasse 11, 8044 Zürich",
    "stop": "Susenbergstrasse 118, 8044 Zürich",
    "via_lat": 47.3815,
    "via_lon": 8.5659,
    "station_id": "Zch_Heubeeribüel"
}, {
    "start": "Nordstrasse 174, 8037 Zürich",
    "stop": "Nordstrasse 354, 8037 Zürich",
    "via_lat": 47.3943,
    "via_lon": 8.5253,
    "station_id": "Zch_Rosengartenbrücke"
}]

# DAG default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 18),
}

# fetch route data from API
def get_route(start, end, via_lat, via_lon):
    url = f'{ROUTES_API_URL}?key={API_KEY}'

    start = {
        "address": start
    }
    end = {
        "address": end
    }
    via = [{"location": {
        "latLng": {
            "latitude": via_lat,
            "longitude": via_lon
        }}, "via": True
    }]

    response = requests.post(url, json={
        'origin': start,
        'destination': end,
        'intermediates': via,
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE_OPTIMAL",
    }, headers={
        # specify the fields to return
        'X-Goog-FieldMask': 'routes.distanceMeters,routes.duration,routes.staticDuration'
    })

    if response.status_code == 200:
        data = response.json()
        return data
    elif response.status_code == 403:
        print("Error: Authentication failed, no valid API key. Unable to fetch directions.")
        return None
    elif response.status_code == 400:
        print("Error: Malformed request. Unable to fetch directions.")
        return None
    else:
        print("Error: Unable to fetch directions.")
        return None

# save fetched route data to database
def save_to_db(data, station):
    data = ast.literal_eval(data).get('routes')[0]

    # prepare data
    distance = int(data.get('distanceMeters'))
    duration = int(data.get('duration').replace('s', ''))
    static_duration = int(data.get('staticDuration').replace('s', ''))
    route_id = station
    current_timestamp = datetime.now(LOCAL_TZ)

    # Connect to the database
    mysql_hook = MySqlHook(mysql_conn_id='datalake-db')
    conn = mysql_hook.get_conn()

    # Define the SQL query
    sql_query = """
        INSERT INTO route_request (distance, duration, static_duration, route_id, observed) 
        VALUES (%s, %s, %s, %s, %s)
    """

    # prepare data to insert
    data = (distance, duration, static_duration, route_id, current_timestamp)

    # Execute the SQL query
    with conn.cursor() as cursor:
        cursor.execute(sql_query, data)
        conn.commit()

    # Close the connection
    conn.close()




# Define DAG
with DAG(dag_id='ingest_gmaps_routes', default_args=default_args, schedule_interval='@hourly', catchup=False,
         tags=["data-lake"]) as dag:
    # create tasks for every route
    for i, route in enumerate(routes):
        # Fetching the route ETA
        fetch_route_task_id = f'fetch_route_{i}_{route["station_id"]}'
        fetch_route_task = PythonOperator(
            task_id=fetch_route_task_id,
            python_callable=get_route,
            op_kwargs={'start': route['start'], 'end': route['stop'], 'via_lat': route['via_lat'],
                       'via_lon': route['via_lon'],
                       'key_prefix': 'gmaps_routes', 'station': route['station_id']},
            provide_context=True
        )

        # save to database
        save_to_db_task_id = f'save_to_db_{i}_{route["station_id"]}'
        save_to_db_task = PythonOperator(
            task_id=save_to_db_task_id,
            python_callable=save_to_db,
            op_kwargs={'data': "{{ task_instance.xcom_pull(task_ids='" + fetch_route_task_id + "') }}",
                       'station': route['station_id']},
            provide_context=True
        )

        # Set up task dependencies
        fetch_route_task >> save_to_db_task
