# Step 1: Importing Modules
# To initiate the DAG Object
# Importing datetime and timedelta modules for scheduling the DAGs
import json
import os
from datetime import datetime

import requests
from airflow import DAG
from airflow.models import Variable
# Importing operators
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Step 2: Initiating the default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 18),
}

API_KEY = Variable.get("GMAPS_API_KEY")
ROUTES_API_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"


def get_route(start, end, via):
    url = f'{ROUTES_API_URL}?key={API_KEY}'

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


def ingest_route(start: str, stop: str, via_lat: float, via_lon: float):
    start = {
        "address": start
    }
    end = {
        "address": stop
    }
    via = [{"location": {
        "latLng": {
            "latitude": via_lat,
            "longitude": via_lon
        }}, "via": True
    }]

    route_response = get_route(start, end, via)

    return route_response


def save_to_file(**kwargs) -> None:
    data = kwargs['ti'].xcom_pull(task_ids='ingest_route')

    hook = S3Hook('S3_Conn')

    print(f"saving data {data}")

    # Convert dictionary to JSON
    json_data = json.dumps(data)

    # Retrieve S3 bucket name from Airflow variables
    s3_bucket_name = Variable.get("S3_BUCKET_NAME")

    current_timestamp = datetime.now()

    key = os.path.join(kwargs['key_prefix'], kwargs['station'], f'{current_timestamp.isoformat()}.json')

    print(f"write to {s3_bucket_name}/{key}")

    hook.load_string(json_data, key=key, bucket_name=s3_bucket_name)

    return {}


# Step 3: Creating DAG Object
dag = DAG(dag_id='ingest_gmaps_route',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False
          )

# Step 4: Creating task
# Creating first task
ingest_route = PythonOperator(task_id='ingest_route', dag=dag, python_callable=ingest_route,
                              op_kwargs={
                                  'start': 'Stampfenbachstrasse 52, 8006 Zürich',
                                  'stop': 'Schaffhauserstrasse 40, 8006 Zürich',
                                  'via_lat': 47.3868,
                                  'via_lon': 8.5398,
                              })

save_route = PythonOperator(task_id='save_to_file', dag=dag, python_callable=save_to_file,
                            op_kwargs={'key_prefix': 'gmaps_routes', 'station': "station-Zch_Rosengartenstrasse"},
                            provide_context=True)

# Step 5: Setting up dependencies
ingest_route >> save_route
