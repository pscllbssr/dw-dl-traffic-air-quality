import os
from datetime import datetime

import requests
from dotenv import load_dotenv
import json

load_dotenv()  # take environment variables from .env.

API_KEY = os.getenv("GMAPS_API_KEY")
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


def ingest_data():
    # maybe this can be passed in by the lambda function
    start = {
        "address": "Escher-Wyss-Platz, Zürich"
    }
    end = {
        "address": "Bucheggplatz, Zürich"
        # or even better: PlaceId (see geocodingResults in respone)
    }
    via = [{"location": {
        "latLng": {
            "latitude": 47.3943,
            "longitude": 8.5253
        }}, "via": True
    }
    ]

    route_response = get_route(start, end, via)

    current_timestamp = datetime.now()
    station_id = "station-Zch_Rosengartenstrasse"

    if route_response is not None:
        file_name = f'data/{current_timestamp.isoformat()}_route-{station_id}.json'

        with open(file_name, "w") as json_file:
            json.dump(route_response, json_file)


if __name__ == "__main__":
    ingest_data()
