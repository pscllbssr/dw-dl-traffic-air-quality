import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_transform_store_traffic_data',
    default_args=default_args,
    description='A DAG to fetch, process, and store traffic data from an API to a SQL database',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    tags=["data-lake"]
)


def fetch_data(**kwargs):
    token = Variable.get("API_TOKEN")
    url = "https://api.opentransportdata.swiss/TDP/Soap_Datex2/Pull"
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'Authorization': f'Bearer {token}',
        'SOAPAction': 'http://opentransportdata.swiss/TDP/Soap_Datex2/Pull/v1/pullMeasuredData'
    }
    payload = """<?xml version="1.0" encoding="UTF-8"?>
    <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                        xmlns:dx223="http://datex2.eu/schema/2/2_0">
        <SOAP-ENV:Body>
            <dx223:d2LogicalModel xsi:type="dx223:D2LogicalModel" modelBaseVersion="2">
                <dx223:exchange xsi:type="dx223:Exchange">
                    <dx223:supplierIdentification xsi:type="dx223:InternationalIdentifier">
                        <dx223:country xsi:type="dx223:CountryEnum">ch</dx223:country>
                        <dx223:nationalIdentifier xsi:type="dx223:String">OTD</dx223:nationalIdentifier>
                    </dx223:supplierIdentification>
                </dx223:exchange>
            </dx223:d2LogicalModel>
        </SOAP-ENV:Body>
    </SOAP-ENV:Envelope>"""
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200:
        kwargs['ti'].xcom_push(key='raw_data', value=response.text)
    else:
        raise ValueError(f"Failed to fetch data: {response.status_code} {response.text}")


def parse_and_process_data(**kwargs):
    xml_data = kwargs['ti'].xcom_pull(key='raw_data')
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        logging.error(f"Failed to parse XML data: {e}")
        raise

    ns = {'dx223': 'http://datex2.eu/schema/2/2_0'}
    data_records = []
    for site_meas in root.findall('.//dx223:siteMeasurements', ns):
        site_id_elem = site_meas.find('.//dx223:measurementSiteReference', ns)
        site_id = site_id_elem.get('id') if site_id_elem is not None else None
        measurement_time_elem = site_meas.find('.//dx223:measurementTimeDefault', ns)
        measurement_time = measurement_time_elem.text if measurement_time_elem is not None else None
        for measured_val in site_meas.findall('.//dx223:measuredValue', ns):
            index = measured_val.get('index')
            vehicle_type = 'Light Vehicles' if index in ['11',
                                                         '12'] else 'Heavy Vehicles' if index == '21' else 'Unclassified'
            vehicle_flow_rate_elem = measured_val.find('.//dx223:vehicleFlowRate', ns)
            vehicle_flow_rate = vehicle_flow_rate_elem.text if vehicle_flow_rate_elem is not None else None
            speed_elem = measured_val.find('.//dx223:speed', ns)
            speed = speed_elem.text if speed_elem is not None else None
            num_input_values_elem = measured_val.find('.//dx223:averageVehicleSpeed', ns)
            num_input_values = num_input_values_elem.get(
                'numberOfInputValuesUsed') if num_input_values_elem is not None else None
            data_records.append({
                'site_id': site_id,
                'measurement_time': measurement_time,
                'vehicle_type': vehicle_type,
                'vehicle_flow_rate': vehicle_flow_rate,
                'speed': speed,
                'num_input_values': num_input_values
            })
    df = pd.DataFrame(data_records)
    df['measurement_time'] = pd.to_datetime(df['measurement_time'], errors='coerce')
    if df['measurement_time'].dt.tz is None:
        df['measurement_time'] = df['measurement_time'].dt.tz_localize('UTC')
    df['measurement_time'] = df['measurement_time'].dt.tz_convert('Europe/Zurich')
    df['vehicle_flow_rate'] = pd.to_numeric(df['vehicle_flow_rate'], errors='coerce')
    df['speed'] = pd.to_numeric(df['speed'], errors='coerce')
    df['num_input_values'] = pd.to_numeric(df['num_input_values'], errors='coerce')

    # Handle NaN values: Fill NaN values with a default value or remove rows containing NaN values
    df.fillna({
        'vehicle_flow_rate': 0,
        'speed': 0,
        'num_input_values': 0
    }, inplace=True)

    traffic_data_unique = df.drop_duplicates(
        subset=['site_id', 'measurement_time', 'vehicle_flow_rate', 'speed', 'num_input_values'])
    aggregated_data = traffic_data_unique.groupby('site_id').agg({
        'vehicle_flow_rate': 'sum',
        'measurement_time': 'first',
        'num_input_values': 'sum',
        'speed': 'mean'
    }).reset_index()
    aggregated_data['location_id'] = aggregated_data['site_id'].str.extract(r'(.*)\.')[0]
    aggregated_data_by_location = aggregated_data.groupby('location_id').agg({
        'vehicle_flow_rate': 'sum',
        'measurement_time': 'first',
        'num_input_values': 'sum',
        'speed': 'mean'
    }).reset_index()
    station_ids = ['CH:0581', 'CH:0813', 'CH:0066', 'CH:0383', 'CH:0577', 'CH:0240', 'CH:0020', 'CH:03287', 'CH:0194',
                   'CH:0301', 'CH:0562']
    specific_locations_data = aggregated_data_by_location[aggregated_data_by_location['location_id'].isin(station_ids)]
    aggregated_specific_locations = specific_locations_data.groupby('location_id').agg({
        'vehicle_flow_rate': 'sum',
        'measurement_time': 'first',
        'num_input_values': 'sum',
        'speed': 'mean'
    }).reset_index()

    aggregated_specific_locations['measurement_time'] = aggregated_specific_locations['measurement_time'].dt.strftime(
        '%Y-%m-%d %H:%M:%S')
    kwargs['ti'].xcom_push(key='processed_data', value=aggregated_specific_locations.to_dict(orient='records'))


def store_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='processed_data')
    if not data:
        raise ValueError("No data to store")

    mysql_hook = MySqlHook(mysql_conn_id='datalake-db')
    conn = mysql_hook.get_conn()

    sql_query = """
        INSERT INTO traffic_flow_data (location_id, vehicle_flow_rate, measurement_time, num_input_values, speed) 
        VALUES (%s, %s, %s, %s, %s)
    """

    with conn.cursor() as cursor:
        for record in data:
            cursor.execute(sql_query, (
                record['location_id'], record['vehicle_flow_rate'], record['measurement_time'],
                record['num_input_values'],
                record['speed']))
        conn.commit()
    conn.close()


with dag:
    t1 = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='parse_and_process_data',
        python_callable=parse_and_process_data,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='store_data',
        python_callable=store_data,
        provide_context=True
    )

    t1 >> t2 >> t3
