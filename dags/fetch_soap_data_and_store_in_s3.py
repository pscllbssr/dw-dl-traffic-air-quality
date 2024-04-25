from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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
    default_args=default_args,
    description='DAG to fetch and store traffic data from SOAP API',
    schedule_interval='@daily',
    start_date=datetime(2024, 4, 12),
    catchup=False,
)

def fetch_data():
    url = "https://api.opentransportdata.swiss/TDP/Soap_Datex2/Pull"
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'Authorization': 'Bearer eyJvcmciOiI2NDA2NTFhNTIyZmEwNTAwMDEyOWJiZTEiLCJpZCI6ImY0YzMxNDBkNDdiMTRlY2FiMjIwMDEyNjQ1NjFmY2Q0IiwiaCI6Im11cm11cjEyOCJ9',
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
        return response.text
    else:
        raise ValueError(f"Failed to fetch data: {response.status_code}, {response.text}")

def store_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    with open('traffic_data.xml', 'w', encoding='utf-8') as file:
        file.write(data)
    print("Data fetched and written to 'traffic_data.xml'")

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
