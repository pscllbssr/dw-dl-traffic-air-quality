import json
import os
from datetime import datetime

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_to_s3(key_prefix: str) -> None:
    hook = S3Hook('S3_Conn')

    # Your Python dictionary
    data = {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3",
        "key4": "value4"
    }

    # Convert dictionary to JSON
    json_data = json.dumps(data)

    # Retrieve S3 bucket name from Airflow variables
    s3_bucket_name = Variable.get("S3_BUCKET_NAME")

    key = os.path.join(key_prefix, 'my_new_example.json')

    print(f'upload to {s3_bucket_name}/{key}')

    hook.load_string(json_data, key=key, bucket_name=s3_bucket_name)


with DAG(
        dag_id='s3_dag',
        schedule_interval='@daily',
        start_date=datetime(2024, 4, 12),
        catchup=False,
        tags=["development"]
) as dag:
    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'key_prefix': 'example',
        }
    )
