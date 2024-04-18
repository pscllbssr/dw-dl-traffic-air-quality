#!/bin/bash

# load .ENV-variables
export $(xargs < .env)

AIRFLOW_VERSION=2.9.0

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.9.0 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.8.txt

echo "My configuration:"
echo $AIRFLOW_HOME
echo $AIRFLOW_USER_NAME
echo $AIRFLOW__CORE__DAGS_FOLDER

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install 'apache-airflow[amazon]'

airflow db migrate

airflow users create \
    --username $AIRFLOW_USER_NAME \
    --firstname First \
    --lastname Last \
    --role Admin \
    --email EMAIL \
    --p $AIRFLOW_USER_PW

