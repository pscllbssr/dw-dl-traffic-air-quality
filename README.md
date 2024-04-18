# Air Quality & Traffic Analysis 

Project in Data Lake & Data Warehouse module at HSLU, FS24.

## Setup locally

### install
Make sure you have a virtual-environment ready. This will install airflow running with a SQLite-Database, which is not recommended for production.
```shell
# set airflow directory to the current directory
export AIRFLOW_HOME=$(pwd)/airflow

# set dag home folder
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags

# install with script
./install-airflow-locally.sh
```

### run

```shell
airflow webserver --port 8080

airflow scheduler
```