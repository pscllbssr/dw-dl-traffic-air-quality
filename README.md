# Air Quality & Traffic Analysis 

Project in Data Lake & Data Warehouse module at HSLU, FS24.

## Setup

We mostly follow the [airflow-docker-setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). Make sure you have docker installed.

### initialize

On Linux, we have to create the folders first

```shell
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

To initialize the database run

```shell
docker compose up airflow-init
```

### run

Once initialized, start things up. May take 1-2 minutes. 

```shell
docker compose up
```

Now you should be able to access the web-interface at <http://localhost:8080>

### connect S3

In order to use S3 we need to do the following:

1) connect our bucket via web-interface (Admin --> Connections --> Add a new record).
2) add the bucket name as variable  (Admin --> Variables --> Add a new record). Use the key `S3_BUCKET_NAME`.

In code you may now use it as follows:
```python
s3_bucket_name = Variable.get("S3_BUCKET_NAME")
```

### other variables

Make sure to add all other necessary variables:

- `GMAPS_API_KEY`
- ...

## Develop

Create new DAGs in the `/dags`-folder. As it is mounted as a volume to the docker-container, new dags should appear in the web-interface after some seconds. 

TODO: run DAGS from the CLI with docker setup.