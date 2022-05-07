# SDE Assignment: Data Lake, Cassandra, API
SDE Assignment is a docker-compose ready-to-run project. The project consist in three parts.

1. Data Lake:
- Power by spark framework following a bronce, silver, golden zones for data ETL. 
- Use of Cassandra-driver as the client to 
- Use of open-source storage framework Delta Lake for ACID table enablement for spark
2. Cassandra:
- Bitnami docker image for cassandra
3. RESTful API:
- Use of FastAPI to request data from cassandra db

## Requirements
- Git
- Docker desktop

## Quick Start

1. Clone git repo
2. Run docker-compose with the command below in the root folder
```bash
docker-compose up
```
3. Await for the download and creation of the three containers:
- sde_jupyter_spark: make sure to copy the link with the url token to access jupyter lab (as image below). You might need to change the root of the url to localhost. Example: "http://127.0.0.1:8888/lab?token=xxxxxxxxxxxxxxxxxxx" -> "http://localhost:8888/lab?token=xxxxxxxxxxxxxxxxxxx"
IMAGE PLACEHOLDER
- sde_cassandra: await for the complete setup and start of cassandra. Output of terminal looks like:
IMAGE PLACEHOLDER
- sde_fast_api: await until the message as below. As the api app requires cassandra to started it retries for three time every 60 seconds. If cassandra is not ready within three minutes, the app will be exit with code 0 and it will be required to run the command below
COMMAND PLACEHOLDER
4. Once all the container up and healthy. Open Jupyter lab using the url with the token copied from the terminal. Open the notebook work/assingment/main.ipynb and run all the cells to run all pipelines end to end. 

## pipeline runs 

