# SDE Assignment: Data Lake, Cassandra, API
SDE Assignment is a docker-compose ready-to-run project. The project consists of three parts.

1. Data Lake:
- Power by spark framework following a bronze(landingzone), silver(raw), and golden(use_case) zones for data ETL. 
- Use of Cassandra-driver as the client to 
- Use of open-source storage framework Delta Lake for ACID table enablement for spark
2. Cassandra:
- Bitnami docker image for Cassandra
3. RESTful API:
- Use of FastAPI to request data from Cassandra

## Requirements
- Git
- Docker desktop

## Quick Start
**NOTE**: The ETL jobs are set up in debug mode which processes only a slice of the assignment data. To run this in full mode change the variable in work/assignment/p2_lz_to_raw.ipynb located in the first cell as image below:

IMAGE_PLACEHOLDER

1. Clone git repo
2. Run docker-compose with the command below in the root folder
```bash
docker-compose up
```
3. Await for the download and creation of the three containers:
- sde_jupyter_spark: make sure to copy the link with the URL token to access Jupyter lab (as image below). You might need to change the root of the URL to localhost. Example: "http://127.0.0.1:8888/lab?token=xxxxxxxxxxxxxxxxxxx" -> "http://localhost:8888/lab?token=xxxxxxxxxxxxxxxxxxx"
IMAGE PLACEHOLDER
- sde_cassandra: await for the complete setup and start of Cassandra. The output of the terminal looks like this:
IMAGE PLACEHOLDER
- sde_fast_api: await until the message as below. As the API app requires Cassandra to start it retries three times every 60 seconds. If Cassandra is not ready within three minutes, the app will be an exit with code 0 and it will be required to run the command below
COMMAND PLACEHOLDER
4. Once all the containers are up and healthy. Open Jupyter lab using the URL with the token copied from the terminal. Open the notebook work/assignment/main.ipynb and run all the cells to run all pipelines end to end. 

**WARNINGS!**
- The sde_cassandra container will persist a db instance in the root folder of the project
- The ETL pipeline jobs will persist data within the folder ./assignment/data_lake 


## ETL Pipeline jobs
 **NOTE**:This project was developed as a Proof of Concept, hence the ETL jobs are a combination of .py and .ipynb files, the design decision was taken to enable faster debugging. A production-ready project would have been developed as per the orchestrator in mind, suchs a Airflow or Azure Data Factory and in turn adopting the correct practices such as designing the jobs in DAGs or Activities.
### p01_re_create_tables.ipynb
1. Pipeline create keyspace and required tables in Cassandra this by running internally the script init_scripts_cassandra.py
2. Create the delta tables required in the data lake.

### p02_start_w_stream_to_delta.ipynb
1. Defines and start a read and write stream in a foreacbatch mode which listens to the delta table r_event_sessions in raw zone
2. UPSERT the micro-batch data into uc_delta_event_sessions uc_case zone

### p03_start_w_stream_cass.ipynb
1. Defines and start a read and write stream in a foreachbatch mode which listens to the delta table r_event_sessions in raw zone 
2. Append the micro batches data into Cassandra instance tables.

### p1_url_to_lz.ipynb
1. Download and load the assingment zipped data and save in landingzone

### p2_lz_to_raw
1. Read the assignment zipped data from landingzone and write it into a delta table r_session_events. This is set up such that it **mimics a stream in micro-batches** per each data date. This is to bring the solution design closer to a real-life scenario.

## Pipeline Architecture
Platform current and desired solution architecture.

### AS-IS

### TO-BE

## Improvements ideas and known issues






