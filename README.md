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
**NOTE**: The ETL jobs are set up in debug mode which processes only a slice of the assignment data. To run this in full mode change the variable in work/assignment/p2_lz_to_raw.ipynb located in the first cell to ```False```. Refer to image below:
![debug_true](https://user-images.githubusercontent.com/54493284/167284020-ab97afbd-a0ab-4c8d-856e-6c78709466aa.PNG)

**Steps**
1. Clone repo

2. Run docker-compose with the command below in the root folder
```bash
docker-compose up
```
3. Await for the download and creation of the three containers:
- sde_jupyter_spark: make sure to copy the link with the URL token to access Jupyter lab (as image below). You might need to change the root of the URL to localhost. Example: "http://127.0.0.1:8888/lab?token=xxxxxxxxxxxxxxxxxxx" -> "http://localhost:8888/lab?token=xxxxxxxxxxxxxxxxxxx"
![jupyter_token](https://user-images.githubusercontent.com/54493284/167284029-b480f93b-e122-407f-946f-764b72f5d13d.PNG)
- sde_cassandra: await for the complete setup and start of Cassandra. The output of the terminal looks like this:
![cassandra_ready](https://user-images.githubusercontent.com/54493284/167284037-12b871b3-82b0-49c5-b502-5b6ac3ac112a.PNG)
- sde_fast_api: await until the message as below.
![sde_fastapi_started](https://user-images.githubusercontent.com/54493284/167284574-f7ecacaa-cdce-4ee1-abb7-7796b4d05a63.PNG) 

The API app requires Cassandra to be up a running for a successful connection, if host is unavailable it retries three times every 60 seconds. If Cassandra is not ready within three minutes, the app will be an exit with code 0 and it will be required to run the command below
```bash
docker exec -it sde_fast_api uvicorn main:app --host 0.0.0.0 --reload
```
4. Once all the containers are up and healthy. Open Jupyter lab using the URL with the token copied from the terminal. Open the notebook work/assignment/main.ipynb and run all the cells to run all pipelines end to end. 

**WARNINGS!**
- The sde_cassandra container will persist a db instance in the root folder of the project
- The ETL pipeline jobs will persist data within the folder ./assignment/data_lake 


## ETL Pipeline jobs
 **NOTE**:This project was developed as a Proof of Concept, hence the ETL jobs are a combination of .py and .ipynb files, the design decision was taken to enable faster debugging. A production-ready project would have been developed as per the orchestrator in mind, suchs a Airflow or Azure Data Factory and in turn adopting the correct practices such as designing the jobs in DAGs or Activities.
### p01_re_create_tables.ipynb
1. Pipeline create keyspace  
2. Creates required tables in Cassandra this by running internally the script init_scripts_cassandra.py
3. Create the delta tables required in the data lake.

### p02_start_w_stream_to_delta.ipynb
1. Defines and start a readstream from r_event_session in raw zone
2. Defines and start a writestream in a foreacbatch 
3. UPSERT the micro-batch data into uc_delta_event_sessions uc_case zone

### p03_start_w_stream_cass.ipynb
1. Defines and start a readstream from r_event_session in raw zone
2. Define and start a writestream in a foreachbatch
3. Append the micro batches data into Cassandra instance tables.

### p1_url_to_lz.ipynb
1. Download and load the assingment zipped data and 
2. Save it in landingzone

### p2_lz_to_raw
1. Read the assignment zipped data from landingzone  
2. Write it into a delta table r_session_events. This is set up such that it **mimics a stream in micro-batches** per each data date. This is to bring the solution design closer to a real-life scenario.

## API
Once the above pipelines are complete, you should be able to access the API UI at the following URL: http://localhost:8000/docs 

### /health
Responds 'healthy' if the API app is running 

### /models/completed-sessions/{player_id}
Has a parameter ```player_id``` and accepts strings you will see a JSON respond like
```
{
  "items": [
    {
      "player_id": "string",
      "session_id": "string",
      "country": "string",
      "ts": "2022-05-08T03:18:25.196Z"
    }
  ],
  "count": 0
}
```
### /models/started_sessions/{country}/{hours}
Has two parameters ```country``` accepts strings and ```hours``` accepts integers which is the number of x hours from the last started session. Max accepted hours is 24. you will see a JSON response like
```
{
  "items": [
    {
      "player_id": "string",
      "session_id": "string",
      "country": "string",
      "ts": "2022-05-08T03:29:22.793Z"
    }
  ],
  "count": 0
}
```
***NOTE*** for demo purposes current time is fixed to 2016-11-06. 


## Pipeline Architecture
Current and desired solution architecture.

### AS-IS
![architecture_asis](https://user-images.githubusercontent.com/54493284/167284046-0f34dd25-e29a-46a2-9244-64c69159d95a.PNG)
### TO-BE
![architecture_tobe](https://user-images.githubusercontent.com/54493284/167284043-4a19ffe7-6388-4fd3-9d5f-c47361a02444.PNG)

**Changes**
1. Implementation of an orchestration/platform/integration tool as the main tool to manage the jobs. For example either of the below:
- Airflow: to orchestrate and trigger the containerized jobs and monitor progress
- Azure Data Factory: to integrate and orchestrate jobs
- Databricks + (Airflow, Azure Data Factory): launch cluster images with dependencies for jobs
2. Lakehouse architecture powered by spark and Delta lake for acid tables. 
3. Data quality engine for batch/scheduled jobs. For example Amazon Dequee
4. Metadata DB: Capture logs and data quality states for downstream consumers. 

## Improvements ideas and known issues
Apart from the improvements with the implementation of production like TO-BE architecture. Below are some improvements to the AS-IS and overall application.



- [ ] Set ETL_DEBUG as an environment variable to be managed in the docker-compose. This is to replace the debug mode mentioned above
2. Improve the writing performance of the UPSERT into uc_delta_session_events by:
- Specify the partition in the merge statement. Delta table uc_delta_session_events and its writestream both have a new attribute column EVENT_DATE, if this is specified as new_record_microbatch.EVENT_DATE =< delta_table.EVENT_DATE, spark would only need to look up session_id on the last couple of partitions.
- Reduce the microbatch records. Currently, the micro-batch from assignment data is a loop in event date to mimic a micro-batch source. However, these microbatches contain a high number of records that makes the append or upsert foreachbatch not performant enough. As mentioned above, this was to mimic a real-life scenario and architecture. 
3. Increase the scope of the pytest.
- Currently, the pytest only covers spark session creation.
- Extend to fastAPI app
- Extend to Cassandra connection
4. Use of keys and password to access the FastAPI
5. Enable Paging Large Queries:
- The current API app does not support paging large queries. 
- Cassandra-driver for python contains a method to handle queries over 5000 rows.
6. Improve API latency
- Tunning Cassandra table definition: 1. add support for UUID data type in the transformation 2. Test performance on a different table partition and bucketing 3. Schedule scripts to archive data older than one year.
- Reduce the response parsing time by using Cassandra-driver query factories to return a more performant response.



