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
2. Run docker-compose
