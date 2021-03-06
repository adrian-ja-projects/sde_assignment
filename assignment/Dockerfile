# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
# taken from https://github.com/jupyter/docker-stacks/blob/master/pyspark-notebook/Dockerfile
FROM jupyter/pyspark-notebook:spark-3.2.1

ARG DELTA_CORE_VERSION="1.2.0"
RUN pip install --quiet --no-cache-dir delta-spark==${DELTA_CORE_VERSION} && \
     fix-permissions "${HOME}" && \
     fix-permissions "${CONDA_DIR}"

ARG PYTEST_VERSION="7.1.2" 
RUN pip install --quiet --no-cache-dir pytest==${PYTEST_VERSION} && \
     fix-permissions "${HOME}" && \
     fix-permissions "${CONDA_DIR}"

ARG CASSANDRA_DRIVE_VERSION="3.25.0" 
RUN pip install --quiet --no-cache-dir cassandra-driver==${CASSANDRA_DRIVE_VERSION} && \
     fix-permissions "${HOME}" && \
     fix-permissions "${CONDA_DIR}"

USER root

#set spark-cassandra connector package
ENV  PYSPARK_SUBMIT_ARGS="--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,io.delta:delta-core_2.12:1.2.0 pyspark-shell"

#set spark default config for delta and spark-cassandra connector
RUN echo 'spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension' >> "${SPARK_HOME}/conf/spark-defaults.conf" && \
    echo 'spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog' >> "${SPARK_HOME}/conf/spark-defaults.conf" 

USER ${NB_UID}