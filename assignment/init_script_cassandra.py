#Author Adrian J

#Script to create the keyspace and tables for the job in cassandra db
from delta import *
from pyspark.sql import SparkSession 

builder = (
    SparkSession
    .builder
    .master("local")
    .appName("init_script_cassandra")
    .config("spark.cassandra.connection.host", "dockertests-cassandra-1")
    .config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
    # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    .config("spark.sql.catalog.casscatalog","com.datastax.spark.connector.datasource.CassandraCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

#create database for the assingment
spark.sql("""
DROP DATABASE IF EXISTS casscatalog.session_events CASCADE;""")
spark.sql("""
CREATE DATABASE IF NOT EXISTS casscatalog.session_events
  WITH DBPROPERTIES (class='SimpleStrategy', replication_factor='1');
""")
print("INFO: keyspace session_events re-created in cassandra db")
#START create table for the query of x hours
spark.sql("""
DROP TABLE IF EXISTS casscatalog.session_events.api_start_session_by_hour;""")
spark.sql("""
CREATE TABLE IF NOT EXISTS casscatalog.session_events.api_start_session_by_hour (
  country      STRING,
  player_id    STRING,
  session_id   STRING,
  ts           TIMESTAMP,
  event_date   DATE,
  cs_insert_ts TIMESTAMP)
  USING cassandra PARTITIONED BY (session_id)
  TBLPROPERTIES  (clustering_key='event_date.asc');
""")
#   CLUSTERED BY (event_date)
#    SORTED BY (ts);
#PARTITIONED  BY (event_date);
print("INFO: table ession_events.api_start_session_by_hour re-created in cassandra db")
#create table for completed sessions of a giveng specified player_id
spark.sql("""
DROP TABLE IF EXISTS casscatalog.session_events.api_completed_sessions;""")
spark.sql("""
CREATE TABLE IF NOT EXISTS casscatalog.session_events.api_completed_sessions (
  country      STRING,
  player_id    STRING,
  session_id   STRING,
  ts           TIMESTAMP,
  event_date   DATE,
  cs_insert_ts TIMESTAMP)
  USING cassandra PARTITIONED BY (player_id, session_id)
  TBLPROPERTIES (clustering_key='event_date.asc');
""")
#PARTITIONED  BY (event_date);
print("INFO: table ession_events.api_start_session_by_hour re-created in cassandra db")
spark.stop()