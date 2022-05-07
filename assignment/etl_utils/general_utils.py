#General utils for the project assignment for unity
#Author: Adrian Jimenez 2022-05
#TO-DO: Execption management from delta tables
from delta import *
from pyspark.sql import SparkSession 
from pyspark.sql import DataFrame as SparkDataFrame


def persist_table(spark: SparkSession, df: SparkDataFrame, path:str, partition_key:str, table_name: str):
    """
    method to persist table and save it as a delta table. 
    @spark: SparkSession
    @df: The DataFrame to persist
    @path: the base filesystem path to which the table will be persisted
    @partition_Key: The columns in which the table will partition
    ---
    TO-DO support of more than one partition key
    """
    df.write.format('delta').partitionBy(partition_key).save(path)
    print(f'INFO: Table {table_name} persisted in location {path}')

def create_table_in_metastore(spark: SparkSession, schema_name:str, table_name:str, path:str):
    """
    method to create in spark-warehouse
    @spark: SparkSession
    @schema_name: schema name in where table will be created
    @table_name: the table named to be checked
    @path: the base filesystem path to which the table will be persisted
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} USING DELTA LOCATION '{path}'")
    
def upsert_into_table(spark: SparkSession, inputDF: SparkDataFrame, table_name: str, schema_name: str, path: str, unique_id: str,  dl_insert_ts_column: str, upsert_mode: str) -> None:
    """Method to upsert delta tables that contain only and only one unique column.
    @inputDF: DataFrame that contain the records to be updated, appended or overwritten
    @table_name: Table name in the data lake
    @schema_name: Schema name in the data lake (databricks)
    @path: path location in the data lake adls
    @unique_id: column to compare in the upsert merge statement
    @dl_insert_ts_column: ts of write into the delta table. It doesn't get updated in update upsert_mode
    @upsert_mode:
    append -> only insert the rows where there is no unique column match
    update-> only updates matching unique columns, it updates all columns except for the dl_insert_ts_column
    overwrite-> overwrite the entire dataset with the inputDF
    -------------------------------------------------------------
    To-Do: Support of multi-column id
    """
    if upsert_mode == 'append': #Only append new data into the table
        existingTable = DeltaTable.forPath(spark, path)
        #upsert will only append new records
        (existingTable.alias('e')
         .merge(inputDF.alias('i'),
                f'e.{unique_id} == i.{unique_id}')
         .whenNotMatchedInsertAll()
        ).execute()
        #provide number of rows appended or 0
        metadata = (existingTable.history(1))
        print(f'INFO: Upsert for table {table_name} completed')
        print(f'INFO: Upsert Mode -> {upsert_mode}')
        print(f'INFO: metadata = {metadata.collect()[0]}')
    elif upsert_mode == 'update':
        existingTable = DeltaTable.forPath(spark, path)
        #upsert will only update matched id records
        (existingTable.alias('e')
         .merge(inputDF.alias('i'),
                f'e.{unique_id} == i.{unique_id}')
         .whenMatchedUpdate(
             #skip DL_INSERT_TS update
             set = {c : "i."+str(c) for c in inputDF.columns if c not in dl_insert_ts_column})
        ).execute()
        metadata = (existingTable.history(1))
        print(f'INFO: Upsert Mode -> {upsert_mode}')
        print(f"INFO: metadata = {metadata.collect()[0]}")
    elif upsert_mode == 'overwrite':
        (inputDF.write.option("overwriteSchema", "true").format('delta').mode("overwrite").save(path))
        print(f'INFO: Upsert Mode -> {upsert_mode}')
        print(f"INFO: table {schema_name}.{table_name} was successfully overwritten")
    else:
        e = "ERROR: incorrect upsert mode"
        raise Exception(e)
        
def object_exists(path: str)->bool:
    "TODO"
    pass