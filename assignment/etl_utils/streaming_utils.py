#Author: Adrian J
#Contain utils for the streaming jobs
from delta import *
from pyspark.sql import SparkSession 

def start_upsert_to_delta(microbatch_input, batchId, spark):
    "Method used for upsert start events into event delta table"
    sink_start_dl_schema = 'uc_assignment'
    sink_start_table_name = 'uc_delta_session_events'
    sink_start_dl_uc_path = f'/home/jovyan/work/data_lake/use_case/{sink_start_dl_schema}/{sink_start_table_name}'
    event_delta_table = DeltaTable.forPath(spark, sink_start_dl_uc_path)
    #merge statement 
    event_delta_table.alias("t").merge(
        microbatch_input.alias("i"),
        "i.session_id = t.session_id"
    ).whenMatchedUpdate(
        #add start session time and country
        set = {"country": "i.country", "start_ts": "i.ts", "DL_UPDATE_TS":"i.DL_UPDATE_TS"}
    ).whenNotMatchedInsert(
         values  = {"country": "i.country", "player_id":"i.player_id", "session_id": "i.session_id", "start_ts": "i.ts"
                , "session_status":"i.session_status", "DL_INSERT_TS": "i.DL_INSERT_TS", "DL_UPDATE_TS":"i.DL_UPDATE_TS", "EVENT_DATE":"i.EVENT_DATE"} 
    ).execute()


def end_upsert_to_delta(microbatch_input, epoch_id):
    "Method used for end start events into event delta table"
    #reading delta table
    sink_dl_schema = 'uc_assignment'
    sink_table_name = 'uc_delta_session_events'
    sink_dl_uc_path = f'/home/jovyan/work/data_lake/use_case/{sink_dl_schema}/{sink_table_name}'
    event_delta_table = DeltaTable.forPath(spark, sink_dl_uc_path)
    #merge statement
    (event_delta_table.alias("t").merge(
        microbatch_input.alias("i"),
        "i.session_id = t.session_id"
    ).whenMatchedUpdate(
        #add start session time and country
        set = {"end_ts": "i.ts", "DL_UPDATE_TS":"i.DL_UPTED_TS", "session_status":"i.session_status"}
    ).whenNotMatchedInsert(
         set = {"country": "i.country", "player_id":"i.player_id", "session_id": "i.session_id", "end_ts": "i.ts", 
                "session_status":"i.session_status", "DL_INSERT_TS": "i.DL_INSERT_TS", "DL_UPDATE_TS":"i.DL_UPDATE_TS", "EVENT_DATE":"i.EVENT_DATE"} )
    )
    
def end_events_write_to_cassandra(microbatch_input, epoch_id):
    (microbatch_input
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(table="api_completed_sessions", keyspace="session_events")
     .save()
    )

def start_events_write_to_cassandra(microbatch_input, epoch_id):
    (microbatch_input
     .write
     .format('org.apache.spark.sql.cassandra')
     .mode('append')
     .options(table="api_start_session_by_hour", keyspace="session_events")
     .save()
    )