#Author Adrian J
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
cluster = Cluster(['dockertests-cassandra-1'], port=9042, auth_provider=auth_provider)
session = cluster.connect()

#drop keyspace if exists
drop_keyspace = session.execute("DROP KEYSPACE IF EXISTS session_events;")
print("INFO: table ession_events.api_start_session_by_hour re-created in cassandra db")

#create keyspace if exists
create_keyspace = session.execute("""CREATE KEYSPACE IF NOT EXISTS session_events 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")
print("INFO: keyspace session_events re-created in cassandra db")

#drop table api_started_sessions
drop_table = session.execute("DROP TABLE IF EXISTS session_events.api_start_session_by_hour;")

create_table_api_completd_sessions = session.execute("""
CREATE TABLE IF NOT EXISTS session_events.api_start_session_by_hour (
  country      text,
  player_id    text,
  session_id   text,
  ts           timestamp,
  event_date   date,
  cs_insert_ts timestamp,
  PRIMARY KEY (session_id, ts, event_date) )
  WITH CLUSTERING ORDER BY (ts DESC);
""")

print("INFO: table ession_events.api_start_session_by_hour re-created in cassandra db")

#drop table api_completed sessions
drop_table = session.execute("DROP TABLE IF EXISTS session_events.api_completed_sessions;")

create_table_api_completd_sessions = session.execute("""
CREATE TABLE IF NOT EXISTS session_events.api_completed_sessions (
  country      text,
  player_id    text,
  session_id   text,
  ts           timestamp,
  event_date   date,
  cs_insert_ts timestamp,
  PRIMARY KEY (player_id, ts) )
  WITH CLUSTERING ORDER BY (ts DESC);
""")