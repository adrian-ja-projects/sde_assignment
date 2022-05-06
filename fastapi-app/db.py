import os
import pathlib

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine.connection import register_connection, set_default_connection

from . import config

settings = config.get_db_setting()

CASSANDRA_DB_CLIENT_ID = settings.cass_db_client_id
CASSANDRA_DB_DB_USERNAME = settings.cass_db_username
CASSANDRA_DB_PASSWORD = settings.cass_db_password
CASSANDRA_DB_PORT = settings.cass_db_port
CASSANDRA_DB_KEYSPACE = settings.cass_db_keyspace


def get_cluster_conn():
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_DB_DB_USERNAME, password=CASSANDRA_DB_PASSWORD)
    cluster = Cluster([CASSANDRA_DB_CLIENT_ID], port=CASSANDRA_DB_PORT, auth_provider=auth_provider)
    return cluster

def get_session():
    cluster = get_cluster_conn()
    session = cluster.connect(CASSANDRA_DB_KEYSPACE)
    register_connection(str(session), session=session)
    set_default_connection(str(session))
    return session