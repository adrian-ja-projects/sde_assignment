#Author: Adrian J 2022-05
from cassandra.cluster import Cluster, ExecutionProfile, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine.connection import register_connection, set_default_connection
from tenacity import retry, stop_after_attempt, retry_if_exception_type, wait_fixed

from api_config import config #TO-DO structure project better

settings = config.get_db_setting()

CASSANDRA_DB_CLIENT_ID = settings.cass_db_client_id
CASSANDRA_DB_DB_USERNAME = settings.cass_db_username
CASSANDRA_DB_PASSWORD = settings.cass_db_password
CASSANDRA_DB_PORT = settings.cass_db_port
CASSANDRA_DB_KEYSPACE = settings.cass_db_keyspace

#retry used as patch to handle exception on host not available for the delay on the cassandra
#container to build TO-DO find the correct way to handle using cassandra driver. 
#risk put stress on the cluster

def get_cluster_conn():
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_DB_DB_USERNAME, password=CASSANDRA_DB_PASSWORD)
    profile_long = ExecutionProfile(request_timeout=30)
    cluster = Cluster([CASSANDRA_DB_CLIENT_ID], port=CASSANDRA_DB_PORT, auth_provider=auth_provider, execution_profiles={'long': profile_long})
    return cluster
    
@retry(stop=stop_after_attempt(3), wait=wait_fixed(60), retry=retry_if_exception_type(NoHostAvailable))
def get_session():
    cluster = get_cluster_conn()
    session = cluster.connect()
    register_connection(str(session), session=session)
    set_default_connection(str(session))
    return session