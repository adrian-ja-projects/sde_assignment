from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
import pytest


def test_cassandra_conn():
    """Testing cassandra connection response
    TO-DO create class to instanciate cassandra connection properly include methods in test
    """
    auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
    cluster = Cluster(['sde_cassandra'], port=9042, auth_provider=auth_provider)
    session = cluster.connect()
    #return a dict response from query
    session.row_factory = dict_factory
    #get system metadata
    row = session.execute("SELECT * FROM system.local").one()
    assert row['key'] == 'local'
    assert row['rpc_port'] == 9042
    assert row['rpc_address'] == '0.0.0.0' 