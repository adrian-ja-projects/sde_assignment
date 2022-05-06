import os
from functools import lru_cache

from pydantic import BaseSettings, Field

class CassandraDbSettings(BaseSettings):
    cass_db_client_id: str
    cass_db_username: str
    cass_db_password: str
    cass_db_port : int
    cass_db_keyspace : str
    
    class Config:
        env_file = "db.env"

@lru_cache
def get_db_setting():
    return CassandraDbSettings()
