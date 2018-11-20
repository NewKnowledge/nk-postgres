from sqlalchemy import create_engine
from psycopg2 import connect 
from retrying import retry

def validate_db_config(db_config):
    assert 'host' in db_config
    assert 'dbname' in db_config
    assert 'port' in db_config
    assert 'user' in db_config
    assert 'password' in db_config

@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=1 * 1000, 
    stop_max_attempt_number=3
)
def wait_for_pg_service(db_config):
    return connect(**db_config)

def _config_hash(db_config): 
    return hash(frozenset(db_config))
