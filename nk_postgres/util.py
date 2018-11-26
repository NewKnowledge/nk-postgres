from time import sleep, time
from sqlalchemy import create_engine
from psycopg2 import connect 

def validate_db_config(db_config):
    assert 'host' in db_config
    assert 'dbname' in db_config
    assert 'port' in db_config
    assert 'user' in db_config
    assert 'password' in db_config

def wait_for_pg_service(db_config, max_wait_seconds=10.0):
    validate_db_config(db_config)

    timeout = max_wait_seconds
    start_time = time()
    while True: 
        if time() - start_time >= timeout:
            raise RuntimeError(f'postgres db {db_config["dbname"]} '
                    'failed to come up after {timeout} seconds')

        try:
            connect(**db_config)
            return # if connect succeeds, service is up
        except:
            pass

        sleep(0.1)

def _config_hash(db_config): 
    return hash(f'{db_config}')

