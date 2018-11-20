import datetime
from uuid import uuid4
from contextlib import contextmanager

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from .util import validate_db_config, wait_for_pg_service, _config_hash

_config_to_sqla = {}

def _register_config(db_config):
    if _config_hash(db_config) in _config_to_sqla:
        return

    validate_db_config(db_config)
    wait_for_pg_service(db_config)

    user = db_config['user']
    password = db_config['password']
    host = db_config['host']
    port = db_config['port']
    dbname = db_config['dbname']

    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}', pool_pre_ping=True)
    _config_to_sqla[_config_hash(db_config)] = { 
            'engine': engine,
            'session': sessionmaker(bind=engine)
            }

def sqla_metadata(db_config, schema): 
    """ get metadata from the bind """ 
    _register_config(db_config)
    return MetaData(bind=_config_to_sqla[_config_hash(db_config)]['engine'], reflect=False, schema=schema)

def sqla_engine(db_config):
    _register_config(db_config)
    return _config_to_sqla[_config_hash(db_config)]['engine']
    

@contextmanager
def sqla_cursor(db_config):
    _register_config(db_config)

    session = _config_to_sqla[_config_hash(db_config)]['session']()
    session.expire_on_commit = False
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close() 


