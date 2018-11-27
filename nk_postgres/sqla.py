from contextlib import contextmanager

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from nk_logger import get_logger
logger = get_logger(__name__)

from .util import validate_db_config, wait_for_pg_service, _config_hash

_config_to_sqla = {}

def _register_config(db_config):
    """ store engine and sessionmaker objects for this specific db config """
    if _config_hash(db_config) in _config_to_sqla:
        return

    validate_db_config(db_config)
    wait_for_pg_service(db_config)

    user = db_config['user']
    password = db_config['password']
    host = db_config['host']
    port = db_config['port']
    dbname = db_config['dbname']

    logger.debug(f'creating sqlalchemy connection pool (engine) for dbname = {dbname}')

    pre_ping = True
    if 'pre-ping' in db_config:
        pre_ping = db_config['pre-ping']

    engine = create_engine(f'postgresql+psycopg2://{user}:'
            f'{password}@{host}:{port}/{dbname}', 
            pool_pre_ping=pre_ping)
    _config_to_sqla[_config_hash(db_config)] = { 
            'engine': engine,
            'session': sessionmaker(bind=engine)
            }

def sqla_metadata(db_config, schema): 
    """ 
    get metadata reflected from the existing db, via the engine. 
    Only use this if you want to reflect. If you are constructing ORM classes
    via DeclarativeBase, just construct a client side metadata via class statements
    see tests/test_sqla.py for example patterns
    """ 
    _register_config(db_config)
    return MetaData(bind=_config_to_sqla[_config_hash(db_config)]['engine'], reflect=True, schema=schema)

def sqla_engine(db_config):
    """ retrieve the engine object out of the global registry """
    _register_config(db_config)
    return _config_to_sqla[_config_hash(db_config)]['engine']

@contextmanager
def sqla_cursor(db_config):
    """ 
    get a cursor object for sqlalchemy connections. uses the Session model.
    see tests for patterns, but generally: `with sqla_cursor(DB_CONFIG) as c:` 
    """
    _register_config(db_config)
    session = _config_to_sqla[_config_hash(db_config)]['session']()
    session.expire_on_commit = False
    try:
        yield session
        session.commit()
    except:
        logger.exception(f'exception handled during yielded sqla_cursor. rolling back and reraising.')
        session.rollback()
        raise
    finally:
        session.close() 


