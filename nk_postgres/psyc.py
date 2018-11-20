""" Session model for a direct psycopg2 connection """
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor, execute_values

from nk_logger import get_logger
logger = get_logger(__name__, level='DEBUG')

from .util import validate_db_config, wait_for_pg_service, _config_hash

def _complete_config(db_config, sslmode='require', cursor_factory=None):
    """ fill out missing config values with defaults then return the completed config object """
    validate_db_config(db_config)
    extras = dict(
            sslmode=sslmode,
            cursor_factory=cursor_factory
    )
    extras.update(db_config)
    return extras

_config_to_mgr = {}
def _register_config(db_config): 
    """ create a connection manager for this specific config """
    if _config_hash(db_config) in _config_to_mgr:
        return
    validate_db_config(db_config)
    wait_for_pg_service(db_config)
    _config_to_mgr[_config_hash(db_config)] = PostgresConnectionManager(db_config)

@contextmanager
def psycopg_cursor(db_config, sslmode='require', cursor_factory=None):
    """ use `with psycopg_cursor(DB_CONFIG) as c:` to use the connection pool """
    db_config = _complete_config(db_config, sslmode=sslmode, cursor_factory=cursor_factory)
    print('cursor for', db_config, _config_hash(db_config))
    _register_config(db_config)
    with _config_to_mgr[_config_hash(db_config)].cursor() as cursor:
        yield cursor

class PostgresConnectionManager:
    """ A connection manager for psycopg connection pools. instantiate this per-config """

    def __init__(self, db_config, name=None):
        """ store the db config and attempt to provide a good name for the connection """
        self.name = name or db_config['dbname'] or 'db'
        self.db_config = db_config
        self.pool = self.get_connection_pool()

    @contextmanager
    def cursor(self):
        """ yield a cursor that supports c.execute() and execute_values(c). 
        This function gets a connection from the pool, yields the cursor, then
        returns the connection to the pool. If it encounters an 
        psycopg2.InterfaceError, it resets the connection pool, then errors out.
        
        TODO: restore retry wrapper? 
        - The retry wrapper will catch the error and retry with exponential backoff 
        - until it hits the max number of retries. 
        """
        try:
            # get a connection from the pool and execute the given query with that connection
            logger.debug(f"getting db connection from {self.name} connection pool")
            db_conn = self.pool.getconn()

            #yield db_conn
            with db_conn.cursor(cursor_factory=self.db_config['cursor_factory']) as cursor:
                logger.debug(f"yielding cursor {self.db_config['cursor_factory']}")
                yield cursor
            db_conn.commit()

            logger.debug(f"putting connection back into {self.name} connection pool")
            self.pool.putconn(db_conn)

        # if we get an error with the connection, reset the pool and error out (possibly retrying)
        except psycopg2.InterfaceError as err:
            logger.exception(
                f"error while executing query, resetting {self.name} connection pool "
            )
            self.pool.putconn(db_conn)
            self.reset_connection_pool()
            raise err

        except: 
            logger.exception(f'generic error around yielded db cursor {self.name}')
            self.pool.putconn(db_conn)
            self.reset_connection_pool()
            raise

    def close_connection_pool(self):
        """ close all connections in pool """
        logger.debug(f"closing all connections in {self.name} connection pool")
        self.pool.closeall()

    def reset_connection_pool(self):
        """ close all connections and reset the connection pool """
        logger.debug(
            f"closing all connections, then resetting {self.name} connection pool"
        )
        self.pool.closeall()
        self.pool = self.get_connection_pool()

    def get_connection_pool(self, threaded=False, minconn=2, maxconn=5):
        """ create a connection pool """
        logger.debug(f"creating a connection using config: {self.db_config}")
        if threaded:
            return pool.ThreadedConnectionPool(minconn, maxconn, **self.db_config)
        else:
            return pool.SimpleConnectionPool(minconn, maxconn, **self.db_config)

