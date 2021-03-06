""" Session model for a direct psycopg2 connection """
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor, execute_values

from .util import validate_db_config, wait_for_pg_service, _config_hash

def _complete_config(db_config):
    """ 
    fill out missing config values with defaults then return the completed config object.
    this will alter the user's provided config 
    """
    validate_db_config(db_config)
    if 'sslmode' not in db_config:
        db_config['sslmode'] = 'require'
    return db_config

_config_to_mgr = {}
def _register_config(db_config): 
    """ create a connection manager for this specific config """
    if _config_hash(db_config) in _config_to_mgr:
        return
    validate_db_config(db_config)
    wait_for_pg_service(db_config)
    _config_to_mgr[_config_hash(db_config)] = PostgresConnectionManager(db_config)

def _psycopg_reset(db_config):
    """ reset connections for this db config """
    _register_config(db_config)
    _config_to_mgr[_config_hash(db_config)].reset_connection_pool()


@contextmanager
def psycopg_cursor(db_config, cursor_factory=DictCursor):
    """ 
    Use `with psycopg_cursor(DB_CONFIG) as c:` to perform sql executes
    and execute_values. The with statement provides error handling that
    ensures the connection pool is reset after each use. 
    """
    db_config = _complete_config(db_config)
    _register_config(db_config)
    mgr = _config_to_mgr[_config_hash(db_config)]
    with mgr.cursor(cursor_factory=cursor_factory) as cursor:
        yield cursor

def psycopg_query(db_config, query, query_params=None, fetch_type="all"): 
    """ helper function to do a simple query """
    with psycopg_cursor(db_config) as cursor:
        cursor.execute(query, vars=query_params)
        if not cursor.description:
            return
        if fetch_type == "all": 
            return cursor.fetchall()
        if fetch_type == "one":
            return cursor.fetchone()

def psycopg_query_exec(db_config, query, query_params=None):
    """ execute a query with no results """
    with psycopg_cursor(db_config) as cursor:
        cursor.execute(query, vars=query_params)

def psycopg_query_one(db_config, query, query_params=None):
    """ execute a query and fetch one """
    with psycopg_cursor(db_config) as cursor:
        cursor.execute(query, vars=query_params)
        return cursor.fetchone()

def psycopg_query_all(db_config, query, query_params=None):
    """ execute a query and fetch all """
    with psycopg_cursor(db_config) as cursor:
        cursor.execute(query, vars=query_params)
        return cursor.fetchall()



class PostgresConnectionManager:
    """ A connection manager for psycopg connection pools. instantiate this per-config """

    def __init__(self, db_config, name=None):
        """ store the db config and attempt to provide a good name for the connection """
        self.name = name or db_config['dbname'] or 'unnamed-db'
        self.db_config = db_config
        self.pool = self.make_pool()
        self._pre_ping = True
        if 'pre-ping' in db_config:
            self._pre_ping = db_config['pre-ping']

    @contextmanager
    def cursor(self, cursor_factory=None):
        """ 
        yield a cursor that supports c.execute() and execute_values(c). 
        This function gets a connection from the pool, yields the cursor, then
        returns the connection to the pool. If it encounters an 
        psycopg2.InterfaceError, it resets the connection pool, then errors out.
        
        TODO: restore retry wrapper? 
        - The retry wrapper will catch the error and retry with exponential backoff 
        - until it hits the max number of retries. 
        """
        db_conn = None 

        # first, pre ping
        if self._pre_ping:
            try:
                db_conn = self.pool.getconn()
                with db_conn:
                    with db_conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
            except (psycopg2.InterfaceError, psycopg2.OperationalError) as pg_err:
                self.reset_pool()
                db_conn = self.pool.getconn()
            except psycopg2.pool.PoolError as pool_err:
                self.pool = self.make_pool()
                db_conn = self.pool.getconn()
        
        # now, provide the cursor 
        try:
            if not self._pre_ping:
                db_conn = self.pool.getconn()
            # with statement around the connection should get us automatic rollback
            with db_conn:
                with db_conn.cursor(cursor_factory=cursor_factory) as cursor:
                    yield cursor
            db_conn.commit()

            self.pool.putconn(db_conn)

        # if we get an error with the connection, reset the pool and error out
        except: 
            self.pool.putconn(db_conn)
            self.reset_pool()
            raise

    def reset_pool(self):
        """ close all connections and reset the connection pool """
        self.pool.closeall()
        self.pool = self.make_pool()

    def make_pool(self, minconn=2, maxconn=5):
        """ create a connection pool """
        return pool.SimpleConnectionPool(minconn, maxconn, **self.db_config)

