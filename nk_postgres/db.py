""" This module manages db connections and connection pools and includes methods for querying a db given a connection or connection pool."""
import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor, execute_values
from retrying import retry

from nk_logger import get_logger

logger = get_logger(__name__)


class PostgresConnectionManager:
    def __init__(self, db_config, name="db"):
        """ A connection manager """
        self.name = name
        self.db_config = db_config
        self.pool = self.get_connection_pool()

    @retry(
        wait_exponential_multiplier=1000,
        wait_exponential_max=10000,
        stop_max_attempt_number=3,
    )
    def execute(self, query, query_params=None):
        """ Execute a query against the db using a connection pool. This function gets a connection from the pool, executes the query, then
        returns the connection to the pool. If it encounters an psycopg2.InterfaceError, it resets the connection pool, then errors out.
        The retry wrapper will catch the error and retry with exponential backoff until it hits the max number of retries. """

        try:
            # get a connection from the pool and execute the given query with that connection
            logger.debug(f"getting db connection from {self.name} connection pool")
            db_conn = self.pool.getconn()

            logger.debug(f"making query {query} with params {query_params}")
            result = execute_query(db_conn, query, query_params=query_params)

            logger.debug(f"putting connection back into {self.name} connection pool")
            self.pool.putconn(db_conn)

            return result

        # if we get an error with the connection, reset the pool and error out (possibly retrying)
        except psycopg2.InterfaceError as err:
            logger.exception(
                f"error while executing query, resetting {self.name} connection pool "
            )
            self.pool.putconn(db_conn)
            self.reset_connection_pool()
            raise err

    def execute_values(self, query, row_values):
        try:
            # get a connection from the pool and execute the given query with that connection
            logger.debug(f"getting db connection from {self.name} connection pool")
            db_conn = self.pool.getconn()

            logger.debug(f"making execute_values query {query}")

            with db_conn.cursor() as cursor:
                execute_values(cursor, query, row_values)
            db_conn.commit()  # NOTE: commit here bc execute_values does weird stuff sometimes

            logger.debug(f"putting connection back into {self.name} connection pool")
            self.pool.putconn(db_conn)

        # if we get an error with the connection, reset the pool and error out (possibly retrying)
        except psycopg2.InterfaceError as err:
            logger.exception(
                f"error during execute_values query, resetting {self.name} connection pool "
            )
            self.pool.putconn(db_conn)
            self.reset_connection_pool()
            raise err

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
        conn_conf = complete_config(self.db_config)
        logger.debug(f"creating a connection using config: {conn_conf}")
        if threaded:
            return pool.ThreadedConnectionPool(minconn, maxconn, **conn_conf)
        else:
            return pool.SimpleConnectionPool(minconn, maxconn, **conn_conf)


@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_attempt_number=3,
)
def get_pg_connection(db_config={}):
    """ get a pg connection object using the given db_config, with defaults set in complete_config. """
    conn_conf = complete_config(db_config)
    return psycopg2.connect(**conn_conf)


def execute_query(db_connection, query, query_params=None, fetch_type="all"):
    """ Executes a query using the given db connection. This function creates a
    cursor, executes the query and either fetches the results or returns the
    cursor. If fetch_type is 'all', fetchall() is called on the cursor and the
    results are returned as a list. If instead fetch_type='one', only the first
    row is returned using fetchone(). Otherwise (e.g. if fetch_type=None), the
    connection's cursor is returned instead of fetching the results. """

    # the connection 'with' block makes sure the transaction is automatically committed if no exception is raised in the with block.
    # If an exception is raised, rollback() is automatically called, preventing any changes from taking effect.
    with db_connection as conn:
        # get a cursor using the given connection in a with block to make sure the cursor is closed afterwards
        with conn.cursor() as cursor:
            # use the cursor to execute the query, including query params if any are given
            cursor.execute(query, vars=query_params)
            #  if there are results to be returned (cursor.description will be non-null)
            if cursor.description:
                if fetch_type is "all":
                    # return the resulting list of rows from the query
                    return cursor.fetchall()
                if fetch_type is "one":
                    # only return the first row of the query results
                    return cursor.fetchone()
                # o.w. return the cursor
                return cursor


def complete_config(db_config={}):
    """ fill out missing config values with defaults then return the completed config object """
    conn_config = dict(
        user="postgres",
        password="",
        host="localhost",
        dbname="postgres",
        port=5432,
        sslmode="require",
        cursor_factory=DictCursor,
    )
    conn_config.update(db_config)
    return conn_config
