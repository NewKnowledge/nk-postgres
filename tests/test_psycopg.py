import psycopg2
from psycopg2.extras import execute_values, DictCursor
import pytest

from nk_postgres import validate_db_config, psycopg_cursor
from tests.fixtures import compose_pg, TEST_DB_CONFIG

def test_valid_db_config(compose_pg):
    validate_db_config(TEST_DB_CONFIG)

def test_basic(compose_pg):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")

@pytest.fixture()
def blank_foo(request, compose_pg):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor:
        cursor.execute("DROP TABLE IF EXISTS foo")
        cursor.execute("CREATE TABLE IF NOT EXISTS foo (x int, y float)")
        cursor.execute("INSERT INTO foo VALUES (20, .9876)")

def test_query(compose_pg, blank_foo):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor:
        cursor.execute("SELECT * FROM foo")
        xys = cursor.fetchall()
        assert len(xys)

def test_execute_values(compose_pg, blank_foo):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
        values = [(1, 3.1), (2, 200.1), (3, 0.004)]
        execute_values(cursor, "INSERT INTO foo VALUES %s", values)

def test_dict_cursor(compose_pg, blank_foo):
    with psycopg_cursor(TEST_DB_CONFIG, cursor_factory=DictCursor) as cursor:
        cursor.execute("SELECT * FROM foo")
        xys = cursor.fetchall()
        assert len(xys)
        for xy_dict in xys:
            assert 'x' in xy_dict
            assert type(xy_dict['x']) == int
            assert 'y' in xy_dict
            assert type(xy_dict['y']) == float 

