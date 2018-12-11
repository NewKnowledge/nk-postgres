import psycopg2
from psycopg2.extras import execute_values, DictCursor
import pytest

from nk_postgres import (
        validate_db_config, psycopg_cursor, wait_for_pg_service,
        psycopg_query_exec, psycopg_query_one, psycopg_query_all
        )
from tests.conftest import TEST_DB_CONFIG

def test_valid_db_config():
    validate_db_config(TEST_DB_CONFIG)

def test_basic(session_pg):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")

@pytest.fixture()
def blank_foo(request, session_pg):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor:
        cursor.execute("DROP TABLE IF EXISTS foo")
        cursor.execute("CREATE TABLE IF NOT EXISTS foo (x int, y float)")
        cursor.execute("INSERT INTO foo VALUES (20, .9876)")

def test_query(session_pg, blank_foo):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor:
        cursor.execute("SELECT * FROM foo")
        xys = cursor.fetchall()
        assert len(xys)

def test_execute_values(session_pg, blank_foo):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
        values = [(1, 3.1), (2, 200.1), (3, 0.004)]
        execute_values(cursor, "INSERT INTO foo VALUES %s", values)

def test_helpers(session_pg, blank_foo): 
    psycopg_query_exec(TEST_DB_CONFIG, "INSERT INTO foo VALUES (8, 2.5), (9, 9.9), (10, 10.10)")
    assert 2.5 == psycopg_query_one(TEST_DB_CONFIG, "SELECT * FROM foo WHERE x = 8")["y"]
    foos = psycopg_query_all(TEST_DB_CONFIG, "SELECT * FROM foo")
    assert len(foos)
    for foo in foos:
        assert 'x' in foo
        assert 'y' in foo


def test_dict_cursor(session_pg, blank_foo):
    with psycopg_cursor(TEST_DB_CONFIG, cursor_factory=DictCursor) as cursor:
        cursor.execute("SELECT * FROM foo")
        xys = cursor.fetchall()
        assert len(xys)
        for xy_dict in xys:
            assert 'x' in xy_dict
            assert type(xy_dict['x']) == int
            assert 'y' in xy_dict
            assert type(xy_dict['y']) == float 

def test_server_down(killer_pg, docker_services):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")
    docker_services.shutdown()
    with pytest.raises(psycopg2.OperationalError):
        with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
            cursor.execute("SELECT 1")

def test_server_down_up(session_pg, docker_services):
    with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")
    docker_services.shutdown()
    with pytest.raises(psycopg2.OperationalError):
        with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
            cursor.execute("SELECT 1")
    docker_services.start('test-pg-db')
    wait_for_pg_service(TEST_DB_CONFIG)
    with psycopg_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")

def test_set_work_mem(session_pg):
    with psycopg_cursor(TEST_DB_CONFIG) as c:
        c.execute("set work_mem to '4GB'")

