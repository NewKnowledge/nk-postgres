from nk_postgres import validate_db_config, psycopg_cursor

import psycopg2
from psycopg2.extras import execute_values, DictCursor

import pytest



DB_CONFIG = { 
        'host': 'localhost',
        'port': '5432',
        'user': 'socialexplore',
        'password': '', 
        'dbname': 'social'
        } 

def test_valid_db_config():
    validate_db_config(DB_CONFIG)

def test_basic():
    with psycopg_cursor(DB_CONFIG, sslmode='allow') as cursor: 
        cursor.execute("""SELECT 1""")


@pytest.fixture()
def blank_foo(request):
    with psycopg_cursor(DB_CONFIG, sslmode='allow') as cursor:
        cursor.execute("DROP TABLE IF EXISTS foo")
        cursor.execute("CREATE TABLE IF NOT EXISTS foo (x int, y float)")
        cursor.execute("INSERT INTO foo VALUES (20, .9876)")


def test_query(blank_foo):
    with psycopg_cursor(DB_CONFIG, sslmode='allow') as cursor:
        cursor.execute("SELECT * FROM foo")
        xys = cursor.fetchall()
        assert len(xys)

def test_execute_values(blank_foo):
    with psycopg_cursor(DB_CONFIG, sslmode='allow') as cursor: 
        values = [(1, 3.1), (2, 200.1), (3, 0.004)]
        execute_values(cursor, """INSERT INTO foo VALUES %s""", values)

def test_dict_cursor(blank_foo):
    with psycopg_cursor(DB_CONFIG, sslmode='allow', cursor_factory=DictCursor) as cursor:
        cursor.execute("SELECT * FROM foo")
        xys = cursor.fetchall()
        assert len(xys)
        for xy_dict in xys:
            assert 'x' in xy_dict
            assert type(xy_dict['x']) == int
            assert 'y' in xy_dict
            assert type(xy_dict['y']) == float 


