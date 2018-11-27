import pytest
from sqlalchemy import Column, DateTime, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.exc

from nk_postgres import (
        validate_db_config, 
        wait_for_pg_service,
        sqla_cursor, 
        sqla_metadata, 
        sqla_engine
        )

from tests.conftest import TEST_DB_CONFIG


def test_valid_db_config():
    validate_db_config(TEST_DB_CONFIG)

def test_basic(session_pg):
    with sqla_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")

@pytest.fixture()
def blank_foo(session_pg, request):
    with sqla_cursor(TEST_DB_CONFIG) as cursor:
        cursor.execute("DROP TABLE IF EXISTS foo")
        cursor.execute("CREATE TABLE IF NOT EXISTS foo (x int, y float)")
        cursor.execute("INSERT INTO foo VALUES (20, .9876)")

def test_query(session_pg, blank_foo):
    with sqla_cursor(TEST_DB_CONFIG) as cursor:
        xys = cursor.execute("SELECT * FROM foo").fetchall()
        assert len(xys)

OrmTestBase = declarative_base()
class OrmFoo(OrmTestBase):
    __tablename__ = 'orm_foos'
    x = Column(Integer, primary_key=True)
    y = Column(Float)

@pytest.fixture() 
def blank_orm(session_pg, request):
    OrmTestBase.metadata.drop_all(bind=sqla_engine(TEST_DB_CONFIG), checkfirst=True)
    OrmTestBase.metadata.create_all(bind=sqla_engine(TEST_DB_CONFIG), checkfirst=True)

def test_orm_basic(session_pg, blank_orm):
    with sqla_cursor(TEST_DB_CONFIG) as cursor:
        a = OrmFoo(y=1.3)
        b = OrmFoo(y=0.5)
        c = OrmFoo(y=4.1)
        cursor.add_all([a, b, c])

    with sqla_cursor(TEST_DB_CONFIG) as cursor:
        xys = cursor.query(OrmFoo).all()
        assert len(xys)
        print(xys)

def test_server_down(killer_pg, docker_services):
    with sqla_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")
    docker_services.shutdown()
    with pytest.raises(sqlalchemy.exc.OperationalError):
        with sqla_cursor(TEST_DB_CONFIG) as cursor: 
            cursor.execute("SELECT 1")

def test_server_down_up(session_pg, docker_services):
    with sqla_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")
    docker_services.shutdown()
    with pytest.raises(sqlalchemy.exc.OperationalError):
        with sqla_cursor(TEST_DB_CONFIG) as cursor: 
            cursor.execute("SELECT 1")
    docker_services.start('test-pg-db')
    wait_for_pg_service(TEST_DB_CONFIG)
    with sqla_cursor(TEST_DB_CONFIG) as cursor: 
        cursor.execute("SELECT 1")

