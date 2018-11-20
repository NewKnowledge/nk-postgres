import pytest
from sqlalchemy import Column, DateTime, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

from nk_postgres import validate_db_config, sqla_cursor, sqla_metadata, sqla_engine

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
    with sqla_cursor(DB_CONFIG) as cursor: 
        cursor.execute("""SELECT 1""")

@pytest.fixture()
def blank_foo(request):
    with sqla_cursor(DB_CONFIG) as cursor:
        cursor.execute("DROP TABLE IF EXISTS foo")
        cursor.execute("CREATE TABLE IF NOT EXISTS foo (x int, y float)")
        cursor.execute("INSERT INTO foo VALUES (20, .9876)")

def test_query(blank_foo):
    with sqla_cursor(DB_CONFIG) as cursor:
        xys = cursor.execute("SELECT * FROM foo").fetchall()
        assert len(xys)

OrmTestBase = declarative_base()
class OrmFoo(OrmTestBase):
    __tablename__ = 'orm_foos'
    x = Column(Integer, primary_key=True)
    y = Column(Float)

@pytest.fixture() 
def blank_orm(request):
    OrmTestBase.metadata.drop_all(bind=sqla_engine(DB_CONFIG), checkfirst=True)
    OrmTestBase.metadata.create_all(bind=sqla_engine(DB_CONFIG), checkfirst=True)

def test_orm_basic(blank_orm):
    with sqla_cursor(DB_CONFIG) as cursor:
        a = OrmFoo(y=1.3)
        b = OrmFoo(y=0.5)
        c = OrmFoo(y=4.1)
        cursor.add_all([a, b, c])

    with sqla_cursor(DB_CONFIG) as cursor:
        xys = cursor.query(OrmFoo).all()
        assert len(xys)
        print(xys)
        