from nk_postgres import validate_db_config, psycopg_cursor


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
    print('test_basic') 
    with psycopg_cursor(DB_CONFIG, sslmode='allow') as cursor: 
        cursor.execute("""SELECT 1""")
