# nk-postgres
Utilities for managing connections and making queries against a PostgreSQL database. See test files for more usage patterns. 

psycopg usage: 
```
from nk_postgres import psycopg_cursor
from config import DB_CONFIG

with psycopg_cursor(DB_CONFIG) as cursor:
    cursor.execute("select * from some_table").fetchall()
    execute_values(cursor, "INSERT INTO some_table VALUES %s", some_iterator)
```

## Pre-ping Retries
both psycopg and sqla cursors implement pre-ping connection tries before yielding their cursor object. Disable this with `DB_CONFIG['pre-ping'] = False`. Pre ping has a slight performance impact of an extra network io to the server, but no computational impact. 

This fixes errors due to closed idle connections (closed by pg or a firewall). This fails rather immediately if the pg is actually down. 

## TODO: Patient Retries
We need a pluggable policy for retries. Some services want a quick fail. Some want a heartbeat. Some are willing to wait for 30 minutes for the db to come back up 

we can do retries with a chain cursor class like so...
```
def Retry10MinCursor():
    def __init__(self, sub_cursor): 
        self._cursor = sub_cursor

    @retry(...)
    def execute(query, query_args=None):
        self._cursor.execute(query, vals=query_args)
```

For now, user retry strategies should be handled by a try/except block catching the appropriate error.

## run tests
```
    pipenv shell
    pytest --random-order --count 3
```
