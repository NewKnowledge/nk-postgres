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

## run tests
```
    pipenv shell
    pytest --random-order --count 3
```

## TODO: retries
we need a pluggable policy for retries. some services want a quick fail. some want a heartbeat. some are willing to wait for 30 minutes for the db to come back up 

