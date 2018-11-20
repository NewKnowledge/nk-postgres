# nk-postgres
Utilities for managing connections and making queries against a PostgreSQL database


## TODO 
* logging
  * configurable service name
  * add logging to sqla (psycopg has some) 

* retries
  * we need a pluggable policy for retries. some services want a quick fail. some want a heartbeat. some are willing to wait for 30 minutes for the db to come back up 

