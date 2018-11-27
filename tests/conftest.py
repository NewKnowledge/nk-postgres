import pytest
import typing
from pytest_docker_compose import NetworkInfo
from time import sleep, time

from nk_postgres import wait_for_pg_service

pytest_plugins = ['docker_compose']

TEST_DB_CONFIG = {
        'host': 'invalid host name',
        'port': '5432',
        'user': 'test-user',
        'password': 'test-abcd', 
        'dbname': 'test-db-name',
        'sslmode': 'allow'
        }
    
@pytest.fixture(scope='session')
def session_pg(docker_services):
    """ 
    brings one docker container up for the entire session
    about 10 seconds total overhead across N tests
    """
    docker_services.start('test-pg-db')
    public_port = docker_services.port_for('test-pg-db', 5432)
    TEST_DB_CONFIG['host'] = f'{docker_services.docker_ip}'
    TEST_DB_CONFIG['port'] = f'{public_port}'
    wait_for_pg_service(TEST_DB_CONFIG, max_wait_seconds=10.0)

@pytest.fixture(name='compose_pg', scope='function')
def compose_pg(
        docker_network_info: typing.Dict[str, typing.List[NetworkInfo]],
) -> None:
    """ 
    THIS ONE CYCLES THE CONTAINER UP AND DOWN. TAKES ABOUT 10 SECONDS

    adjust global db config based on docker compose host info 
    then wait for the service to come up. 

    environment variables aren't currently available via this api, so
    we still hardcode user, password, and dbname.
    https://github.com/AndreLouisCaron/pytest-docker/issues/12
    """
    
    # ``docker_network_info`` is grouped by service name.
    pg_service = None
    for service_name, dni in docker_network_info.items(): 
        if 'test-pg-db' in service_name:
            pg_service = dni[0]
    if not pg_service: 
        raise RuntimeError('docker-compose.yml does not contain a test-pg-db service')

    TEST_DB_CONFIG['host'] = pg_service.hostname
    TEST_DB_CONFIG['port'] = pg_service.host_port

    wait_for_pg_service(TEST_DB_CONFIG, max_wait_seconds=10.0)

    # this import is ugly, but remains with us until we figure out a retry strategy
    # currently the docker compose image is cycled between tests
    # but the connection manager isnt destroyed, so the connections
    # in the pool get closed. this resets all those closed connections
    # between tests

    # TODO: retry feature 
    from nk_postgres import _psycopg_reset
    _psycopg_reset(TEST_DB_CONFIG)

