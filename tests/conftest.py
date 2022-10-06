import os

import pytest
from dotenv import load_dotenv
from importer.db_connections_factory import get_db_connection

load_dotenv("env_files/mongo_dev.env")
load_dotenv("env_files/postgres_dev.env")

# we need this to fix github actions issues
with open("/etc/os-release") as f:
    OS_NAME = [i.split("=")[-1].strip() for i in f if i.lower().startswith("id=")][0]


@pytest.fixture(scope="session")
def postgres_test_db_conn():
    return get_db_connection(
        "postgresql",
        dict(
            database="test_db",
            host="localhost" if OS_NAME == "linuxmint" else "postgres",
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD"),
            port=5434,
        ),
    )


# params for all postgres testing databases that are already loaded
@pytest.fixture(scope="session")
def local_postgres_world_database_connection_params():
    return dict(
        database="world",
        host="localhost" if OS_NAME == "linuxmint" else "postgres",
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        port=5434,
        # optional - required if database tables are in custom (non public) schema,
        # if not set, you may not be able to find/copy tables that you need.
        schema_name="micro_scheme",
    )


@pytest.fixture(scope="session")
def remote_postgres_connection_params():
    return dict(
        # sample params for api.elephantsql.com instance
        host="lucky.db.elephantsql.com",
        database="orvcbokj",
        user="orvcbokj",
        password="Ay08Xjf3_bdyTfjLVZWZpvw0LiiI4nsz",
        port="5432",
        # different remote postgres source with real data, but sometimes has some issues...
        # host="hh-pgsql-public.ebi.ac.uk",
        # database="pfmegrnargs",
        # user="reader",
        # password="NWDMCE5xdipIjRrp",
        # port="5432",
    )


@pytest.fixture(scope="session")
def local_mongo_connection_params():
    return dict(
        host="localhost" if OS_NAME == "linuxmint" else "mongo",
        username=os.environ.get("MONGO_INITDB_ROOT_USERNAME"),
        password=os.environ.get("MONGO_INITDB_ROOT_PASSWORD"),
        port=27017,
    )


@pytest.fixture(scope="session")
def local_mongo_client(local_mongo_connection_params):
    return get_db_connection("mongodb", local_mongo_connection_params)


@pytest.fixture(scope="session")
def local_pg_client_world_db(local_postgres_world_database_connection_params):
    return get_db_connection("postgresql", local_postgres_world_database_connection_params)
