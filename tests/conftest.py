import os

import pytest
from dotenv import load_dotenv
from importer.db_connections_factory import get_db_connection

load_dotenv("env_files/mongo_dev.env")
load_dotenv("env_files/postgres_dev.env")


@pytest.fixture(scope="session")
def mongo_client():

    return get_db_connection(
        "mongodb",
        dict(
            host="localhost",
            username=os.environ.get("MONGO_INITDB_ROOT_USERNAME"),
            password=os.environ.get("MONGO_INITDB_ROOT_PASSWORD"),
            port=27019,
        ),
    )


@pytest.fixture(scope="session")
def postgres_test_db_cursor():
    return get_db_connection(
        "postgresql",
        dict(
            database="test_db",
            host="localhost",
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD"),
            port=5434,
        ),
    )


# params for all postgres testing databases that are already loaded
@pytest.fixture(scope="session")
def postgres_world_database_connection_params():
    return dict(
        database="world",
        host="localhost",
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        port=5434,
    )


# to get access to all databases, 1 params is enough
@pytest.fixture(scope="session")
def mongo_connection_params():
    return dict(
        host="localhost",
        username=os.environ.get("MONGO_INITDB_ROOT_USERNAME"),
        password=os.environ.get("MONGO_INITDB_ROOT_PASSWORD"),
        port=27019,
    )


# @pytest.fixture(scope="session")
# def postgres_world_data_db_cursor():
#     client = psycopg2.connect(
#         database="world",
#         host="localhost",
#         user=os.environ.get("POSTGRES_USER"),
#         password=os.environ.get("POSTGRES_PASSWORD"),
#         port=5434,
#     )

#     cursor = client.cursor()

#     return cursor
