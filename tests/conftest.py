import os

import psycopg2
import pytest
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv("env_files/mongo_dev.env")
load_dotenv("env_files/postgres_dev.env")


@pytest.fixture(scope="session")
def mongo_client():
    client = MongoClient(
        host="localhost",
        username=os.environ.get("MONGO_INITDB_ROOT_USERNAME"),
        password=os.environ.get("MONGO_INITDB_ROOT_PASSWORD"),
        port=27019,
        authSource="admin",
    )

    return client


@pytest.fixture(scope="session")
def postgres_test_db_cursor():
    client = psycopg2.connect(
        database="test_db",
        host="localhost",
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        port=5434,
    )

    cursor = client.cursor()

    return cursor
