import psycopg2
from pymongo import MongoClient

from . import MongoConnectionArgsModel, PostgresConnectionArgsModel


def get_db_connection(db_name, params):

    if db_name == "mongodb":
        # may raise validationerror for not proper args
        params = MongoConnectionArgsModel(**params).dict()

        client = MongoClient(**params)

        return client

    elif db_name == "postgresql":
        # may raise validationerror for not proper args
        params = PostgresConnectionArgsModel(**params).dict()

        client = psycopg2.connect(**params)

        cursor = client.cursor()

        return cursor

    else:
        raise ValueError("db_name argument must be mongodb or postgresql!")
