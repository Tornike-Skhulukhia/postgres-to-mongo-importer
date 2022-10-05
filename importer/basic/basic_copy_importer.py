import time

from importer import RICH_CONSOLE, show_nice_texts_on_process_start_and_end_in_cli
from importer.db_connections_factory import get_db_connection
from importer.postgres_to_bson_helpers import refine_postgres_doc_for_mongodb
from rich.progress import Progress


def _copy_table_to_mongo(
    pg_cursor,
    mongo_conn,
    table_name,
    progressbar_description,
    destination_db_name_in_mongo,
    fetch_many_num=1000,
):

    ### will someone want to sql inject themselves ? :-)
    # get data to copy
    pg_cursor.execute(f"""SELECT * FROM {table_name}""")

    table_column_names = [i[0] for i in pg_cursor.description]

    next_rows_to_insert = pg_cursor.fetchmany(fetch_many_num)

    with Progress() as progress:
        task = progress.add_task(f"[purple]{progressbar_description}", total=pg_cursor.rowcount)

        while len(next_rows_to_insert) > 0:
            # process rows
            mongo_conn[destination_db_name_in_mongo][table_name].insert_many(
                documents=(
                    refine_postgres_doc_for_mongodb(dict(zip(table_column_names, row)))
                    for row in next_rows_to_insert
                )
            )

            # update progressbar
            progress.update(task, advance=len(next_rows_to_insert))

            # get next batch
            next_rows_to_insert = pg_cursor.fetchmany(fetch_many_num)


def _get_current_pg_database_table_names(pg_cursor):
    pg_cursor.execute(
        """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""
    )

    return [i[0] for i in pg_cursor.fetchall()]


@show_nice_texts_on_process_start_and_end_in_cli
def do_basic_import(
    postgres_params,
    mongo_params,
    destination_db_name_in_mongo,
    delete_existing_mongo_db=True,
):
    """
    Connect to PostgreSQL database with given params
    and copy all available data directly to MongoDB.

    args:
        1. postgres_params - dict with keys to use when creating psycopg2 connection to PostgreSQL,
                            for more info visit https://www.psycopg.org/docs/module.html#psycopg2.connect.

        2. mongo_params - dict with keys to use when creating pymongo client connection to MongoDB,
                        for more info visit https://pymongo.readthedocs.io/en/stable/examples/authentication.html#scram-sha-256-rfc-7677

        3. destination_db_name_in_mongo - database name to copy postgres tables as collections into
        4. delete_existing_mongo_db - do we want to delete existing MongoDB database with all its collections or not?
                                    default=True.
    """

    # allows access to all databases
    mongo_conn = get_db_connection("mongodb", mongo_params)

    # allows access to specific database, given in postgres_params as "database" key
    pg_cursor = get_db_connection("postgresql", postgres_params)

    table_names = _get_current_pg_database_table_names(pg_cursor)

    # delete existing mongodb database data?
    if delete_existing_mongo_db:
        RICH_CONSOLE.print(
            f". Deleting MongoDB Database {destination_db_name_in_mongo} ⛔ ",
            style="bold red",
        )
        mongo_conn.drop_database(destination_db_name_in_mongo)
        print()

    for index, table_name in enumerate(table_names):
        progressbar_description = (
            f"Copying table {index + 1}/{len(table_names)} | {table_name:>20} "
        )

        _copy_table_to_mongo(
            pg_cursor=pg_cursor,
            mongo_conn=mongo_conn,
            table_name=table_name,
            progressbar_description=progressbar_description,
            destination_db_name_in_mongo=destination_db_name_in_mongo,
        )
