from importer import RICH_CONSOLE, show_nice_texts_on_process_start_and_end_in_cli
from importer.db_connections_factory import get_db_connection
from importer.postgres_to_bson_helpers import refine_postgres_doc_for_mongodb
from rich.progress import Progress
from importer.copy.postgres_helpers import (
    _get_pg_database_table_names,
    _get_pg_database_table_primary_key_columns,
)
from .general_helper_functions import _only_leave_items_that_match_the_patterns


def _copy_table_to_mongo(
    pg_conn,
    mongo_conn,
    table_name,
    progressbar_description,
    destination_db_name_in_mongo,
    convert_primary_keys_to_mongo_ids,
    postgres_schema_name,
    columns_to_copy,
    columns_not_to_copy,
    fetch_many_num=1000,
):

    table_primary_key_columns = _get_pg_database_table_primary_key_columns(
        pg_conn=pg_conn,
        table_name=table_name,
        schema_name=postgres_schema_name,
    )

    ### will someone want to sql inject themselves ? :-)
    # get data to copy

    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(f"""SELECT COUNT(*) from "{table_name}" ;""")
        total_rows_number = pg_cursor.fetchone()[0]

        # get all column names of table
        pg_cursor.execute(f"""SELECT * FROM "{table_name}" LIMIT 0;""")
        pg_cursor.fetchmany(0)  # to make description available
        table_column_names = [i[0] for i in pg_cursor.description]

        # leave only columns that we requested
        table_column_names = _only_leave_items_that_match_the_patterns(
            table_column_names,
            leave_patterns=columns_to_copy,
            remove_patterns=columns_not_to_copy,
        )

        # if we want primary keys to be auto translated, but not asked
        # for primary key column/s explicitely, make sure that this column/s
        # will still be retrieved
        if convert_primary_keys_to_mongo_ids:
            table_column_names.extend(
                list(set(table_primary_key_columns) - set(table_column_names))
            )

    with pg_conn.cursor("custom_cursor") as pg_cursor:
        pg_cursor.itersize = fetch_many_num

        pg_cursor.execute(f"""SELECT "{'", "'.join(table_column_names)}" FROM "{table_name}" ;""")

        # get actual data from postgres only for columns we want

        next_rows_to_insert = pg_cursor.fetchmany(fetch_many_num)

        with Progress() as progress:
            task = progress.add_task(f"[purple]{progressbar_description}", total=total_rows_number)

            while len(next_rows_to_insert) > 0:

                # reshape data
                current_insert_documents_batch = [
                    refine_postgres_doc_for_mongodb(
                        dict(zip(table_column_names, row)),
                        convert_primary_keys_to_mongo_ids=convert_primary_keys_to_mongo_ids,
                        pk_columns=table_primary_key_columns,
                    )
                    for row in next_rows_to_insert
                ]

                # save in mongo
                mongo_conn[destination_db_name_in_mongo][table_name].insert_many(
                    documents=current_insert_documents_batch
                )

                # update progressbar
                progress.update(task, advance=len(next_rows_to_insert))

                # get next batch
                next_rows_to_insert = pg_cursor.fetchmany(fetch_many_num)


@show_nice_texts_on_process_start_and_end_in_cli
def do_basic_import(
    postgres_params,
    mongo_params,
    destination_db_name_in_mongo,
    delete_existing_mongo_db=True,
    tables_to_copy=None,
    tables_not_to_copy=None,
    columns_to_copy=None,
    columns_not_to_copy=None,
    convert_primary_keys_to_mongo_ids=False,
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

        5. tables_to_copy - list of strings or regex patterns of table names that we want to copy.
                                if set to None(default) - all tables will match.

                                For example, if you have tables named 'customers' and 'customer_orders'
                                and set this parameter to ["customer"], both tables will match as string is
                                interpreted as regex, to match only first table name you can use ["customers"] or ["^customers$"].

        6. tables_not_to_copy - list of strings or regex patterns of table names that we want to skip/not copy.
                                if set to None(default) - no tables will be filtered out.

                                For example, if you have tables named 'customers' and 'customer_orders'
                                and set this parameter to ["customer"], both tables will match as string is
                                interpreted as regex, so no data will be downloaded at all. To only filter out first
                                table, you can try ["customers"] or ["^customers$"] here.

        7. columns_to_copy - dictionary with exact table names as keys and column name patterns list as values
                             that we want to get. if set to None(default), all columns will be retrieved.

                             ex: {"users": ["id", "^name$"] } - will get only column/s that contain "id" in them and
                             + one called "names" exactly, from "users" table. as you probably guessed this patterns
                             mechanism is very similar to tables_to_copy & tables_not_to_copy arguments behaviour,
                             but in this case as we need details about each table, we use dictionary with exact table
                             names as keys and desired patterns as values.

        8. columns_not_to_copy - dictionary with exact table names as keys and column name patterns list as values
                             that we do not want to get. if set to None(default), all columns will be retrieved.

                             ex: {"users": ["id", "^surname$"] } - will not copy columns that contain "id" text in them,
                             or are exactly called "surname", so if we had columns ["id", "parent_id", "surname", "age"]
                             in a database, we will only get "age" field for each record.

                             as you probably guessed this patterns mechanism is very similar to tables_to_copy &
                             tables_not_to_copy arguments behaviour, but in this case as we need details about each
                             table, we use dictionary with exact table names as keys and desired patterns as values.


        9. convert_primary_keys_to_mongo_ids - if set to True(default=False), postgresql table primary keys will
                                        be used as object ids(primary keys) in mongodb.
    """
    postgres_schema_name = postgres_params.get("schema_name", "public")

    columns_to_copy = columns_to_copy if isinstance(columns_to_copy, dict) else {}
    columns_not_to_copy = columns_not_to_copy if isinstance(columns_not_to_copy, dict) else {}

    # allows access to all databases
    mongo_conn = get_db_connection("mongodb", mongo_params)

    # allows access to specific database, given in postgres_params as "database" key
    pg_conn = get_db_connection("postgresql", postgres_params)

    table_names = _get_pg_database_table_names(pg_conn=pg_conn, schema_name=postgres_schema_name)

    RICH_CONSOLE.print(
        f". Found total of {len(table_names)} table{'s' if len(table_names) > 1 else ''} in postgres database "
        f"{postgres_params['database']} and schema '{postgres_schema_name}' ????",
        style="bold blue",
    )

    # which tables does user want to copy/import ?
    table_names = _only_leave_items_that_match_the_patterns(
        table_names, leave_patterns=tables_to_copy, remove_patterns=tables_not_to_copy
    )

    # delete existing mongodb database data?
    if delete_existing_mongo_db:
        RICH_CONSOLE.print(
            f". Deleting MongoDB Database {destination_db_name_in_mongo} ??? ",
            style="bold red",
        )
        mongo_conn.drop_database(destination_db_name_in_mongo)

    if len(table_names) == 0:
        RICH_CONSOLE.print(
            f". No tables matched to given arguments/filters ???",
            style="bold red",
        )
        return

    # use existing primary keys also in MongoDB?
    if convert_primary_keys_to_mongo_ids:
        RICH_CONSOLE.print(
            f". Using existing PostgreSQL primary key/s for Mongo primary keys(_id)  ????",
            style="bold blue",
        )

    RICH_CONSOLE.print(
        f". {len(table_names)} table{'s' if len(table_names) > 1 else ''} matched, starting to download "
        f"{'it ' if len(table_names) == 1 else 'them '}"
        f"in mongo database '{destination_db_name_in_mongo}' ????\n",
        style="bold blue",
    )

    _max_table_name_length = max((len(i) for i in table_names)) + 3
    _max_table_name_length = max(_max_table_name_length, 20)

    for index, table_name in enumerate(table_names):
        progressbar_description = f"Copying table {index + 1}/{len(table_names)} | {table_name:>{_max_table_name_length}} "

        _copy_table_to_mongo(
            pg_conn=pg_conn,
            mongo_conn=mongo_conn,
            table_name=table_name,
            progressbar_description=progressbar_description,
            destination_db_name_in_mongo=destination_db_name_in_mongo,
            postgres_schema_name=postgres_schema_name,
            convert_primary_keys_to_mongo_ids=convert_primary_keys_to_mongo_ids,
            columns_to_copy=columns_to_copy.get(table_name),
            columns_not_to_copy=columns_not_to_copy.get(table_name),
        )
