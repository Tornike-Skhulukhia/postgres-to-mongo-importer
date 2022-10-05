import os

from importer.basic.basic_copy_importer import do_basic_import


# def test_basic_copy_importer_scenario_1(mongo_client, postgres_world_data_db_cursor):
def test_basic_copy_importer_scenario_1(
    postgres_world_database_connection_params, mongo_connection_params
):
    """
    Copy all databases from postgres to mongo and make some tests to make
    sure that data seems correctly copied.
    """

    do_basic_import(
        postgres_params=postgres_world_database_connection_params,
        mongo_params=mongo_connection_params,
        destination_db_name_in_mongo="world_data_db",
        delete_existing_mongo_db=True,
    )

    # breakpoint()
    # print(mongo_client.list_database_names())
