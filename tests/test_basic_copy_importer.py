import pytest
from bson import ObjectId
from importer.basic.basic_copy_importer import do_basic_import


# @pytest.mark.skip
def test_basic_local_copy_importer_scenario_1(
    local_postgres_world_database_connection_params,
    local_mongo_connection_params,
    local_mongo_client,
    local_pg_client_with_sample_world_countries_data_loaded,
):

    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world_data_db",
        delete_existing_mongo_db=True,
    )

    mongo_client = local_mongo_client["world_data_db"]

    assert mongo_client["city"].count_documents({}) == 4079
    assert mongo_client["country"].count_documents({}) == 239
    assert mongo_client["countrylanguage"].count_documents({}) == 984

    # counts of languages in each country match
    pg_query = """
        select countrycode, count(*)
        from countrylanguage
        where isofficial is True
        group by countrycode
        order by count(*) desc, countrycode asc
    """
    with local_pg_client_with_sample_world_countries_data_loaded.cursor() as pg_cursor:
        pg_cursor.execute(pg_query)
        pg_result = [dict(zip(["country", "languages_num"], i)) for i in pg_cursor.fetchall()]

    mongo_agg_pipeline = [
        {
            "$match": {
                "isofficial": True,
            }
        },
        {
            "$group": {
                "_id": "$countrycode",
                "languages_num": {
                    "$sum": 1,
                },
            },
        },
        {
            "$sort": {"languages_num": -1, "_id": 1},
        },
        {
            "$replaceWith": {
                "country": "$_id",
                "languages_num": "$languages_num",
            }
        },
    ]
    mongo_result = list(mongo_client["countrylanguage"].aggregate(mongo_agg_pipeline))

    assert mongo_result == pg_result


# @pytest.mark.skip
def test_import_from_remote_postgres_works(
    local_mongo_connection_params,
    remote_postgres_connection_params,
    local_mongo_client,
):
    """
    Copy all databases from postgres to mongo and make some tests to make
    sure that data seems correctly copied.
    """

    # warning - later consider creating separate readonly user in elephantsql postgres instance for this!
    do_basic_import(
        postgres_params=remote_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="elephant_sql_data",
        delete_existing_mongo_db=True,
        only_copy_these_tables=["programming_languages"],
    )

    mongo_client = local_mongo_client["elephant_sql_data"]

    assert mongo_client.list_collection_names() == ["programming_languages"]

    # only this data should be in database
    assert list(
        mongo_client["programming_languages"].find({}, {"_id": 0}).sort("creation_year", 1)
    ) == [
        {"id": 1, "language": "python", "creation_year": 1991},
        {"id": 3, "language": "php", "creation_year": 1994},
        {"id": 2, "language": "javascript", "creation_year": 1995},
    ]


def test_do_basic_import_flags(
    remote_postgres_connection_params,
    local_mongo_connection_params,
    local_mongo_client,
):
    programming_langs_col = local_mongo_client["elephant_sql_data"]["programming_languages"]

    # if imported with delete_existing_mongo_db as True, only 3 records must be found
    do_basic_import(
        postgres_params=remote_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="elephant_sql_data",
        delete_existing_mongo_db=True,
        only_copy_these_tables=["programming_languages"],
    )

    assert programming_langs_col.count_documents({}) == 3

    # after same operation without clearing database, records must be 6
    do_basic_import(
        postgres_params=remote_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="elephant_sql_data",
        delete_existing_mongo_db=False,
        only_copy_these_tables=["programming_languages"],
    )

    assert programming_langs_col.count_documents({}) == 6

    # make sure primary key columns are correctly translated into mongodb _id fields
    ## initially they are ObjectIds
    assert isinstance(programming_langs_col.find_one({})["_id"], ObjectId)

    do_basic_import(
        postgres_params=remote_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="elephant_sql_data",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=True,
        only_copy_these_tables=["programming_languages"],
    )
    # but now they are integers from Postgres
    assert isinstance(programming_langs_col.find_one({})["_id"], int)

    # make sure only_copy_these_tables works
    ## initially it has just 1 table as we asked above
    assert local_mongo_client["elephant_sql_data"].list_collection_names() == [
        "programming_languages"
    ]

    # but we can get more(currently there is 1 more)
    do_basic_import(
        postgres_params=remote_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="elephant_sql_data",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=True,
    )

    assert sorted(local_mongo_client["elephant_sql_data"].list_collection_names()) == [
        "pg_stat_statements",
        "programming_languages",
    ]
