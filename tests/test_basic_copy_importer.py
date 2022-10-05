import os

from importer.basic.basic_copy_importer import do_basic_import
from importer.db_connections_factory import get_db_connection


# def test_basic_copy_importer_scenario_1(mongo_client, postgres_world_data_db_cursor):
def test_basic_copy_importer_scenario_1(
    postgres_world_database_connection_params,
    mongo_connection_params,
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

    mongo_client = get_db_connection("mongodb", mongo_connection_params)["world_data_db"]
    pg_client = get_db_connection("postgresql", postgres_world_database_connection_params)

    # make sure counts match | later split this and other checks for this scenario using
    # shared fixture that runs once to do copy operation and these functions check results
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
    pg_client.execute(pg_query)
    pg_result = [dict(zip(["country", "languages_num"], i)) for i in pg_client.fetchall()]

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
