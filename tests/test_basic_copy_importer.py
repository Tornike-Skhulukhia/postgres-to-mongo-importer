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
        tables_to_copy=["programming_languages"],
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


def test_basic_import_flags_work__delete_existing_mongo_db(
    local_test_db_postgres_connection_params,
    local_mongo_connection_params,
    local_mongo_client,
):
    mongo_col = local_mongo_client["test_db"]["test_table"]

    # if imported with delete_existing_mongo_db as True, only 3 records must be found
    do_basic_import(
        postgres_params=local_test_db_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="test_db",
        delete_existing_mongo_db=True,
        tables_to_copy=["test_table"],
    )

    assert mongo_col.count_documents({}) == 3

    # after same operation without clearing database, records must be 6
    do_basic_import(
        postgres_params=local_test_db_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="test_db",
        delete_existing_mongo_db=False,
        tables_to_copy=["test_table"],
    )

    assert mongo_col.count_documents({}) == 6

    # but if cleared, next time it will be 3
    do_basic_import(
        postgres_params=local_test_db_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="test_db",
        delete_existing_mongo_db=True,
        tables_to_copy=["test_table"],
    )

    assert mongo_col.count_documents({}) == 3


def test_basic_import_flags_work__convert_primary_keys_to_mongo_ids(
    local_postgres_world_database_connection_params,
    local_mongo_connection_params,
    local_mongo_client,
):
    mongo_col = local_mongo_client["countrylanguage"]["countrylanguage"]

    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="countrylanguage",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=False,
        tables_to_copy=["countrylanguage"],
    )

    # make sure mongo generated _id fields for us
    assert isinstance(mongo_col.find_one({})["_id"], ObjectId)

    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="countrylanguage",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=True,
        tables_to_copy=["countrylanguage"],
    )

    # make sure primary key columns are correctly translated into mongodb _id fields
    id_val = mongo_col.find_one({})["_id"]

    # in this case actually composite key must be created, so
    # object should be saved as _id value
    assert isinstance(id_val, dict)

    assert "countrycode" in id_val and "language" in id_val

    # one more test with non composite _id value
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="country",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=True,
        tables_to_copy=["country$"],
    )
    assert isinstance(local_mongo_client["country"]["country"].find_one({})["_id"], str)


def test_basic_import_flags_work__tables_to_copy_tables_not_to_copy(
    local_postgres_world_database_connection_params,
    local_mongo_connection_params,
    local_mongo_client,
):

    # make sure tables_to_copy and tables_not_to_copy work as expected
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        tables_to_copy=["country"],
    )

    # as country is interpreted as regex, countrylanguage table should also match
    assert sorted(local_mongo_client.world.list_collection_names()) == [
        "country",
        "countrylanguage",
    ]

    # all table names start with 'c', so skip all
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        tables_not_to_copy=["^c"],
    )

    assert sorted(local_mongo_client.world.list_collection_names()) == []

    # copy tables with country or city in names and not language in name
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        tables_to_copy=["country", "city"],
        tables_not_to_copy=["language"],
    )

    assert sorted(local_mongo_client.world.list_collection_names()) == ["city", "country"]


def test_basic_importer_flags_work__columns_to_copy_columns_not_to_copy(
    local_postgres_world_database_connection_params,
    local_mongo_connection_params,
    local_mongo_client,
):
    mongo_col = local_mongo_client["world"]["city"]

    # make sure all fields are present without restricting any column
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=True,
        tables_to_copy=["city"],
    )

    assert sorted(mongo_col.find_one({}).keys()) == [
        "_id",
        "countrycode",
        "district",
        "name",
        "population",
    ]

    # copy only some columns
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=False,
        tables_to_copy=["city"],
        columns_to_copy={
            "city": ["id", "name", "population"],
        },
    )

    assert sorted(mongo_col.find_one({}).keys()) == [
        "_id",  # this will be automatically added by mongo as we set convert_primary_keys_to_mongo_ids to False
        "id",
        "name",
        "population",
    ]

    # now we request "id" key but as it is the primary key, we wil download it, but will not save in mongodb
    # as convert_primary_keys_to_mongo_ids is True, so "id" values will be saved as "_id"
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=True,
        tables_to_copy=["city"],
        columns_to_copy={
            "city": ["id", "name", "population"],
        },
    )

    assert sorted(mongo_col.find_one({}).keys()) == [
        "_id",
        # "id",
        "name",
        "population",
    ]

    assert isinstance(mongo_col.find_one({})["_id"], int)

    # lets also test columns_not_to_copy arg
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=False,
        tables_to_copy=["city"],
        columns_not_to_copy={
            "city": ["name", "population"],
        },
    )

    assert sorted(mongo_col.find_one({}).keys()) == [
        "_id",
        "countrycode",
        "district",
        "id",
    ]

    assert isinstance(mongo_col.find_one({})["_id"], ObjectId)

    # make sure if we did not asked for primary key, but asked to convert primary keys
    # to mongo _ids, we still get primary keys downloaded and stored as values for _id

    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=True,
        tables_to_copy=["city"],
        columns_to_copy={"city": ["name"]},
    )

    assert sorted(mongo_col.find_one({}).keys()) == ["_id", "name"]

    assert isinstance(mongo_col.find_one({})["_id"], int)  # this integer must come from postgres

    # and use both args
    do_basic_import(
        postgres_params=local_postgres_world_database_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="world",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=False,
        tables_to_copy=["country$"],
        # starts with letter 'g' or ends with digit or contains text "population"
        columns_to_copy={
            "country": [r"^g|\d$", "population"],
        },
        columns_not_to_copy={
            "country": ["old"],  # contains text "old" anywhere
        },
    )
    assert sorted(local_mongo_client["world"]["country"].find_one({}).keys()) == [
        "_id",
        "code2",
        "gnp",
        "governmentform",
        "population",
    ]
