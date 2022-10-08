from importer.copy.basic_copy_importer import do_basic_import
from importer.denormalize.denormalizer import denormalize_mongo

# we need these as sometimes we may have field that has null value
# meaning, it has no record in another table | usually this should not happen often
POSSIBLE_NO_DATA_VALUES_LIST = [None, False, "", {}, []]


def test_mongodb_is_populated_with_test_data(local_mongo_client_with_sample_world_data_imported):

    assert sorted(
        local_mongo_client_with_sample_world_data_imported["world"].list_collection_names()
    ) == [
        "city",
        "country",
        "countrylanguage",
    ]

    countries_col = local_mongo_client_with_sample_world_data_imported["world"]["country"]

    total_relevant_docs_num_in_countries = countries_col.count_documents(
        {"capital": {"$nin": POSSIBLE_NO_DATA_VALUES_LIST}}
    )

    assert total_relevant_docs_num_in_countries > 0

    # initially all values for capital field must be integers
    assert (
        countries_col.count_documents(
            {
                "capital": {"$not": {"$type": "int"}, "$nin": POSSIBLE_NO_DATA_VALUES_LIST},
            }
        )
        == 0
    )


def test_mongo_denormalizer__as_objects_and_delete_other_collection(
    local_mongo_client_with_sample_world_data_imported,
):
    countries_col = local_mongo_client_with_sample_world_data_imported["world"]["country"]

    # initial data in countries has foreign key to cities using field capital,
    # lets make sure that capital field stores not just id of a city but full
    # information about city that is present in cities

    denormalize_mongo(
        mongo_client=local_mongo_client_with_sample_world_data_imported,
        database="world",
        collection="country",
        other_collection="city",
        field_name="capital",
        other_field_name="_id",  # not "id", but "_id" as we used auto key transfer from postgres to mongo option
        new_field_name="capital",
        as_array=False,
        delete_source_field_name_after_lookup=False,
        delete_other_collection=True,
    )

    # make sure that now we have capitals info stored as documents
    assert (
        countries_col.count_documents(
            {
                "capital": {"$not": {"$type": "object"}, "$nin": POSSIBLE_NO_DATA_VALUES_LIST},
            }
        )
        == 0
    )

    # and they have all fields that were present in foreign table
    assert sorted(
        countries_col.find_one(
            {"capital": {"$nin": POSSIBLE_NO_DATA_VALUES_LIST}},
        )["capital"].keys()
    ) == [
        "_id",
        "countrycode",
        "district",
        "name",
        "population",
    ]

    # make sure other collection is deleted
    assert sorted(
        local_mongo_client_with_sample_world_data_imported["world"].list_collection_names()
    ) == ["country", "countrylanguage"]


def test_mongo_denormalizer__delete_source_field(
    local_mongo_client_with_sample_world_data_imported,
):
    countries_col = local_mongo_client_with_sample_world_data_imported["world"]["country"]

    denormalize_mongo(
        mongo_client=local_mongo_client_with_sample_world_data_imported,
        database="world",
        collection="country",
        other_collection="city",
        field_name="capital",
        other_field_name="_id",  # not "id", but "_id" as we used auto key transfer from postgres to mongo option
        new_field_name="the_capital",
        as_array=False,
        delete_source_field_name_after_lookup=True,
        delete_other_collection=False,
    )

    _keys = countries_col.find_one(
        {"the_capital": {"$nin": POSSIBLE_NO_DATA_VALUES_LIST}},
    ).keys()

    # make sure old field deleted properly
    assert "capital" not in _keys and "the_capital" in _keys


def test_mongo_denormalizer__3(
    local_mongo_client_with_sample_world_data_imported,
):
    countries_col = local_mongo_client_with_sample_world_data_imported["world"]["country"]

    denormalize_mongo(
        mongo_client=local_mongo_client_with_sample_world_data_imported,
        database="world",
        collection="country",
        other_collection="city",
        field_name="capital",
        other_field_name="_id",  # not "id", but "_id" as we used auto key transfer from postgres to mongo option
        new_field_name="the_capitals",
        as_array=True,
        delete_source_field_name_after_lookup=True,
        delete_other_collection=False,
    )

    # make sure storing as array worked correctly
    _field_value = countries_col.find_one(
        {"the_capitals": {"$nin": POSSIBLE_NO_DATA_VALUES_LIST}},
    )["the_capitals"]

    assert isinstance(_field_value, list)
    assert len(_field_value) == 1
    assert sorted(_field_value[0].keys()) == [
        "_id",
        "countrycode",
        "district",
        "name",
        "population",
    ]


def test_mongo_denormalizer__skip_fields(
    local_mongo_client_with_sample_world_data_imported,
):
    countries_col = local_mongo_client_with_sample_world_data_imported["world"]["country"]

    denormalize_mongo(
        mongo_client=local_mongo_client_with_sample_world_data_imported,
        database="world",
        collection="country",
        other_collection="city",
        field_name="capital",
        other_field_name="_id",  # not "id", but "_rid" as we used auto key transfer from postgres to mongo option
        new_field_name="the_capitals",
        as_array=True,
        delete_source_field_name_after_lookup=True,
        delete_other_collection=False,
        do_not_copy_fields=["_id", "countrycode"],
    )

    # make sure storing as array worked correctly
    _field_value = countries_col.find_one(
        {"the_capitals": {"$nin": POSSIBLE_NO_DATA_VALUES_LIST}},
    )["the_capitals"]

    assert isinstance(_field_value, list)
    assert len(_field_value) == 1
    assert sorted(_field_value[0].keys()) == [
        # "_id",
        # "countrycode",
        "district",
        "name",
        "population",
    ]


def test_denormalizer_using_rivers_example_from_readme(
    local_rivers_db_postgres_connection_params,
    local_mongo_connection_params,
    local_mongo_client,
):
    do_basic_import(
        postgres_params=local_rivers_db_postgres_connection_params,
        mongo_params=local_mongo_connection_params,
        destination_db_name_in_mongo="rivers_db",
        delete_existing_mongo_db=True,
        convert_primary_keys_to_mongo_ids=True,
    )

    assert sorted(local_mongo_client["rivers_db"].list_collection_names()) == [
        "capital_cities",
        "countries",
        "countries_rivers",
        "rivers",
    ]

    # move specific river info to countries_rivers and delete rivers table
    denormalize_mongo(
        mongo_client=local_mongo_client,
        database="rivers_db",
        collection="countries_rivers",
        other_collection="rivers",
        field_name="river_id",
        other_field_name="_id",
        new_field_name="river",
        as_array=False,
        delete_source_field_name_after_lookup=True,
        delete_other_collection=True,
    )

    doc = local_mongo_client["rivers_db"]["countries_rivers"].find_one()

    assert "river" in doc and "river_id" not in doc

    assert isinstance(doc["river"], dict)
    assert sorted((doc["river"].keys())) == ["_id", "length", "name"]

    assert "rivers" not in local_mongo_client["rivers_db"].list_collection_names()

    # move countries_rivers info to countries directly as a list/array
    denormalize_mongo(
        mongo_client=local_mongo_client,
        database="rivers_db",
        collection="countries",
        other_collection="countries_rivers",
        field_name="_id",
        other_field_name="country_code",
        new_field_name="rivers",
        as_array=True,
        delete_source_field_name_after_lookup=False,
        delete_other_collection=True,
        leave_only_this_key_values_in_array="river",
        # do_not_copy_fields=["_id"],
    )

    # make sure Canada has 2 rivers
    doc = local_mongo_client["rivers_db"].countries.find_one({"_id": "CAN"})

    assert len(doc["rivers"]) == 2
    assert sorted(doc["rivers"][0].keys()) == ["_id", "length", "name"]
    assert doc["rivers"][0]["name"] == "Mississippi"
    assert doc["rivers"][1]["name"] == "Mackenzie"

    # add cities info directly to countries
    denormalize_mongo(
        mongo_client=local_mongo_client,
        database="rivers_db",
        collection="countries",
        other_collection="capital_cities",
        field_name="capital_id",
        other_field_name="_id",
        new_field_name="capital",
        as_array=False,
        delete_source_field_name_after_lookup=True,
        delete_other_collection=True,
        do_not_copy_fields=["_id"],
    )

    doc = local_mongo_client["rivers_db"].countries.find_one({"_id": "CHN"})

    assert sorted(doc.keys()) == [
        "_id",
        "capital",
        "continent",
        "name",
        "rivers",
    ]

    assert sorted(doc["capital"].keys()) == ["name", "population"]

    assert doc["capital"]["name"] == "Beijing"
    assert doc["capital"]["population"] == 7472000
