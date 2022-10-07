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


def test_mongo_denormalizer_on_one_to_one_relationship_1(
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
        as_list=False,
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
