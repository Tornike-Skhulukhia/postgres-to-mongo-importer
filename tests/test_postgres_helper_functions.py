from importer.copy.postgres_helpers import (
    _get_pg_database_table_names,
    _get_pg_database_table_primary_key_columns,
)


def test_get_pg_database_1_table_names(
    local_pg_client_with_sample_test_data_loaded,
    local_pg_client_with_sample_world_countries_data_loaded,
):
    # make sure 1 table correctly shows up in results
    assert _get_pg_database_table_names(local_pg_client_with_sample_test_data_loaded, "public") == [
        "test_table"
    ]

    # and no tables are in abc_schema schema
    assert (
        _get_pg_database_table_names(local_pg_client_with_sample_test_data_loaded, "abc_schema")
        == []
    )

    ## similar tests for different database
    # make sure sorted 3 table names correctly show up
    assert _get_pg_database_table_names(
        local_pg_client_with_sample_world_countries_data_loaded, "micro_schema"
    ) == ["city", "country", "countrylanguage"]

    # this time 0 tables must be in public schema
    assert (
        _get_pg_database_table_names(
            local_pg_client_with_sample_world_countries_data_loaded, "public"
        )
        == []
    )


def test_get_pg_database_table_primary_key_columns(
    local_pg_client_with_sample_test_data_loaded,
    local_pg_client_with_sample_world_countries_data_loaded,
):
    # we do not have primary keys here
    assert (
        _get_pg_database_table_primary_key_columns(
            local_pg_client_with_sample_test_data_loaded, "test_table", "public"
        )
        == []
    )

    assert (
        _get_pg_database_table_primary_key_columns(
            local_pg_client_with_sample_test_data_loaded, "test_table", "nonexistent_schema"
        )
        == []
    )

    # we do have primary key here
    assert _get_pg_database_table_primary_key_columns(
        local_pg_client_with_sample_world_countries_data_loaded, "city", "micro_schema"
    ) == ["id"]
    assert _get_pg_database_table_primary_key_columns(
        local_pg_client_with_sample_world_countries_data_loaded, "country", "micro_schema"
    ) == ["code"]

    # and composite primary key with 2 columns here
    assert _get_pg_database_table_primary_key_columns(
        local_pg_client_with_sample_world_countries_data_loaded, "countrylanguage", "micro_schema"
    ) == ["countrycode", "language"]

    # one more nonexistent schema check
    assert (
        _get_pg_database_table_primary_key_columns(
            local_pg_client_with_sample_world_countries_data_loaded, "countrylanguage", "abcdefg"
        )
        == []
    )
