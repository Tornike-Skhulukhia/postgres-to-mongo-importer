"""
Make sure we can connect to local test databases for other tests to run properly
"""


def test_mongo(local_mongo_client):
    # no errors and initial mongo dbs present
    assert len(local_mongo_client.list_database_names()) > 0


def test_postgres(local_pg_client_with_sample_test_data_loaded):
    # no errors
    with local_pg_client_with_sample_test_data_loaded.cursor() as pg_cursor:
        pg_cursor.execute(
            "select * from test_table;",
        )

        data = pg_cursor.fetchall()

        assert sorted(data) == [(1, "Aaa"), (2, "Bbb"), (3, "Ccc")]
