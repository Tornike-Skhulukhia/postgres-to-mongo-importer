"""
Make sure we can connect to local test databases for other tests to run properly
"""


def test_mongo(mongo_client):
    # no errors and initial mongo dbs present
    assert len(mongo_client.list_database_names()) > 0


def test_postgres(postgres_test_db_cursor):
    # no errors
    postgres_test_db_cursor.execute(
        "select * from test_table;",
    )

    data = postgres_test_db_cursor.fetchall()

    assert sorted(data) == [(1, "Aaa"), (2, "Bbb"), (3, "Ccc")]
