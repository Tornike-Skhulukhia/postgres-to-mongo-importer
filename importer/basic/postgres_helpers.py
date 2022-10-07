"""
Helper functions for working with PostgreSQL database
"""


def _get_pg_database_table_names(pg_conn, schema_name):
    """
    Get current psycopg2 database connection and schema name
    and return list of tables in it sorted in ascending order
    """
    with pg_conn.cursor() as pg_cursor:

        pg_cursor.execute(
            f"""SELECT table_name FROM information_schema.tables """
            f"""WHERE table_schema = '{schema_name}'"""
        )

        table_names = sorted([i[0] for i in pg_cursor.fetchall()])

        return table_names


def _get_pg_database_table_primary_key_columns(pg_conn, table_name, schema_name):
    """
    Get current psycopg2 database connection, schema and table name
    and return primary key columns list in given table
    """

    sql = f"""
    SELECT
    pg_attribute.attname,
    format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
    FROM pg_index, pg_class, pg_attribute, pg_namespace
    WHERE
    pg_class.oid = '{table_name}'::regclass AND
    indrelid = pg_class.oid AND
    nspname = '{schema_name}' AND
    pg_class.relnamespace = pg_namespace.oid AND
    pg_attribute.attrelid = pg_class.oid AND
    pg_attribute.attnum = any(pg_index.indkey)
    AND indisprimary
    """

    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(sql)

        pk_columns = [i[0] for i in pg_cursor.fetchall()]

        return pk_columns
