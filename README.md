![Postgres to Mongo image](static/postgres_to_mongo_image.png 'Postgres to Mongo image')

![tests](https://github.com/Tornike-Skhulukhia/postgres-to-mongo-importer/actions/workflows/main.yml/badge.svg)

# Module allows to

- [x] Easily copy data from postgres database to mongodb.

- [ ] Denormalize/Reshape data saved in step 1 to make it better stored in MongoDB, like adding nested objects from other collection for 1 to many relationships, so that we can avoid joins in queries e.t.c

# examples & howtos

1. To import data from any(local/remote) PostgreSQL database to any (local/remote) MongoDB, pass the appropriate parameters and relax while looking live progress in shell.  
   In the example that follows, most code is comment to explain what can be customized and how, so do not be intimidated by the length of just one function call:

```python

from importer.basic.basic_copy_importer import do_basic_import

# Postgres connection info
pg_params = dict(
    # from which database to copy data? | if database does not exist, you will get error
    database="some_database",
    # where is Postgres running? example if running on remote server: http://your_server_ip_address_or_domain
    host="localhost",
    # port where postgres is running. Make sure port is open and accessible from your PC
    port=5434,
    # user to authenticate
    user="some_postgres_user",
    # password to authenticate
    password="some_postgres_user_password",

    # # optional, if not set, you may not be able to find/copy tables that you need if they
    # # are not in default 'public' schema.
    # schema_name="public_or_something_else",
)

# Mongo connection info
mongo_params = dict(
    # host where mongodb is running
    host="localhost",
    # and port
    port=27017,
    # username to authenticate | not required if authentication is not needed
    username="john",
    # password to authenticate | not required if authentication is not needed
    password="strong_password",
)

do_basic_import(
    ######################
    # required arguments
    ######################
    postgres_params=pg_params,
    mongo_params=mongo_params,
    # which mongodb database to use to save data from postgres
    destination_db_name_in_mongo="data_from_postgres_or_some_more_descriptive_name",

    ######################
    # optional arguments
    ######################
    # do you want to clear contents of mongo datase before data retrieval starts? default is True,
    # so make sure it does not exist, or data in it is not useful or is backed up
    delete_existing_mongo_db=True,
    # if we do not want all tables from given postgres database and schema, list
    # or patterns for matching only some of them can be passed this way:
    tables_to_copy=["planets"], # any string here will be used as regex pattern, so
                                # in this case table with exact name 'planets' and other tables
                                # with 'planets' in their names will match(ex: 'solar_planets'),
                                #  of course if they exist in given database and schema.
    tables_not_to_copy=["user_", "country_"], # opposite of previous argument with similar syntax. here we filter
                                        # out some tables that matched previously so that  we get only tables we want.
                                        # in this example this way we will not download data for table 'user_planets'.
                                        # we can set tables_not_to_copy or tables_to_copy separately, both, or None.

    columns_to_copy = None, # dictionary with exact table names as keys and column name patterns list as values
                            # that we want to get. if set to None(default), all columns will be retrieved.
                            # ex: {"users": ["id", "^name$"] } - will get only column/s that contain "id" in them and
                            #  + one called "names" exactly, from "users" table.
    columns_not_to_copy = None,#  dictionary with exact table names as keys and column name patterns list as values
                            #  that we do not want to get. if set to None(default), all columns will be retrieved.

                            #  ex: {"users": ["id", "^surname$"] } - will not copy columns that contain "id" text in them,
                            #  or are exactly called "surname", so if we had columns ["id", "parent_id", "surname", "age"]
                            #  in a database, we will only get "age" field for each record.

                            #  as you probably guessed this patterns mechanism is very similar to tables_to_copy &
                            #  tables_not_to_copy arguments behaviour, but in this case as we need details about each
                            #  table, we use dictionary with exact table names as keys and desired patterns as values.

    # if set to True, for each row of postgres data, if it has primary key/keys, this key/keys
    # will be used to create similar field - '_id' in saved mongo data, so mongo will not autocreate
    # new _id fields for us and we will save some space. default is False.
    convert_primary_keys_to_mongo_ids=True,
)

```

After this you should see info about progress for each table that is being copied to mongo and the total number of them that we found in postgres.

For example:  
![Basic copy example CLI image 1](static/basic_copy_import_image_1.png 'Basic copy example CLI image 1')

Another example output with a bit different supplied flags to do_basic_import function:  
![Basic copy example CLI image 2](static/basic_copy_import_image_2.png 'Basic copy example CLI image 2')

If data is really large and network speed is not very high, process may take a looong time, but you can still do some tests with new data in mongodb as it arrives in 1000 row chunks at a time by default.

If something went wrong, follow the output and most probably you will quickly find incorrect credentials errors from pymongo(mongo) or psycopg2(postgres), if it is not the case, please open new issue, or contact me directly.

If something is not clear about arguments/functions, please open tests folder and look at argument/function that you want to see more examples for.

# how it works

# installation

1. install poetry

```bash
python3 -m pip install poetry
```

2. from source folder with pyproject.toml file inside, run

```bash
poetry install
```

# running tests

1. make sure you have docker, docker-compose and make installed

2. run

```bash
make up
```

3. when output stops, press CTRL + C and then type

```bash
make test
```

# supported Python versions

Developed on 3.10, should also work on earlier versions

# some thougts for future

. add continuation option (?)  
. add live sync option (?)  
. test & support replicated/sharded clusters
