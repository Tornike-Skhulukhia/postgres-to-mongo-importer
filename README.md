![Postgres to Mongo image](static/postgres_to_mongo_image.png 'Postgres to Mongo image')

![tests](https://github.com/Tornike-Skhulukhia/postgres-to-mongo-importer/actions/workflows/main.yml/badge.svg)

# Module allows to

- [x] Easily copy data from postgres database to mongodb.

- [ ] Denormalize/Reshape data saved in step 1 to make it better stored in MongoDB, like adding nested objects from other collection for 1 to many relationships, so that we can avoid joins in queries e.t.c

# examples & howtos

1. To import data from any(local/remote) PostgreSQL database to any (local/remote) MongoDB, pass the appropriate parameters and relax while looking live progress in shell

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
    postgres_params=pg_params,
    mongo_params=mongo_params,
    # which mongodb database to use to save data from postgres
    destination_db_name_in_mongo="data_from_postgres_or_some_more_descriptive_name",
    # do you want to clear contents of it before data retrieval starts? default is True,
    # so make sure it does not exist, or data in it is not useful or is backed up
    delete_existing_mongo_db=True,
    # optional - if we do not want all tables from given postgres database and schema, list
    # of only some of them can be passed this way
    only_copy_these_tables=["programming_languages"],
    # optional - if set to True, for each row of postgres data, if it has primary key/keys, this key/keys
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

If data is really large and network speed is not very high, process may take a looong time.

If something went wrong, follow the output and most probably you will quickly find incorrect credentials errors from pymongo(mongo) or psycopg2(postgres), if it is not the case, please open new issue, or contact me directly.

# later todos

. test & support replicated/sharded clusters

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
