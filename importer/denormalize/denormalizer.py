from importer import RICH_CONSOLE, show_nice_texts_on_process_start_and_end_in_cli
import json

# from rich import print
from rich.layout import Layout
from rich.align import Align
from rich.text import Text
from rich.layout import Layout
from importer.postgres_to_bson_helpers import _convert_bson_decimal128_to_strs


def _draw_before_and_after_example_docs_on_screen_side_by_side(doc_1, doc_2, field_1, field_2):
    print()
    layout = Layout()

    layout.split_column(
        Layout(name="upper", ratio=1),
        Layout(name="lower", ratio=3),
    )

    layout["lower"].split_row(
        Layout(name="left"),
        Layout(name="right"),
    )

    layout["upper"].update(
        Align.center(
            "Sample documents before and after denormalization",
            vertical="middle",
            style="bold blue",
        )
    )

    layout["left"].update(
        Align.center(
            json.dumps(
                _convert_bson_decimal128_to_strs(doc_1),
                indent=2,
            ).replace(f'"{field_1}"', f'[bold red]"{field_1}"[/bold red]'),
        )
    )

    layout["right"].update(
        Align.center(
            json.dumps(
                _convert_bson_decimal128_to_strs(doc_2),
                indent=2,
            ).replace(f'"{field_2}"', f'[bold red]"{field_2}"[/bold red]'),
        )
    )

    RICH_CONSOLE.print(layout)


@show_nice_texts_on_process_start_and_end_in_cli
def denormalize_mongo(
    mongo_client,
    database,
    collection,
    other_collection,
    field_name,
    other_field_name,
    new_field_name,
    as_array=True,
    delete_source_field_name_after_lookup=False,
    delete_other_collection=False,
    do_not_copy_fields=None,
):
    """
    This is mainly the simple wrapper around MongoDB's $lookup and $merge aggregation stages to make
    our life a bit easier when downloading and transforming data in some ways.

    It allows to denormalize data, meaning adding columns with data from other
    collection documents, which is called join in SQL/RDBM databases.

    We can get all fields from matching documents, or some of them, delete
    the other collection or leave it, delete the existing key that stored data
    as foreign key, or not.

    args:
        * to understand what they do, please continue reading for small example

        1. mongo_client - Pymongo client object to mongo database
        2. database - mongo database name we are working on
        3. collection - source collecton (where we want to add data from other collection)
        4. other_collection - collecton that stores additional data about some field value
        5. field_name - field in starting collection objects that are connected to other collection
        6. other_field_name - field in other collection objects that are connected to collection
        7. new_field_name - name of field to store new info under, it can be same field_name or new one
        8. as_array - if set to True (default), all matching documents will be stored as array in new_field_name value
        9. delete_source_field_name_after_lookup - if set to True (default is False), source field will be deleted
        10. delete_other_collection - if set to True, other collection will be deleted after operation succedes
        11. do_not_copy_fields - list of fields that we do not want to get/store using lookup/join

    example:
        lets say we have 2 collections - 'countries' and 'cities' in mongo database named 'countries_database'
        and in documents of 'countries' collection we have field 'capital_city_id' with id value of
        city that is stored in 'cities' collection.

        We want to directly add data from 'cities' collection to 'countries' collection about capitals, so that
        on simple queries we do not need to lookup/join data from any other sources to get city info.

        document in 'countries' collection example:
            {
                "_id": "FR",
                "name": "France",
                "population": 67000000,
                "capital_city_id": 1,
            }

        document in 'cities' collection example:
            {
                "_id": 1,
                "name": "Paris",
                "population": 3000000,
                "area": 105.4,
            }

        what we want to get as a result is this document in 'countries'
            {
                "_id": "FR",
                "name": "France",
                "population": 67000000,
                "capital": {
                    "name": "Paris",
                    "population": 3000000,
                    "area": 105.4,
                }
            }

        In this case the argument values will be as follows:
            1. mongo_client -        active Pymongo client

            2. database -           'countries_database'      | our data is here
            3. collection -         'countries'               | in this collection
            4. other_collection -   'cities'                  | we want to add more data from this
            5. field_name -         'capital_city_id'         | field in current collection
            6. other_field_name -   '_id'                     | field in other collection
            7. new_field_name -     'capital'                 | how to save joined/looked up info (can be same as field_name)
            8. as_array -           False                     | as we have only 1 capital for given country, storing it as array makes less sense
            9. delete_source_field_name_after_lookup - True   | if set to False, we will still have 'capital_city_id' field
            10. delete_other_collection -              False  | if non capital cities are not useful for us, we can delete them after lookup
            11. do_not_copy_fields -                   ["_id] | as you can see, in result document, "capital" field value has no key "_id"

    In this case, we had one to one relationship, but if you have one to many and want to add data into one side
    from many side, you can set as_array argument to True, so that what matches in other collection, will be stored
    as an array in current collection.

    """

    # sanity checks
    if (
        mongo_client[database][collection].count_documents(
            {
                field_name: {"$exists": True},
            }
        )
        == 0
    ):
        raise ValueError(
            f"In database {database} & collection {collection} no document found "
            f"with field name {field_name}, denormalization can not be done "
        )

    if (
        mongo_client[database][other_collection].count_documents(
            {
                other_field_name: {"$exists": True},
            }
        )
        == 0
    ):
        raise ValueError(
            f"In database {database} & collection {other_collection} no document found "
            f"with field name {other_field_name}, denormalization can not be done "
        )

    # actual work
    pipeline = [
        {
            "$project": {
                "_id": 1,
                field_name: 1,
            }
        },
        {
            "$lookup": {
                "from": other_collection,
                "localField": field_name,
                "foreignField": other_field_name,
                "as": new_field_name,
            }
        },
        {"$addFields": {new_field_name: {"$first": f"${new_field_name}"}}},
        {
            "$merge": {
                "into": collection,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]

    if as_array:
        del pipeline[-2]

    # add validation, so that we can not save just {}-s if removed all fields (?)
    if do_not_copy_fields and isinstance(do_not_copy_fields, list):
        pipeline.insert(-1, {"$unset": [f"{new_field_name}.{i}" for i in do_not_copy_fields]})

    _doc_before = mongo_client[database][collection].find_one()

    # Lets do it!
    agg_result = list(mongo_client[database][collection].aggregate(pipeline))

    if delete_source_field_name_after_lookup:

        if field_name == new_field_name:
            raise ValueError(
                f"field_name and new_field_name are same ({field_name})",
                f"deleting newly added data is not probably what you want!",
            )

        # new_expected_type = [] if as_array else {}
        new_expected_type = "array" if as_array else "object"

        update_result = mongo_client[database][collection].update_many(
            {
                new_field_name: {
                    "$type": new_expected_type,
                }
            },
            {"$unset": {field_name: ""}},
        )

    # show before and after docs after pipeline runs to make visual comparison faster
    # in the CLI (maybe we should not show all fields if there are a lot in 1 doc)
    _draw_before_and_after_example_docs_on_screen_side_by_side(
        _doc_before,
        mongo_client[database][collection].find_one(),
        field_name,
        new_field_name,
    )

    if delete_other_collection:
        mongo_client[database][other_collection].drop()

        RICH_CONSOLE.print(f". Collection '{other_collection}' deleted ⛔", style="bold red")

    return True
