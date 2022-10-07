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
    """ """

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
