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
            ).replace(f'"{field_1}"', f'[red]"{field_1}"[/red]'),
        )
    )

    layout["right"].update(
        Align.center(
            json.dumps(
                _convert_bson_decimal128_to_strs(doc_2),
                indent=2,
            ).replace(f'"{field_2}"', f'[red]"{field_2}"[/red]'),
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
    as_list=True,
    delete_other_collection=False,
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

    if as_list:
        del pipeline[-2]

    _doc_before = mongo_client[database][collection].find_one()

    # Lets do it!
    agg_result = list(mongo_client[database][collection].aggregate(pipeline))

    _doc_after = mongo_client[database][collection].find_one()

    # show before and after docs after pipeline runs to make visual comparison faster
    # in the CLI (maybe we should not show all fields if there are a lot in 1 doc)
    _draw_before_and_after_example_docs_on_screen_side_by_side(
        _doc_before,
        _doc_after,
        field_name,
        new_field_name,
    )

    if delete_other_collection:
        mongo_client[database][other_collection].drop()

        RICH_CONSOLE.print(f". Collection '{other_collection}' deleted ⛔", style="bold red")

    return True
