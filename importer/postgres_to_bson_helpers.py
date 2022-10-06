from decimal import Decimal
from bson.decimal128 import Decimal128


def _convert_python_decimals_to_bson_decimal128(dict_item):
    """
    This function iterates a dictionary looking for types of Decimal and converts them to Decimal128
    Embedded dictionaries and lists are called recursively.

    source: https://stackoverflow.com/questions/61456784/pymongo-cannot-encode-object-of-type-decimal-decimal
    """

    if dict_item is None:
        return None

    for k, v in list(dict_item.items()):
        if isinstance(v, dict):
            _convert_python_decimals_to_bson_decimal128(v)
        elif isinstance(v, list):
            for l in v:
                _convert_python_decimals_to_bson_decimal128(l)
        elif isinstance(v, Decimal):
            dict_item[k] = Decimal128(str(v))

    return dict_item


def _reshape_doc_to_make_sure_postgres_primary_keys_will_be_primary_keys_in_mongo(doc, pk_columns):
    """
    In doc, we have just basic dictionary of PostgreSQL data in { column_1 : value_1,  } format
    and using this function, we move these keys into _id field and remove
    existing keys to not save same data twice.

    This way, primary key/s from postgres will also be primary keys in Mongo.

    Mongo _id field values will be existing primary key values if 1 column was used
    as primary key in postgres, otherwise dict/object of {col_1: val_1, col_2:val_2...}
    shape will be used
    """

    if len(pk_columns) == 1:
        doc["_id"] = doc.pop(pk_columns[0])
    else:
        doc["_id"] = {i: doc.pop(i) for i in pk_columns}

    return doc


def refine_postgres_doc_for_mongodb(
    doc,
    convert_primary_keys_to_mongo_ids=False,
    pk_columns=False,
):
    """
    Later we may add more steps to refine document
    based on things that should be changed when moving from postgres to Mongo,
    for now we try to bypass just simple errors that showed up.
    """
    doc = _convert_python_decimals_to_bson_decimal128(doc)

    if convert_primary_keys_to_mongo_ids:

        # maybe we copy lots of tables and some of them do not have primary key
        # if this is the case, just skip this step for these docs
        if len(pk_columns) > 0:

            assert all(
                (i in doc for i in pk_columns)
            ), f"Not all primary keys were found in given document.\n document:{doc} primary_keys:{pk_columns}"

            doc = _reshape_doc_to_make_sure_postgres_primary_keys_will_be_primary_keys_in_mongo(
                doc, pk_columns=pk_columns
            )

    return doc
