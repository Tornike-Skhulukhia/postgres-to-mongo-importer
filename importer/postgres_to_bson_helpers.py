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


def refine_postgres_doc_for_mongodb(doc):
    """
    Later we may add more steps to refine document
    based on things that should be changed when moving from postgres to Mongo,
    for now we try to bypass just simple errors that showed up.
    """
    doc = _convert_python_decimals_to_bson_decimal128(doc)

    return doc
