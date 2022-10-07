import re


def _only_leave_items_that_match_the_patterns(
    items_list, leave_patterns=None, remove_patterns=None
):
    """
    Get list of items and return only part of them,
    based on leave_patterns and remove patterns that we provide.

    items are first filtered out by leave_patterns and
    what remains is then filtered out by remove_patterns.

    Step will be skipped if patterns variable is None(default).

    pattern can be normal string or raw string. If you are using special characters,
    like \ in regex/pattern, make sure to use two of them instead of one or use a raw string r'\'

    For a few examples see test_only_leave_items_that_match_the_patterns function in tests.

    args:
        1. items_list - list of string of items, ex: ["cat", "dog", "cool", "coffee"]

        2. leave_patterns - list of strings of regex patterns of items that we want to leave,
                            ex: ["cat", "^c"]

        3. remove_patterns - list of strings of regex patterns of items that we want to remove,
                            ex: ["ff"]

        in this example, result should be ["cat", "cool"] as "dog" does not satisfy
        any leave_patterns and "coffee" was removed by remove_patterns as it contains "ff".
    """

    # filter out using leave_patterns
    if leave_patterns is not None:
        items_list = [i for i in items_list if any([re.search(p, i) for p in leave_patterns])]

    # filter out using remove_patterns
    if remove_patterns is not None:
        items_list = [i for i in items_list if not any([re.search(p, i) for p in remove_patterns])]

    return items_list
