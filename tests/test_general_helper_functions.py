from importer.basic.general_helper_functions import (
    _only_leave_items_that_match_the_patterns,
)


def test_only_leave_items_that_match_the_patterns():
    items_list = ["cat", "dog", "cool", "coffee"]

    # very basic tests
    assert (
        _only_leave_items_that_match_the_patterns(
            items_list=items_list,
        )
        == items_list
    )

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list,
        leave_patterns=["cat", "^c"],
    ) == ["cat", "cool", "coffee"]

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list,
        remove_patterns=["^c"],
    ) == ["dog"]

    assert (
        _only_leave_items_that_match_the_patterns(
            items_list=items_list,
            remove_patterns=["^(c|d)"],
        )
        == []
    )

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list,
        leave_patterns=["(t|g)$", "cool"],
    ) == ["cat", "dog", "cool"]

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list,
        remove_patterns=["(t|g)$", "cool"],
    ) == ["coffee"]

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list,
        remove_patterns=[r"(.)\1{1}"],
    ) == ["cat", "dog"]

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list,
        leave_patterns=["cat", "^c"],
        remove_patterns=["ff"],
    ) == ["cat", "cool"]

    # a bit more realistic tests
    items_list = [
        "id",
        "title",
        "director",
        "filming_started_at",
        "filming_ended_at",
        "duration",
        "actors_number",
        "imdb_rating",
        "imdb_voters_number",
    ]

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list, leave_patterns=["_at$"]
    ) == [
        "filming_started_at",
        "filming_ended_at",
    ]

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list, remove_patterns=["_at$"]
    ) == [
        "id",
        "title",
        "director",
        "duration",
        "actors_number",
        "imdb_rating",
        "imdb_voters_number",
    ]

    assert (
        _only_leave_items_that_match_the_patterns(
            items_list=items_list,
            leave_patterns=[],
            remove_patterns=["_at$"],
        )
        == []
    )

    assert _only_leave_items_that_match_the_patterns(
        items_list=items_list,
        leave_patterns=["_number", "title", "d"],
        remove_patterns=["id", "imdb"],
    ) == [
        "title",
        "director",
        "filming_started_at",
        "filming_ended_at",
        "duration",
        "actors_number",
    ]
