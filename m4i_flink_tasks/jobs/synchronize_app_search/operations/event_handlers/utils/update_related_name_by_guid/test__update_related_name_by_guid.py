import pytest

from .update_related_name_by_guid import (GuidNotFoundError,
                                          ListLengthMismatchError,
                                          update_related_name_by_guid)


def test__update_related_name_by_guid():
    guids = ["1", "2", "3"]
    names = ["Alice", "Bob", "Carol"]
    guid_to_update = "2"
    new_name = "Bobby"

    updated_names = update_related_name_by_guid(
        guids=guids,
        names=names,
        guid=guid_to_update,
        name=new_name
    )

    assert updated_names == ["Alice", "Bobby", "Carol"]
# END test__update_related_name_by_guid


def test__update_related_name_by_guid_guid_not_found():
    guids = ["1", "2", "3"]
    names = ["Alice", "Bob", "Carol"]
    guid_to_update = "4"
    new_name = "Bobby"

    with pytest.raises(GuidNotFoundError):
        update_related_name_by_guid(
            guids=guids,
            names=names,
            guid=guid_to_update,
            name=new_name
        )
    # END WITH
# END test__update_related_name_by_guid_guid_not_found


def test__update_related_name_by_guid_mismatched_lengths():
    guids = ["1", "2", "3"]
    names = ["Alice", "Bob"]

    with pytest.raises(ListLengthMismatchError):
        update_related_name_by_guid(
            guids=guids,
            names=names,
            guid="2",
            name="Bobby"
        )
    # END WITH
# END test__update_related_name_by_guid_mismatched_lengths


def test__update_related_name_by_guid_empty_lists():
    guids = []
    names = []

    with pytest.raises(GuidNotFoundError):
        update_related_name_by_guid(
            guids=guids,
            names=names,
            guid="1",
            name="Alice"
        )
    # END WITH
# END test__update_related_name_by_guid_empty_lists
