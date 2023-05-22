import pytest

from ..model import AppSearchDocument
from .breadcrumb import (Breadcrumb, BreadcrumbLengthMismatchError,
                         BreadcrumbMetadata)


@pytest.fixture
def app_search_document():
    return AppSearchDocument(
        guid="1234",
        name="test",
        referenceablequalifiedname="test",
        typename="test_type",
        breadcrumbguid=["1", "2", "3"],
        breadcrumbname=["Item 1", "Item 2", "Item 3"],
        breadcrumbtype=["Type 1", "Type 2", "Type 3"]
    )
# END app_search_document


def test__from_app_search_document(app_search_document: AppSearchDocument):
    breadcrumb = Breadcrumb.from_app_search_document(app_search_document)

    assert len(breadcrumb) == 3
    assert breadcrumb == ["1", "2", "3"]
    assert breadcrumb.metadata == {
        "1": BreadcrumbMetadata(guid="1", name="Item 1", type_name="Type 1"),
        "2": BreadcrumbMetadata(guid="2", name="Item 2", type_name="Type 2"),
        "3": BreadcrumbMetadata(guid="3", name="Item 3", type_name="Type 3"),
    }
# END test__from_app_search_document

def test__from_app_search_document_length_mismatch():
    document = AppSearchDocument(
        guid="1234",
        name="test",
        referenceablequalifiedname="test",
        typename="test_type",
        breadcrumbguid=["guid1", "guid2"],
        breadcrumbname=["name1", "name2", "name3"],
        breadcrumbtype=["type1", "type2"]
    )

    with pytest.raises(BreadcrumbLengthMismatchError):
        Breadcrumb.from_app_search_document(document)
    # END WITH
# END test__from_app_search_document_length_mismatch


def test__breadcrumb_initialization():
    breadcrumb = Breadcrumb()

    assert len(breadcrumb) == 0
    assert len(breadcrumb.metadata) == 0
# END test__breadcrumb_initialization




def test__breadcrumb_append():
    breadcrumb = Breadcrumb()

    breadcrumb.append("guid1", "name1", "type1")

    assert len(breadcrumb) == 1
    assert len(breadcrumb.metadata) == 1
    assert breadcrumb[0] == "guid1"
    assert breadcrumb.metadata["guid1"].name == "name1"
# END test__breadcrumb_append


def test__breadcrumb_extend():
    breadcrumb1 = Breadcrumb()
    breadcrumb1.append("guid1", "name1", "type1")

    breadcrumb2 = Breadcrumb()
    breadcrumb2.append("guid2", "name2", "type2")

    breadcrumb1.extend(breadcrumb2)

    assert len(breadcrumb1) == 2
    assert len(breadcrumb1.metadata) == 2
    assert breadcrumb1[1] == "guid2"
    assert breadcrumb1.metadata["guid2"].name == "name2"
# END test__breadcrumb_extend


def test__breadcrumb_setitem():
    breadcrumb = Breadcrumb()
    breadcrumb.append("guid1", "name1", "type1")

    breadcrumb[0] = "guid2", "name2", "type2"

    assert len(breadcrumb) == 1
    assert len(breadcrumb.metadata) == 1
    assert breadcrumb[0] == "guid2"
    assert breadcrumb.metadata["guid2"].name == "name2"
    assert "guid1" not in breadcrumb.metadata
# END test__breadcrumb_setitem


def test__breadcrumb_insert():
    breadcrumb = Breadcrumb()
    breadcrumb.append("guid1", "name1", "type1")

    breadcrumb.insert(0, "guid2", "name2", "type2")

    assert len(breadcrumb) == 2
    assert len(breadcrumb.metadata) == 2
    assert breadcrumb[0] == "guid2"
    assert breadcrumb.metadata["guid2"].name == "name2"
# END test__breadcrumb_insert


def test__breadcrumb_remove():
    breadcrumb = Breadcrumb()
    breadcrumb.append("guid1", "name1", "type1")
    breadcrumb.append("guid2", "name2", "type2")

    breadcrumb.remove("guid1")

    assert len(breadcrumb) == 1
    assert len(breadcrumb.metadata) == 1
    assert breadcrumb[0] == "guid2"
    assert "guid1" not in breadcrumb.metadata
# END test__breadcrumb_remove


def test__breadcrumb_delitem():
    breadcrumb = Breadcrumb()
    breadcrumb.append("guid1", "name1", "type1")
    breadcrumb.append("guid2", "name2", "type2")

    del breadcrumb[0]

    assert len(breadcrumb) == 1
    assert len(breadcrumb.metadata) == 1
    assert breadcrumb[0] == "guid2"
    assert "guid1" not in breadcrumb.metadata
# END test__breadcrumb_delitem


def test__breadcrumb_pop_no_index():
    breadcrumb = Breadcrumb()
    breadcrumb.append("guid1", "name1", "type1")
    breadcrumb.append("guid2", "name2", "type2")

    popped_guid = breadcrumb.pop()

    assert len(breadcrumb) == 1
    assert len(breadcrumb.metadata) == 1
    assert breadcrumb[0] == "guid1"
    assert popped_guid == "guid2"
    assert "guid2" not in breadcrumb.metadata
# END test__breadcrumb_pop_no_index


def test__breadcrumb_pop_with_index():
    breadcrumb = Breadcrumb()
    breadcrumb.append("guid1", "name1", "type1")
    breadcrumb.append("guid2", "name2", "type2")

    popped_guid = breadcrumb.pop(0)

    assert len(breadcrumb) == 1
    assert len(breadcrumb.metadata) == 1
    assert breadcrumb[0] == "guid2"
    assert popped_guid == "guid1"
    assert "guid1" not in breadcrumb.metadata
# END test__breadcrumb_pop_with_index

def test__serialize():
    breadcrumb = Breadcrumb()
    breadcrumb.append("1", "Item 1", "Type 1")
    breadcrumb.append("2", "Item 2", "Type 2")
    breadcrumb.append("3", "Item 3", "Type 3")

    guids, names, type_names = breadcrumb.serialize()

    assert guids == ["1", "2", "3"]
    assert names == ["Item 1", "Item 2", "Item 3"]
    assert type_names == ["Type 1", "Type 2", "Type 3"]
# END test__serialize
