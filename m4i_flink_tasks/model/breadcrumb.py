from dataclasses import dataclass
from typing import Dict, List, Tuple

from .app_search_document import AppSearchDocument


class BreadcrumbLengthMismatchError(Exception):
    """
    Raised when the lengths of the lists of guids, names, and type_names
    used to construct a Breadcrumb object are not equal.

    Attributes:
        guids_len (int): Length of the list of guids.
        names_len (int): Length of the list of names.
        type_names_len (int): Length of the list of type_names.
    """

    def __init__(self, guids_len: int, names_len: int, type_names_len: int):
        error_message = (
            "The lengths of the breadcrumb lists are not equal:\n"
            f"  - GUIDs: {guids_len}\n"
            f"  - Names: {names_len}\n"
            f"  - Type Names: {type_names_len}\n"
            "Please ensure that the input lists for guids, names, and type names have the same length."
        )
        super().__init__(error_message)
    # END __init__
# END BreadcrumbLengthMismatchError


@dataclass
class BreadcrumbMetadata():
    """
    Represents metadata for a breadcrumb item.
    """

    guid: str
    name: str
    type_name: str
# END BreadcrumbMetadata


class Breadcrumb(List[str]):
    """
    Represents a breadcrumb as a list of GUIDs with associated metadata for each item.
    """

    def __init__(self):
        super().__init__()
        self.metadata: Dict[str, BreadcrumbMetadata] = {}
    # END __init__

    @classmethod
    def from_app_search_document(cls, document: AppSearchDocument):
        """
        Constructs a new Breadcrumb instance from an AppSearchDocument object.

        Args:
            document (AppSearchDocument): An AppSearchDocument object containing breadcrumb data.

        Returns:
            Breadcrumb: A Breadcrumb instance populated with data from the AppSearchDocument object.

        Raises:
            BreadcrumbLengthMismatchError: If the lengths of the guids, names, and type_names lists are not equal.
        """

        if not len(document.breadcrumbguid) == len(document.breadcrumbname) == len(document.breadcrumbtype):
            raise BreadcrumbLengthMismatchError(
                len(document.breadcrumbguid),
                len(document.breadcrumbname),
                len(document.breadcrumbtype)
            )
        # END IF

        result = cls()

        for guid, name, type_name in zip(document.breadcrumbguid, document.breadcrumbname, document.breadcrumbtype):
            result.append(guid, name, type_name)
        # END LOOP

        return result
    # END from_app_search_document

    def serialize(self):
        """
        Serializes the Breadcrumb object into a tuple containing three lists:
        guids, names, and type_names.

        Returns:
            tuple: A tuple containing three lists:
                   - guids: A list of breadcrumb guids.
                   - names: A list of breadcrumb names associated with each guid.
                   - type_names: A list of breadcrumb type names associated with each guid.
        """

        guids = self.copy()
        names = [self.metadata[guid].name for guid in self]
        type_names = [self.metadata[guid].type_name for guid in self]

        return guids, names, type_names
    # END serialize

    def append(self, guid: str, name: str, type_name: str):
        """
        Appends a new breadcrumb item and its metadata.
        """

        super().append(guid)
        self.metadata[guid] = BreadcrumbMetadata(guid, name, type_name)
    # END append

    def extend(self, breadcrumb: 'Breadcrumb'):
        """
        Extends the breadcrumb with another breadcrumb and merges metadata.
        """

        super().extend(breadcrumb)
        self.metadata.update(breadcrumb.metadata)
    # END extend

    def insert(self, index: int, guid: str, name: str, type_name: str):
        """
        Inserts a breadcrumb item and its metadata at the specified index.
        """

        super().insert(index, guid)
        self.metadata[guid] = BreadcrumbMetadata(guid, name, type_name)
    # END insert

    def remove(self, guid: str):
        """
        Removes a breadcrumb item and its associated metadata.
        """

        super().remove(guid)
        del self.metadata[guid]
    # END remove

    def pop(self, index: int = -1):
        """
        Pops a breadcrumb item and removes its associated metadata.
        """

        guid = super().pop(index)
        del self.metadata[guid]
        return guid
    # END pop

    def clear(self):
        """
        Clears the breadcrumb and metadata.
        """

        super().clear()
        self.metadata.clear()
    # END clear

    def __delitem__(self, index: int):
        """
        Deletes a breadcrumb item and its associated metadata.
        """

        guid = self[index]
        super().__delitem__(index)
        del self.metadata[guid]
    # END __delitem__

    def __setitem__(self, index: int, guid_metadata: Tuple[str, str, str]):
        """
        Sets a breadcrumb item and updates the associated metadata.
        """

        guid, name, type_name = guid_metadata
        old_guid = self[index]

        # Update the list with the new guid.
        super().__setitem__(index, guid)

        # Remove the old metadata associated with the old guid.
        del self.metadata[old_guid]

        # Update the metadata dictionary with the new guid and metadata.
        self.metadata[guid] = BreadcrumbMetadata(guid, name, type_name)
    # END __setitem__
# END Breadcrumb
