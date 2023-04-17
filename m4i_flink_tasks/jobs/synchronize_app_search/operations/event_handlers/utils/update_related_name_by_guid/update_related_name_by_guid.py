from typing import List


class ListLengthMismatchError(Exception):
    """
    An exception raised when the given lists of guids and names are not of equal length.

    Attributes:
        len_guids (int): The length of the guids list.
        len_names (int): The length of the names list.
    """

    def __init__(self, len_guids: int, len_names: int):
        error_message = f"Length of guids ({len_guids}) and names ({len_names}) lists must be equal."
        super().__init__(error_message)
    # END __init__
# END ListLengthMismatchError


class GuidNotFoundError(Exception):
    """
    An exception raised when no matching entry is found for a given guid.

    Attributes:
        guid (str): The guid for which no matching entry is found.
    """

    def __init__(self, guid: str):
        error_message = f"GUID '{guid}' not found in the provided list of GUIDs."
        super().__init__(error_message)
    # END __init__
# END GuidNotFoundError


def update_related_name_by_guid(guids: List[str], names: List[str], guid: str, name: str) -> List[str]:
    """
    Update the name corresponding to a guid in a list of names.

    Args:
        guids (List[str]): A list of guids.
        names (List[str]): A list of names corresponding to the guids.
        guid (str): The guid for which the name should be updated.
        name (str): The new name to replace the current name.

    Returns:
        List[str]: A new list with the updated name.

    Raises:
        ListLengthMismatchError: If the lengths of guids and names lists are not equal.
        GuidNotFoundError: If the guid is not found in the guids list.
    """

    if len(guids) != len(names):
        raise ListLengthMismatchError(len(guids), len(names))
    # END IF

    try:
        name_index = guids.index(guid)
    except ValueError as exc:
        raise GuidNotFoundError(guid) from exc
    # END TRY

    result = names.copy()
    result[name_index] = name

    return result
# END update_related_name_by_guid
