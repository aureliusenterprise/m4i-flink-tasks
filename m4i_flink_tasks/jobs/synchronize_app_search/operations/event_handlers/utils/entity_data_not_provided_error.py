class EntityDataNotProvidedError(Exception):
    """
    Raised when the required entity data is not provided with the received change message.
    """

    def __init__(self, message_guid: str):
        """
        Initialize the error message using the given message GUID.

        :param message_guid: The GUID of the message where the entity data is missing.
        """

        error_message = f"Entity data is missing in the received change message with GUID: {message_guid}"
        super().__init__(error_message)
    # END __init__
# END EntityDataNotProvidedError