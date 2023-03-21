from typing import List

from m4i_atlas_core import AtlasChangeMessage
from pyflink.datastream.functions import MapFunction

from .....services import DeadLetterBoxService
from .event_handlers import EVENT_HANDLERS


def handle_determine_change(kafka_notification: str) -> List[str]:
    """
    Determine the change event type from a Kafka notification and call the appropriate event handler.

    Args:
        kafka_notification (str): A JSON-formatted string containing the AtlasChangeMessage.

    Returns:
        List[str]: A list of JSON-formatted strings containing change information.

    Raises:
        NotImplementedError: If the operation_type in the AtlasChangeMessage is not found in the EVENT_HANDLERS dictionary.
    """

    change_message = AtlasChangeMessage.from_json(kafka_notification)

    operation_type = change_message.message.operation_type

    if operation_type not in EVENT_HANDLERS:
        raise NotImplementedError(
            f"unknown event type: {operation_type}"
        )
    # END IF

    event_handler = EVENT_HANDLERS[operation_type]
    messages = event_handler(change_message)

    return [message.to_json() for message in messages]
# END handle_determine_change


class DetermineChange(MapFunction):
    """
    A class that implements the main operation for determining changes in entities from Kafka notifications.

    Attributes:
        dead_letter_box_service (DeadLetterBoxService): An instance of DeadLetterBoxService for handling errors.
    """

    def __init__(self, dead_letter_box_service: DeadLetterBoxService):
        """
        Initialize the DetermineChange class.

        Args:
            dead_letter_box_service (DeadLetterBoxService): An instance of DeadLetterBoxService for handling errors.
        """

        self.dead_letter_box_service = dead_letter_box_service
    # END __init__

    def map(self, value: str):
        """
        Process a Kafka notification by determining the entity change and returning the appropriate EntityMessages.

        Any exception that occurs during the processing will be caught and passed to the DeadLetterBoxService.

        Args:
            value (str): A JSON-formatted string containing the AtlasChangeMessage.

        Returns:
            List[str]: A list of JSON-formatted strings containing change information.
        """

        try:
            res = handle_determine_change(value)
            return res
        except NotImplementedError as not_implemented:
            self.dead_letter_box_service.handle_error(
                error=not_implemented,
                payload=value
            )
        # END TRY
    # END map
# END DetermineChange
