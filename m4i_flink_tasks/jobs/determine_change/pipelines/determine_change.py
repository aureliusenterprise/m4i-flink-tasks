from pyflink.common.typeinfo import Types
from pyflink.datastream import DataStream

from ....services import DeadLetterBoxService
from ..operations import DetermineChange, GetResultDetermineChange


class DetermineChangePipeline:
    """
    A pipeline class that processes a stream of changes by applying the DetermineChange
    and GetResultDetermineChange operations. The pipeline performs a series of transformations
    on the input DataStream object.

    Attributes:
    changes (DataStream): A stream of all changes derived from the given change events.
    parsed_changes (DataStream): Emits each change found one after the other.
    """

    def __init__(self, changes_stream: DataStream, dead_letter_box_service: DeadLetterBoxService):
        """
        Initialize the DetermineChangePipeline by setting up a series of transformations on the changes stream.

        Args:
        changes_stream (DataStream): The input DataStream object containing the changes.
        dead_letter_box_service (DeadLetterBoxService): An instance of DeadLetterBoxService to handle errors.
        """

        self.changes = (
            changes_stream
            .map(DetermineChange(dead_letter_box_service), Types.LIST(Types.STRING()))
            .name("calculate determine_change")
        )

        self.parsed_changes = (
            self.changes
            .filter(bool)
            .flat_map(GetResultDetermineChange(), Types.STRING())
            .name("flatten results determine_change")
        )
    # END __init__

# END DetermineChangePipeline
