
from dataclasses import dataclass
from tokenize import Double

from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class DeadLetterBoxMesage(DataClassJsonMixin):
    '''
    This data class "DeadLetterBoxMessage" describes the structure of a message forwarded to the dead letter box.
    This is a Kafka topic which contains a message for each input notification that could not be handled by any of the jobs implemented.
    '''

    timestamp: Double
    original_notification: str
    job: str
    description: str
    exception_class: str
    remark: str
