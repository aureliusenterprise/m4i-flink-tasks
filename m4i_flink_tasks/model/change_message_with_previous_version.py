from dataclasses import dataclass
from typing import Optional

from dataclasses_json import LetterCase, dataclass_json
from m4i_atlas_core import AtlasChangeMessage, Entity


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class AtlasChangeMessageWithPreviousVersion(AtlasChangeMessage):
    previous_version: Optional[Entity] = None
# END AtlasChangeMessageWithPreviousVersion
