
from dataclasses import dataclass, field
from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json
from typing import List


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class AppSearchSchema(DataClassJsonMixin):
    id: str
    sourcetype: str
    name: str
    guid: str
    
    typename: str
    referenceablequalifiedname:str

    m4isourcetype: List[str]
    supertypenames: List[str]

    dqscore_accuracy: float = field(default_factory=float)
    dqscore_timeliness: float = field(default_factory=float)
    dqscoresum_accuracy: float = field(default_factory=float)
    dqscore_validity: float = field(default_factory=float)
    dqscore_completeness: float = field(default_factory=float)
    dqscoresum_validity: float = field(default_factory=float)
    dqscorecnt_overall: float = field(default_factory=float)
    dqscorecnt_timeliness: float = field(default_factory=float)
    dqscoresum_timeliness: float = field(default_factory=float)
    dqscoresum_completeness: float = field(default_factory=float)
    dqscore_uniqueness: float = field(default_factory=float)
    dqscorecnt_completeness: float = field(default_factory=float)
    dqscoresum_uniqueness: float = field(default_factory=float)
    dqscore_overall: float = field(default_factory=float)
    dqscorecnt_uniqueness: float = field(default_factory=float)
    businessruleid: float = field(default_factory=float)
    dqscoresum_overall: float = field(default_factory=float)
    dqscorecnt_accuracy: float = field(default_factory=float)
    dqscorecnt_validity: float = field(default_factory=float)

    parentguid: str =  field(default_factory=str)
    entityname : str =  field(default_factory=str)
    definition: str = field(default_factory=str)
    email: str = field(default_factory=str)
    deriveddataownerguid: str = field(default_factory=str)
    deriveddomainleadguid: str = field(default_factory=str)
    deriveddatastewardguid: str = field(default_factory=str)
    
    derivedfield: List[str] = field(default_factory=list)
    deriveddataattribute: List[str] = field(default_factory=list)
    deriveddataentity: List[str] = field(default_factory=list)
    qualityguid_completeness: List[str] = field(default_factory=list)
    deriveddataentityguid: List[str] = field(default_factory=list)
    derivedsystem: List[str] = field(default_factory=list)
    qualityguid_timeliness: List[str] = field(default_factory=list)
    deriveddataset: List[str] = field(default_factory=list)
    derivedsystemguid: List[str] = field(default_factory=list)
    breadcrumbname: List[str] = field(default_factory=list)
    breadcrumbguid: List[str] = field(default_factory=list)
    deriveddataattributeguid: List[str] = field(default_factory=list)
    deriveddatasetnames: List[str] = field(default_factory=list)
    derivedperson: List[str] = field(default_factory=list)
    derivedfieldguid: List[str] = field(default_factory=list)
    derivedentityguids: List[str] = field(default_factory=list)
    deriveddatasetguids: List[str] = field(default_factory=list)
    deriveddatasetguid: List[str] = field(default_factory=list)
    classificationstext: List[str] = field(default_factory=list) 
    qualityguid_uniqueness: List[str] = field(default_factory=list)
    derivedpersonguid: List[str] = field(default_factory=list)
    qualityguid_accuracy: List[str] = field(default_factory=list)
    derivedcollection: List[str] = field(default_factory=list)
    deriveddatadomainguid: List[str] = field(default_factory=list)
    derivedcollectionguid: List[str] = field(default_factory=list)
    qualityguid_validity: List[str] = field(default_factory=list)
    deriveddatadomain: List[str] = field(default_factory=list)
    derivedentitynames: List[str] = field(default_factory=list)
    breadcrumbtype: List[str] = field(default_factory=list)
    