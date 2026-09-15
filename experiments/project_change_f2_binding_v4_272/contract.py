"""Role-specific, source-traceable references for a normalized state or claim."""
from dataclasses import dataclass
from enum import Enum

from experiments.project_change_contracts_272.states import EngineeringState as BaseState


class EvidenceRole(str, Enum):
    STATE_VALUE = 'STATE_VALUE'
    SUBJECT_IDENTITY = 'SUBJECT_IDENTITY'
    SCOPE_BINDING = 'SCOPE_BINDING'
    COUNTER_EVIDENCE = 'COUNTER_EVIDENCE'
    CONDITION_SUPPORT = 'CONDITION_SUPPORT'
    CONFLICT_EVIDENCE = 'CONFLICT_EVIDENCE'
    CHANGE_OBSERVATION = 'CHANGE_OBSERVATION'


@dataclass(frozen=True)
class EvidenceBinding:
    evidence_id: str
    role: EvidenceRole
    side: str
    subject: str
    claim_id: str
    raw_path: str
    provenance: dict
    witness_validated: bool

    def __post_init__(self):
        object.__setattr__(self, 'role', EvidenceRole(self.role))
        for name in ('evidence_id', 'subject', 'claim_id', 'raw_path'):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError('Missing binding field: ' + name)
        if self.side not in {'old', 'new'}:
            raise ValueError('Binding side must be old or new')
        for key in ('raw_response_hash', 'packet_hash', 'document_version', 'page', 'source_receipt'):
            if not self.provenance.get(key):
                raise ValueError('Missing binding provenance: ' + key)


@dataclass(frozen=True)
class EngineeringState(BaseState):
    evidence_bindings: tuple = ()

    def __post_init__(self):
        super().__post_init__()
        object.__setattr__(self, 'evidence_bindings', tuple(self.evidence_bindings))
        if any(not isinstance(b, EvidenceBinding) for b in self.evidence_bindings):
            raise ValueError('State requires typed evidence bindings')
        if set(self.evidence_ids) != {b.evidence_id for b in self.evidence_bindings}:
            raise ValueError('State evidence IDs must equal the union of bound roles')
        if len({b.side for b in self.evidence_bindings}) > 1:
            raise ValueError('State cannot mix OLD and NEW evidence')

    def ids_for(self, role):
        return tuple(dict.fromkeys(b.evidence_id for b in self.evidence_bindings if b.role == role))

    @property
    def state_value_evidence_ids(self):
        return self.ids_for(EvidenceRole.STATE_VALUE)

    @property
    def subject_identity_evidence_ids(self):
        return self.ids_for(EvidenceRole.SUBJECT_IDENTITY)

    def to_dict(self):
        return super().to_dict() | dict(
            state_value_evidence_ids=self.state_value_evidence_ids,
            subject_identity_evidence_ids=self.subject_identity_evidence_ids,
            evidence_role_ids={r.value: self.ids_for(r) for r in EvidenceRole if self.ids_for(r)})
