"""Typed engineering states. Unknown dimensions are explicit, never guessed."""
from dataclasses import asdict, dataclass, field
from enum import Enum


class StateRole(str, Enum):
    INPUT_CRITERION = 'INPUT_CRITERION'
    REQUIREMENT = 'REQUIREMENT'
    CALCULATED_RESULT = 'CALCULATED_RESULT'
    SELECTED_EQUIPMENT = 'SELECTED_EQUIPMENT'
    INSTALLED_CONFIGURATION = 'INSTALLED_CONFIGURATION'
    TOPOLOGY = 'TOPOLOGY'
    ROUTING = 'ROUTING'
    OPERATING_MODE = 'OPERATING_MODE'
    CAPACITY = 'CAPACITY'
    COUNT = 'COUNT'
    OTHER = 'OTHER'


class Cardinality(str, Enum):
    ONE_TO_ONE = '1→1'
    ONE_TO_MANY = '1→N'
    MANY_TO_ONE = 'N→1'
    MANY_TO_MANY = 'N→M'


@dataclass(frozen=True)
class EngineeringState:
    engineering_subject: str
    scope: str
    state_role: StateRole
    value: str
    consumer_composition: tuple | None = None
    local_or_global: str | None = None
    component_or_total: str | None = None
    operating_mode: str | None = None
    # Engineering phase (design/installed/etc.), not OLD/NEW version direction.
    stage_phase: str | None = None
    physical_quantity: str | None = None
    unit: str | None = None
    functional_role: str | None = None
    branch_identity: str | None = None
    comparison_cardinality: Cardinality = Cardinality.ONE_TO_ONE
    # Members of the parent functional subject, not forced pairwise replacements.
    members: tuple = ()
    calculation_basis: str | None = None
    evidence_ids: tuple = ()
    provenance: dict = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, 'state_role', StateRole(self.state_role))
        object.__setattr__(self, 'comparison_cardinality', Cardinality(self.comparison_cardinality))
        for name in ('engineering_subject', 'scope', 'value'):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError('Required state field: ' + name)
        for name in ('local_or_global', 'component_or_total', 'operating_mode', 'stage_phase',
                     'physical_quantity', 'unit', 'functional_role', 'branch_identity', 'calculation_basis'):
            if getattr(self, name) is not None and not isinstance(getattr(self, name), str):
                raise ValueError('State dimension must be a string or explicit unknown: ' + name)
        for name in ('consumer_composition', 'members', 'evidence_ids'):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, (tuple, list)) or any(not isinstance(x, str) or not x.strip() for x in value):
                    raise ValueError('Invalid typed sequence: ' + name)
                if len(value) != len(set(value)):
                    raise ValueError('Duplicate typed member: ' + name)
                object.__setattr__(self, name, tuple(value))

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class StateTransition:
    old: EngineeringState
    new: EngineeringState
    identity_basis: str
    mapping_basis: str = ''
    # Source conflict prevents a direct event; no voting between source records.
    source_conflict: str = ''
    # Explicitly part of the local source-audit fixture or future witness audit.
    # This is not inferred from a matching pair of numeric strings.
    state_support: str = 'UNKNOWN'
    design_use: str = ''

    def cardinality_errors(self):
        old, new = self.old, self.new
        if old.comparison_cardinality != new.comparison_cardinality:
            return ['CARDINALITY_MISMATCH']
        card = old.comparison_cardinality
        if card == Cardinality.ONE_TO_ONE:
            return [] if len(old.members) <= 1 and len(new.members) <= 1 else ['NOT_ONE_TO_ONE']
        if not self.mapping_basis.strip() or not old.members or not new.members:
            return ['PARENT_TRANSFORMATION_MAPPING_REQUIRED']
        counts = (len(old.members), len(new.members))
        valid = {Cardinality.ONE_TO_MANY: counts[0] == 1 and counts[1] > 1,
                 Cardinality.MANY_TO_ONE: counts[0] > 1 and counts[1] == 1,
                 Cardinality.MANY_TO_MANY: counts[0] > 1 and counts[1] > 1}
        return [] if valid.get(card, False) else ['INVALID_TRANSFORMATION_CARDINALITY']
