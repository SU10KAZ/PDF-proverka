"""Requirement-level delivery coverage; page presence is never sufficient.

COMPLETE certifies delivery of a bounded, source-grounded requirement. It does
not certify an engineering claim, OCR accuracy, or absence outside that scope.
"""
from dataclasses import asdict, dataclass
from enum import Enum
import hashlib
import json


class EvidenceType(str, Enum):
    TEXT_SECTION = 'TEXT_SECTION'
    TABLE_COMPLETE = 'TABLE_COMPLETE'
    GRAPHIC_REGION = 'GRAPHIC_REGION'
    NOTE = 'NOTE'
    EQUIPMENT_SCHEDULE = 'EQUIPMENT_SCHEDULE'
    COUNTER_EVIDENCE = 'COUNTER_EVIDENCE'


class Completeness(str, Enum):
    COMPLETE = 'COMPLETE'
    PARTIAL = 'PARTIAL'
    MISSING = 'MISSING'
    WRONG_SCOPE = 'WRONG_SCOPE'
    HEADER_ONLY = 'HEADER_ONLY'
    STAMP_ONLY = 'STAMP_ONLY'
    TRUNCATED = 'TRUNCATED'


PARTS = {
    EvidenceType.TEXT_SECTION: ('section',),
    EvidenceType.TABLE_COMPLETE: ('heading', 'rows', 'columns', 'footnotes'),
    EvidenceType.GRAPHIC_REGION: ('raster', 'region', 'labels', 'notes', 'related_nodes'),
    EvidenceType.NOTE: ('note',),
    EvidenceType.EQUIPMENT_SCHEDULE: ('heading', 'rows', 'columns', 'footnotes'),
    EvidenceType.COUNTER_EVIDENCE: ('counter_context',),
}


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class EvidenceRequirement:
    requirement_id: str
    subject: str
    side: str
    evidence_role: str
    document: str
    document_version: str
    page: int
    expected_semantic_content: str
    evidence_type: EvidenceType
    provenance: dict
    # An explicit region/section binding from the source audit or retriever.
    # A page number, query hit, or an LLM assertion alone cannot set this.
    scope_binding: str = ''
    required_parts: tuple = ()
    completeness: Completeness = Completeness.MISSING

    def __post_init__(self):
        for name in ('requirement_id', 'subject', 'evidence_role', 'document',
                     'document_version', 'expected_semantic_content'):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError('Missing requirement field: ' + name)
        if self.side not in {'OLD', 'NEW'} or type(self.page) is not int or self.page < 1:
            raise ValueError('Invalid evidence direction/page')
        object.__setattr__(self, 'evidence_type', EvidenceType(self.evidence_type))
        object.__setattr__(self, 'completeness', Completeness(self.completeness))
        if not self.provenance:
            raise ValueError('Requirement provenance required')
        if self.evidence_type == EvidenceType.COUNTER_EVIDENCE and self.side != 'OLD':
            raise ValueError('Counter-evidence must use same-version OLD')

    @property
    def parts(self):
        return set(self.required_parts or PARTS[self.evidence_type])


def assess(requirement, evidence):
    """Assess actual delivered units, never trust a caller's COMPLETE flag."""
    r = requirement
    located = [e for e in evidence if e.get('side') == r.side and e.get('page') == r.page]
    matched = [e for e in located if e.get('document') == r.document
               and e.get('document_version') == r.document_version
               and e.get('subject') == r.subject]
    status, reason, parts = Completeness.MISSING, 'No required evidence delivered', set()
    if located and not matched:
        status, reason = Completeness.WRONG_SCOPE, 'Subject/document/physical version differs'
    elif matched:
        kinds = {e.get('content_kind') for e in matched}
        complete_units = [e for e in matched if e.get('scope_binding') == r.scope_binding
                          and r.scope_binding and e.get('boundary_complete') is True
                          and not e.get('truncated') and e.get('provenance')
                          and e.get('content_kind') not in {'STAMP', 'HEADER'}
                          and (e.get('text') or e.get('raster'))
                          and (r.evidence_type != EvidenceType.GRAPHIC_REGION or e.get('raster'))]
        for e in complete_units:
            parts.update(e.get('delivered_parts', []))
        if r.parts <= parts:
            status, reason = Completeness.COMPLETE, ''
        elif kinds <= {'STAMP'}:
            status, reason = Completeness.STAMP_ONLY, 'Only title block/stamp delivered'
        elif kinds <= {'HEADER', 'STAMP'}:
            status, reason = Completeness.HEADER_ONLY, 'Heading without required body/rows'
        elif any(e.get('truncated') for e in matched):
            status, reason = Completeness.TRUNCATED, 'Required unit cut before its boundary'
        else:
            status = Completeness.PARTIAL
            reason = ('No verified semantic scope/boundary or missing parts: '
                      + ', '.join(sorted(r.parts - parts)))
    return dict(requirement=asdict(r) | {'required_parts': list(r.required_parts), 'completeness': status.value},
                completeness=status.value, missing_reason=reason,
                evidence_ids=[e['evidence_id'] for e in matched],
                delivered_parts=sorted(parts),
                provenance=[e.get('provenance', {}) for e in matched])


def coverage_receipt(requirements, evidence):
    ids = [r.requirement_id for r in requirements]
    if len(ids) != len(set(ids)):
        raise ValueError('Duplicate requirement IDs')
    rows = [assess(r, evidence) for r in requirements]
    subjects = []
    for subject in sorted({r.subject for r in requirements}):
        group = [r for r in rows if r['requirement']['subject'] == subject]
        subjects.append(dict(subject=subject, **{
            side.lower() + '_evidence': [r for r in group if r['requirement']['side'] == side]
            for side in ('OLD', 'NEW')}))
    return dict(schema='EVIDENCE_COVERAGE/1', requirements=rows, subjects=subjects,
                complete=bool(rows) and all(r['completeness'] == 'COMPLETE' for r in rows),
                requirements_hash=fingerprint([asdict(r) for r in requirements]),
                delivery_hash=fingerprint(evidence), absence_proven=False,
                completeness_meaning='Delivery of source-grounded requirements, not semantic truth')


def comparison_readiness(receipt, subject, novelty=False):
    rows = [r for r in receipt['requirements'] if r['requirement']['subject'] == subject]
    reasons = []
    for side in ('OLD', 'NEW'):
        selected = [r for r in rows if r['requirement']['side'] == side]
        if not selected or any(r['completeness'] != 'COMPLETE' for r in selected):
            reasons.append(side + '_EVIDENCE_INCOMPLETE')
    if novelty and not any(r['requirement']['evidence_type'] == 'COUNTER_EVIDENCE'
                           and r['completeness'] == 'COMPLETE' for r in rows):
        reasons.append('SAME_VERSION_OLD_COUNTER_EVIDENCE_REQUIRED')
    return dict(ready=not reasons, reasons=reasons, absence_proven=False)
