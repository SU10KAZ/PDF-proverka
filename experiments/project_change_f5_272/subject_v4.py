"""Native-grounded refinement of the frozen F5 retrieval roster, before requirements.

The roster is an addressable set of research questions, not a set of proven
systems. Refinement can leave a question unresolved. It never silently merges,
splits, or equates systems sharing a mark. No diagnostic answers are inputs.
"""
from dataclasses import asdict, dataclass
import re

import fitz

from .common import fingerprint
from .subject_discovery import FUNCTIONS

SUBJECT_STATUSES = {'SUBJECT_PROVEN', 'SUBJECT_POSSIBLE', 'SUBJECT_UNRESOLVED'}


def subject_confidence(*, native_support, explicit_function=False, system_bound=False,
                       location_bound=False, role_bound=False, conflicting=False):
    if not native_support or conflicting:
        return 'SUBJECT_UNRESOLVED'
    if explicit_function and system_bound and location_bound and role_bound:
        return 'SUBJECT_PROVEN'
    return 'SUBJECT_POSSIBLE'


def normalized_code(value):
    return re.sub(r'[^\w\d]', '', value, flags=re.UNICODE).replace('_', '').upper()


def document_identity(text, expected, *, proven_bindings=()):
    # Document ciphers must contain a design discipline and a project prefix.
    # A sheet number, equipment mark, reference to SP/GOST, or an isolated ИОС
    # mention is not a competing document identity.
    lines = text.splitlines()
    found = []
    for i, line in enumerate(lines):
        if (re.search(r'(?:ИОС\s*[-.]?\s*\d|[-_]АР\d)', line, re.I)
                and re.search(r'[\w]+[-_][\w\d]+[-_/]', line)
                and len(line.strip()) < 110
                and any(re.search(r'\bЛист\b', x, re.I) for x in lines[max(0, i-2):i+5])):
            found.append(line.strip())
    allowed = [normalized_code(expected), *map(normalized_code, proven_bindings)]
    def matches(code):
        code = normalized_code(code)
        # A part qualifier does not change the project or discipline/cipher.
        # Do not strip arbitrary suffixes, project prefixes, or system numbers.
        return any(code == a or code in (a + 'ТЧ', a + 'ГЧ', a + 'ПЗ') for a in allowed)
    foreign = sorted({x for x in found if not matches(x)})
    return dict(status='DOCUMENT_IDENTITY_MISMATCH' if foreign else 'MATCHED' if found else 'NOT_OBSERVED',
        usable='NO' if foreign else 'YES', reason='WRONG_DOCUMENT_OR_CIPHER' if foreign else '',
        expected_document=expected, observed_ciphers=sorted(set(found)), foreign_ciphers=foreign,
        proven_bindings=list(proven_bindings), native_text_sha256=fingerprint(text),
        limitation='No visible cipher is not proof of identity; containing PDF remains hash-bound')


def clean_content(text):
    """Remove the title block and pagination, not engineering paragraphs."""
    result = []
    stamp = re.compile(r'^\s*(?:Лист|Изм\..*|Кол\.уч.*|№\s*док.*|Подп\..*|Дата|'
                       r'Взам\.\s*инв\..*|Подпись и дата|Инв\.\s*№.*)\s*$', re.I)
    for line in text.splitlines():
        if (re.search(r'(?:ИОС\s*[-.]?\s*\d|[-_]АР\d)', line, re.I)
                and re.search(r'[\w]+[-_][\w\d]+[-_/]', line) and len(line) < 110):
            continue
        if stamp.match(line):
            continue
        if not result and (not line.strip() or re.fullmatch(r'\s*\d+\s*', line)):
            continue
        result.append(line)
    # Native drawing order can place title blocks before, after, or between
    # engineering content. Never truncate the remaining page at a cipher.
    while result and (not result[-1].strip() or re.fullmatch(r'\s*\d+\s*', result[-1])):
        result.pop()
    return '\n'.join(result).strip()


def matching_paragraphs(text, key):
    """Sentence/paragraph candidates only; this does not certify their extent."""
    native = re.sub(r'(?<=[A-Za-zА-Яа-яЁё])[-\u00ad]\s*\n\s*(?=[A-Za-zА-Яа-яЁё])', '', clean_content(text))
    parts = re.split(r'(?<=[.!?])\s+(?=[А-ЯЁA-Z\d])|\n\s*\n', native)
    return [p.strip() for p in parts if re.search(FUNCTIONS[key][0], p, re.I)]


def local_role(key, text, discipline):
    if key == 'water_supply' and re.search(r'кабел|проводов', text, re.I) and re.search(r'автоматизац|диспетчеризац', text, re.I):
        return 'CABLE_ROUTING_FOR_WATER_AUTOMATION'
    if key == 'air_pressurization' and discipline == 'AR':
        return 'ARCHITECTURAL_POSITION_AND_LABEL'
    if key == 'openings' and discipline == 'HVAC':
        return 'OPENING_CONDITION_FOR_VENTILATION'
    return key


def identity_relation(a, b):
    """Marks are deliberately absent from identity decisions."""
    if (a['functional_role'], a['system'], a['discipline']) != (b['functional_role'], b['system'], b['discipline']):
        return 'DIFFERENT_SUBJECT'
    sa, sb = set(a['subject_scope']['locations']), set(b['subject_scope']['locations'])
    if 'UNKNOWN' not in sa | sb and sa and sb and not sa & sb:
        return 'DIFFERENT_SUBJECT'
    return 'POSSIBLE_SAME_SUBJECT'


@dataclass(frozen=True)
class CanonicalEngineeringSubject:
    discipline: str
    system: str
    subsystem: str
    functional_role: str
    equipment_or_group: tuple
    location: tuple
    zone: tuple
    floor: tuple
    served_consumers: tuple
    document_section: tuple
    source_labels: tuple
    source_pages: tuple
    subject_scope: dict


def source_support(region, key, doc):
    text = region['native_text']
    snippets = matching_paragraphs(text, key)
    if not snippets:
        return dict(accepted=False, reason='NO_NATIVE_SUBJECT_SUPPORT', snippets=[])
    if key == 'acoustic':
        # A normative bibliography and legend vocabulary establish neither a
        # project-specific requirement nor an installed silencer.
        normative = all(re.search(r'СП\s*\d|СНиП|ГОСТ|перечень|норматив', p, re.I) for p in snippets)
        legend = bool(re.search(r'условн\w*\s+обозначен|легенд', text, re.I))
        project_use = any(re.search(r'предусмотр|установ|принят|должен|должна|не\s+более|не\s+менее', p, re.I) for p in snippets)
        if normative or (legend and not project_use):
            return dict(accepted=False, reason='REFERENCE_OR_LEGEND_WITHOUT_PROJECT_BINDING', snippets=snippets)
    locators = []
    if region['source_type'] == 'GRAPHIC':
        box = fitz.Rect(region['bbox_norm'])
        locators = [x for x in doc.geometry(region['page'])['labels']
                    if re.search(FUNCTIONS[key][0], x['text'], re.I)
                    and box.intersects(fitz.Rect(x['bbox_norm']))]
        if not locators:
            return dict(accepted=False, reason='NATIVE_SUBJECT_OUTSIDE_GRAPHIC_REGION', snippets=snippets)
        if re.search(r'обозначени\w*\s+системы', text, re.I) and re.search(r'электродвигател|воздухонагревател', text, re.I):
            return dict(accepted=False, reason='EQUIPMENT_TABLE_IS_NOT_A_FUNCTIONAL_SCHEME', snippets=snippets)
    return dict(accepted=True, reason='NATIVE_SUBJECT_CONTEXT', snippets=snippets,
                native_label_locators=locators)


def refine_subjects(prepared, docs):
    invs, subjects = prepared['inventories'], prepared['subjects']
    guards, audits, canonical = {}, [], {}
    for side, doc in docs.items():
        inv = invs[side]
        for page, r in doc.text.items():
            guard = document_identity(r['native_text'], inv['document'])
            guards[(side, page)] = guard | dict(side=side, page=page,
                document_version=inv['document_version'], source_pdf=inv['source']['pdf'])
    for sid, original in sorted(subjects.items()):
        side, key = original['side'].lower(), original['functional_key']
        inv, doc = invs[side], docs[side]
        discipline = 'AR' if re.search(r'АР\d', inv['document']) else 'HVAC'
        accepted, rows, roles = [], [], set()
        for rid in original['supporting_evidence_refs']:
            r = doc.regions[rid]
            guard = guards[(side, r['page'])]
            support = source_support(r, key, doc)
            if guard['usable'] == 'NO':
                support = support | dict(accepted=False, reason='WRONG_DOCUMENT_OR_CIPHER')
            row = dict(region_id=rid, page=r['page'], source_type=r['source_type'],
                native_sha256=fingerprint(r['native_text']), **support)
            rows.append(row)
            if support['accepted']:
                accepted.append(r)
                roles.update(local_role(key, s, discipline) for s in support['snippets'])
        role = next(iter(roles)) if len(roles) == 1 else 'SCOPED_CONTEXT_GROUP' if roles else 'UNESTABLISHED'
        pages = tuple(sorted({r['page'] for r in accepted}))
        scope = dict(kind='IDENTITY_PROBE' if not accepted else
            'LOCAL_POSITION_LABEL' if role == 'ARCHITECTURAL_POSITION_AND_LABEL' else 'NATIVE_FUNCTIONAL_CONTEXT_GROUP',
            functional_key=key, roles=sorted(roles), locations=original['scope'],
            member_region_ids=sorted(r['region_id'] for r in accepted),
            exclusions='No inferred whole-system, equipment equivalence, numeric claim, or topology',
            exhaustive=False)
        value = CanonicalEngineeringSubject(discipline=discipline,
            system='VENTILATION' if discipline == 'HVAC' and key not in {'water_supply', 'power_supply'} else 'UNESTABLISHED',
            subsystem='UNESTABLISHED', functional_role=role,
            equipment_or_group=(), location=tuple(original['scope']),
            zone=tuple(s for s in original['scope'] if not s.startswith('FLOOR:')),
            floor=tuple(s[6:] for s in original['scope'] if s.startswith('FLOOR:')),
            served_consumers=(), document_section=tuple(sorted({h['title'] for h in doc.headings if h['page'] in pages})),
            source_labels=tuple(sorted({m for r in accepted for m in r['labels']})),
            source_pages=pages, subject_scope=scope)
        # Native mention proves the local context, not a named system identity.
        status = subject_confidence(native_support=bool(accepted), explicit_function=bool(roles),
            system_bound=False, location_bound=False, role_bound=len(roles) == 1)
        canonical[sid] = dict(subject_id=sid, side=side.upper(), **asdict(value),
            confidence=status, evidence_support=rows,
            provenance=dict(inventory_hash=fingerprint(inv), legacy_discovery_subject_id=sid,
                method='NATIVE_CONTEXT_AND_DOCUMENT_IDENTITY/4'))
        audits.append(dict(subject_id=sid, before=original, after=canonical[sid],
            excluded_regions=[r for r in rows if not r['accepted']]))
    return canonical, audits, list(guards.values())


def refine_candidate(original, canonical):
    sides = {s: [canonical[sid] for sid in original[s]] for s in ('old', 'new')}
    supported = {s: [v for v in values if v['confidence'] != 'SUBJECT_UNRESOLVED'] for s, values in sides.items()}
    status = 'SUBJECT_POSSIBLE' if all(supported.values()) else 'SUBJECT_UNRESOLVED'
    incompatible = bool(all(supported.values()) and not any(
        identity_relation(a, b) == 'POSSIBLE_SAME_SUBJECT' for a in supported['old'] for b in supported['new']))
    if incompatible:
        status = 'SUBJECT_UNRESOLVED'
    confidence = original['confidence']
    reason = None
    if confidence == 'STRONG' and (status == 'SUBJECT_UNRESOLVED' or any(
            v['confidence'] == 'SUBJECT_UNRESOLVED' for values in sides.values() for v in values)):
        confidence = 'UNRESOLVED' if status == 'SUBJECT_UNRESOLVED' else 'POSSIBLE'
        reason = 'NATIVE_SUBJECT_SUPPORT_MISSING_OR_FUNCTIONALLY_INCOMPATIBLE_ENDPOINT'
    return original | dict(confidence=confidence, subject_confidence=status,
        canonical_subjects={s: [v['subject_id'] for v in values] for s, values in sides.items()},
        strong_downgrade_reason=reason,
        identity_scope='SCOPED_RETRIEVAL_QUESTION_NOT_PROVEN_SYSTEM_IDENTITY',
        source_subject_splitting=False, semantic_verdict=None)
