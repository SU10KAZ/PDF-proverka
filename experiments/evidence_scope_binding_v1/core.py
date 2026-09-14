"""Representation-neutral, provenance-bearing ownership over certified intervals.

No identity or state decisions live here. Positions locate a fragment inside a
source container; they never identify engineering entities across versions.
"""
from copy import deepcopy
import hashlib
import json

SOURCE_TYPES = ('NARRATIVE_TEXT', 'TABLE', 'GRAPHIC', 'TITLE_BLOCK',
                'PAGE_FURNITURE', 'MIXED', 'UNKNOWN')
DIMENSIONS = ('discipline', 'system', 'subsystem', 'building_section',
              'apartment_or_unit', 'floor', 'room', 'zone', 'equipment_group',
              'engineering_function', 'heading', 'table', 'specification_group')
APPROACHES = ('nearest_heading', 'structural_intervals', 'certified_hierarchy')


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':')).encode()).hexdigest()


def gate(source_type, route):
    """MIXED/UNKNOWN never become admitted solely from a requested route."""
    if source_type not in SOURCE_TYPES:
        raise ValueError('Unknown source type')
    return 'PROVEN' if {'TEXT': 'NARRATIVE_TEXT', 'TABLE': 'TABLE'}.get(route) == source_type else 'REVIEW'


def node(*, version, dimension, value, start, end, pages, evidence,
         parent_ids=(), status='PROVEN', container_id=None):
    if dimension not in DIMENSIONS or start > end or not evidence:
        raise ValueError('Invalid scope node')
    key = digest([version, dimension, value, start, end, container_id])[:24]
    return dict(scope_id='scope_' + key, document_version=version,
                dimension=dimension, value=value, interval=[start, end],
                pages=pages, parent_scope_ids=list(parent_ids),
                scope_evidence=evidence, status=status, container_id=container_id)


def bind(fragment, nodes, approach='certified_hierarchy'):
    if approach not in APPROACHES:
        raise ValueError('Unknown approach')
    source_type = fragment['source_type']
    route = fragment['route']
    reasons = []
    if gate(source_type, route) != 'PROVEN':
        reasons.append('SOURCE_TYPE_NOT_ADMITTED')
    start, end = fragment['interval']
    if start > end:
        raise ValueError('Invalid fragment interval')
    candidates = [n for n in nodes if n['document_version'] == fragment['document_version']]
    eligible = [n for n in candidates if n['interval'][0] <= start and end <= n['interval'][1]]
    if approach == 'nearest_heading':
        # Intentionally unsafe diagnostic baseline: ignores owner end boundary.
        eligible = [n for n in candidates if n['dimension'] not in ('discipline', 'table')
                    and set(n['pages']) & set(fragment['pages'])]
        eligible = sorted(eligible, key=lambda n: (abs(n['interval'][0]-start), n['scope_id']))[:1]
    elif approach == 'structural_intervals':
        eligible = [n for n in eligible if n['pages'] == fragment['pages']]
    selected = []
    for n in eligible:
        if n['status'] != 'PROVEN':
            reasons.append('UNCERTAIN_PARENT:' + n['scope_id'])
            continue
        # A table-local owner cannot escape its table unless the adapter supplies
        # an explicit continuation edge (represented by allowed container IDs).
        if (n.get('container_id') and fragment.get('container_id') and
                n['container_id'] != fragment['container_id'] and
                n['container_id'] not in fragment.get('continued_container_ids', [])):
            continue
        selected.append(n)
    index = {n['scope_id']: n for n in candidates}
    selected_ids = {n['scope_id'] for n in selected}
    if approach != 'nearest_heading':
        for n in selected:
            for parent in n['parent_scope_ids']:
                if parent not in selected_ids or parent not in index:
                    reasons.append('PARENT_CHAIN_INCOMPLETE:' + n['scope_id'])
        def cyclic(sid, trail):
            if sid in trail:
                return True
            return any(cyclic(p, trail | {sid}) for p in index[sid]['parent_scope_ids'] if p in index)
        if any(cyclic(n['scope_id'],set()) for n in selected):
            reasons.append('CYCLIC_SCOPE_ANCESTRY')
    dimensions = {d: [] for d in DIMENSIONS}
    for n in selected:
        if n['value'] not in dimensions[n['dimension']]:
            dimensions[n['dimension']].append(n['value'])
    # Distinct peers of a single physical dimension are ambiguous unless the
    # source explicitly declares simultaneous ownership. Ancestor headings and
    # distinct system/room references can legitimately be plural.
    for d in ('floor', 'apartment_or_unit', 'equipment_group', 'building_section'):
        if len(dimensions[d]) > 1 and d not in fragment.get('multiple_dimensions', []):
            reasons.append('CONFLICTING_PARENTS:' + d)
    substantive = [n for n in selected if n['dimension'] not in ('discipline', 'table')]
    if not substantive:
        reasons.append('ENGINEERING_OWNER_UNRESOLVED')
    if not fragment.get('source_verified', False):
        reasons.append('SOURCE_LOCATION_UNVERIFIED')
    reasons.extend(fragment.get('review_reasons', []))
    status = 'REVIEW' if reasons else 'PROVEN'
    result = dict(schema='evidence-scope.v1', fragment_id=fragment['fragment_id'],
                  document_version=fragment['document_version'], source_type=source_type,
                  route=route, container_id=fragment.get('container_id'),
                  parent_scope_ids=[n['scope_id'] for n in selected],
                  dimensions={d: v or None for d, v in dimensions.items()},
                  scope_nodes=deepcopy(selected), source_evidence=fragment['source_evidence'],
                  status=status, confidence='HIGH' if status == 'PROVEN' else 'LOW',
                  reasons=sorted(set(reasons)), method=approach)
    validate(result)
    return result


def validate(binding):
    """Runtime semantic checks additional to the published JSON Schema."""
    if binding['source_type'] not in SOURCE_TYPES or binding['status'] not in ('PROVEN', 'REVIEW'):
        raise ValueError('Invalid binding enum')
    ids = {n['scope_id'] for n in binding['scope_nodes']}
    if ids != set(binding['parent_scope_ids']):
        raise ValueError('Scope ID mismatch')
    for n in binding['scope_nodes']:
        if n['document_version'] != binding['document_version'] or not n['scope_evidence']:
            raise ValueError('Cross-version or unsupported parent')
    if binding['status'] == 'PROVEN' and gate(binding['source_type'], binding['route']) != 'PROVEN':
        raise ValueError('Source contamination')
    return binding


def local_package(fragment, nodes, context=()):
    """Optional bounded ownership-only AI input; no provider/AI promotion here."""
    parents = [n for n in nodes if n['document_version'] == fragment['document_version']]
    parents = sorted(parents, key=lambda n: abs(n['interval'][0]-fragment['interval'][0]))[:8]
    package = dict(task='OWNERSHIP_ONLY', allowed_answers=['SCOPE_IDS', 'MULTIPLE', 'UNCERTAIN'],
                   fragment=fragment, candidate_parents=parents, local_context=list(context)[:4])
    if len(json.dumps(package, ensure_ascii=False)) > 24000:
        raise ValueError('Local ownership package budget exceeded')
    return package
