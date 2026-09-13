"""Entity resolution, typed diff and event collapse over TABLE observations.

The input is a route-internal evidence view, not a second ProjectChange schema.
No row similarity, model identity, implicit continuation or cross-route inference.
"""
from collections import defaultdict
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import re

from experiments.project_change_text_v1.contract import validate as validate_shared
from .common import digest, norm

APPROACHES = ('entity_first', 'row_event_clustering', 'hybrid_entity_event_collapse')
IDENTITY_FIELDS = ('system', 'building', 'room', 'floor', 'engineering_function',
                   'equipment_class', 'group')
INDEPENDENT = {'count', 'mode', 'material', 'requirement', 'composition'}
CAPACITY = {'flow', 'pressure', 'power', 'heat_load', 'capacity'}
PRODUCT = CAPACITY | {'mass', 'length', 'width', 'height', 'dimensions', 'speed', 'voltage'}
TYPES = {'model': 'EQUIPMENT_REPLACED', 'count': 'EQUIPMENT_COUNT_CHANGED',
         'capacity': 'CAPACITY_CHANGED', 'composition': 'SYSTEM_CONFIGURATION_CHANGED',
         'mode': 'SYSTEM_MODE_CHANGED', 'requirement': 'REQUIREMENT_CHANGED',
         'parameter': 'OTHER_ENGINEERING_CHANGE', 'added': 'EQUIPMENT_ADDED',
         'removed': 'EQUIPMENT_REMOVED'}
UNITS = {'шт.': ('count', Decimal(1)), 'шт': ('count', Decimal(1)),
         'квт': ('power', Decimal(1000)), 'вт': ('power', Decimal(1)),
         'мвт': ('power', Decimal(1000000)), 'кг': ('mass', Decimal(1)),
         'мм': ('length', Decimal('0.001')), 'м': ('length', Decimal(1)),
         'па': ('pressure', Decimal(1)), 'кпа': ('pressure', Decimal(1000)),
         'мпа': ('pressure', Decimal(1000000)), 'м3/ч': ('flow', Decimal(1)),
         'м³/ч': ('flow', Decimal(1)), 'л/с': ('flow', Decimal('3.6'))}


def comparable(value):
    text = norm(value['value'])
    unit = norm(value.get('unit') or '')
    try:
        if not re.fullmatch(r'[-+]?\d+(?:[.,]\d+)?', text):
            raise InvalidOperation
        number = Decimal(text.replace(',', '.'))
        dimension, scale = UNITS.get(unit, (unit, Decimal(1)))
        return dimension, str((number * scale).normalize())
    except InvalidOperation:
        return unit, text


def identity(record):
    s = record['subject']
    anchor = next(((name, norm(s[name])) for name in ('stable_id', 'mark') if s.get(name)), None)
    strong = bool(anchor)
    if not anchor and s.get('system') and s.get('engineering_function'):
        anchor = ('function', norm(s['engineering_function']))
        strong = True
    if not anchor and s.get('position') and (s.get('equipment_class') or s.get('engineering_function')):
        anchor = ('position', norm(s['position']))
        # Position stability must be independently certified; an ordinal is insufficient.
        strong = bool(record.get('stable_position'))
    if not anchor:
        return None, False
    qualifiers = tuple((k, norm(s[k])) for k in IDENTITY_FIELDS if s.get(k))
    return (record['project_scope'], anchor, qualifiers), strong


def address(value):
    # Units are states checked separately, so equivalent units can compare.
    return value['property'], value.get('mode', ''), value.get('basis', '')


def unique_evidence(evidence):
    return sorted({e['evidence_id']: e for e in evidence}.values(), key=lambda e: e['evidence_id'])


def scope_absence(pair, side, records):
    witness = pair.get(side + '_scope') or {}
    if not (witness.get('scope_key') == pair['comparison_scope'] and
            witness.get('coverage_status') == 'COMPLETE' and
            witness.get('boundary_status') == 'PROVEN' and
            witness.get('identity_alternatives_resolved') is True and
            witness.get('unknown_rows') == 0 and witness.get('closed_inventory') is True and
            witness.get('table_keys') and
            set(witness['table_keys']) == set(witness.get('all_table_keys', [])) and
            set(witness.get('record_ids', [])) == {r['record_id'] for r in records} and
            witness.get('evidence') and
            witness['evidence']['locator'].get('kind') == 'scope_inventory'):
        return None
    if any(not identity(r)[1] or r.get('review_reasons') for r in records):
        return None
    return witness['evidence']


def extract_differences(pair):
    groups = {side: defaultdict(list) for side in ('old', 'new')}
    unanchored = []
    for side in groups:
        for r in pair[side + '_records']:
            key, strong = identity(r)
            if key is None:
                unanchored.append(dict(side=side, record_id=r['record_id'], reason='NO_TABLE_ENTITY_ANCHOR'))
            else:
                groups[side][key].append((r, strong))
    facts, matches, unresolved = [], [], unanchored
    for key in sorted(set(groups['old']) | set(groups['new']), key=str):
        sides = {side: groups[side].get(key, []) for side in groups}
        subject = deepcopy(next(iter(sides['old'] or sides['new']))[0]['subject'])
        entity_key = digest([pair['comparison_scope'], key])
        reasons = sorted({reason for rs in sides.values() for r, _ in rs for reason in r.get('review_reasons', [])})
        if not all(strong for rs in sides.values() for _, strong in rs):
            reasons.append('POSITION_CONTINUITY_UNPROVEN')
        if sides['old'] and sides['new']:
            matches.append(dict(entity_key=entity_key, subject=subject, status='REVIEW' if reasons else 'PROVEN',
                                old_records=[r['record_id'] for r, _ in sides['old']],
                                new_records=[r['record_id'] for r, _ in sides['new']], review_reasons=reasons))
            values = {side: defaultdict(list) for side in sides}
            for side in sides:
                for record, _ in sides[side]:
                    for value in record['values']:
                        values[side][address(value)].append((value, record))
            for addr in sorted(set(values['old']) | set(values['new'])):
                ov, nv = values['old'][addr], values['new'][addr]
                if not ov or not nv:
                    unresolved.append(dict(entity_key=entity_key, property=addr, reason='PROPERTY_NOT_OBSERVED_ON_BOTH_SIDES',
                                           evidence=unique_evidence([e for v, _ in ov + nv for e in v['evidence']])))
                    continue
                old_states = {comparable(v) for v, _ in ov}
                new_states = {comparable(v) for v, _ in nv}
                if len(old_states) != 1 or len(new_states) != 1:
                    unresolved.append(dict(entity_key=entity_key, property=addr, reason='CONFLICTING_TABLE_VALUES',
                                           evidence=unique_evidence([e for v, _ in ov + nv for e in v['evidence']])))
                    continue
                if old_states == new_states:
                    continue
                fact_reasons = list(reasons)
                for observations in (ov, nv):
                    if len({r['record_id'] for _, r in observations}) > 1 and not all(r.get('repeated_representation') for _, r in observations):
                        fact_reasons.append('DUPLICATE_ENTITY_PROPERTY_OWNER')
                old_dim = next(iter(old_states))[0]
                new_dim = next(iter(new_states))[0]
                if old_dim != new_dim:
                    fact_reasons.append('INCOMPATIBLE_UNITS')
                if any(v.get('status') != 'PROVEN' for v, _ in ov + nv):
                    fact_reasons.append('PARAMETER_SEMANTICS_UNRESOLVED')
                if addr[0] == 'count' and old_dim != 'count':
                    fact_reasons.append('QUANTITY_IS_NOT_EQUIPMENT_COUNT')
                fact = dict(entity_key=entity_key, subject=subject, property=addr[0], mode=addr[1], basis=addr[2],
                            old=ov[0][0], new=nv[0][0], review_reasons=fact_reasons,
                            old_evidence=unique_evidence([e for v, r in ov for e in v['evidence'] + r['evidence']]),
                            new_evidence=unique_evidence([e for v, r in nv for e in v['evidence'] + r['evidence']]),
                            product_characteristic=all(v.get('product_characteristic', False) for v, _ in ov + nv),
                            changed_cell_occurrences=max(len({e['evidence_id'] for v, _ in ov for e in v['evidence']}),
                                                         len({e['evidence_id'] for v, _ in nv for e in v['evidence']})),
                            row_group=(ov[0][0]['evidence'][0]['locator'].get('row_key', ov[0][1]['record_id']),
                                       nv[0][0]['evidence'][0]['locator'].get('row_key', nv[0][1]['record_id'])))
                fact['fact_id'] = 'tf_' + digest([entity_key, addr, sorted(old_states), sorted(new_states)])[:24]
                facts.append(fact)
        else:
            present = 'old' if sides['old'] else 'new'
            absent = 'new' if present == 'old' else 'old'
            kind = 'removed' if present == 'old' else 'added'
            pe = unique_evidence([e for r, _ in sides[present] for e in r['evidence']])
            # Check completeness of BOTH corresponding scopes, not just the empty side.
            witnesses = {side: scope_absence(pair, side, pair[side + '_records']) for side in sides}
            if not all(witnesses.values()):
                reasons.append('ABSENCE_NOT_PROVEN_IN_COMPLETE_CORRESPONDING_SCOPE')
            if not subject.get('equipment_class'):
                reasons.append('EQUIPMENT_CLASS_UNRESOLVED')
            absence_evidence = [witnesses[absent]] if all(witnesses.values()) else []
            values = {present: dict(value='PRESENT', quote=pe[0]['quote'] or 'TABLE presence witness', unit=None),
                      absent: dict(value='ABSENT' if absence_evidence else 'UNKNOWN',
                                   quote='TABLE closed-scope absence certificate' if absence_evidence else 'UNKNOWN', unit=None)}
            facts.append(dict(entity_key=entity_key, subject=subject, property=kind, mode='', basis='',
                              old=values['old'], new=values['new'], review_reasons=reasons,
                              old_evidence=pe if present == 'old' else absence_evidence,
                              new_evidence=pe if present == 'new' else absence_evidence,
                              row_group=tuple(r['record_id'] for r, _ in sides[present]),
                              product_characteristic=False,
                              changed_cell_occurrences=0,
                              fact_id='tf_' + digest([entity_key, kind])[:24]))
    # An unresolved property of the same entity vetoes an otherwise strong event.
    bad_entities = {u['entity_key'] for u in unresolved if u.get('reason') == 'CONFLICTING_TABLE_VALUES'}
    for f in facts:
        if f['entity_key'] in bad_entities:
            f['review_reasons'].append('CONFLICTING_ENTITY_OBSERVATIONS')
    return dict(facts=facts, entity_matches=matches, unresolved=unresolved)


def family(fact):
    p = fact['property']
    return 'capacity' if p in CAPACITY else p if p in TYPES else 'parameter'


def group_facts(facts, approach):
    if approach not in APPROACHES:
        raise ValueError(approach)
    replacement = {(f['entity_key'], f['mode'], f['basis']) for f in facts
                   if f['property'] == 'model' and not any(r in f['review_reasons'] for r in
                       ('PARAMETER_SEMANTICS_UNRESOLVED', 'CONFLICTING_ENTITY_OBSERVATIONS', 'DUPLICATE_ENTITY_PROPERTY_OWNER'))}
    groups = defaultdict(list)
    for f in facts:
        event = family(f)
        context = (f['entity_key'], f['mode'], f['basis'])
        if context in replacement and f['property'] not in INDEPENDENT | {'model', 'added', 'removed'} and f['product_characteristic']:
            event = 'model'
        if approach == 'entity_first':
            k = (f['entity_key'],)
        elif approach == 'row_event_clustering':
            k = (*context, f['row_group'], event)
        else:
            # Unrelated important properties are not merged simply by adjacency.
            k = (*context, event, f['property'] if event == 'parameter' else '')
        groups[k].append(f)
    return list(groups.values())


def summarize(kind, label, old, new, proven):
    if not proven:
        return f'Проверить возможное изменение {label}: {old or "не установлено"} → {new or "не установлено"}.'
    verbs = {'EQUIPMENT_REPLACED': 'заменено', 'EQUIPMENT_COUNT_CHANGED': 'изменено количество',
             'CAPACITY_CHANGED': 'изменена производительность', 'SYSTEM_CONFIGURATION_CHANGED': 'изменён состав',
             'SYSTEM_MODE_CHANGED': 'изменён режим', 'REQUIREMENT_CHANGED': 'изменено требование',
             'OTHER_ENGINEERING_CHANGE': 'изменён существенный параметр'}
    if kind in {'EQUIPMENT_ADDED', 'EQUIPMENT_REMOVED'}:
        return f'{label}: {"добавлено" if kind == "EQUIPMENT_ADDED" else "удалено"} оборудование.'
    return f'{label}: {verbs[kind]}; {old} → {new}.'


def build_change(pair, fs):
    s = fs[0]['subject']
    properties = {f['property'] for f in fs}
    first = next((f for f in fs if f['property'] == 'model'), fs[0])
    event_family = 'model' if 'model' in properties else family(first)
    kind = TYPES[event_family]
    reasons = sorted({r for f in fs for r in f['review_reasons']})
    if kind == 'EQUIPMENT_REPLACED' and not s.get('equipment_class'):
        reasons.append('EQUIPMENT_CLASS_UNRESOLVED')
    proven = not reasons
    entity_key = fs[0]['entity_key']
    label = ' '.join(str(s[k]) for k in ('equipment_class', 'mark') if s.get(k)) or s.get('engineering_function') or s.get('system') or s.get('position') or 'табличная сущность'
    entity = dict(entity_id='te_' + entity_key[:24], system=s.get('system'),
                  equipment_class=s.get('equipment_class'), mark=s.get('mark'),
                  equipment_model_old=first['old']['value'] if event_family == 'model' else None,
                  equipment_model_new=first['new']['value'] if event_family == 'model' else None,
                  room=s.get('room'), floor=s.get('floor'), engineering_function=s.get('engineering_function'),
                  semantic_subject=label, scope_key=pair['comparison_scope'],
                  identity_basis=['TABLE explicit scoped anchor; model excluded',
                                  'qualifiers=' + str({k: v for k, v in s.items() if v})],
                  resolution='EXPLICIT' if proven else 'AMBIGUOUS')
    old_e = unique_evidence([e for f in fs for e in f['old_evidence']])
    new_e = unique_evidence([e for f in fs for e in f['new_evidence']])
    supporting = []
    for f in fs:
        # Unknown absence never gets fictional AtomicFactChange values/citations.
        if not f['old_evidence'] or not f['new_evidence']:
            continue
        supporting.append(dict(fact_id=f['fact_id'], property='presence' if f['property'] in {'added', 'removed'} else f['property'],
                               old={k: f['old'][k] for k in ('value', 'quote', 'unit')},
                               new={k: f['new'][k] for k in ('value', 'quote', 'unit')},
                               evidence_old=[e['evidence_id'] for e in f['old_evidence']],
                               evidence_new=[e['evidence_id'] for e in f['new_evidence']]))
    event_key = digest([pair['comparison_scope'], entity_key, kind,
                        sorted(f['fact_id'] for f in fs)])
    old_state = '; '.join(f"{f['property']}: {f['old']['value']} {f['old'].get('unit') or ''}".strip() for f in fs) if old_e else None
    new_state = '; '.join(f"{f['property']}: {f['new']['value']} {f['new'].get('unit') or ''}".strip() for f in fs) if new_e else None
    change = dict(project_change_id='pc_' + digest(event_key)[:24], comparison_scope=pair['comparison_scope'],
                  change_type=kind, engineering_subject=entity, old_state=old_state, new_state=new_state,
                  short_summary_ru=summarize(kind, label, first['old']['value'] if old_e else None,
                                             first['new']['value'] if new_e else None, proven),
                  importance='MATERIAL' if proven and kind != 'OTHER_ENGINEERING_CHANGE' else 'UNASSESSED',
                  scope='EQUIPMENT' if s.get('equipment_class') else 'SYSTEM' if s.get('system') else 'LOCAL_ASSERTION',
                  status='PROVEN' if proven else 'REVIEW', confidence='HIGH' if proven else 'LOW',
                  evidence_old=old_e, evidence_new=new_e, supporting_fact_changes=supporting,
                  decision_reasons=['One scoped entity and one event family; all supporting differences retained',
                                    'Replacement absorbs only selected-product characteristics in the same mode and basis; count stays independent'],
                  review_reasons=reasons, event_key=event_key, routes=['TABLE'], conflicts=[])
    validate_shared(change, text_only=False)
    return change


def compare(pair, approach='hybrid_entity_event_collapse'):
    result = extract_differences(pair)
    grouped = group_facts(result['facts'], approach)
    result['project_changes'] = sorted([build_change(pair, fs) for fs in grouped], key=lambda c: c['project_change_id'])
    result['fact_ownership'] = {f['fact_id']: c['project_change_id'] for fs in grouped
                                for c in [build_change(pair, fs)] for f in fs}
    assert len(result['fact_ownership']) == len(result['facts'])
    return result
