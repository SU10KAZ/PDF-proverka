"""Dimension-checked TABLE states and evidence-bearing engineering events.

The legacy producer is frozen. This adapter separates property address (including
operating basis) from event ownership; a replacement can own several addresses.
It never uses model, row number or values to establish subject identity.
"""
from collections import defaultdict
from copy import deepcopy
from decimal import Decimal
import re

from experiments.engineering_subject_resolver_v1.bridge import table_record
from experiments.project_change_text_v1.contract import validate
from experiments.table_project_change_v1.engine import extract_differences, build_change, family
from experiments.text_comparison_v1.common import digest


def norm(value):
    return re.sub(r'\s+', ' ', value.casefold().replace('ё', 'е').replace(';', ' ')).strip()


UNITS = {
    'вт': ('power', '1'), 'квт': ('power', '1000'), 'мвт': ('power', '1000000'),
    'па': ('pressure', '1'), 'кпа': ('pressure', '1000'), 'мпа': ('pressure', '1000000'),
    'м.в.ст': ('pressure_head', '1'), 'м.в.ст.': ('pressure_head', '1'),
    'м3/ч': ('flow', '1'), 'м³/ч': ('flow', '1'), 'л/с': ('flow', '3.6'),
    'м3/сут': ('flow', '0.04166666666666666666666666667'),
    'м³/сут': ('flow', '0.04166666666666666666666666667'),
    'кг': ('mass', '1'), 'мм': ('length', '.001'), 'м': ('length', '1'),
    'шт': ('count', '1'), 'шт.': ('count', '1'), 'зон': ('composition', '1'),
}
UNIT_RE = re.compile(r'(?<![\w])(' + '|'.join(re.escape(u) for u in sorted(UNITS, key=len, reverse=True)) + r')(?![\w])')
MODEL_RE = re.compile(r'(?<![\w])(?=[A-Za-zА-Яа-я0-9./_-]*[A-Za-zА-Яа-я])(?=[A-Za-zА-Яа-я0-9./_-]*\d)[A-Za-zА-Яа-я][A-Za-zА-Яа-я0-9]*(?:[-/][A-Za-zА-Яа-я0-9.]+)+(?![\w])')
NUMBER_RE = re.compile(r'^[-+]?\d+(?:[.,]\d+)?$')
DESIGNATION = re.compile(r'наименован|тип.*марка|модель|обозначение оборудования')
INDEPENDENT = {'count', 'composition', 'material', 'requirement', 'mode'}


def semantic_header(header):
    """Return property, observed unit and address, or explicit uncertainty.

Units constrain semantics rather than assigning a meaning from dimensions alone.
Power consumed, cooling capacity and heating capacity retain separate addresses.
"""
    h = norm(header)
    units = {m[0] for m in UNIT_RE.finditer(h)}
    if len(units) > 1:
        return None, None, None, 'MULTIPLE_HEADER_UNITS'
    unit = next(iter(units), None)
    dim = UNITS[unit][0] if unit else None
    prop = basis = expected = None
    if re.search(r'модель|тип.*марка|^тип установки|^серия$', h):
        return 'model', None, 'designation', None
    if re.search(r'кол[.-]?\s*во|количество|^кол\.', h):
        prop, basis, expected = 'count', 'inventory', {'count'}
    elif re.search(r'холод|охлажд', h) and re.search(r'производительност|мощност|нагрузк', h):
        prop, basis, expected = 'capacity', 'cooling', {'power'}
    elif re.search(r'тепло|нагрев', h) and re.search(r'производительност|мощност|нагрузк', h):
        prop, basis, expected = 'heat_load', 'heating', {'power'}
    elif re.search(r'потребляемая.*мощност|электрическая.*мощност|установленная.*мощност|^мощность', h):
        prop, basis, expected = 'power', ('consumed' if 'потреб' in h else 'installed' if 'установ' in h else 'unspecified_power'), {'power'}
    elif re.search(r'расход|производительность', h):
        prop, basis, expected = 'flow', ('daily' if unit and 'сут' in unit else 'hourly' if unit and '/ч' in unit else 'second'), {'flow'}
    elif re.search(r'напор|давлен|гидравлическое сопротивление', h):
        prop, basis, expected = 'pressure', ('head' if dim == 'pressure_head' else 'pressure'), {'pressure', 'pressure_head'}
    elif re.search(r'^масса|^вес', h):
        prop, basis, expected = 'mass', 'selected', {'mass'}
    elif re.search(r'^длина|^ширина|^высота', h):
        prop = next(p for word, p in [('длина','length'),('ширина','width'),('высота','height')] if word in h)
        basis, expected = 'selected', {'length'}
    if prop is None:
        return None, unit, None, 'UNKNOWN_HEADER_SEMANTICS'
    if dim not in expected:
        return prop, unit, basis, 'PROPERTY_UNIT_CONFLICT' if unit else 'UNIT_MISSING'
    return prop, unit, basis, None


def model_from_designation(text, header, subject):
    if not DESIGNATION.search(norm(header)) or not subject['clues']['equipment_class']:
        return None
    # A designation may contain a mark as well as a model. Never select a mark
    # or an ambiguous set of product codes; absence of a parse stays unknown.
    marks = {norm(m).replace('№', '').strip() for m in subject['clues']['mark']}
    matches = [m[0] for m in MODEL_RE.finditer(text)
               if norm(m[0]).replace('№', '').strip() not in marks]
    if re.search(r'\bГОСТ|\bТУ\b', text):
        return None
    return matches[0] if len(matches) == 1 else None


def record(subject, certificate):
    result = table_record(subject, certificate)
    result['values'] = []
    result['coverage_gaps'] = []
    headers = subject['headers']
    for col, cell in enumerate(subject['cells']):
        if not cell.strip():
            continue
        # Only column headers with actual semantic meaning; scope labels must
        # not become units or property bases. No implicit cross-page header.
        hs = [r[col] for r in headers if col < len(r) and r[col].strip()]
        recognized = [h for h in hs if semantic_header(h)[0] is not None or DESIGNATION.search(norm(h))]
        header = recognized[0] if recognized else ''
        if not header and headers and any(UNIT_RE.search(norm(h)) for h in hs):
            # Blank cells under a merged parent are interpreted only when an
            # explicit unit subheader and a compatible nearest parent exist.
            parent = next((headers[0][j] for j in range(min(col,len(headers[0])-1),-1,-1)
                           if headers[0][j].strip()), '')
            if semantic_header(parent)[0] is not None:
                header = parent
        if header and not UNIT_RE.search(norm(header)):
            children = [h for h in hs if h != header and UNIT_RE.search(norm(h))]
            if len(children) == 1:
                header += ' ' + children[0]
        evidence = [e for e in subject['evidence'] if e['locator']['column_index'] == col]
        prop, unit, basis, issue = semantic_header(header)
        model = model_from_designation(cell, header, subject)
        if model:
            result['values'].append(dict(property='model', value=model, quote=model, unit=None,
                mode='', basis='designation', product_characteristic=True, status='PROVEN', evidence=evidence))
        if prop == 'model':
            if not model:
                result['coverage_gaps'].append(dict(column=col, reason='MODEL_UNRESOLVED', quote=cell))
            continue
        if prop is None:
            if not model:
                result['coverage_gaps'].append(dict(column=col, reason=issue, quote=cell))
            continue
        value = cell.strip()
        if not NUMBER_RE.fullmatch(value):
            issue = issue or 'SCALAR_PARSE_UNRESOLVED'
        result['values'].append(dict(property=prop, value=value, quote=value, unit=unit,
            mode='', basis=basis or 'unknown', product_characteristic=prop not in INDEPENDENT,
            status='REVIEW' if issue else 'PROVEN', evidence=evidence))
        if issue:
            result['coverage_gaps'].append(dict(column=col, reason=issue, quote=cell))
    if not result['values']:
        # Preserve already explicit vertical fields without widening inference.
        legacy = table_record(subject, certificate)
        for value in legacy['values']:
            if value['unit'] in UNITS and value['property'] in {'pressure','composition'}:
                result['values'].append(value)
    return result


def compare_records(pair, approach='typed_event'):
    """A: frozen caller; B: row event; C: typed addresses + event ownership."""
    result = extract_differences(pair)
    for fact in result['facts']:
        a, b = fact['old'], fact['new']
        if norm(a.get('unit') or '') != norm(b.get('unit') or '') and norm(a['value']) == norm(b['value']):
            fact['review_reasons'].append('UNIT_ONLY_CHANGE_REQUIRES_ENGINEERING_WITNESS')
    groups = defaultdict(list)
    replacements = {f['entity_key'] for f in result['facts'] if f['property'] == 'model' and not f['review_reasons']}
    for f in result['facts']:
        if approach == 'row_event':
            key = f['entity_key'], f['row_group']
        elif f['entity_key'] in replacements and f['property'] not in INDEPENDENT:
            key = f['entity_key'], 'replacement'
        else:
            # Several design-time flow bases describe one demand-state change;
            # retain all bases in facts. Different properties/modes stay apart.
            key = f['entity_key'], family(f), f['property'], f['mode']
        groups[key].append(f)
    result['project_changes'] = []
    result['fact_ownership'] = {}
    for facts in groups.values():
        change = build_change(pair, facts)
        change['decision_reasons'] = [
            'Cross-version subject certificate is independent of model and parameter values.',
            'Replacement owns changed product characteristics; operating bases remain in atomic evidence. Independent quantity/requirements keep separate events.']
        for fact in facts:
            result['fact_ownership'][fact['fact_id']] = change['project_change_id']
        validate(change, text_only=False)
        result['project_changes'].append(change)
    return result


def apply(packet, relation, approach='typed_event'):
    out = dict(candidate_id=packet['candidate_id'], source_type='TABLE',
               identity_relation=relation['relation'], project_changes=[], facts=[], unresolved=[])
    if relation['relation'] != 'SAME_SUBJECT' or relation['confidence'] != 'HIGH' or relation['review_required']:
        out['unresolved'].append(dict(reason='IDENTITY_OR_RESTRUCTURING_UNPROVEN'))
        return out
    old = next(r['subject'] for r in packet['old_candidates'] if r['subject']['subject_id'] == relation['old_subject_ids'][0])
    new = packet['new']
    if relation['packet_hash'] != packet['packet_hash']:
        raise ValueError('Certificate packet hash mismatch')
    if new['subject_id'] not in relation['new_subject_ids']:
        raise ValueError('Certificate belongs to another NEW subject')
    if any(s['source_type'] != 'TABLE' or s['purity'] != 'PROVEN' for s in [old,new]):
        out['unresolved'].append(dict(reason='SOURCE_PURITY_UNPROVEN'))
        return out
    if old['document_version'] == new['document_version']:
        out['unresolved'].append(dict(reason='SAME_VERSION_CONTROL'))
        return out
    qualifiers = {}
    for f in ['equipment_class','system','room','floor','mark','engineering_function']:
        a, b = old['clues'][f], new['clues'][f]
        if len(a) == len(b) == 1 and norm(a[0]) == norm(b[0]):
            qualifiers[f] = a[0]
    certificate = dict(resolved_id='resolved_' + digest([new['comparison_scope'],relation['old_subject_ids'],relation['new_subject_ids']])[:24],
                       qualifiers=qualifiers, relation=relation['relation'], confidence='HIGH',
                       identity_evidence=relation['witnesses'], identity_basis=relation.get('identity_basis', relation['reason']))
    a, b = record(old,certificate), record(new,certificate)
    result = compare_records(dict(comparison_scope=new['comparison_scope'],old_records=[a],new_records=[b]), approach)
    out.update(result)
    out['bridge_records'] = dict(old=a,new=b)
    out['subject_certificate'] = certificate
    out['scope_evidence'] = {side:s['context'] for side,s in [('old',old),('new',new)]}
    out['state_coverage_gaps'] = {side:r['coverage_gaps'] for side,r in [('old',a),('new',b)]}
    for change in out['project_changes']:
        change['engineering_subject']['identity_basis'].append(certificate['identity_basis'])
    return out
