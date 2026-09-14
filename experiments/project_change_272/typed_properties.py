"""Keep operating conditions in addresses; unknown scalars remain coverage gaps."""
from collections import defaultdict
from copy import deepcopy
from decimal import Decimal
import re

from experiments.project_change_master.state import record, compare_records
from experiments.text_comparison_v1.common import digest


def exact_sum(value):
    """Admit only an explicitly written, arithmetically correct scalar sum."""
    match = re.fullmatch(r'\s*(\d+(?:[.,]\d+)?)\s*\+\s*(\d+(?:[.,]\d+)?)\s*=\s*(\d+(?:[.,]\d+)?)\s*', value)
    if not match:
        return None
    a, b, total = [Decimal(x.replace(',', '.')) for x in match.groups()]
    return match[3] if a + b == total else None


def condition(headers, column):
    content = ' '.join(h[column] for h in headers if column < len(h) and h[column])
    # Explicit condition modifiers supplement units/bases; bare unit equality
    # does not collapse normal demand and simultaneous fire-fighting demand.
    qualifiers = re.findall(r'при\s+пожаре|пожар\w*|максим\w*|макс\.?|средн\w*|летн\w*|зимн\w*', content.casefold())
    return '|'.join(sorted(set(qualifiers))) or 'normal_or_unspecified'


def apply(packet, relation):
    old = next(r['subject'] for r in packet['old_candidates'] if r['subject']['subject_id'] == relation['old_subject_ids'][0])
    new = packet['new']
    if relation['packet_hash'] != packet['packet_hash'] or relation['relation'] != 'SAME_SUBJECT' or relation['confidence'] != 'HIGH':
        raise ValueError('Invalid subject certificate')
    certificate = dict(resolved_id='resolved_' + digest([new['comparison_scope'], old['subject_id'], new['subject_id']])[:24],
        qualifiers={'engineering_function': new['clues']['engineering_function'][0]},
        relation='SAME_SUBJECT', confidence='HIGH', identity_evidence=relation['witnesses'], identity_basis=relation['reason'])
    records = {}
    gaps = {}
    for side, subject in [('old', old), ('new', new)]:
        item = record(subject, certificate)
        values = []
        excluded = []
        for source in item['values']:
            value = deepcopy(source)
            cols = {e['locator']['column_index'] for e in value['evidence']}
            if len(cols) != 1:
                excluded.append(dict(reason='PROPERTY_COLUMN_UNRESOLVED', observation=value))
                continue
            value['basis'] += '|' + condition(subject['headers'], next(iter(cols)))
            total = exact_sum(value['value'])
            if total is not None:
                value['value'] = total
                value['status'] = 'PROVEN'
            if value['status'] != 'PROVEN':
                excluded.append(dict(reason='UNKNOWN_SCALAR_NOT_A_CONTRADICTORY_STATE', observation=value))
                continue
            values.append(value)
        item['values'] = values
        records[side] = item
        gaps[side] = dict(excluded=excluded, parsing_gaps=item['coverage_gaps'])
    result = compare_records(dict(comparison_scope=new['comparison_scope'], old_records=[records['old']], new_records=[records['new']]))
    result['coverage_gaps'] = gaps
    for change in result['project_changes']:
        change['decision_reasons'].append('Only explicitly parsed corresponding addresses are claimed; unresolved cells remain coverage gaps. Operating conditions are part of the address.')
        change['short_summary_ru'] = new['clues']['engineering_function'][0].capitalize() + ': ' + '; '.join(
            f"{f['old']['quote']} → {f['new']['quote']} {f['new']['unit'] or ''}" for f in change['supporting_fact_changes']) + '.'
    return result
