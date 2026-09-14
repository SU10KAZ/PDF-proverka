"""Explicit cardinal states and narrowly typed total cooling-demand assertions."""
from collections import defaultdict, Counter
from copy import deepcopy
from decimal import Decimal
import re

from experiments.project_change_text_v1.engine import canonical, analyze, make_change, evidence
from experiments.project_change_text_v1.contract import validate
from experiments.text_comparison_v1.common import digest

NUMBERS = {'одна': '1', 'один': '1', 'одно': '1', 'две': '2', 'два': '2', 'три': '3', 'четыре': '4'}
CARDINAL = re.compile(r'\b(одна|один|одно|две|два|три|четыре|\d+)\s+(зон(?:а|ы)?|контур(?:а|ов|ы)?)\b')
COOLING_OLD = re.compile(r'расчет мощности систем кондиционирования.+?составляет\s+(\d+(?:[.,]\d+)?)\s*квт', re.I)
COOLING_NEW = re.compile(r'суммарная потребность в холоде\s+(\d+(?:[.,]\d+)?)\s*квт', re.I)


def observation(unit, kind):
    text = canonical(unit['text'])
    if kind == 'cardinal':
        hits = list(CARDINAL.finditer(text))
        if len(hits) != 1 or re.search(r'не более|не менее|не предусмотр|отсутств', text):
            return None
        m = hits[0]
        noun = 'zone' if m[2].startswith('зон') else 'circuit'
        # Other explicit numeric properties are state too, not subject
        # identity. Their original values remain in the complete OLD/NEW
        # assertion; this event claims only the certified cardinal change.
        template = CARDINAL.sub('<' + noun + '_count>', analyze(text)['template'])
        return dict(unit=unit, property=noun + '_count', value=NUMBERS.get(m[1], m[1]), quote=m[0],
                    unit_name='count', template=template)
    if not any('кондиционирован' in canonical(c['title']) for c in unit.get('section_context', [])):
        return None
    match = COOLING_OLD.search(text) or COOLING_NEW.search(text)
    if not match:
        return None
    # The state is explicitly a total design demand, not a per-area rate,
    # individual machine rating, or implied sum over missing observations.
    if re.search(r'квт\s*/', text) or len(re.findall(r'\d+(?:[.,]\d+)?\s*квт', text)) != 1:
        return None
    value = format(Decimal(match[1].replace(',', '.')).normalize(), 'f')
    scalar = re.search(re.escape(match[1]) + r'\s*квт', match[0])[0]
    return dict(unit=unit, property='total_cooling_demand', value=value, quote=scalar,
                unit_name='kW', template='EXPLICIT_TOTAL_COOLING_DEMAND_IN_CONDITIONING_SECTION')


def parent_units(units):
    """Restore source paragraph sentences without borrowing another line's scope."""
    groups = defaultdict(list)
    for u in units:
        refs = tuple((r['page'], r['block_id'], r['markdown_line']) for r in u['source_refs'])
        groups[refs].append(u)
    out = []
    for siblings in groups.values():
        u = deepcopy(siblings[0])
        u['text'] = ' '.join(s['text'] for s in siblings)
        u['unit_id'] = 'parent_' + digest([s['unit_id'] for s in siblings])[:24]
        out.append(u)
    return out


def compare(scope, old_units, new_units):
    changes = []
    outcomes = Counter()
    for kind in ['cardinal', 'cooling_demand']:
        indexed = {s: defaultdict(list) for s in ['old', 'new']}
        for side, units in [('old', old_units), ('new', new_units)]:
            for u in units if kind == 'cardinal' else parent_units(units):
                item = observation(u, kind)
                if item:
                    indexed[side][item['template']].append(item)
        for template in sorted(indexed['old'].keys() & indexed['new'].keys()):
            aa, bb = indexed['old'][template], indexed['new'][template]
            if len(aa) != 1 or len(bb) != 1:
                outcomes['NONUNIQUE_EXPLICIT_STATE'] += 1
                continue
            a, b = aa[0], bb[0]
            if a['value'] == b['value']:
                outcomes['UNCHANGED'] += 1
                continue
            ua, ub = a['unit'], b['unit']
            fact = dict(fact_id='fact_' + digest([scope, template, a['value'], b['value']])[:24],
                property=a['property'],
                old=dict(value=a['value'], quote=a['quote'], unit=a['unit_name']),
                new=dict(value=b['value'], quote=b['quote'], unit=b['unit_name']),
                evidence_old=[evidence(ua)['evidence_id']], evidence_new=[evidence(ub)['evidence_id']])
            sa, sb = analyze(ua['text']), analyze(ub['text'])
            sa['template'] = sb['template'] = template
            change = make_change(scope, ua, ub, sa, sb, [fact], [], [],
                                 'CAPACITY_CHANGED' if kind == 'cooling_demand' else 'SYSTEM_CONFIGURATION_CHANGED')
            if kind == 'cooling_demand':
                change['engineering_subject'].update(semantic_subject='Суммарная расчетная потребность в холоде',
                    engineering_function='Суммарная расчетная потребность в холоде',
                    identity_basis=['Explicit total cooling-demand assertion within conditioning section; unique on both sides'], resolution='EXPLICIT')
                change['short_summary_ru'] = 'Изменена суммарная потребность в холоде: ' + a['quote'] + ' → ' + b['quote'] + '.'
            else:
                change['short_summary_ru'] = 'Изменена конфигурация системы: ' + a['quote'] + ' → ' + b['quote'] + '.'
            change['decision_reasons'] = ['Unique explicit engineering-state assertions with preserved complete subject and source scope; no inferred absence.']
            validate(change)
            changes.append(change)
            outcomes['EXPLICIT_STATE_CHANGED'] += 1
    return dict(project_changes=changes, outcomes=dict(outcomes))
