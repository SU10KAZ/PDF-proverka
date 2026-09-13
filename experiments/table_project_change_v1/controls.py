"""Constructed DEV controls. Never represented as real project truth.

Expected event partitions are authored independently of grouping output. These
exercise evidence contracts, not accuracy of table OCR or corpus extraction.
"""
from copy import deepcopy
from .common import digest


def ev(side, row, column, value):
    return dict(evidence_id='ev_' + digest([side, row, column, value])[:24], route='TABLE',
                document_version='constructed-' + side, document_code='CONSTRUCTED-DEV', quote=value,
                source_refs=[], source_receipts={'pdf': dict(path='constructed://fixture.pdf', sha256='0' * 64)},
                local_unit_id='fixture-table', section_titles=[], text_purity_basis=[],
                locator=dict(kind='constructed_table_cell', row_key=str(row), column_index=column,
                             fixture=True, page=1))


def record(side, mark='П1', values=None, vertical=False, review=(), building='А', **subject):
    values = values or {'model': ('A', None), 'flow': ('1000', 'м3/ч')}
    s = dict(mark=mark, system='Приточная вентиляция', equipment_class='установка', building=building,
             engineering_function='приток', **subject)
    rid = f'{side}/{building}/{mark}'
    vs = []
    for i, (prop, (value, unit)) in enumerate(values.items()):
        row = rid + '/' + str(i) if vertical else rid
        evidence = ev(side, row, i, value)
        vs.append(dict(property=prop, value=value, quote=value, unit=unit,
                       mode='', basis='inventory' if prop == 'count' else 'selected',
                       product_characteristic=prop not in {'count', 'material', 'mode', 'composition', 'requirement'},
                       status='PROVEN', evidence=[evidence]))
    identity_e = ev(side, rid + '/identity', 0, mark or 'anonymous')
    return dict(record_id=rid, project_scope='constructed-project', subject=s, values=vs,
                evidence=[identity_e], review_reasons=list(review), table_key='fixture-table')


def pair(old, new, name='control'):
    return dict(comparison_scope='constructed/' + name, project_scope='constructed-project', old_records=old, new_records=new)


def complete_scope(p, side):
    evidence = ev(side, 'inventory', 0, 'Closed TABLE inventory')
    evidence['quote'] = None
    evidence['locator'] = dict(kind='scope_inventory', fixture=True, coverage_status='COMPLETE')
    return dict(scope_key=p['comparison_scope'], coverage_status='COMPLETE', boundary_status='PROVEN',
                identity_alternatives_resolved=True,
                unknown_rows=0, closed_inventory=True, table_keys=['fixture-table'], all_table_keys=['fixture-table'],
                record_ids=[r['record_id'] for r in p[side + '_records']], evidence=evidence)


def cases():
    out = []

    def add(name, old, new, expected, review=0, complete=False):
        p = pair(old, new, name)
        if complete:
            for side in ('old', 'new'):
                p[side + '_scope'] = complete_scope(p, side)
        out.append(dict(name=name, pair=p, expected=expected, expected_review=review))

    # 20 selected characteristics plus explicit model across source rows.
    properties = ['flow', 'pressure', 'power', 'mass', 'length', 'width', 'height', 'dimensions', 'speed', 'voltage']
    old = record('old', values={'model': ('A', None)}, vertical=True)
    new = record('new', values={'model': ('B', None)}, vertical=True)
    for i in range(20):
        prop = properties[i % len(properties)]
        # Same event selected-product facts; dimension/basis is explicit.
        for side, r, value in [('old', old, str(i + 1)), ('new', new, str(i + 2))]:
            e = ev(side, f'{side}/parameter/{i}', i, value)
            r['values'].append(dict(property=prop, value=value, quote=value, unit='fixture-unit',
                                    mode='', basis='selected', product_characteristic=True, status='PROVEN',
                                    evidence=[e]))
        # Distinct explicit property labels; don't fake the same property twice.
        if i >= 10:
            old['values'][-1]['property'] = new['values'][-1]['property'] = 'characteristic_' + str(i)
    add('vertical_replacement_20_parameters', [old], [new],
        [('EQUIPMENT_REPLACED', {'model', *properties, *('characteristic_' + str(i) for i in range(10, 20))})])
    add('replacement_and_count', [record('old', values={'model': ('A', None), 'flow': ('1000', 'м3/ч'), 'count': ('1', 'шт.')})],
        [record('new', values={'model': ('B', None), 'flow': ('1500', 'м3/ч'), 'count': ('2', 'шт.')})],
        [('EQUIPMENT_REPLACED', {'model', 'flow'}), ('EQUIPMENT_COUNT_CHANGED', {'count'})])
    add('independent_capacity_material', [record('old', values={'flow': ('1000', 'м3/ч'), 'material': ('сталь', None)})],
        [record('new', values={'flow': ('1500', 'м3/ч'), 'material': ('медь', None)})],
        [('CAPACITY_CHANGED', {'flow'}), ('OTHER_ENGINEERING_CHANGE', {'material'})])
    add('two_buildings_same_mark', [record('old', building=b, values={'model': ('A', None)}) for b in ('А', 'Б')],
        [record('new', building=b, values={'model': ('B', None)}) for b in ('А', 'Б')],
        [('EQUIPMENT_REPLACED', {'model'}), ('EQUIPMENT_REPLACED', {'model'})])
    add('capacity_set', [record('old', values={'flow': ('1000', 'м3/ч'), 'pressure': ('100', 'Па')})],
        [record('new', values={'flow': ('1500', 'м3/ч'), 'pressure': ('150', 'Па')})], [('CAPACITY_CHANGED', {'flow', 'pressure'})])
    add('row_move_only', [record('old')], [record('new')], [])
    add('equivalent_units', [record('old', values={'power': ('1', 'кВт')})],
        [record('new', values={'power': ('1000', 'Вт')})], [])
    add('boundary_review_replacement', [record('old', review=['TABLE_V3_BOUNDARY_REVIEW'])],
        [record('new', values={'model': ('B', None), 'flow': ('2000', 'м3/ч')})],
        [('EQUIPMENT_REPLACED', {'model', 'flow'})], review=1)
    add('unsafe_removal', [record('old')], [], [('EQUIPMENT_REMOVED', set())], review=1)
    add('unsafe_addition', [], [record('new')], [('EQUIPMENT_ADDED', set())], review=1)
    add('complete_removal', [record('old')], [], [('EQUIPMENT_REMOVED', {'presence'})], complete=True)
    add('complete_addition', [], [record('new')], [('EQUIPMENT_ADDED', {'presence'})], complete=True)
    add('composition', [record('old', values={'composition': ('1 рабочий + 1 резервный', None)})],
        [record('new', values={'composition': ('2 рабочих + 1 резервный', None)})], [('SYSTEM_CONFIGURATION_CHANGED', {'composition'})])
    add('important_parameter', [record('old', values={'material': ('сталь', None)})],
        [record('new', values={'material': ('медь', None)})], [('OTHER_ENGINEERING_CHANGE', {'material'})])
    add('different_modes', [record('old', values={'power': ('1', 'кВт')})],
        [record('new', values={'power': ('2', 'кВт')})], [])
    out[-1]['pair']['new_records'][0]['values'][0]['mode'] = 'fire'
    add('missing_parameter', [record('old')], [record('new', values={'model': ('A', None)})], [])
    add('units_incompatible', [record('old', values={'pressure': ('100', 'Па')})],
        [record('new', values={'pressure': ('100', 'м')})], [('CAPACITY_CHANGED', {'pressure'})], review=1)
    add('quantity_metres', [record('old', values={'count': ('10', 'м')})],
        [record('new', values={'count': ('20', 'м')})], [('EQUIPMENT_COUNT_CHANGED', {'count'})], review=1)
    return out
