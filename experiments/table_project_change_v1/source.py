"""Read pinned Table V3 components only; retain unresolved roles as coverage gaps."""
from collections import Counter
from pathlib import Path
import hashlib
import re

from experiments.table_materialization_v3.model import row_values
from .common import digest, file_hash, norm, read

# Explicit header vocabulary, shared by wide and vertical tables. Generic noun
# names cannot supply a model, mark, units or owner that the source does not say.
HEADERS = {
    'mark': r'^(?:марка (?:установки|оборудования)|обозначение оборудования|маркировка счетчика|электроприемник)$',
    'system': r'^(?:система|обозн[а-я.-]* системы|обозначение системы|наименование системы)$',
    'position': r'^(?:поз\.?|позиция)$',
    'stable_id': r'^(?:идентификатор оборудования|идентификатор слота)$',
    'engineering_function': r'^(?:назначение|функция|инженерная функция)$',
    'equipment_class': r'^(?:класс оборудования|оборудование)$',
    'room': r'^(?:номер пом\.?|номер помещения|помещение)$',
    'floor': r'^(?:этаж|№ этажа)$', 'building': r'^корпус$',
    'label': r'^(?:наименование|наименование и техническая характеристика|описание)$',
    'model': r'^(?:модель|тип установки|тип, марка.*|серия)$',
    'count': r'^(?:кол\.?|количество|кол-во|кол\. си-?стем|кол\. систем)$',
    'unit': r'^(?:ед\.?\s*изм.*|ед\.? измерения|единица измерения)$',
    'mass': r'^(?:масса.*|вес.*)$',
    'flow': r'^(?:расход.*|производительность.*|actual flow.*)$',
    'pressure': r'^(?:напор.*|давление.*|требуемое давление.*|pressure.*)$',
    'power': r'^(?:мощность.*|установленная мощность.*|[рp]уст.*)$',
    'heat_load': r'^тепловая нагрузка.*$',
    'length': r'^длина.*$', 'width': r'^ширина.*$', 'height': r'^высота.*$',
    'dimensions': r'^габариты.*$', 'speed': r'^(?:частота вращения.*|motor speed.*)$',
    'mode': r'^режим$', 'material': r'^материал$', 'requirement': r'^требование$',
    'composition': r'^состав системы$',
}
HEADERS = {k: re.compile(v) for k, v in HEADERS.items()}
IDENTITY = {'mark', 'system', 'position', 'stable_id', 'engineering_function',
            'equipment_class', 'room', 'floor', 'building'}
VALUES = set(HEADERS) - IDENTITY - {'label', 'unit'}
CLASS = re.compile(r'\b(насос|вентилятор|установка|электродвигатель|клапан|извещатель|счетчик|шкаф|щит)\b', re.I)
UNIT = re.compile(r'(?<![а-яa-z])(м³/ч|м3/ч|л/с|м³/сут|м3/сут|мпа|кпа|па|мвт|квт|вт|кг|мм|шт\.?|°c|°с|м)(?![а-яa-z])', re.I)


def role(cell):
    return next((k for k, pat in HEADERS.items() if pat.fullmatch(norm(cell))), None)


def typed_value(property_name, value, header, unit_cell=None):
    value = value.strip()
    if norm(value) in {'', '-', '—', '–', 'нет данных'}:
        return None
    unit_match = UNIT.search(norm(header))
    unit = unit_match[0] if unit_match else None
    if property_name == 'count':
        unit = unit_cell or ('шт.' if norm(header) in {'кол. систем', 'кол. си-стем'} else None)
    inline = re.fullmatch(r'([-+]?\d+(?:[.,]\d+)?)\s+(.+)', value)
    if inline and UNIT.fullmatch(norm(inline[2])):
        value, unit = inline[1], inline[2]
    numeric = property_name not in {'model', 'material', 'mode', 'composition', 'requirement', 'dimensions'}
    numeric_ok = re.fullmatch(r'[-+]?\d+(?:[.,]\d+)?', value) is not None
    status = 'PROVEN' if not numeric or (numeric_ok and unit) else 'REVIEW'
    if property_name == 'model' and (re.search(r'\b(?:ГОСТ|ТУ)\b', value) or norm(value) in {'сборная', 'встроенный'}):
        status = 'REVIEW'
    return dict(property=property_name, value=value, quote=value, unit=unit,
                mode='', basis='selected' if property_name != 'count' else 'inventory',
                product_characteristic=property_name not in {'count', 'material', 'requirement', 'mode', 'composition'},
                status=status)


def evidence(ledger, table, row, column, header_lines, artifact_receipts):
    line = row['line_refs'][0]
    block = ledger['blocks'][ledger['columns']['block_ref'][line]]
    locator = dict(kind='table_cell', table_key=table['table_key'], row_key=row['row_key'],
                   ledger_line=line, markdown_line=ledger['columns']['markdown_line'][line],
                   page=row['page'], block_id=block['block_id'], column_index=column,
                   header_ledger_lines=header_lines, artifact_receipts=artifact_receipts,
                   table_segments=table['segments'])
    return dict(evidence_id='ev_' + digest([ledger['document_version'], table['table_key'], line, column])[:24],
                route='TABLE', document_version=ledger['document_version'], document_code=ledger['document_code'],
                quote=row['cells'][column], source_refs=[],
                source_receipts={k: {x: v[x] for x in ('path', 'sha256')} for k, v in ledger['sources'].items()
                                 if k in {'pdf', 'blocks', 'work_md'}}, local_unit_id=table['table_key'],
                section_titles=[], locator=locator, text_purity_basis=[])


def project_scope(document):
    path = Path(document['artifacts']['pdf']['path'])
    parts = path.parts
    if 'objects' not in parts:
        raise ValueError('Explicit project scope is required')
    return parts[parts.index('objects') + 1]


def load_document(document, directory, verify=True):
    directory = Path(directory) / document['document_version']
    tables_path, ledger_path = directory / 'tables.json', directory / 'ledger.json'
    tables, ledger = read(tables_path), read(ledger_path)
    if tables['document_version'] != document['document_version'] or ledger['document_version'] != document['document_version']:
        raise ValueError('Version mismatch')
    if tables['schema'] != 'logical-tables.v3':
        raise ValueError('Expected frozen Table V3')
    if verify:
        for name in ('pdf', 'work_md', 'blocks'):
            receipt = ledger['sources'][name]
            if receipt['sha256'] != document['artifacts'][name]['sha256'] or file_hash(receipt['path']) != receipt['sha256']:
                raise ValueError('Source receipt mismatch: ' + name)
    raw = Path(ledger['sources']['work_md']['path']).read_text().splitlines()
    artifact_receipts = {k: dict(path=str(p), sha256=file_hash(p)) for k, p in
                         [('tables', tables_path), ('ledger', ledger_path)]}
    project = project_scope(document)
    records, gaps, inventory = [], [], []
    adjacent = {i for r in tables['candidate_relations'] for i in (r['left_table'], r['right_table'])}
    for ti, table in enumerate(tables['tables']):
        rows = [row_values(table, i, ledger, raw) for i in range(len(table['rows']['line']))]
        for row in rows:
            line = row['line_refs'][0]
            if ledger['columns']['kind'][line] != 'TABLE_ROW' or ledger['columns']['owner'][line] not in table['segments']:
                raise ValueError('Non-TABLE ownership')
        inventory.append(dict(table_key=table['table_key'], pages=table['pages'], rows=len(rows),
                              boundary_status='REVIEW' if ti in adjacent else 'PROVEN',
                              rows_sha256=digest([r['cells'] for r in rows])))
        base_reasons = ['TABLE_V3_BOUNDARY_REVIEW'] if ti in adjacent else []
        header_indexes = [i for i, row in enumerate(rows) if sum(role(c) is not None for c in row['cells']) >= 2]
        used = set()

        def make_record(subject, values, refs, row_id, review):
            if not subject or not values:
                return
            records.append(dict(record_id=row_id, project_scope=project, subject=subject, values=values,
                                evidence=list({e['evidence_id']: e for e in refs}.values()),
                                review_reasons=sorted(set(review)), table_key=table['table_key'], stable_position=False))

        # Vertical observations require an explicit in-table subject. Parameter
        # labels alone and model/series alone never identify an equipment slot.
        vertical = [r for r in rows if len(r['cells']) == 2 and role(r['cells'][0])]
        if vertical and any(role(r['cells'][0]) in {'mark', 'stable_id', 'system'} for r in vertical):
            subject, values, refs = {}, [], []
            duplicate_roles = Counter(role(r['cells'][0]) for r in vertical)
            review = base_reasons + (['DUPLICATE_VERTICAL_ROLES'] if any(n > 1 for n in duplicate_roles.values()) else [])
            for r in vertical:
                prop = role(r['cells'][0])
                ev = evidence(ledger, table, r, 1, [r['line_refs'][0]], artifact_receipts)
                refs.append(ev)
                if prop in IDENTITY:
                    subject[prop] = r['cells'][1]
                elif prop in VALUES:
                    val = typed_value(prop, r['cells'][1], r['cells'][0])
                    if val:
                        val['evidence'] = [ev]
                        values.append(val)
                used.add(r['row_key'])
            if subject.get('system') and not subject.get('mark'):
                subject['engineering_function'] = subject.get('engineering_function') or 'система'
            # A vertical record spans source rows; row-first deliberately fragments it.
            make_record(subject, values, refs, table['table_key'] + '/vertical', review)
        else:
            current, headers, group = None, [], None
            for i, r in enumerate(rows):
                cells = r['cells']
                if i in header_indexes:
                    current = [role(c) for c in cells]
                    headers = [r['line_refs'][0]]
                    header_cells = cells
                    used.add(r['row_key'])
                    continue
                if current is None:
                    continue
                if len(cells) != len(current) or not any(cells):
                    continue
                nonempty = [j for j, c in enumerate(cells) if c]
                if len(nonempty) == 1 and current[nonempty[0]] in {'label', 'position'}:
                    group = cells[nonempty[0]]
                    used.add(r['row_key'])
                    continue
                if all(re.fullmatch(r'\d+', c) for c in cells if c):
                    continue
                subject = {prop: cells[j] for j, prop in enumerate(current) if prop in IDENTITY and cells[j]}
                if group:
                    subject['group'] = group
                if subject.get('system'):
                    subject['engineering_function'] = subject.get('engineering_function') or 'система'
                label = next((cells[j] for j, p in enumerate(current) if p == 'label'), '')
                class_match = CLASS.search(norm(label))
                if class_match:
                    subject['equipment_class'] = class_match[1]
                if 'model' in current and any(norm(c) == 'тип установки' for c in header_cells):
                    subject['equipment_class'] = 'установка'
                if any(norm(c) == 'электроприемник' for c in header_cells):
                    subject['equipment_class'] = 'электроприемник'
                unit = next((cells[j] for j, p in enumerate(current) if p == 'unit'), None)
                values, refs = [], []
                repeated = Counter(p for p in current if p)
                for j, prop in enumerate(current):
                    if prop in VALUES and repeated[prop] == 1:
                        value = typed_value(prop, cells[j], header_cells[j], unit)
                        if value:
                            ev = evidence(ledger, table, r, j, headers, artifact_receipts)
                            value['evidence'] = [ev]
                            refs.append(ev)
                            values.append(value)
                    elif prop in IDENTITY and cells[j]:
                        refs.append(evidence(ledger, table, r, j, headers, artifact_receipts))
                if values:
                    review = list(base_reasons)
                    if any(c and p is None for c, p in zip(cells, current)):
                        review.append('PARTIAL_COLUMN_SEMANTICS')
                    make_record(subject, values, refs, r['row_key'], review)
                    used.add(r['row_key'])
        uncovered = [r['row_key'] for r in rows if r['row_key'] not in used and any(r['cells'])]
        if uncovered:
            gaps.append(dict(table_key=table['table_key'], reason='UNRESOLVED_TABLE_ROW_SEMANTICS', rows=uncovered))
    return dict(document_version=document['document_version'], document_code=document['document_code'],
                project_scope=project, records=records, gaps=gaps, inventory=inventory,
                table_count=len(tables['tables']), table_rows=sum(t['rows'] for t in inventory),
                boundary_proven=sum(b['basis'] == 'PROVEN' for b in tables['boundaries']),
                boundary_review=sum(b['decision'] == 'REVIEW' for b in tables['boundaries']),
                artifact_receipts=artifact_receipts)


def verify_evidence(evidence_list):
    """Independent persisted-cell verification, including header ownership."""
    cache, checked = {}, set()
    for ev in evidence_list:
        if ev['evidence_id'] in checked:
            continue
        checked.add(ev['evidence_id'])
        if ev['route'] != 'TABLE':
            raise ValueError('Cross-route evidence')
        loc = ev['locator']
        for receipt in list(ev['source_receipts'].values()) + list(loc['artifact_receipts'].values()):
            p = receipt['path']
            if p not in cache:
                cache[p] = file_hash(p)
            if cache[p] != receipt['sha256']:
                raise ValueError('Receipt changed')
        lp, tp = loc['artifact_receipts']['ledger']['path'], loc['artifact_receipts']['tables']['path']
        for p in (lp, tp):
            if ('json', p) not in cache:
                cache['json', p] = read(p)
        ledger, tables = cache['json', lp], cache['json', tp]
        if ev['document_version'] != ledger['document_version'] or ev['document_version'] != tables['document_version']:
            raise ValueError('Evidence version mismatch')
        table = next(t for t in tables['tables'] if t['table_key'] == loc['table_key'])
        md = ev['source_receipts']['work_md']['path']
        if ('lines', md) not in cache:
            cache['lines', md] = Path(md).read_text().splitlines()
        line = loc['ledger_line']
        row = row_values(table, table['rows']['line'].index(line), ledger, cache['lines', md])
        for required_line in [line] + loc['header_ledger_lines']:
            if ledger['columns']['kind'][required_line] != 'TABLE_ROW' or ledger['columns']['owner'][required_line] not in table['segments']:
                raise ValueError('TABLE header/row ownership mismatch')
        if row['cells'][loc['column_index']] != ev['quote'] or row['row_key'] != loc['row_key']:
            raise ValueError('Cell evidence mismatch')
        if row['page'] != loc['page'] or ledger['columns']['markdown_line'][line] != loc['markdown_line']:
            raise ValueError('Locator mismatch')
    return len(checked)
