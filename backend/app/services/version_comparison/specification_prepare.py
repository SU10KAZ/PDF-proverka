"""Prepare a Version Comparison pilot from existing page word caches.

This is initial MD preparation when no authoritative results.md exists. It
never supplements/replaces an existing MD, extracts PDF text or reads raster.
PDF vector rulings locate cells; every cached word is accounted for in receipts.
Input is an explicit list of projects. All adjacent versions are prepared by
default; --latest-pair restricts a diagnostic pilot to the last transition.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import re

import fitz
from . import spec

LABELS = ['Поз.', 'Наименование', 'Марка', 'Код', 'Поставщик', 'Ед. изм.', 'Количество', 'Масса', 'Примечание']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inside(word, box):
    x, y = (word[0] + word[2]) / 2, (word[1] + word[3]) / 2
    return box[0] <= x <= box[2] and box[1] <= y <= box[3]


def cell_text(words):
    lines = []
    for word in sorted(words, key=lambda w: ((w[1] + w[3]) / 2, w[0])):
        cy = (word[1] + word[3]) / 2
        if lines and abs(cy - lines[-1][0]) < 2.5:
            lines[-1][1].append(word)
        else:
            lines.append((cy, [word]))
    return ' '.join(str(w[4]) for _, line in lines for w in sorted(line, key=lambda w: w[0]))


def page_table(page, words, metadata):
    """Use geometry only. Do not call Page.get_text or Page.get_pixmap."""
    header = spec._header(words, page.rect.width, page.rect.height)
    if not header:
        raise ValueError('Не найдена полная шапка таблицы')
    vertical, horizontal = spec._segments(page)
    # Outlined letters also have short vector strokes. Table column edges must
    # span at least 20 pt; reject anything other than the nine known columns.
    columns = spec._columns(header, [(x, a, b) for x, a, b in vertical if b-a >= 20])
    if not columns or [c['field'] for c in columns] != list(spec.FIELDS):
        raise ValueError('Сетка не даёт девять однозначных граф СО')
    x0, x1 = columns[0]['x0'], columns[-1]['x1']
    by_y = defaultdict(list)
    for y, a, b in horizontal:
        by_y[round(y*2)/2].append((max(x0, a), min(x1, b)))
    bounds = []
    for y, segments in by_y.items():
        covered, end = 0., -1e9
        for a, b in sorted(segments):
            if b > a:
                covered += max(0., b-max(a, end)); end = max(end, b)
        if covered >= (x1-x0)*.5:
            bounds.append(y)
    anchors = [w for w in header if str(w[4]).casefold().startswith(('поз', 'наименование'))]
    below = sorted(y for y in bounds if y > max((w[1]+w[3])/2 for w in anchors))
    if not below:
        raise ValueError('Не найдена нижняя граница шапки')
    top = below[0]
    zones = [z for z in metadata.get('zones', []) if z['type'] in {'stamp', 'release_stamp', 'margin'}]
    grouped, excluded = defaultdict(list), []
    # Additional revision rows can extend above the cached short stamp. Only
    # a complete revision signature in its narrow table is service content.
    stamp_box = (metadata.get('stamp') or {}).get('zone')
    revision_words = set()
    if stamp_box:
        for i, word in enumerate(words):
            if str(word[4]).casefold() not in {'зам.', 'нов.'}:
                continue
            cy = (word[1]+word[3])/2
            if not stamp_box[1]-30 <= cy <= stamp_box[1] or not stamp_box[0]-5 <= word[0] <= stamp_box[0]+190:
                continue
            nearby = [(j,w) for j,w in enumerate(words) if abs((w[1]+w[3])/2-cy) < 6
                      and stamp_box[0]-5 <= (w[0]+w[2])/2 <= stamp_box[0]+190]
            if any(re.fullmatch(r'\d+/\d+', str(w[4])) for _,w in nearby) and any(re.fullmatch(r'\d{2}\.\d{4}', str(w[4])) for _,w in nearby):
                revision_words.update(j for j,_ in nearby)
    for index, word in enumerate(words):
        x, y = (word[0]+word[2])/2, (word[1]+word[3])/2
        zone = next((z['type'] for z in zones if inside(word, z['bbox'])), None)
        if zone or index in revision_words or y <= top or x < x0 or x > x1:
            excluded.append({'index': index, 'reason': zone or ('revision_row' if index in revision_words else 'header' if y <= top else 'outside_table'), 'word': word})
        else:
            grouped[bisect_right(below, y)-1].append((index, word))
    lines, receipts = [], []
    assigned = set()
    for index in sorted(grouped):
        row = grouped[index]
        cells, cell_indices = [], []
        for column in columns:
            selected = [(i, w) for i, w in row if column['x0'] <= (w[0]+w[2])/2 < column['x1']]
            cells.append(cell_text([w for _, w in selected])); cell_indices.extend(i for i, _ in selected)
        if set(cell_indices) != {i for i, _ in row}:
            raise ValueError('Слова строки потеряны на границах граф')
        numbered_header = all(re.fullmatch(str(i)+r'(?:\s+[а-яa-z])?', value) for i,value in enumerate(cells,1))
        repeated_header = cells[0].casefold().startswith('поз') and cells[1].casefold().startswith('наименование')
        if numbered_header or repeated_header:
            excluded.extend({'index': i, 'reason': 'column_number_header', 'word': w} for i, w in row)
            continue
        if any('|' in cell or '\n' in cell or '\r' in cell for cell in cells):
            raise ValueError('Текст содержит разделитель MD; требуется отдельная подготовка')
        y0 = below[index]
        y1 = below[index+1] if index+1 < len(below) else page.rect.height
        separators = [x for x,a,b in vertical if x0+2 < x < x1-2 and min(b,y1)-max(a,y0) >= .8*(y1-y0)]
        # A merged section heading or note is text, not values arbitrarily
        # assigned to whichever columns the centred heading crosses.
        is_note = cells[0].casefold().startswith('примечание:')
        # Нумерованный заголовок раздела («1.Электрооборудование») в графе «Наименование» строки сетки,
        # остальные графы пусты, — тоже текст раздела, а не позиция (сетка у раздела не объединена).
        is_heading = (not any(cells[i] for i in range(len(cells)) if i != 1)
                      and re.fullmatch(r'\d+(?:\.\d+)*\.?\s*[^\W\d_].*', cells[1]) is not None)
        line = ('| ' + ' | '.join(cells) + ' |') if separators and not is_note and not is_heading \
            else cell_text([w for _,w in row])
        lines.append(line)
        assigned.update(cell_indices)
        receipts.append({'line_in_table': len(lines)+2, 'word_indices': sorted(cell_indices), 'text': line})
    if assigned & {r['index'] for r in excluded} or assigned | {r['index'] for r in excluded} != set(range(len(words))):
        raise ValueError('Не все слова учтены ровно один раз')
    if not lines:
        raise ValueError('Таблица пуста')
    return ['| ' + ' | '.join(LABELS) + ' |', '| ' + ' | '.join(['---']*9) + ' |', *lines], {
        'cached_words': len(words), 'table_words': len(assigned), 'columns': columns,
        'table_rows': receipts, 'excluded_words': excluded}


def prepare(object_dir, project_codes, output, *, latest_pair=False):
    if output.exists():
        raise ValueError('Выходной каталог уже существует; исходный MD нельзя перезаписывать')
    catalog = json.loads((object_dir/'catalog.json').read_text())
    pairs, receipt, source_files = [], [], {}
    for code in project_codes:
        project = next(p for p in catalog['projects'] if p['code'] == code)
        if len(project['versions']) < 2:
            raise ValueError('Нужно две версии: '+code)
        version_ids = project['versions'][-2:] if latest_pair else project['versions']
        prepared_versions = {}
        for vid in version_ids:
            version = catalog['versions'][vid]
            profiles = list((object_dir/'profiles'/version['sha256']).glob('*/pages.json'))
            if len(profiles) != 1:
                raise ValueError('Нужен один сохранённый профиль версии')
            profile_path = profiles[0]; folder = profile_path.parent
            profile = json.loads(profile_path.read_text()); pages = {p['page']: p for p in profile['pages']}
            inventory_path = folder/'spec_so3.json'
            inventory = json.loads(inventory_path.read_text())['pages']
            selected = {p['page'] for p in inventory}
            def words_of(number):
                path = folder/'words'/f'p{number:04d}.json.gz'
                return json.loads(gzip.decompress(path.read_bytes())) if path.exists() else None
            # Название в профиле — нижняя строка штампа («ЩУАХП» от «Монтажная схема ЩУАХП»): смотреть весь штамп.
            plausible = {p['page'] for p in pages.values() if p.get('kind') == 'spec'
                         and not spec.drawing_stamp(p, words_of(p['page']))}
            if not selected:
                # СО без текстового слоя (кривые, скан) или её нет: переходы с этой версией описывает ИИ
                # по листам (specification_export.text_spec), полное сравнение их не берёт.
                continue
            if plausible-selected or any(not p['parsed'] for p in inventory):
                raise ValueError('Неполная область спецификации: '+code+'/'+vid)
            pdf = object_dir/'sources'/f"{version['sha256']}.pdf"
            if digest(pdf) != version['sha256']:
                raise ValueError('Хеш PDF не совпадает с каталогом')
            text = [f"# Document: {version['file_name']}", 'Path: cached-version-comparison-pilot', '']
            page_receipts = []
            with fitz.open(pdf) as doc:
                for ordinal, number in enumerate(sorted(selected), 1):
                    metadata = pages[number]
                    if metadata.get('scan'):
                        raise ValueError('Скан требует отдельного графического пути')
                    word_path = folder/'words'/f'p{number:04d}.json.gz'
                    words = json.loads(gzip.decompress(word_path.read_bytes()))
                    if len(words) != metadata['words_count']+metadata.get('shx_words_count', 0):
                        raise ValueError('Неполный кэш слов')
                    rows, coverage = page_table(doc[number-1], words, metadata)
                    stamp = metadata['stamp']; sheet = stamp.get('sheet_no') or ''
                    bid = hashlib.sha256(f"{version['sha256']}:{number}".encode()).hexdigest()[:32]
                    text += [f'## Page {number} — Sheet {sheet} — Спецификация',
                             f'### BLOCK #{ordinal} [TEXT]: blk_{bid}',
                             f"> **Stamp:** Code: {stamp.get('designation') or code} | Sheet: {sheet} | Name: Спецификация", '', *rows, '']
                    page_receipts.append({'page': number, 'words_path': str(word_path), 'words_sha256': digest(word_path), **coverage})
            name = f"{vid}_results.md"
            source_files[name] = '\n'.join(text)
            prepared_versions[vid] = {'path': name, 'label': version['user_label'], 'pdf_sha256': version['sha256'],
                                      'pages': sorted(selected), 'version_id': vid}
            receipt.append({'project': code, 'version_id': vid, 'pdf_sha256': version['sha256'],
                            'profile_sha256': digest(profile_path), 'inventory_sha256': digest(inventory_path),
                            'pages': page_receipts})
        # Только соседние версии, у обеих есть текстовая СО; пропущенная версия не склеивает соседей.
        for a, b in zip(version_ids, version_ids[1:]):
            if a in prepared_versions and b in prepared_versions:
                old, new = prepared_versions[a], prepared_versions[b]
                pairs.append({'id': a+'--'+b, 'title': code, 'project_id': project['project_id'], 'old': old, 'new': new})
    output.mkdir(parents=True)
    for name, text in source_files.items():
        (output/name).write_text(text, encoding='utf-8')
    (output/'manifest.json').write_text(json.dumps({'schema': 'whole-specification-input/v1', 'pairs': pairs},ensure_ascii=False,indent=2)+'\n')
    (output/'preparation.json').write_text(json.dumps({'source': 'cached words + vector table geometry; no new PDF text extraction', 'versions': receipt},ensure_ascii=False,indent=2)+'\n')
    return {'projects':len(project_codes),'transitions':len(pairs),'versions':len(source_files),'manifest':str(output/'manifest.json')}
