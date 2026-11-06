"""Experimental, bounded operations on authoritative TEXT blocks only.

Results are arithmetic questions and explicit table gaps, never a judgement of
system adequacy. Unknown units, DN-only sizes, separate blocks and ambiguous
bindings are recorded as not_checked rather than guessed.
"""
from __future__ import annotations

from decimal import Decimal, localcontext
import hashlib
import re

NUMBER = r'\d+(?:[.,]\d+)?'
FLOW = re.compile(rf'=\s*({NUMBER})\s*м[³3]\s*/\s*ч\s*[;.]?\s*$', re.I)
PIPE = re.compile(rf'[Ø∅]\s*({NUMBER})\s*[×xх]\s*({NUMBER})\s*мм', re.I)
SPEED = re.compile(rf'\bv\s*=\s*({NUMBER})\s*м\s*/\s*(?:сек|с)\b', re.I)
PI = Decimal('3.1415926535897932384626433832795028841971693993751')


def enabled():
    from backend.app.core import config
    return bool(getattr(config, 'CLASSIC_AUDIT_TEXT_OPERATIONS_ENABLED', False))


def number(value):
    return Decimal(value.replace(',', '.'))


def half_last_digit(value):
    """Display rounding only; no engineering acceptance tolerance."""
    return Decimal(1).scaleb(number(value).as_tuple().exponent) / 2


def record(kind, page, block, line, quote, status, **extra):
    key = f'{kind}|{page}|{block}|{line}|{quote}'
    return {'operation_id': 'mdop_' + hashlib.sha256(key.encode()).hexdigest()[:20],
            'kind': kind, 'page': page, 'block_id': block, 'line_in_block': line,
            'source_quote': quote, 'status': status, **extra}


def velocity_checks(page, block):
    lines = block.text.splitlines()
    previous = ''
    for i, line in enumerate(lines):
        if not re.match(r'^\s*выбираем\s+трубопровод\b', line, re.I):
            if line.strip():
                previous = line.strip()
            continue
        quote = previous + '\n' + line.strip()
        base = dict(kind='pipe_velocity', page=page, block=block.block_id, line=i + 1, quote=quote)
        flow = FLOW.search(previous) if re.match(r'^G', previous, re.I) else None
        sizes, speeds = list(PIPE.finditer(line)), list(SPEED.finditer(line))
        if not flow or len(sizes) != 1 or len(speeds) != 1:
            yield record(**base, status='not_checked', reason='Need adjacent volumetric G, one explicit outer diameter × wall in mm and v in m/s.')
            previous = line.strip()
            continue
        q_raw, outer_raw, wall_raw, v_raw = flow[1], sizes[0][1], sizes[0][2], speeds[0][1]
        q, outer, wall, stated = map(number, (q_raw, outer_raw, wall_raw, v_raw))
        internal_mm = outer - 2 * wall
        if q <= 0 or wall <= 0 or internal_mm <= 0 or stated <= 0:
            yield record(**base, status='not_checked', reason='Nonpositive or inconsistent declared inputs.')
            previous = line.strip()
            continue
        with localcontext() as ctx:
            ctx.prec = 40
            area = PI * (internal_mm / 1000) ** 2 / 4
            velocity = q / 3600 / area
            q_half, v_half = half_last_digit(q_raw), half_last_digit(v_raw)
            low, high = max(Decimal(0), q - q_half) / 3600 / area, (q + q_half) / 3600 / area
            mismatch = high < stated - v_half or low > stated + v_half
        item = record(**base, status='arithmetic_question' if mismatch else 'consistent_with_display_rounding',
                      inputs={'G_m3_h': str(q), 'outer_mm': str(outer), 'wall_mm': str(wall), 'stated_v_m_s': str(stated)},
                      formula='v=(G/3600)/(pi*((D_outer-2*wall)/1000)^2/4)',
                      computed_v_m_s=str(velocity), computed_rounding_interval=[str(low), str(high)],
                      stated_rounding_interval=[str(stated-v_half), str(stated+v_half)],
                      limitation='Uses declared geometry and volumetric flow. No manufacturing tolerance, normative limit or system failure inferred.')
        if mismatch:
            item['finding'] = (f'По указанным в одном расчёте G={q_raw} м³/ч и трубе Ø{outer_raw}×{wall_raw} мм '
                               f'внутренний диаметр равен {internal_mm} мм. '
                               f'v=(G/3600)/(π×Dвн²/4)={velocity:.6f} м/с; записано v={v_raw} м/с. '
                               'Уточнить исходные данные и методику расчёта скорости. '
                               'Это арифметическое несоответствие указанных значений; недостаточность трубопровода не установлена.')
        yield item
        previous = line.strip()


def cells(line):
    return [cell.strip() for cell in line.strip().strip('|').split('|')]


def norm(value):
    return re.sub(r'[^a-zа-я0-9]', '', value.lower().replace('ё', 'е'))


def blank_quantity_checks(page, block):
    """Only actual specification rows; headings/sets/continuations are not gaps."""
    lines = block.text.splitlines()
    table = []
    tables = []
    for i, line in enumerate(lines + ['']):
        if line.strip().startswith('|'):
            table.append((i + 1, cells(line)))
        elif table:
            tables.append(table)
            table = []
    for rows in tables:
        header = rows[0][1]
        headers = [norm(c) for c in header]
        quantity = next((i for i, c in enumerate(headers) if c in {'колво', 'количество'}), None)
        description = next((i for i, c in enumerate(headers) if c.startswith('наименование')), None)
        position = next((i for i, c in enumerate(headers) if c in {'поз', 'позиция'}), None)
        unit = next((i for i, c in enumerate(headers) if c.startswith('ед') and 'измер' in c), None)
        if quantity is None or description is None or position is None:
            continue
        section = ''
        for offset, (line_no, row) in enumerate(rows[1:], 1):
            if len(row) != len(header) or all(re.fullmatch(r'[:\- ]*', c) for c in row):
                continue
            desc, pos, qty = row[description], row[position], row[quantity]
            if desc and not pos and not qty and (unit is None or not row[unit]):
                if not re.search(r'[Ø∅]|\bDN\d', desc, re.I) and not re.match(r'^[-•]?[а-яa-z]\)', desc, re.I):
                    section = desc
            if qty or not pos or not desc:
                continue
            has_size = bool(re.fullmatch(rf'[Ø∅]\s*{NUMBER}(?:\s*[×xх]\s*{NUMBER})?', desc, re.I))
            has_unit = unit is not None and norm(row[unit]) in {'шт', 'комп', 'компл', 'м', 'м2', 'м3', 'кг', 'т'}
            if not (has_size or has_unit):
                continue
            if re.search(r'итого|всего|в комплект|в том числе', desc, re.I):
                continue
            explanation = ' '.join(row[i] for i, h in enumerate(headers) if 'примеч' in h)
            next_row = rows[offset + 1][1] if offset + 1 < len(rows) else []
            continuation = (len(next_row) == len(header) and not next_row[position] and not next_row[description] and bool(next_row[quantity]))
            explained = (bool(re.search(r'учт[её]н|включ[её]н|комплект|см\.', explanation, re.I))
                         and not re.search(r'\bне\s+(?:учт|включ)', explanation, re.I))
            item = record('blank_quantity', page, block.block_id, line_no, '| ' + ' | '.join(row) + ' |',
                          ('quantity_on_continuation' if continuation else 'inclusion_note_not_verified')
                          if explained or continuation else 'quantity_question',
                          section=section, position=pos, description=desc,
                          limitation='Empty cell is not zero. No physical shortage or required quantity inferred.')
            if not explained and not continuation:
                item['finding'] = (f'В строке спецификации «{pos}: {desc}»'
                                   + (f' (раздел «{section}»)' if section else '')
                                   + ' графа «Количество» пуста. Уточнить количество либо дать адресную ссылку на строку, '
                                   'где оно учтено. Пустая графа не принята за ноль; нехватка материала не установлена.')
            yield item


def scan_operations(pages):
    checks = []
    for page in pages:
        for block in page.blocks:
            if block.block_type != 'TEXT':
                continue
            checks.extend(velocity_checks(page.page, block))
            checks.extend(blank_quantity_checks(page.page, block))
    return checks
