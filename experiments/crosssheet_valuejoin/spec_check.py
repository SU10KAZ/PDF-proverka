#!/usr/bin/env python3
"""Спека-лейн — проверка ПРИСУТСТВИЯ (пересортица): каждый кабель (марка+сечение),
применённый на однолинейной схеме, обязан быть в спецификации, и наоборот.

Почему это, а не «сумма» первым: нужен только СХЕМА + СПЕКА (оба есть почти везде;
журнал — редкость, см. батч Тип A). Детерминированно, без допуска, 0 токенов.

  Сторона A (схема) — вектограф: feeders_flat[].cable → множество (марка,сечение) + счётчик
  Сторона S (спека) — парсер кабельного раздела: марка → {сечение → метраж}

Кандидаты:
  * на схеме есть, в спеке НЕТ  → пересортица / пропуск в спеке (действенный сигнал)
  * в спеке есть, на схеме нет   → слабее (может быть на планах/журнале/запас) — справочно

Guardrails: (марка,сечение) канонизируются одинаково с обеих сторон (х/x, «мм2», (А)/(A));
ключ — по значению кабеля, не по коду. Всё — КАНДИДАТЫ.

experiments-проба. Запуск: PYTHONPATH=<repo> python3 spec_check.py ["<project_dir>"]
"""
from __future__ import annotations
import sys, re, json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE))
import fitz  # noqa: E402
import valuejoin_mvp as vj  # noqa: E402

SPEC_SECT_RE = re.compile(r'^(\d+\s*[хx]\s*[\d().+хx ]+?)\s*мм2$')
SEC_IN_CABLE = re.compile(r'\d+\s*[хx]\s*[\d().+хx ]*[\d)]')
INNER_SEC = re.compile(r'\d+\s*[хx]\s*\d+')  # одножилка внутри пучка: 1х70


def split_cable(cable: str):
    """'ППГнг(А)-FRHF 3х1.5' → ('ППГнг(А)-FRHF', '3х1.5'). None если сечение не найдено."""
    m = SEC_IN_CABLE.search(cable)
    if not m:
        return None
    return cable[:m.start()].strip(), m.group(0).strip()


def atomic_sections(section: str):
    """Пучок '4х(1х70)+(1х50)' → ['1х70','1х50'] (как в спеке); простое сечение → [оно само].
    Спека раскладывает многожильные кабели на одножилки — сверяем по компонентам."""
    if '(' in section:
        comps = INNER_SEC.findall(section)
        return comps or [section]
    return [section]


def spec_range(doc, journal_pages):
    """Диапазон страниц спеки. Якорь — шапка таблицы спеки «Наименование и техническая
    характеристика» (есть на РЕАЛЬНОМ листе спеки, нет на «Общие данные», где спека лишь
    упомянута). Конец — ближайший журнал ПОСЛЕ якоря (журнал может идти и ДО спеки — тогда
    до конца документа). Не полагаемся на «спека всегда до журнала»."""
    n = doc.page_count
    anchors = [pi for pi in range(n) if 'аименование и техническ' in doc[pi].get_text()]
    if not anchors:
        return []
    start = min(anchors)
    jafter = [p for p in (journal_pages or []) if p > start]
    end = min(jafter) if jafter else n
    return list(range(start, end))


def extract_spec_cables(doc, pages):
    """(canon марка, canon сечение) → {'mark','section','metres'}."""
    out = {}
    cur_mark = None
    for pi in pages:
        lines = [l.strip() for l in doc[pi].get_text().splitlines() if l.strip()]
        i = 0
        while i < len(lines):
            l = lines[i]
            if vj.MARK_RE.match(l) and 'мм2' not in l:
                cur_mark = l
                i += 1
                continue
            m = SPEC_SECT_RE.match(l)
            if cur_mark and m:
                section = m.group(1).strip()
                unit = lines[i + 1].strip() if i + 1 < len(lines) else ''
                qty = lines[i + 2].strip() if i + 2 < len(lines) else ''
                if unit in ('м', 'м.') and re.match(r'^\d+$', qty):
                    key = (vj.canon_val(cur_mark), vj.canon_val(section))
                    out.setdefault(key, {'mark': cur_mark, 'section': section, 'metres': int(qty)})
                    i += 3
                    continue
            i += 1
    return out


def extract_schema_cables(pdf, pages):
    """(canon марка, canon сечение) → {'mark','section','n_feeders','codes'[]}."""
    schema, _ = vj.extract_schema(pdf, pages)
    out = {}
    for code, v in schema.items():
        cab = v.get('cable') or ''
        sp = split_cable(cab)
        if not sp:
            continue
        mark, section = sp
        for atom in atomic_sections(section):  # пучок → компоненты (как в спеке)
            key = (vj.canon_val(mark), vj.canon_val(atom))
            rec = out.setdefault(key, {'mark': mark, 'section': atom, 'n_feeders': 0, 'codes': []})
            rec['n_feeders'] += 1
            if len(rec['codes']) < 6:
                rec['codes'].append(code)
    return out


def main():
    proj = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'projects/214. Alia (ASTERUS)/EOM/13АВ-РД-ЭМ-К1')
    if not proj.is_absolute():
        proj = REPO / proj
    pdf = sorted(proj.glob('*.pdf'))[0]
    dg = json.loads((proj / '_output' / 'document_graph.json').read_text())
    doc = fitz.open(str(pdf))

    src = vj.detect_sources(doc, dg.get('pages', []))
    spg = spec_range(doc, src['journal'])
    print('=== Источники ===')
    print(f"  однолинейки: {src['single_line']}")
    print(f"  спека:       {spg}")
    if not src['single_line'] or not spg:
        print('  Нет схемы или спеки — лейн недоступен.'); return

    schema = extract_schema_cables(pdf, src['single_line'])
    spec = extract_spec_cables(doc, spg)
    print(f"\n=== Кабели (уникальные марка+сечение) ===")
    print(f"  на схеме: {len(schema)}  |  в спеке: {len(spec)}")
    if not spec:
        print('  ⚠ спека не распарсилась (0 кабелей) — кандидаты НЕ выдаём (иначе всё ложное).')
        return

    schema_not_spec = sorted(set(schema) - set(spec))
    spec_not_schema = sorted(set(spec) - set(schema))
    both = set(schema) & set(spec)
    print(f"  в обоих:  {len(both)}")

    print(f"\n=== КАНДИДАТЫ: на схеме есть — в спеке НЕТ ({len(schema_not_spec)}) ===")
    cand = []
    for k in schema_not_spec:
        r = schema[k]
        print(f"  {r['mark']} {r['section']}   ({r['n_feeders']} лин., напр. {', '.join(r['codes'][:4])})")
        cand.append({'kind': 'schema_not_in_spec', **r})
    print(f"\n--- справочно: в спеке есть — на схеме нет ({len(spec_not_schema)}) ---")
    for k in spec_not_schema:
        r = spec[k]
        print(f"  {r['mark']} {r['section']}   ({r['metres']} м в спеке)")

    outp = HERE / 'out' / f'{proj.name}_spec_presence.json'
    outp.parent.mkdir(exist_ok=True)
    outp.write_text(json.dumps({
        'sources': {'single_line': src['single_line'], 'spec': spg},
        'schema_cables': len(schema), 'spec_cables': len(spec), 'both': len(both),
        'candidates_schema_not_in_spec': cand,
        'spec_not_in_schema': [{'kind': 'spec_not_in_schema', **spec[k]} for k in spec_not_schema],
    }, ensure_ascii=False, indent=2))
    print(f'\nОтчёт: {outp}')


if __name__ == '__main__':
    main()
