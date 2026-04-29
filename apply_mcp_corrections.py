"""
Применяет исправления номеров пунктов норм по результатам поиска через MCP norms.
Работает со случаями, где fix_paragraph_refs.py не справился (claimed_quote не содержит
правильного пункта в mismatch_details, но MCP semantic search дал ответ).

Принцип:
  Для каждого флагованного finding (desc содержит [Пункт нормы...]) сравниваем
  claimed_quote с паттернами из MCP-результатов и заменяем старый пункт на правильный.
"""

import json, re, shutil
from pathlib import Path

# ── Таблица исправлений: (norm_code, ключевое слово в claimed_quote) → правильный пункт
# Заполнена на основе результатов MCP semantic search
MCP_FIXES: list[tuple[str, str, str]] = [
    # norm_code, фрагмент claimed_quote (in lower), correct_paragraph
    # СП 63.13330.2018
    ("СП 63.13330.2018", "морозостойкость", "6.1.8"),
    ("СП 63.13330.2018", "марку бетона по морозостойкости", "6.1.8"),
    ("СП 63.13330.2018", "периодического водонасыщения и попеременного заморажива", "6.1.8"),
    ("СП 63.13330.2018", "арматура, применяемая для железобетонных конструкций, должна соответствовать требованиям стандартов", "6.2.2"),
    ("СП 63.13330.2018", "соответствовать требованиям стандартов на арматурный прокат", "6.2.2"),
    ("СП 63.13330.2018", "в качестве рабочей арматуры железобетонных конструкций следует применять арматурные стали", "6.2.4"),
    ("СП 63.13330.2018", "а500с, а600, а800, а1000", "6.2.4"),
    ("СП 63.13330.2018", "толщину защитного слоя бетона следует принимать", "10.3.2"),
    ("СП 63.13330.2018", "минимальные значения толщины слоя бетона рабочей арматуры", "10.3.2"),
    ("СП 63.13330.2018", "загиб арматурных стержней следует осуществлять", "11.2.5"),
    ("СП 63.13330.2018", "специальных оправок, обеспечивающих необходимые значения радиуса кривизны", "11.2.5"),
    ("СП 63.13330.2018", "для выполнения требований по эксплуатационной пригодности", "4.3"),
    ("СП 63.13330.2018", "конструктивные решения железобетонных элементов должны обеспечивать несущую способность", "4.2"),
    ("СП 63.13330.2018", "бетонные и железобетонные конструкции всех типов должны удовлетворять", "4.1"),
    # ГОСТ 21.501-2018
    ("ГОСТ 21_501-2018", "в состав чертежей монолитной железобетонной конструкции", "6.4.1"),
    ("ГОСТ 21_501-2018", "схемы армирования монолитной конструкции", "6.4.1"),
    ("ГОСТ 21_501-2018", "на схемах армирования указывают", "6.4.3"),
    ("ГОСТ 21_501-2018", "размеры, определяющие проектное положение арматурных изделий", "6.4.3"),
    ("ГОСТ 21_501-2018", "в состав основного комплекта рабочих чертежей конструктивных решений", "6.1.2"),
    ("ГОСТ 21_501-2018", "ведомость расхода стали на монолитную", "6.1.2"),
    ("ГОСТ 21_501-2018", "рабочие чертежи арматурных и закладных изделий", "6.4.6"),
    ("ГОСТ 21_501-2018", "самостоятельных документов, в состав основного комплекта рабочих чертежей не включают", "6.4.6"),
    # ГОСТ Р 21.101-2020
    ("ГОСТ Р 21_101-2020", "в состав рабочей документации, передаваемой заказчику", "4.2.1"),
    ("ГОСТ Р 21_101-2020", "рабочие чертежи, предназначенные для производства строительных и монтажных работ", "4.2.1"),
    ("ГОСТ Р 21_101-2020", "на первых листах каждого основного комплекта рабочих чертежей приводят общие данные", "4.3.1"),
    ("ГОСТ Р 21_101-2020", "ведомость рабочих чертежей основного комплекта", "4.3.1"),
    ("ГОСТ Р 21_101-2020", "общие указания", "4.3.1"),
    # СП 48.13330.2019
    ("СП 48.13330.2019", "утверждение проектной (рабочей) документации", "5.1"),
    ("СП 48.13330.2019", "организационно-технологические решения, приведенные в ппр", "6.18"),
]

FLAG = '[Пункт нормы'
_P_RE = re.compile(r'п\.\s*([\d]+(?:\.[\d]+)+)')


def _fix_paragraph_in_norm(norm_str: str, old_p: str, new_p: str) -> str:
    return re.sub(r'п\.\s*' + re.escape(old_p), f'п. {new_p}', norm_str)


def fix_project(output_path: Path, dry_run: bool = False) -> dict:
    findings_file = output_path / '03_findings.json'
    norm_file = output_path / 'norm_checks.json'
    if not findings_file.exists() or not norm_file.exists():
        return {'skipped': 'missing files'}

    with open(findings_file, encoding='utf-8') as f:
        findings_data = json.load(f)
    with open(norm_file, encoding='utf-8') as f:
        nc = json.load(f)

    para_by_fid: dict[str, list[dict]] = {}
    for pc in nc.get('paragraph_checks', []):
        if not pc.get('paragraph_verified', True):
            para_by_fid.setdefault(pc['finding_id'], []).append(pc)

    findings = findings_data.get('findings', [])
    stats = {'fixed': 0, 'cleared_flag': 0, 'unchanged': 0}
    changes = []

    for finding in findings:
        desc = finding.get('description', '') or ''
        if FLAG not in desc:
            continue

        fid = finding['id']
        checks = para_by_fid.get(fid, [])
        norm_field = finding.get('norm', '') or ''
        made_change = False

        for pc in checks:
            code = (pc.get('matched_code') or '').strip()
            quote = (pc.get('claimed_quote') or '').lower()
            old_paras = _P_RE.findall(pc.get('norm') or '')
            if not old_paras:
                continue
            old_p = old_paras[0]

            # Найти подходящее правило
            new_p = None
            for fix_code, fix_kw, fix_para in MCP_FIXES:
                if fix_code == code and fix_kw.lower() in quote:
                    new_p = fix_para
                    break

            if new_p and new_p != old_p:
                new_norm = _fix_paragraph_in_norm(norm_field, old_p, new_p)
                new_desc = re.sub(r'п\.\s*' + re.escape(old_p), f'п. {new_p}', desc)
                if new_norm != norm_field or new_desc != desc:
                    changes.append({
                        'finding_id': fid,
                        'code': code,
                        'old_para': old_p,
                        'new_para': new_p,
                    })
                    norm_field = new_norm
                    desc = new_desc
                    made_change = True
                    stats['fixed'] += 1

        if made_change:
            # Убираем флаг для этого finding если все его checks теперь хоть частично исправлены
            # (флаг был поставлен за каждый неисправимый check — убираем соответствующий)
            new_desc = desc
            remaining_flags = 0
            for pc in checks:
                code = (pc.get('matched_code') or '').strip()
                quote = (pc.get('claimed_quote') or '').lower()
                norm_str = pc.get('norm') or ''
                flag_text = f'{FLAG} {norm_str} требует ручной сверки] '
                if flag_text in new_desc:
                    # Проверим: применено ли исправление для этого check
                    old_paras = _P_RE.findall(norm_str)
                    was_fixed = any(
                        fc == code and fk.lower() in quote
                        for fc, fk, _ in MCP_FIXES
                    )
                    if was_fixed:
                        new_desc = new_desc.replace(flag_text, '')
                        stats['cleared_flag'] += 1
                    else:
                        remaining_flags += 1
            desc = new_desc
            finding['description'] = desc
            finding['norm'] = norm_field
        else:
            stats['unchanged'] += 1

    if dry_run:
        print(f"DRY RUN changes:")
        for c in changes:
            print(f"  {c['finding_id']}: [{c['code']}] п.{c['old_para']} → п.{c['new_para']}")
        return {'dry_run': True, 'stats': stats, 'changes': changes}

    if not changes:
        return {'stats': stats, 'changes': []}

    backup = output_path / '03_findings.before_mcp_fix.json'
    shutil.copy(findings_file, backup)

    if 'meta' in findings_data and isinstance(findings_data['meta'], dict):
        findings_data['meta']['mcp_fix_applied'] = True
        findings_data['meta']['mcp_fix_stats'] = stats

    with open(findings_file, 'w', encoding='utf-8') as f:
        json.dump(findings_data, f, ensure_ascii=False, indent=2)

    return {'stats': stats, 'changes': changes, 'backup': str(backup)}


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('project_path', nargs='?', default='.')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--all-kj', action='store_true')
    args = parser.parse_args()

    BASE = Path('projects/214. Alia (ASTERUS)/KJ')
    if args.all_kj:
        targets = [
            BASE / '13АВ-РД-КЖ6-К1К2 (1).pdf',
            BASE / '13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf',
            BASE / '13АВ-РД-КЖ5.17-23.2-К2 (Изм.1).pdf',
            BASE / '13АВ-РД-КЖ5.30-31.2-К2.pdf',
            BASE / '13АВ-РД-КЖ5.39.2-К2.pdf',
        ]
    else:
        targets = [Path(args.project_path)]

    total_fixed = 0
    for proj in targets:
        out = proj / '_output'
        print(f'\n=== {proj.name} ===')
        result = fix_project(out, dry_run=args.dry_run)
        s = result.get('stats', {})
        ch = result.get('changes', [])
        print(f"  Исправлено: {s.get('fixed', 0)}, флагов снято: {s.get('cleared_flag', 0)}, не изменилось: {s.get('unchanged', 0)}")
        for c in ch:
            print(f"    {c['finding_id']}: [{c['code']}] п.{c['old_para']} → п.{c['new_para']}")
        total_fixed += s.get('fixed', 0)
        if result.get('skipped'):
            print(f"  Пропущено: {result['skipped']}")

    print(f'\nИтого исправлено: {total_fixed}')
