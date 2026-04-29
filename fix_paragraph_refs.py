"""
Автоматическое исправление номеров пунктов норм в findings по данным paragraph_checks.

Логика:
  - Читаем norm_checks.json → paragraph_checks
  - Для каждой записи с paragraph_verified=False ищем правильный пункт в mismatch_details
  - Заменяем неверный пункт на правильный в description (и norm_quote если нужно)
  - Если правильный пункт определить не удалось → добавляем пометку [ручная сверка]
"""

import json
import re
import shutil
from pathlib import Path
from collections import defaultdict

# ── regex для извлечения номеров пунктов ──────────────────────────────────────
# Ловит "п. 6.1.6", "п. 10.3.22", "п. A.1.2" и т.д.
_P_RE = re.compile(r'п\.\s*([\d]+(?:\.[\d]+)+)')

def _extract_paragraphs(text: str) -> list[str]:
    return _P_RE.findall(text)


def _replace_paragraph_in_text(text: str, old_p: str, new_p: str) -> str:
    """Заменяет 'п. X.X.X' → 'п. Y.Y.Y' в тексте."""
    return re.sub(
        r'п\.\s*' + re.escape(old_p),
        f'п. {new_p}',
        text,
    )


def fix_project(output_path: Path, dry_run: bool = False) -> dict:
    findings_file = output_path / '03_findings.json'
    norm_file = output_path / 'norm_checks.json'

    if not findings_file.exists():
        return {'error': 'no 03_findings.json'}
    if not norm_file.exists():
        return {'error': 'no norm_checks.json'}

    with open(findings_file, encoding='utf-8') as f:
        findings_data = json.load(f)
    with open(norm_file, encoding='utf-8') as f:
        nc_data = json.load(f)

    para_checks = nc_data.get('paragraph_checks', [])
    if not para_checks:
        return {'skipped': 'no paragraph_checks'}

    # Сгруппируем paragraph_checks по finding_id
    by_finding: dict[str, list[dict]] = defaultdict(list)
    for pc in para_checks:
        if not pc.get('paragraph_verified', True):
            by_finding[pc['finding_id']].append(pc)

    if not by_finding:
        return {'skipped': 'all paragraph_verified=true'}

    findings = findings_data.get('findings', [])
    # Индекс по id
    fmap = {f['id']: f for f in findings}

    stats = {'fixed_paragraph': 0, 'flagged_manual': 0, 'unchanged': 0, 'not_found': 0}
    changes = []

    for finding_id, checks in by_finding.items():
        finding = fmap.get(finding_id)
        if finding is None:
            stats['not_found'] += 1
            continue

        desc_orig = finding.get('description', '')
        nq_orig = finding.get('norm_quote', '')
        desc = desc_orig
        nq = nq_orig
        made_change = False

        finding_norm_orig = finding.get('norm', '')
        finding_norm = finding_norm_orig

        for pc in checks:
            norm_str = pc.get('norm', '')        # e.g. "СП 63.13330.2018, п. 6.1.4"
            mismatch = pc.get('mismatch_details', '')

            # Старый пункт из norm_str (в paragraph_check)
            old_paras = _extract_paragraphs(norm_str)
            if not old_paras:
                continue
            old_p = old_paras[0]   # напр. "6.1.4"

            # Правильный пункт из mismatch_details (кроме старого)
            all_paras_in_mismatch = _extract_paragraphs(mismatch)
            new_candidates = [p for p in all_paras_in_mismatch if p != old_p]

            if new_candidates:
                new_p = new_candidates[0]
                # Исправляем в поле norm (основное хранилище ссылки на пункт)
                new_norm = _replace_paragraph_in_text(finding_norm, old_p, new_p)
                # Исправляем также в description если пункт там упомянут
                new_desc = _replace_paragraph_in_text(desc, old_p, new_p)
                changed_norm = new_norm != finding_norm
                changed_desc = new_desc != desc
                if changed_norm or changed_desc:
                    changes.append({
                        'finding_id': finding_id,
                        'old_para': old_p,
                        'new_para': new_p,
                        'norm': norm_str,
                        'changed_norm': changed_norm,
                        'changed_desc': changed_desc,
                    })
                    finding_norm = new_norm
                    desc = new_desc
                    made_change = True
                    stats['fixed_paragraph'] += 1
            else:
                # Не можем определить правильный пункт → ставим пометку
                flag = f'[Пункт нормы {norm_str} требует ручной сверки] '
                if flag not in desc:
                    desc = flag + desc
                    made_change = True
                    stats['flagged_manual'] += 1

        if finding_norm != finding_norm_orig:
            finding['norm'] = finding_norm

        if made_change:
            finding['description'] = desc
            if nq != nq_orig:
                finding['norm_quote'] = nq
        else:
            stats['unchanged'] += 1

    if dry_run:
        print(f"DRY RUN — changes that would be made:")
        for c in changes:
            print(f"  {c['finding_id']}: п.{c['old_para']} → п.{c['new_para']}  ({c['norm']})")
        return {'dry_run': True, 'stats': stats, 'changes': changes}

    # Бэкап перед записью
    backup = output_path / '03_findings.before_para_fix2.json'
    shutil.copy(findings_file, backup)

    # Пересчёт meta
    if 'meta' in findings_data and isinstance(findings_data['meta'], dict):
        findings_data['meta']['paragraph_fix_applied'] = True
        findings_data['meta']['paragraph_fix_stats'] = stats

    with open(findings_file, 'w', encoding='utf-8') as f:
        json.dump(findings_data, f, ensure_ascii=False, indent=2)

    return {'stats': stats, 'changes': changes, 'backup': str(backup)}


if __name__ == '__main__':
    import argparse, sys

    parser = argparse.ArgumentParser(description='Fix paragraph refs in findings')
    parser.add_argument('project_path', help='Path to project folder (containing _output/)')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--all-kj', action='store_true', help='Run on all KJ projects with paragraph_checks issues')
    args = parser.parse_args()

    BASE = Path('/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ')

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

    for proj in targets:
        output = proj / '_output'
        print(f'\n=== {proj.name} ===')
        result = fix_project(output, dry_run=args.dry_run)
        stats = result.get('stats', {})
        changes = result.get('changes', [])
        print(f"  Исправлено пунктов: {stats.get('fixed_paragraph', 0)}")
        print(f"  Помечено [ручная сверка]: {stats.get('flagged_manual', 0)}")
        print(f"  Не изменилось: {stats.get('unchanged', 0)}")
        if args.dry_run and changes:
            for c in changes:
                print(f"    {c['finding_id']}: п.{c['old_para']} → п.{c['new_para']}")
        if result.get('error'):
            print(f"  ОШИБКА: {result['error']}")
        if result.get('skipped'):
            print(f"  Пропущено: {result['skipped']}")
