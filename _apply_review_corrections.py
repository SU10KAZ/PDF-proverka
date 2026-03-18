import json
import shutil
from pathlib import Path

findings_path = Path(r'D:\Отедел Системного Анализа\1. Calude code\projects\АР\133-23-ГК-АР1\_output\03_findings.json')
pre_review_path = Path(r'D:\Отедел Системного Анализа\1. Calude code\projects\АР\133-23-ГК-АР1\_output\03_findings_pre_review.json')

# Load
with open(findings_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Backup original
with open(pre_review_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
print("Backup saved to 03_findings_pre_review.json")

stats = {"fixed": 0, "removed": 0, "downgraded": 0, "passed": 0}
# Count passed from review
review_passes = {
    "F-005","F-006","F-007","F-009","F-010","F-012","F-014","F-015",
    "F-016","F-018","F-019","F-021","F-028","F-029","F-030","F-031",
    "F-032","F-035","F-037","F-038","F-040","F-041","F-042"
}
stats["passed"] = len(review_passes)

for finding in data['findings']:
    fid = finding['id']

    # === F-008: weak_evidence → narrow_evidence ===
    # Remove 76KC-CYYP-R4H (узел К6.1, shows layer composition, NOT сетку diameter)
    # Keep T3WY-NTPE-YXX (roof plan, shows K6 types context)
    # Add text evidence for page 9 where ведомость покрытий is
    if fid == 'F-008':
        finding['related_block_ids'] = ['T3WY-NTPE-YXX']
        finding['evidence'] = [
            {"type": "image", "block_id": "T3WY-NTPE-YXX", "page": 9},
            {"type": "text", "block_id": None, "page": 9}
        ]
        stats["fixed"] += 1
        print(f"F-008: удалён нерелевантный блок 76KC-CYYP-R4H из evidence (узел К6.1 не содержит данных о диаметре сетки). Добавлено text evidence стр.9.")

    # === F-011: weak_evidence → narrow_evidence ===
    # Remove UJAV-GHF6-LDU from related_block_ids (это ДВ12.4Л, а не ДВ19Л-1)
    # Evidence already correct (text, page 15)
    elif fid == 'F-011':
        finding['related_block_ids'] = []
        # evidence уже text с page=15, оставляем как есть
        stats["fixed"] += 1
        print(f"F-011: удалён нерелевантный блок UJAV-GHF6-LDU из related_block_ids (блок содержит ДВ12.4Л EI30, а не ДВ19Л-1).")

    # === F-017: weak_evidence → narrow_evidence ===
    # Remove T3WY-NTPE-YXX and 76KC-CYYP-R4H (не содержат опечатку в ведомостях)
    # Add block 9UKA-PY7C-NYR (содержит «ТЕХНОНИКОЛЬ» — корректное написание для сравнения)
    # Add text evidence for page 9 (where ведомость с опечаткой)
    elif fid == 'F-017':
        finding['related_block_ids'] = ['9UKA-PY7C-NYR']
        finding['evidence'] = [
            {"type": "text", "block_id": None, "page": 9},
            {"type": "image", "block_id": "9UKA-PY7C-NYR", "page": 9}
        ]
        stats["fixed"] += 1
        print(f"F-017: заменены evidence-блоки. Удалены T3WY-NTPE-YXX и 76KC-CYYP-R4H (не содержат опечатку). Добавлен 9UKA-PY7C-NYR (показывает «ТЕХНОНИКОЛЬ» — корректное написание) + text evidence стр.9.")

    # === F-020: weak_evidence → narrow_evidence ===
    # Remove 77CQ-6TGC-NM4 from related_block_ids (стр.14 узел плинтуса — нерелевантен)
    # Keep text evidence (page 5, where ведомость ссылочных документов is)
    elif fid == 'F-020':
        finding['related_block_ids'] = []
        # Clean up grounding_candidates too if present
        if 'grounding_candidates' in finding:
            del finding['grounding_candidates']
        # Normalize sheet name
        finding['sheet'] = 'Лист 1'
        stats["fixed"] += 1
        print(f"F-020: удалён нерелевантный блок 77CQ-6TGC-NM4 из related_block_ids (стр.14, узел плинтуса — нерелевантен ведомости ссылочных документов на стр.5). Нормализован sheet: «Лист 1».")

    # === F-025: page_mismatch ===
    # Fix evidence[0].page from 4 to 7 (блок 6RDP-9CCH-TP7 реально на стр.7 Лист 3)
    # Finding's own page/sheet correct (page=15, Лист 10)
    elif fid == 'F-025':
        for ev in finding['evidence']:
            if ev.get('block_id') == '6RDP-9CCH-TP7':
                ev['page'] = 7
        stats["fixed"] += 1
        print(f"F-025: исправлена page в evidence[6RDP-9CCH-TP7] с 4 → 7 (Лист 3). Page/sheet самого замечания (стр.15 / Лист 10) корректны.")

    # === F-033: page_mismatch ===
    # Fix finding page from 4 to 6 AND evidence[0].page from 4 to 6
    # Sheet "Лист 2" correct
    elif fid == 'F-033':
        finding['page'] = 6
        for ev in finding['evidence']:
            if ev.get('block_id') == '9K99-7FYY-F3V':
                ev['page'] = 6
        stats["fixed"] += 1
        print(f"F-033: исправлены page замечания и evidence[9K99-7FYY-F3V] с 4 → 6 (Лист 2 корректен).")

    # === F-034: page_mismatch ===
    # Fix finding page from 8 to 7, sheet from "Лист 3.1" to "Лист 3"
    # Fix evidence[0].page from 4 to 7
    elif fid == 'F-034':
        finding['page'] = 7
        finding['sheet'] = 'Лист 3'
        for ev in finding['evidence']:
            if ev.get('block_id') == '6RDP-9CCH-TP7':
                ev['page'] = 7
        stats["fixed"] += 1
        print(f"F-034: исправлены page 8→7, sheet «Лист 3.1»→«Лист 3», evidence[6RDP-9CCH-TP7] page 4→7.")

    # === F-036: page_mismatch ===
    # Fix finding page from 4 to 6 AND evidence[0].page from 4 to 6
    # Sheet "Лист 2" correct
    elif fid == 'F-036':
        finding['page'] = 6
        for ev in finding['evidence']:
            if ev.get('block_id') == '9TX9-3FP4-47A':
                ev['page'] = 6
        stats["fixed"] += 1
        print(f"F-036: исправлены page замечания и evidence[9TX9-3FP4-47A] с 4 → 6 (Лист 2 корректен).")

# Update meta
total_reviewed = 31  # from review file
data['meta']['review_applied'] = True
data['meta']['review_stats'] = {
    "total_reviewed": total_reviewed,
    "passed": stats["passed"],
    "fixed": stats["fixed"],
    "removed": stats["removed"],
    "downgraded": stats["downgraded"]
}

# Save corrected findings
with open(findings_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\n=== ИТОГ ===")
print(f"Проверено: {total_reviewed}")
print(f"Pass (без изменений): {stats['passed']}")
print(f"Исправлено: {stats['fixed']}")
print(f"Удалено: {stats['removed']}")
print(f"Понижено в категории: {stats['downgraded']}")
print(f"Файл сохранён: {findings_path}")
