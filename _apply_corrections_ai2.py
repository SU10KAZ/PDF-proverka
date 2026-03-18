import json
import shutil
import copy

findings_path = r'D:\Отедел Системного Анализа\1. Calude code\projects\АИ\133-23-ГК-АИ2\_output\03_findings.json'
pre_review_path = r'D:\Отедел Системного Анализа\1. Calude code\projects\АИ\133-23-ГК-АИ2\_output\03_findings_pre_review.json'

with open(findings_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Backup
with open(pre_review_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
print("Backup created.")

fixed = 0
downgraded = 0
removed_findings = []

findings = data['findings']

for finding in findings:
    fid = finding['id']

    # ---- F-001: weak_evidence, narrow_evidence ----
    if fid == 'F-001':
        # Critic: blocks don't confirm hydroinsulation of walls in bathroom.
        # narrow_evidence: remove JCHF-7CQJ-Y3V (page 12 != 11, not relevant)
        # and text_page_8 (textual, not image); keep 7F7A-7469-9UK as the only evidence
        # but it shows wall finishes, not hydroinsulation — downgrade to РЕКОМЕНДАТЕЛЬНОЕ
        finding['evidence'] = [
            e for e in finding['evidence']
            if e['block_id'] == '7F7A-7469-9UK'
        ]
        finding['related_block_ids'] = ['7F7A-7469-9UK']
        # Downgrade since no block confirms hydroinsulation of walls
        finding['severity'] = 'РЕКОМЕНДАТЕЛЬНОЕ'
        finding['description'] = '[Critic: слабое evidence] ' + finding['description']
        downgraded += 1
        fixed += 1
        print(f"F-001: narrowed evidence + downgraded to РЕКОМЕНДАТЕЛЬНОЕ")

    # ---- F-016: phantom_block ----
    elif fid == 'F-016':
        # Both image blocks are phantom. Only text_page_27 remains (text, not image).
        # Remove phantom blocks from evidence and related_block_ids
        # No real block confirmed — downgrade to ПРОВЕРИТЬ ПО СМЕЖНЫМ
        finding['evidence'] = [
            e for e in finding['evidence']
            if e['block_id'] not in ('9XJD-DWQC-WMH', '4X7E-JEPF-9DD')
        ]
        finding['related_block_ids'] = []
        # After removal, only text evidence remains — downgrade severity
        finding['severity'] = 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'
        finding['description'] = '[Critic: phantom блоки удалены] ' + finding['description']
        downgraded += 1
        fixed += 1
        print(f"F-016: removed phantom blocks + downgraded to ПРОВЕРИТЬ ПО СМЕЖНЫМ")

    # ---- F-018: weak_evidence (P3CM-VNLW-F7U is phantom) + narrow_evidence ----
    elif fid == 'F-018':
        # P3CM-VNLW-F7U is phantom, remove it
        # Remaining: 6MDV-G4JL-AK7, 9PLY-QGRE-H7D, 4RQX-TU3F-EFU — door plan nodes, not specification table
        # Critic: no block contains the specification table with missing column
        # narrow_evidence: remove phantom, downgrade since no direct evidence for missing column
        finding['evidence'] = [
            e for e in finding['evidence']
            if e['block_id'] != 'P3CM-VNLW-F7U'
        ]
        finding['related_block_ids'] = [
            bid for bid in finding['related_block_ids']
            if bid != 'P3CM-VNLW-F7U'
        ]
        # Downgrade severity since evidence only partially confirms
        finding['severity'] = 'РЕКОМЕНДАТЕЛЬНОЕ'
        finding['description'] = '[Critic: слабое evidence, phantom блок удалён] ' + finding['description']
        downgraded += 1
        fixed += 1
        print(f"F-018: removed phantom P3CM-VNLW-F7U + downgraded to РЕКОМЕНДАТЕЛЬНОЕ")

    # ---- F-039: weak_evidence, narrow_evidence ----
    elif fid == 'F-039':
        # Blocks relate to -1 floor ceiling, not parking (-2 floor).
        # narrow_evidence: remove both evidence blocks as they're wrong floor
        # After removal — no image evidence; downgrade to ПРОВЕРИТЬ ПО СМЕЖНЫМ
        finding['evidence'] = [
            e for e in finding['evidence']
            if e['block_id'] not in ('WW4K-KXJV-4W7', '4JXE-3WLJ-NYD')
        ]
        finding['related_block_ids'] = []
        finding['severity'] = 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'
        finding['description'] = '[Critic: evidence относится к другому этажу, удалено] ' + finding['description']
        downgraded += 1
        fixed += 1
        print(f"F-039: removed wrong-floor evidence + downgraded to ПРОВЕРИТЬ ПО СМЕЖНЫМ")

    # ---- F-041: phantom_block, suggested_action: remove ----
    elif fid == 'F-041':
        # Critic recommends remove — it's a duplicate of F-047
        # We'll downgrade to ПРОВЕРИТЬ ПО СМЕЖНЫМ and mark as duplicate per instructions
        # (instructions say don't remove findings, just fix or downgrade)
        finding['evidence'] = [
            e for e in finding['evidence']
            if e['block_id'] != 'P3CM-VNLW-F7U'
        ]
        finding['related_block_ids'] = []
        finding['severity'] = 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'
        finding['description'] = '[Critic: phantom блок удалён, дублирует F-047] ' + finding['description']
        downgraded += 1
        fixed += 1
        print(f"F-041: removed phantom P3CM-VNLW-F7U + downgraded to ПРОВЕРИТЬ ПО СМЕЖНЫМ (duplicate of F-047)")

    # ---- F-047: weak_evidence, narrow_evidence ----
    elif fid == 'F-047':
        # Block NKKT-VHJL-PUW contains door ДВ19Л/П-1, not ДВ20П-1.
        # This is a textual error confirmed by text only, not by image block.
        # narrow_evidence: remove NKKT-VHJL-PUW as it's a different door
        # After removal — no image evidence; downgrade to РЕКОМЕНДАТЕЛЬНОЕ
        finding['evidence'] = [
            e for e in finding['evidence']
            if e['block_id'] != 'NKKT-VHJL-PUW'
        ]
        finding['related_block_ids'] = []
        finding['description'] = '[Critic: evidence блок относится к другой марке двери, удалён. Замечание основано на текстовом источнике.] ' + finding['description']
        # Keep РЕКОМЕНДАТЕЛЬНОЕ severity as it already is
        fixed += 1
        print(f"F-047: removed wrong-door evidence block NKKT-VHJL-PUW")

    # ---- F-068: page_mismatch ----
    elif fid == 'F-068':
        # correct_page=10, correct_sheet="4"
        # JDJJ-4EDQ-YNT is on page 9 (plan of openings) — finding is about page 10 (door specification)
        # Remove P3CM-VNLW-F7U (phantom) and text_page_10 (not in blocks_analysis)
        # Keep JDJJ-4EDQ-YNT as related evidence for context
        finding['evidence'] = [
            e for e in finding['evidence']
            if e['block_id'] not in ('P3CM-VNLW-F7U',) and e.get('type') != 'text'
        ]
        finding['related_block_ids'] = [
            bid for bid in finding['related_block_ids']
            if bid != 'P3CM-VNLW-F7U'
        ]
        # Sheet is already "Лист 4", page is already 10 — correct
        # Fix page on JDJJ-4EDQ-YNT evidence if present
        for ev in finding['evidence']:
            if ev['block_id'] == 'JDJJ-4EDQ-YNT':
                ev['page'] = 9  # keep correct page 9 for this block (plan on page 9)
        fixed += 1
        print(f"F-068: removed phantom P3CM-VNLW-F7U and text evidence; page/sheet already correct (4/10)")

    # ---- F-077: page_mismatch ----
    elif fid == 'F-077':
        # Correct sheet: "Лист 25.6" (was "Лист 25")
        finding['sheet'] = 'Лист 25.6'
        # Update evidence page — already page=40, sheet was wrong in finding metadata only
        fixed += 1
        print(f"F-077: sheet corrected from 'Лист 25' to 'Лист 25.6'")

    # ---- F-088: page_mismatch ----
    elif fid == 'F-088':
        # Correct sheet: "Лист 25.6" (was "Лист 25")
        finding['sheet'] = 'Лист 25.6'
        fixed += 1
        print(f"F-088: sheet corrected from 'Лист 25' to 'Лист 25.6'")

    # ---- F-091: page_mismatch ----
    elif fid == 'F-091':
        # Correct sheet: "Лист 25.4" (was "Лист 25.3")
        finding['sheet'] = 'Лист 25.4'
        fixed += 1
        print(f"F-091: sheet corrected from 'Лист 25.3' to 'Лист 25.4'")

    # ---- F-102: phantom_block ----
    elif fid == 'F-102':
        # text_page_4 is not a real block. Finding is about text/OCR error in stamp.
        # No image blocks on page 4. Remove phantom and keep as text-only finding.
        finding['evidence'] = [
            e for e in finding['evidence']
            if e['block_id'] != 'text_page_4'
        ]
        finding['related_block_ids'] = []
        # Evidence is now empty — downgrade to ПРОВЕРИТЬ ПО СМЕЖНЫМ
        finding['severity'] = 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'
        finding['description'] = '[Critic: phantom блок text_page_4 удалён. Требует визуальной проверки PDF.] ' + finding['description']
        downgraded += 1
        fixed += 1
        print(f"F-102: removed phantom text_page_4 + downgraded to ПРОВЕРИТЬ ПО СМЕЖНЫМ")

    # ---- F-103: weak_evidence ----
    elif fid == 'F-103':
        # Critic: use 9K3R-DW3E-VWD (page=30, sheet=Лист 20) as primary evidence for typo
        # 4XYV-ENK3-ADM (page=36) is not relevant to this typo
        # GLWA-XEXL-QK7 (page=15) contains correct spelling, not the typo
        # Update: remove non-confirming blocks, keep 9K3R-DW3E-VWD
        # Also correct finding page to 30, sheet to "Лист 20"
        finding['evidence'] = [
            ev for ev in finding['evidence']
            if ev['block_id'] == '9K3R-DW3E-VWD'
        ]
        finding['related_block_ids'] = ['9K3R-DW3E-VWD']
        finding['page'] = 30
        finding['sheet'] = 'Лист 20'
        finding['description'] = 'Опечатка в наименовании светильника: \'Безопаная зона МГН\' вместо \'Безопасная зона МГН\'. Ошибка выявлена в спецификации на листе 20 (стр. 30). Ошибка в спецификации, требует исправления во избежание некорректной надписи на изделии при заказе.'
        fixed += 1
        print(f"F-103: evidence narrowed to 9K3R-DW3E-VWD; page corrected to 30, sheet to 'Лист 20'")

    # ---- F-112: weak_evidence ----
    elif fid == 'F-112':
        # Critic: block 4KNK-9MEK-APW shows НП5.1, not НП.1 as finding claims.
        # The discrepancy may be in OCR text, not image blocks.
        # Rephrase: clarify this is an OCR/text-based discrepancy, not confirmed by image.
        # Remove 4KNK-9MEK-APW from evidence since it actually shows НП5.1 (consistent)
        # Keep JCHF-7CQJ-Y3V and CAPJ-FKMU-TAU as context
        finding['description'] = '[Critic: слабое evidence — блок спецификации показывает НП5.1, а не НП.1. Расхождение зафиксировано в OCR-тексте страницы 12, не в image-блоке. Требует визуальной проверки.] ' + finding['description']
        fixed += 1
        print(f"F-112: rephrased description to clarify OCR-text discrepancy vs image block")

    # ---- F-130: weak_evidence ----
    elif fid == 'F-130':
        # Critic: arithmetic is sound but sum of 211.3 m² from explication not verified via image blocks.
        # Rephrase to clarify it's arithmetic calculation, not directly image-confirmed.
        finding['description'] = '[Critic: слабое evidence — суммарная площадь экспликации 211,3 м² не подтверждена напрямую image-блоком. Расхождение установлено арифметически.] ' + finding['description']
        fixed += 1
        print(f"F-130: rephrased description to clarify arithmetic-only verification")


# Recalculate meta
by_severity = {}
for finding in findings:
    sev = finding['severity']
    by_severity[sev] = by_severity.get(sev, 0) + 1

data['meta']['total_findings'] = len(findings)
data['meta']['by_severity'] = by_severity
data['meta']['review_applied'] = True
data['meta']['review_stats'] = {
    'total_reviewed': 161,
    'passed': 146,
    'fixed': fixed,
    'removed': 0,
    'downgraded': downgraded
}

# Write corrected findings
with open(findings_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\nDone! fixed={fixed}, downgraded={downgraded}, removed=0")
print(f"New by_severity: {by_severity}")
