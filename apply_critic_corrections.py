import json
import shutil

src = r'D:\1.OSA\1. Audit Manager\projects_objects\214._Alia_(ASTERUS)\13АВ-РД-АР1.1-К4 (Изм.2).pdf\_output\03_findings.json'
dst_backup = r'D:\1.OSA\1. Audit Manager\projects_objects\214._Alia_(ASTERUS)\13АВ-РД-АР1.1-К4 (Изм.2).pdf\_output\03_findings_pre_review.json'

REMOVE_IDS = {"F-007", "F-033", "F-034", "F-035", "F-036"}

with open(src, 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"Total findings before: {len(data['findings'])}")
print(f"IDs: {[f['id'] for f in data['findings']]}")

# Backup
shutil.copy2(src, dst_backup)
print(f"Backup written: {dst_backup}")

# Filter
new_findings = [f for f in data['findings'] if f['id'] not in REMOVE_IDS]
print(f"Total findings after: {len(new_findings)}")

# Recount by_severity
by_severity = {}
for f in new_findings:
    sev = f.get('severity', 'UNKNOWN')
    by_severity[sev] = by_severity.get(sev, 0) + 1
print(f"by_severity: {by_severity}")

# Update meta
data['findings'] = new_findings
data['meta']['total_findings'] = len(new_findings)
data['meta']['by_severity'] = by_severity
data['meta']['review_applied'] = True
data['meta']['review_stats'] = {
    "total_reviewed": 56,
    "passed": 51,
    "fixed": 0,
    "removed": 5,
    "downgraded": 0
}

with open(src, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Done. Written to:", src)
