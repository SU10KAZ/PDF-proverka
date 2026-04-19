import json
import shutil

src = "/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР1.1-К4 (Изм.2)/_output/03_findings.json"
backup = "/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР1.1-К4 (Изм.2)/_output/03_findings_pre_review.json"

# Load from backup (original, unmodified)
with open(backup, 'r', encoding='utf-8') as f:
    data = json.load(f)

findings = data['findings']
removed_findings = []

# --- F-002: weak_evidence, narrow_evidence ---
f002 = next(x for x in findings if x['id'] == 'F-002')
f002['related_block_ids'] = [b for b in f002['related_block_ids'] if b != '7K3A-HAA3-NR7']
f002['evidence'] = [e for e in f002['evidence'] if e.get('block_id') != '7K3A-HAA3-NR7']
f002['severity'] = 'РЕКОМЕНДАТЕЛЬНОЕ'
f002['description'] = '[Critic: слабое evidence] ' + f002['description']

# --- F-004: weak_evidence, narrow_evidence ---
f004 = next(x for x in findings if x['id'] == 'F-004')
f004['related_block_ids'] = [b for b in f004['related_block_ids'] if b != '9EDM-4YVT-XTV']
f004['evidence'] = [e for e in f004['evidence'] if e.get('block_id') != '9EDM-4YVT-XTV']
f004['severity'] = 'РЕКОМЕНДАТЕЛЬНОЕ'
f004['description'] = '[Critic: слабое evidence] ' + f004['description']

# --- F-008: not_practical -> remove ---
removed_findings.append({
    "id": "F-008",
    "reason": "формальная опечатка, не влияет на строительство"
})
findings = [f for f in findings if f['id'] != 'F-008']

# --- F-024: not_practical -> remove ---
removed_findings.append({
    "id": "F-024",
    "reason": "формальное замечание по ссылочным документам, нет влияния на строительство"
})
findings = [f for f in findings if f['id'] != 'F-024']

# --- F-025: not_practical -> remove ---
removed_findings.append({
    "id": "F-025",
    "reason": "формальное замечание по ссылочным документам, нет влияния на строительство"
})
findings = [f for f in findings if f['id'] != 'F-025']

# --- Update meta ---
total_findings = len(findings)
by_severity = {}
for f in findings:
    sev = f['severity']
    by_severity[sev] = by_severity.get(sev, 0) + 1

data['meta']['total_findings'] = total_findings
data['meta']['by_severity'] = by_severity
data['meta']['review_applied'] = True
data['meta']['review_stats'] = {
    "total_reviewed": 5,
    "passed": 0,
    "fixed": 2,
    "removed": 3,
    "downgraded": 2
}

if 'quality_summary' in data['meta']:
    data['meta']['quality_summary']['total'] = total_findings

data['findings'] = findings
data['removed_findings'] = removed_findings

with open(src, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Written:", src)
print("Total findings remaining:", total_findings)
print("Removed:", [r['id'] for r in removed_findings])
print("by_severity:", by_severity)
f002c = next(x for x in findings if x['id'] == 'F-002')
print("F-002 severity:", f002c['severity'])
print("F-002 desc[:80]:", f002c['description'][:80])
print("F-002 related_block_ids:", f002c['related_block_ids'])
print("F-002 evidence block_ids:", [e.get('block_id') for e in f002c['evidence']])
f004c = next(x for x in findings if x['id'] == 'F-004')
print("F-004 severity:", f004c['severity'])
print("F-004 desc[:80]:", f004c['description'][:80])
print("F-004 related_block_ids:", f004c['related_block_ids'])
print("F-004 evidence block_ids:", [e.get('block_id') for e in f004c['evidence']])
print("meta review_stats:", data['meta']['review_stats'])
