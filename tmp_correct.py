import json, shutil

findings_path = 'projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР1.1-К4 (Изм.2)/_output/03_findings.json'
pre_review_path = 'projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР1.1-К4 (Изм.2)/_output/03_findings_pre_review.json'

with open(findings_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

print('Current total_findings:', data['meta']['total_findings'])
print('Current by_severity:', data['meta']['by_severity'])
print('Current review_stats:', data['meta'].get('review_stats', {}))

remove_ids = {'F-037', 'F-042', 'F-043', 'F-046', 'F-047'}

removed = []
kept = []
for finding in data['findings']:
    if finding['id'] in remove_ids:
        removed.append({'id': finding['id'], 'severity': finding['severity']})
    else:
        kept.append(finding)

print('Removing:', removed)
print('Kept count:', len(kept))

# Backup
shutil.copy2(findings_path, pre_review_path)
print('Backup written to:', pre_review_path)

# Update meta
by_severity = dict(data['meta']['by_severity'])
for r in removed:
    sev = r['severity']
    if sev in by_severity:
        by_severity[sev] = by_severity[sev] - 1
        if by_severity[sev] == 0:
            del by_severity[sev]

data['meta']['total_findings'] = len(kept)
data['meta']['by_severity'] = by_severity
data['meta']['review_applied'] = True
data['meta']['review_stats'] = {
    'total_reviewed': 5,
    'passed': 0,
    'fixed': 0,
    'removed': 5,
    'downgraded': 0
}
data['findings'] = kept

with open(findings_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print('Corrected findings written. New total:', len(kept))
print('New by_severity:', by_severity)
