import json

path = '/home/coder/projects/PDF-proverka/projects_objects/214._Alia_(ASTERUS)/13АВ-РД-АР1.2-К3 (2).pdf/_output/03_findings.json'

with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)

phantom_id = 'page_019'
fixed_count = 0

for finding in data['findings']:
    if finding['id'] in ('F-020', 'F-021', 'F-022', 'F-023', 'F-024'):
        changed = False

        if phantom_id in finding.get('related_block_ids', []):
            finding['related_block_ids'] = [b for b in finding['related_block_ids'] if b != phantom_id]
            changed = True

        orig_evidence = finding.get('evidence', [])
        new_evidence = [e for e in orig_evidence if e.get('block_id') != phantom_id]
        if len(new_evidence) != len(orig_evidence):
            finding['evidence'] = new_evidence
            changed = True

        if isinstance(finding.get('page'), list) and 19 in finding['page']:
            finding['page'] = [p for p in finding['page'] if p != 19]
            changed = True

        if 'Лист 15' in finding.get('sheet', ''):
            sheets = [s.strip() for s in finding['sheet'].split(',')]
            sheets = [s for s in sheets if s != 'Лист 15']
            finding['sheet'] = ', '.join(sheets)
            changed = True

        if changed:
            fixed_count += 1

prev_stats = data['meta'].get('review_stats', {})
data['meta']['review_stats'] = {
    'total_reviewed': prev_stats.get('total_reviewed', 0) + 5,
    'passed': prev_stats.get('passed', 0),
    'fixed': prev_stats.get('fixed', 0) + fixed_count,
    'removed': prev_stats.get('removed', 0),
    'downgraded': prev_stats.get('downgraded', 0)
}
data['meta']['review_applied'] = True

with open(path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print('Fixed', fixed_count, 'findings')
print('review_stats:', data['meta']['review_stats'])
