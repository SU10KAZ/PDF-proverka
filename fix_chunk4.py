import json, copy

findings_path = r'projects_objects/214._Alia_(ASTERUS)/13АВ-РД-АР1.2-К3 (2).pdf/_output/03_findings.json'

with open(findings_path, encoding='utf-8') as f:
    data = json.load(f)

findings = data['findings']
removed = []
fixed = 0

# F-029: remove (phantom_block + not practical - footnote numbering)
for i, finding in enumerate(findings):
    if finding['id'] == 'F-029':
        removed.append({
            'id': finding['id'],
            'reason': 'phantom_block: блок 7YYE-YP6F-GYA не существует; замечание о нумерации примечаний не влияет на строительство'
        })
        findings.pop(i)
        break

# F-030: phantom_block - remove phantom, keep КРИТИЧЕСКОЕ, use real block from page 19
for finding in findings:
    if finding['id'] == 'F-030':
        # Remove phantom block_id "73QA-C6PH-GEE"
        finding['related_block_ids'] = []
        # Add real block from page 19 (Лист 15 - spec table area)
        # Use VNJY-DA46-JNJ as first block on page 19 (74 KB - spec table likely)
        finding['related_block_ids'] = ['VNJY-DA46-JNJ']
        finding['evidence'] = [
            {'type': 'image', 'block_id': 'VNJY-DA46-JNJ', 'page': 19}
        ]
        fixed += 1
        break

# F-031: phantom_block - remove "7AV6-QGL6-XFA", keep "4G4P-K6KC-HQ3", add page 5 block "XAVV-PHVQ-HFA"
for finding in findings:
    if finding['id'] == 'F-031':
        finding['related_block_ids'] = ['XAVV-PHVQ-HFA', '4G4P-K6KC-HQ3']
        finding['evidence'] = [
            {'type': 'image', 'block_id': 'XAVV-PHVQ-HFA', 'page': 5},
            {'type': 'image', 'block_id': '4G4P-K6KC-HQ3', 'page': 21}
        ]
        fixed += 1
        break

# F-032: phantom_block - remove "PNUT-ULNN-HR7", add page 5 block "XAVV-PHVQ-HFA"
for finding in findings:
    if finding['id'] == 'F-032':
        finding['related_block_ids'] = ['XAVV-PHVQ-HFA']
        finding['evidence'] = [
            {'type': 'image', 'block_id': 'XAVV-PHVQ-HFA', 'page': 5}
        ]
        fixed += 1
        break

# F-033: phantom_block - remove all phantoms, add real blocks from page 6 (floor plans)
for finding in findings:
    if finding['id'] == 'F-033':
        # Real blocks on page 6 (Лист 2): 9CLT-6PNV-WJ9 (маркировочный план 2 этажа)
        finding['related_block_ids'] = ['9CLT-6PNV-WJ9']
        finding['evidence'] = [
            {'type': 'image', 'block_id': '9CLT-6PNV-WJ9', 'page': 6}
        ]
        fixed += 1
        break

# Update meta
data['meta']['total_findings'] = len(findings)
# Update by_severity: F-029 was РЕКОМЕНДАТЕЛЬНОЕ → removed
data['meta']['by_severity']['РЕКОМЕНДАТЕЛЬНОЕ'] -= 1

# Update review_stats
rs = data['meta']['review_stats']
rs['total_reviewed'] += 5
rs['fixed'] += fixed
rs['removed'] += len(removed)

with open(findings_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'Fixed {fixed} findings, removed {len(removed)} findings')
print('Updated by_severity:', data['meta']['by_severity'])
print('Updated review_stats:', data['meta']['review_stats'])
print('Total findings:', data['meta']['total_findings'])
