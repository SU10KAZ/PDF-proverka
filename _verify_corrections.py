import json

with open(r'D:\Отедел Sistemного Анализа\1. Calude code\projects\АР\133-23-ГК-АР1\_output\03_findings.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Print meta
print("META:", json.dumps(data['meta'], ensure_ascii=False, indent=2))
print()

# Print corrected findings
ids_to_check = ['F-008', 'F-011', 'F-017', 'F-020', 'F-025', 'F-033', 'F-034', 'F-036']
for finding in data['findings']:
    if finding['id'] in ids_to_check:
        print(f"=== {finding['id']} ===")
        print(f"  sheet: {finding['sheet']}, page: {finding['page']}")
        print(f"  related_block_ids: {finding.get('related_block_ids', [])}")
        print(f"  evidence: {json.dumps(finding.get('evidence', []), ensure_ascii=False)}")
        print(f"  norm_quote: {finding.get('norm_quote')}")
        print(f"  norm_confidence: {finding.get('norm_confidence')}")
        print()
