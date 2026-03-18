import json

with open(r'D:\Отедел Системного Анализа\1. Calude code\projects\АР\133-23-ГК-АР1\_output\03_findings.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

for finding in data['findings']:
    if finding['id'] in ['F-008', 'F-011', 'F-017', 'F-020']:
        print(json.dumps(finding, ensure_ascii=False, indent=2))
        print('---')
