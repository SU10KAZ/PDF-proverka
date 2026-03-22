import json

path = 'D:/Отедел Системного Анализа/1. Calude code/projects/OV/133_23-ГК-ОВ1.2/_output/03_findings.json'
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)

target = ['F-014','F-015','F-016','F-018','F-019','F-021','F-023','F-025','F-026','F-027','F-029']
other = [f for f in data['findings'] if f['id'] not in target]
out = {'meta': data['meta'], 'quick_index': data.get('quick_index', {}), 'other_findings': other}
print(json.dumps(out, ensure_ascii=False, indent=2))
