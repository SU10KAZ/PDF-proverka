import json

with open('D:/Отедел Системного Анализа/1. Calude code/projects/133_23-ГК-ОВ2.1 (6)/_output/02_blocks_analysis.json', encoding='utf-8') as f:
    data = json.load(f)

sev = {}
cats = {}
total = 0
sheets = {}
pages = set()

for b in data['block_analyses']:
    pages.add(b['page'])
    st = b.get('sheet_type', '?')
    sheets[st] = sheets.get(st, 0) + 1
    for fi in b.get('findings', []):
        total += 1
        s = fi.get('severity', '?')
        sev[s] = sev.get(s, 0) + 1
        c = fi.get('category', '?')
        cats[c] = cats.get(c, 0) + 1

print(f'Blocks: {data["meta"]["blocks_reviewed"]}')
print(f'Coverage: {data["meta"]["coverage_pct"]}%')
print(f'Batches: {data["meta"]["batches_merged"]}')
print(f'Total findings: {total}')
print()
print('By severity:')
for s, c in sorted(sev.items(), key=lambda x: -x[1]):
    print(f'  {s}: {c}')
print()
print('By category:')
for c, n in sorted(cats.items(), key=lambda x: -x[1]):
    print(f'  {c}: {n}')
print()
print('Sheet types:')
for st, n in sorted(sheets.items(), key=lambda x: -x[1]):
    print(f'  {st}: {n}')
print()
print(f'Pages: {sorted(pages)}')
print(f'Blocks with no findings: {sum(1 for b in data["block_analyses"] if not b.get("findings"))}')
