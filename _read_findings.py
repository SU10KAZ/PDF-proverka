import json

data = json.load(open('projects/АР/133-23-ГК-АР1/_output/03_findings.json', 'r', encoding='utf-8'))
for f in data['findings']:
    norm = f.get('norm') or ''
    nq = f.get('norm_quote') or 'None'
    desc = f.get('description') or ''
    print(f"=== {f['id']} | {f['severity']} ===")
    print(f"  norm: {norm}")
    print(f"  norm_quote: {nq[:100]}")
    print(f"  problem: {f.get('problem','')[:100]}")
    print(f"  description: {desc[:120]}")
    print()
