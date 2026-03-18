import json
d = json.load(open('projects/АР/133-23-ГК-АР1/_output/03a_norms_verified.json', 'r', encoding='utf-8'))
print('Findings:', len(d['findings']))
print('norm_verification:', d['meta']['norm_verification'])
revised = [f for f in d['findings'] if f.get('norm_status') != 'ok']
print('Non-ok findings:', len(revised))
for f in revised:
    print(f"  {f['id']} [{f['norm_status']}] => {f.get('norm','')[:70]}")
