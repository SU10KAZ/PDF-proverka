"""Read-only integrity checks; passing does not imply engineering acceptance."""
import hashlib
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent

def read(p):return json.loads(Path(p).read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

r=read(OUT/'register.json')
b=read(OUT/'baseline_preservation_queue.json')
assert len(r['controls'])==47
assert len({x['control_id'] for x in r['controls']})==47
assert len({x['atom_id'] for x in r['assertions']})==len(r['assertions'])
assert len(b)==244
assert not r['summary']['stage_complete']
assert not r['summary']['next_stage_allowed']
assert not r['summary']['production_promotion_allowed']
checked={}

def check(path, expected):
    path=Path(path)
    actual=checked.setdefault(str(path),sha(path))
    assert actual==expected, str(path)

for x in r['inputs']+r['baseline_sources']:
    check(x['path'],x['sha256'])
history={x['control_id']:x for x in read(r['inputs'][0]['path'])}
for c in r['controls']:
    assert c['historical_record']==history.get(c['control_id'])
    assert c['result'] is None
    assert c['current_assertion_ids'] or c['accounting_status']=='primary_adjudication_pending'
for a in r['assertions']:
    assert a['required_semantics'] and a['unsupported_or_refuted_extension']
    for e in a['evidence']:check(e['path'],e['sha256'])
for e in read(OUT/'evidence_integrity.json'):check(e['path'],e['sha256'])
by_source={}
for e in b:
    p=e['source']['path']
    if p not in by_source:by_source[p]={x['id']:x for x in read(p)['findings']}
    assert e['original_record']==by_source[p][e['original_record']['id']]
    assert e['preservation_result'] is None
for name in ['ITP72b_primary_TEXT.json','VK14_primary_TEXT.json','EOM76_primary_GRAPHIC.json','KM3_primary_GRAPHIC.json']:
    e=read(OUT/name);check(e['source_md'],e['source_md_sha256'])
    if 'image' in e:check(e['image'],e['image_sha256'])
    if 'exact_quote' in e:
        text=Path(e['source_md']).read_text();start=e['quote_offset']
        assert text[start:start+len(e['exact_quote'])]==e['exact_quote']
print(f'PASS: 47 controls, {len(r["assertions"])} assertions, 244 full original baseline records, {len(checked)} unchanged files. Engineering acceptance NOT implied.')
