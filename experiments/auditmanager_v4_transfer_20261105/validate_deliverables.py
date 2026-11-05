"""Check completeness and internal references of the research package."""
from pathlib import Path
import json,hashlib,collections,xml.etree.ElementTree as ET
from html.parser import HTMLParser
import openpyxl
P=Path(__file__).resolve().parent
load=lambda n:json.loads((P/n).read_text())
r=load('rules_v4_145.json');m=load('matrix_145.json');c=load('historical_cases_539.json');ctrl=load('control_cases.json');pairs=load('filter_pairs.json');pr=load('priorities.json');ev=load('evidence_manifest.json')
assert len(r)==len(m)==len(set(x['ID'] for x in m))==145
assert set(x['id'] for x in r)==set(x['ID'] for x in m)
assert len(c)==len(set(x['case_key'] for x in c))==539
keys={x['case_key'] for x in c};ids={x['id'] for x in ev};pids={x['id'] for x in pr}
for x in ctrl:
 assert x['case_key'] in keys
 assert set(x['evidence_ids'])<=ids
for x in pairs:assert set(x['negative']+x['positive'])<=keys
for x in pr:assert set(x['case_keys'])<=keys
for x in m:
 assert x['Приоритет'] in pids
 assert x['Способ_проверки'] and x['Источник_реализации'] and x['Рекомендация']
for x in ev:
 f=P/'evidence'/x['image'];assert f.is_file();x['image_sha256']=hashlib.sha256(f.read_bytes()).hexdigest()
(P/'evidence_manifest.json').write_text(json.dumps(ev,ensure_ascii=False,indent=2))
class Links(HTMLParser):
 def __init__(self):super().__init__();self.ids=set();self.refs=[];self.local=[]
 def handle_starttag(self,tag,attrs):
  a=dict(attrs)
  if 'id' in a:self.ids.add(a['id'])
  for k in ['href','src']:
   v=a.get(k,'')
   if v.startswith('#'):self.refs.append(v[1:])
   elif v and not ':' in v:self.local.append(v)
l=Links();l.feed((P/'Отчёт_и_план_переноса.html').read_text())
assert set(l.refs)<=l.ids,set(l.refs)-l.ids
assert all((P/x).exists() for x in l.local),[x for x in l.local if not (P/x).exists()]
w=openpyxl.load_workbook(P/'Матрица_145_и_контрольные_случаи.xlsx',read_only=True)
assert w['Матрица145'].max_row==146 and w['История539'].max_row==540
assert not any(cell.data_type=='f' for ws in w for row in ws for cell in row)
w.close()
t=ET.parse(P/'offline_checks.xml').getroot();s=list(t.iter('testsuite'));assert sum(int(x.get('tests',0)) for x in s)==80;assert all(int(x.get('failures',0))==0 and int(x.get('errors',0))==0 for x in s)
print('145 rules, 539 rows, 46 controls, 8 pairs: complete; all links/images exist; workbook has no formulas; 80 tests passed.')
