"""Offline reproductions, synthetic fixtures; no inference or network permitted."""
import importlib.util,json,socket,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def denied(*a,**kw):raise RuntimeError('Network/subprocess forbidden by this investigation')
socket.socket.connect=denied;socket.socket.connect_ex=denied;socket.create_connection=denied;subprocess.Popen=denied
sys.dont_write_bytecode=True
def mod(name,path):
 s=importlib.util.spec_from_file_location(name,ROOT/path);m=importlib.util.module_from_spec(s);sys.modules[name]=m;s.loader.exec_module(m);return m
g=mod('gate','backend/app/pipeline/stages/block_analysis/finding_evidence_gate.py')
a=mod('absence','backend/app/pipeline/stages/text_analysis/absence_guard.py')
base=dict(problem_class='parameter_mismatch',affected_entity='PUMP-K12',claim_type='contradiction',context_status='sufficient',confidence=.95,counterevidence_checked=True,evidence_kind='drawing',evidence_quote='K12: source A vs source B',severity='ЭКОНОМИЧЕСКОЕ')
fixtures=[dict(base,id='F-power',finding='Мощность K12: 0.55 против 1.5 кВт',parameter='power'),dict(base,id='F-mass',finding='Масса K12: 18 против 28 кг',parameter='mass')]
pub,deferred,report=g.gate_findings(fixtures)
calls=[]
f=[dict(id='A1',problem='Отсутствует насос K12',severity='ЭКОНОМИЧЕСКОЕ',absence_checked=['wrong-page'])]
stats=a.enforce_absence_guard(f,md_text='K12 присутствует в спецификации',verifier=lambda *args:calls.append(args) or {0:'present'})
r={'fixture_kind':'synthetic; demonstrates code, not historical defect','dedup':{'input':fixtures,'published':pub,'deferred':deferred,'report':report},'absence_checked':{'findings':f,'stats':stats,'verifier_calls':len(calls)}}
assert len(pub)==1 and len(deferred)==1
assert len(calls)==0 and f[0]['severity']=='ЭКОНОМИЧЕСКОЕ'
Path(__file__).with_name('local_mechanism_results.json').write_text(json.dumps(r,ensure_ascii=False,indent=2))
print('2 offline reproductions confirmed; no network or model calls')
