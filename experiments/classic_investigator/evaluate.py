"""Local evaluator scaffolding. Never supplied to the independent investigators."""
import argparse
import hashlib
import json
from pathlib import Path


def read(p): return json.loads(p.read_text())
def write(p,d):
    p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n')
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def freeze(work, old):
    out=work/'evaluation';out.mkdir(exist_ok=False)
    controls=old/'evaluation/manual_controls_review.json'
    baseline=old/'results/candidate_c/full_findings_review_queue.json'
    write(out/'historical_controls.json',read(controls))
    write(out/'saved_baseline.json',read(baseline))
    write(out/'manifest.json',{'controls_sha256':digest(controls),'baseline_sha256':digest(baseline),
        'limits':['717 rows are not 717 independently verified defects',
                  'Original temporary raw run artifacts unavailable; preserved final-row archive only',
                  'Historical controls with primary evidence pending cannot count as verified targets',
                  'Discovery history is withheld from investigators; projects remain DEV-known'],
        'acceptance':{'pilot':'Review every novel candidate and every missing previously useful atom against source; no count-only win',
          'retention':'100% of verified useful baseline atoms retained in combined result, or explicit source-supported disposition',
          'false_repeats':'No recurrence of verified negative controls; ambiguous cases tracked separately',
          'recall':'Measure confirmed atom recall on frozen verified controls separately from novel findings',
          'workload':'Record duplicates, questions, engineer minutes and confirmed findings per engineer hour',
          'cost':'Report token usage, retries, elapsed time, quota; missing dollar cost is unknown, not zero',
          'generalization':'Freeze chosen variant before new projects; no promotion from six known projects alone'}})


def queue(work):
    rows=[];statuses=[]
    for p in sorted((work/'runs').glob('*/status.json')):
        state=read(p);statuses.append({'run':p.parent.name,**{k:state.get(k) for k in ('status','steps','elapsed_s','model','error')}})
        for f in state.get('findings',[]):
            rows.append({'run':p.parent.name,'candidate':f,'source_sha256':digest(p),
                'technical_status':'not_checked','control_matches':[], 'baseline_atom_matches':[],
                'engineer_decision':None,'review_minutes':None,'primary_evidence':[],
                'disposition':None})
    write(work/'evaluation/pilot_review_queue.json',{'runs':statuses,'rows':rows,
        'warning':'No automated lexical match establishes semantic equivalence or truth.'})
    print(json.dumps({'runs':statuses,'review_rows':len(rows)},ensure_ascii=False,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['freeze','queue']);p.add_argument('--work',type=Path,required=True);p.add_argument('--old',type=Path)
    a=p.parse_args();freeze(a.work,a.old) if a.command=='freeze' else queue(a.work)
