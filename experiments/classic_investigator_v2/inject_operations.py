"""Prepare additive premerge question inputs; never mutate the classic control."""
import argparse
import copy
import re
from pathlib import Path
from types import SimpleNamespace
from source_contract import Corpus,read,write,sha
from md_operations import scan_operations


def append_questions(text_analysis,checks):
    result=copy.deepcopy(text_analysis)
    original=result.setdefault('text_findings',[])
    used={f.get('id') for f in original}
    next_id=max([int(m.group(1)) for x in used if (m:=re.fullmatch(r'T-(\d+)',str(x)))]+[0])+1
    eligible=[c for c in checks if c.get('status') in {'arithmetic_question','quantity_question'}]
    ids=[c['operation_id'] for c in eligible]
    if len(ids)!=len(set(ids)):raise ValueError('Duplicate operation receipt')
    groups=[]
    velocity=[c for c in eligible if c['status']=='arithmetic_question']
    if velocity:groups.append(velocity)
    groups.extend([c] for c in eligible if c['status']=='quantity_question')
    mapping=[]
    for group in groups:
        if any(not c.get('finding') or not c.get('block_id') or not c.get('source_quote') for c in group):raise ValueError('Incomplete operation evidence')
        cid=f'T-{next_id:03d}';next_id+=1
        if cid in used:raise ValueError('Candidate ID collision')
        used.add(cid)
        question='Вопрос для уточнения проектных исходных данных; не установлен физический дефект.\n'
        if len(group)>1:question+='Общую методику можно уточнить одним ответом; адресные примеры сохраняются отдельно.\n'
        question+='\n'.join(f"[{c['operation_id']}; PDF{c['page']}; {c['block_id']}] {c['finding']}" for c in group)
        finding={'id':cid,'severity':'РЕКОМЕНДАТЕЛЬНОЕ','category':'calculation_clarification' if velocity and group is velocity else 'quantity_clarification',
            'source':'Детерминированная проверка авторитетного MD; PDF '+', '.join(map(str,sorted({c['page'] for c in group}))),
            'finding':question,'norm':None,'norm_quote':None,'related_block_ids':list(dict.fromkeys(c['block_id'] for c in group)),
            'candidate_status':'question','operation_ids':[c['operation_id'] for c in group],'operation_receipts':copy.deepcopy(group)}
        original.append(finding);mapping.append({'candidate_id':cid,'operation_ids':finding['operation_ids'],'expected_output':'question_or_evidence_based_disposition','not_a_confirmed_defect':True})
    return result,mapping


def prepare(work,disc):
    project=next(p for p in read(work/'classic_control/input_manifest.json') if p['discipline']==disc)
    source=work/'sources'/disc;corpus=Corpus(source,work/'evaluation/operations_scratch')
    saved=read(work/'operations'/disc/'results.json')
    if saved['source_md_sha256']!=sha(source/'document.md'):raise ValueError('MD changed')
    pages={}
    for bid,b in corpus.blocks.items():
        pages.setdefault(corpus.meta[bid]['page_label'],[]).append(SimpleNamespace(block_id=bid,block_type=b['kind'],text=b['text']))
    computed=scan_operations([SimpleNamespace(page=p,blocks=b) for p,b in pages.items()])
    if computed!=saved['checks']:raise ValueError('Saved operations do not reproduce from frozen MD')
    baseline=Path(project['output_dir'])/'02_text_analysis.json'
    if not baseline.exists():raise ValueError('Classic text stage not finished')
    text=read(baseline);candidate,mapping=append_questions(text,computed)
    out=work/'operation_arm'/disc;out.mkdir(parents=True,exist_ok=False)
    write(out/'02_text_analysis.json',candidate)
    write(out/'injection.json',{'discipline':disc,'source_md_sha256':saved['source_md_sha256'],'baseline_text_sha256':sha(baseline),
        'operation_result_sha256':sha(work/'operations'/disc/'results.json'),'code_sha256':sha(__file__),
        'existing_candidates':len(text.get('text_findings',[])),'added_questions':len(mapping),'mapping':mapping,
        'status':'prepared_not_merged','scope':'Downstream ablation only; same classic premerge candidates plus reproducible source-only operation questions. No pilot/engineer answers injected.','production_changed':False})
    print(disc,'preserved',len(text.get('text_findings',[])),'added questions',len(mapping))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--discipline',required=True);a=p.parse_args();prepare(a.work,a.discipline)
