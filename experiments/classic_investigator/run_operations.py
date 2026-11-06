"""Execute frozen candidate-C arithmetic rules over all six authoritative sources.

No models, history, PDF text extraction, or production pipeline mutation.
"""
import argparse
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from investigate import Corpus,read,write,sha
from md_operations import scan_operations


def execute(work):
    results=[]
    for project in read(work/'manifest.json'):
        discipline=project['discipline'];source=work/'sources'/discipline
        for name,digest in project['files'].items():
            if sha(source/name)!=digest:raise ValueError(f'Frozen input changed: {discipline}/{name}')
        corpus=Corpus(source,work/'evaluation/operations_scratch')
        pages={}
        for bid,block in corpus.blocks.items():
            page=corpus.meta[bid]['page_label']
            pages.setdefault(page,[]).append(SimpleNamespace(block_id=bid,block_type=block['kind'],text=block['text']))
        checks=scan_operations([SimpleNamespace(page=page,blocks=blocks) for page,blocks in pages.items()])
        velocity=[c for c in checks if c['status']=='arithmetic_question']
        blank=[c for c in checks if c['status']=='quantity_question']
        candidates=[]
        if velocity:
            candidates.append({'id':f'MDOP_{discipline}_velocity','status':'question',
                'claim':'Уточнить общую методику расчёта скоростей по указанным расходам и внутренним диаметрам труб.',
                'source_ids':list(dict.fromkeys(c['block_id'] for c in velocity)),
                'operation_ids':[c['operation_id'] for c in velocity],
                'atoms':[{'operation_id':c['operation_id'],'block_id':c['block_id'],'page':c['page'],'inputs':c['inputs'],'computed_v_m_s':c['computed_v_m_s'],'quote':c['source_quote']} for c in velocity],
                'limits':'Grouping reduces repeated questions but preserves all operations. Not a claim of inadequate pipes. Declared units/geometry used; method/density/regime must be checked.'})
        for c in blank:
            candidates.append({'id':c['operation_id'],'status':'question','claim':c['finding'],'source_ids':[c['block_id']],
                'operation_ids':[c['operation_id']],'atoms':[c],'limits':'Empty cell is not zero, no procurement shortfall inferred.'})
        result={'discipline':discipline,'source_md_sha256':sha(source/'document.md'),'scanner_sha256':sha(Path(__file__).with_name('md_operations.py')),
                'method_origin':'candidate C, frozen existing implementation from experiment/audit-v4-hypothesis; reused, not newly tuned on pilot answers',
                'coverage_limit':'Only explicit adjacent G/diameter/wall/speed and narrowly structured quantity rows. No claim to cover all formulas or objects.',
                'checks':checks,'candidates':candidates,'summary':dict(Counter(c['status'] for c in checks))}
        write(work/'operations'/discipline/'results.json',result)
        results.append({'discipline':discipline,'operations':len(checks),'questions':len(candidates),'summary':result['summary']})
    write(work/'operations/summary.json',{'projects':results,'models_used':False,'production_changed':False})
    for row in results:print(row)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);execute(p.parse_args().work)
