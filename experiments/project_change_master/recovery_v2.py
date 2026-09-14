"""New candidate after source V1 failed: lossless exporter envelope adaptation.

Only recognized page/block envelope lines change; every content line and its
ordinal remain identical. Original source hashes and an exact line map persist.
"""
import argparse
from collections import Counter
import hashlib
from pathlib import Path
import re

import fitz

from .corpus import convert_result
from .run import ROOT,REPO,read,write,sha,immutable,now

BASE=ROOT/'03_VALIDATIONS/recovered_v2'


def canonical_envelope(text):
    lines=text.splitlines();changes=[];ordinal=0;page=None;ids=set()
    for i,line in enumerate(lines):
        p=re.fullmatch(r'## (?:СТРАНИЦА|Page)\s+(\d+)\s*',line)
        b=re.fullmatch(r'### BLOCK(?: #\S+)? \[([^]]+)\]:\s*(\S+)\s*',line)
        replacement=line
        if p:
            page=int(p[1]);ordinal=0;replacement=f'## Page {page}'
        elif b:
            if page is None:raise ValueError('Block without explicit source page')
            ordinal+=1;replacement=f'### BLOCK #{ordinal} [{b[1]}]: {b[2]}'
            ids.add((page,b[2]))
        if replacement!=line:
            changes.append(dict(line=i+1,old=line,new=replacement,kind='EXPORTER_ENVELOPE'))
            lines[i]=replacement
    if not ids:raise ValueError('No supported source envelope')
    return '\n'.join(lines)+('\n' if text.endswith('\n') else ''),changes,ids


def recover_pairs(manifest, base):
    docs=[];pairs=[];outcomes=[]
    for pair in manifest['selected']:
        ds={};failed=False
        for side in ['old','new']:
            row=dict(document=pair['document'],side=side)
            try:
                sources=pair['sources'][side]
                for r in sources.values():
                    if sha(r['path'])!=r['sha256']:raise ValueError('Source drift')
                key=hashlib.sha256(str(Path(pair[side])).encode()).hexdigest()
                pdf=sources['document.pdf'];raw=read(sources['result.json']['path'])
                blocks=convert_result(raw,len(fitz.open(pdf['path'])))
                source_text=Path(sources['document.md']['path']).read_text()
                text,line_map,ids=canonical_envelope(source_text)
                if not ids<={(b['page_index']+1,b['block_id']) for b in blocks['blocks']}:
                    raise ValueError('Source block identity mismatch after envelope adaptation')
                bp=base/'derived'/key/'blocks.json';mp=bp.with_name('document.md')
                immutable(bp,blocks)
                if mp.exists() and mp.read_text()!=text:raise ValueError('Derived input drift')
                mp.write_text(text)
                unchanged=[(a,b) for i,(a,b) in enumerate(zip(source_text.splitlines(),text.splitlines()),1) if i not in {x['line'] for x in line_map}]
                if not all(a==b for a,b in unchanged):raise ValueError('Content changed')
                lineage=dict(original_sources=sources,derived=dict(blocks={'path':str(bp),'sha256':sha(bp)},work_md={'path':str(mp),'sha256':sha(mp)}),
                             changed_envelope_lines=line_map,content_lines_changed=0,line_ordinals_preserved=True)
                immutable(bp.with_name('LINEAGE.json'),lineage)
                doc=dict(document_version=key,document_code=pair['document'],project=pair['project'],discipline=pair['discipline'],version_id=Path(pair[side]).name,
                         artifacts=dict(pdf=pdf,blocks=lineage['derived']['blocks'],work_md=lineage['derived']['work_md']))
                ds[side]=doc;row.update(admitted=True,blocks=len(blocks['blocks']),envelope_lines=len(line_map),content_lines_changed=0)
            except (ValueError,KeyError,TypeError) as e:
                failed=True;row.update(admitted=False,error=type(e).__name__,reason=str(e))
            outcomes.append(row)
        if not failed:
            docs+=list(ds.values());pairs.append(dict(project=pair['project'],discipline=pair['discipline'],pair_key='recovered_v2/'+hashlib.sha256(pair['document'].encode()).hexdigest()[:24],**ds))
    immutable(base/'SOURCE_VALIDATION_RESULT.json',outcomes)
    immutable(base/'DOCUMENTS.json',docs);immutable(base/'PAIRS.json',pairs)
    print('Source versions',len(outcomes),'admitted',sum(r['admitted'] for r in outcomes),'complete pairs',len(pairs))


def dev():
    recover_pairs(read(ROOT/'03_VALIDATIONS/recovered_documents/MANIFEST.json'),ROOT/'01_CYCLES/04_envelope_v2/dev')


def freeze():
    files=[Path(__file__),REPO/'experiments/project_change_master/test_recovery_v2.py']
    target=ROOT/'02_FROZEN_CANDIDATES/end_to_end_v2/MANIFEST.json'
    if target.exists():return
    code={**read(ROOT/'02_FROZEN_CANDIDATES/end_to_end_v1/MANIFEST.json')['code'],**{str(p):sha(p) for p in files}}
    immutable(target,dict(frozen_at=now(),code=code,config={'envelope':'lossless_v2','remainder':'unchanged end_to_end_v1'},prompts=[]))
    for f in files:
        p=target.parent/'code'/f.name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(f.read_bytes())


def select():
    target=BASE/'MANIFEST.json'
    if target.exists():return
    prior=read(ROOT/'03_VALIDATIONS/recovered_documents/MANIFEST.json')
    spent={p['document'] for p in prior['selected']};selected=[];disciplines=Counter();seed='master-new-source-slice-v2'
    eligible=[p for p in prior['eligible'] if p['document'] not in spent]
    for d in sorted(eligible,key=lambda d:hashlib.sha256((seed+d['document']).encode()).hexdigest()):
        if disciplines[d['discipline']]>=1:continue
        d=dict(d);d['sources']={side:{name:dict(path=str(Path(d[side])/'02_work'/name),sha256=sha(Path(d[side])/'02_work'/name)) for name in ['document.pdf','document.md','result.json']} for side in ['old','new']}
        selected.append(d);disciplines[d['discipline']]+=1
        if len(selected)==6:break
    immutable(target,dict(locked_at=now(),seed=seed,selected=selected,excluded_spent_documents=sorted(spent),
                         candidate_manifest_sha256=sha(ROOT/'02_FROZEN_CANDIDATES/end_to_end_v2/MANIFEST.json'),
                         selection_basis='Prior metadata-only eligible universe; new seed; no predictions/labels',independent_projects=0))
    print('Locked new slice',[(p['discipline'],p['document']) for p in selected])


def recover():
    for p,h in read(ROOT/'02_FROZEN_CANDIDATES/end_to_end_v2/MANIFEST.json')['code'].items():
        if sha(p)!=h:raise ValueError('Frozen code drift')
    recover_pairs(read(BASE/'MANIFEST.json'),BASE)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['dev','freeze','select','recover']);globals()[p.parse_args().action]()
