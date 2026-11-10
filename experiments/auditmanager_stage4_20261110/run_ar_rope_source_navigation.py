"""Model selects uninspected source graphics from a literal frozen-MD catalogue.

A new navigation strategy after repeated literal queries; no evaluator answers.
"""
import argparse
import asyncio
from collections import Counter
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
from .run_ar_roof_operations import BASE,immutable
from .frozen_operation_request import load_frozen_operation
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_ar_rope_source_navigation_20261111')
PREVIOUS=Path('/tmp/auditmanager_ar_rope_operations_20261111/O27/context_1')


def prepare():
    ROOT.mkdir(exist_ok=True)
    _,request,binding=load_frozen_operation(PREVIOUS,BASE/'02_work/document.md')
    packet=json.loads(request.messages[-1]['content'])
    result=json.loads((PREVIOUS/'result.json').read_text());assert not result['is_error']
    md=(BASE/'02_work/document.md').read_text()
    hs=list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$',md,re.M|re.I));counts=Counter(h[2] for h in hs)
    rows=[]
    for i,h in enumerate(hs):
        if h[1].upper() not in ('IMAGE','GRAPHIC') or counts[h[2]]!=1:continue
        content=md[h.end():hs[i+1].start() if i+1<len(hs) else len(md)]
        content=re.split(r'^##[ \t]+(?:Page|СТРАНИЦА)\b',content,maxsplit=1,flags=re.M|re.I)[0]
        stamp=re.search(r'^> \*\*Stamp:\*\*.*$',content,re.M)
        summary=re.search(r'^\*\*Summary:\*\*.*$',content,re.M)
        rows.append({'block_id':h[2],'stamp':stamp[0] if stamp else None,'navigation_summary':summary[0] if summary else None,
                     'already_attached':h[2] in packet['attached_image_ids_in_order']})
    from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks
    supplied = set(packet['allowed_source_refs'])
    for bid, text in authoritative_text_blocks(md).items():
        rows.append({'block_id':bid, 'authoritative_text':text, 'already_attached':bid in supplied})
    instruction='''Select source blocks to inspect next for this unresolved object operation.
The previous literal-query search found no additional matches; that is not exhaustive document review.
Use only this source catalogue and the previous unresolved operation. Do not access files or tools.
Catalogue summaries are generated navigation hints, NEVER technical evidence. Stamps are literal prepared MD metadata.
Authoritative text entries contain literal prepared TEXT. Read general notes and document references as well as object-specific blocks.
Select at most 8 not-yet-attached block IDs whose text or drawings could resolve the named missing data.
Explain expected relevance and scope/applicability checks for each. Do not assume a similarly named mark or typical detail applies.
Do not produce findings, calculations, expected dimensions, inferred physical defects or historical answers.
Return JSON: selected_sources:[{block_id, reason, applicability_to_check}], selection_reason, remaining_search_limits.
An empty selection is allowed with a reason. Do not equate it with project-wide absence.'''
    wire={'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps({'object':packet['object'],'unresolved_operation_response':result['json_data'],'source_catalogue':rows},ensure_ascii=False)}],'image_paths':[]}
    manifest={'request_sha256':evidence_digest(wire),'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),'previous_result_sha256':evidence_digest(result),'catalogue_blocks':len(rows),'previously_unsupplied_blocks':sum(not r['already_attached'] for r in rows),'history_or_engineer_answers_supplied':False,'model':'codex/gpt-6-astra','automatic_retries':0,'message_chars':sum(len(m['content']) for m in wire['messages'])}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',manifest)
    print('PREPARED',json.dumps(manifest,ensure_ascii=False),flush=True)
    return wire,rows


async def main(execute):
    wire,rows=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():r=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        r=asdict(await run_codex_json_messages(wire['messages'],model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='source_navigation',project_id='AR-source-navigation',allowed_tools=''))
        immutable(ROOT/'result.json',r);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':r['is_error']}))
    assert not r['is_error'];prepare();data=r['json_data'];selected=data['selected_sources'];assert len(selected)<=8
    ids=[s['block_id'] for s in selected];assert len(ids)==len(set(ids));assert set(ids)<={r['block_id'] for r in rows if not r['already_attached']}
    assert all(isinstance(s.get(k),str) and s[k].strip() for s in selected for k in ('reason','applicability_to_check'))
    immutable(ROOT/'selection_validation.json',{'selected_ids':ids,'navigation_only':True,'applicability_verified':False,'semantic_acceptance':False})
    print('COMPLETE',len(ids),'source selections',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
