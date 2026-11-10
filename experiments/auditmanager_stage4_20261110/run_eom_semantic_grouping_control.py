"""Source-only grouping proposal over all current EOM findings; no expected groups."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from .run_eom_cable_operations import immutable
ROOT=Path('/tmp/auditmanager_eom_semantic_grouping_control_20261111')
SOURCE=Path('/tmp/auditmanager_eom_reviewed_cable_output_20261111/03_findings.json')


def prepare():
    ROOT.mkdir(exist_ok=True)
    document=json.loads(SOURCE.read_text())
    # Findings are current-run output, not engineer labels or evaluation answers.
    fields=('id','problem','description','solution','severity','category','report_section','page','sheet','related_sheets','evidence')
    findings=[{k:f[k] for k in fields if k in f} for f in document['findings']]
    assert len({f['id'] for f in findings})==len(findings)
    prompt='''Propose presentation groups for this audit's findings. This task does not verify truth or remove findings.
Use only the supplied current findings. No tools, files, web or historical answers.
Group only manifestations of the same specific issue with a shared corrective action and compatible scope, severity and report section.
A repeated model, dimension, source page or generic issue category alone does not justify merging distinct issues.
Different physical addresses may belong to one shared document discrepancy only if each address and its individual evidence remains explicit.
Do not sum quantities into a deficit, resolve ambiguous identifiers, rewrite findings, downgrade severity or convert uncertainty to fact.
When uncertain keep separate. Return JSON with groups and keep_separate_ids.
Each group: member_ids (at least two), reason, shared_issue, common_corrective_action, source_refs, distinctions_to_preserve.
source_refs are existing block_id references supporting the proposed common context, not proof that the issue is correct.
Every supplied finding ID must occur exactly once, either in one group or in keep_separate_ids.
Do not invent IDs or a target row count. This is a proposal requiring independent review before application.'''
    wire={'messages':[{'role':'system','content':prompt},{'role':'user','content':json.dumps({'findings':findings},ensure_ascii=False)}],'image_paths':[]}
    binding={'source_document_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),'request_sha256':hashlib.sha256(json.dumps(wire,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),'model':'codex/gpt-6-astra','reasoning_effort':'high','message_chars':sum(len(m['content']) for m in wire['messages']),'findings':len(findings),'history_or_engineer_answers_supplied':False,'expected_groups_supplied':False,'automatic_retries':0,'scope':'Known-development output grouping proposal, not an independent project audit or semantic acceptance.'}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',binding)
    return wire,binding


async def main(execute):
    wire,binding=prepare();print('PREPARED',json.dumps(binding,ensure_ascii=False),flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    if (ROOT/'result.json').exists():result=json.loads((ROOT/'result.json').read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started',**binding},f)
        result=asdict(await run_codex_json_messages(wire['messages'],model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='findings_grouping',project_id='EOM-grouping-control',allowed_tools=''))
        immutable(ROOT/'result.json',result)
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];prepare()
    data=result['json_data']; supplied=json.loads(wire['messages'][1]['content'])['findings'];ids={f['id'] for f in supplied}
    assigned=list(data['keep_separate_ids'])
    refs={e['block_id'] for f in supplied for e in f.get('evidence',[]) if isinstance(e,dict) and isinstance(e.get('block_id'),str)}
    for g in data['groups']:
        assert len(g['member_ids'])>=2
        assert all(isinstance(g.get(k),str) and g[k].strip() for k in ('reason','shared_issue','common_corrective_action'))
        assert g['source_refs'] and set(g['source_refs'])<=refs
        assert g['distinctions_to_preserve']
        assigned.extend(g['member_ids'])
    assert len(assigned)==len(set(assigned)) and set(assigned)==ids
    immutable(ROOT/'structural_validation.json',{'all_findings_accounted_once':True,'groups':len(data['groups']),'unmerged':len(data['keep_separate_ids']),'semantic_acceptance':False,'original_document_mutated':False})
    print('COMPLETE',len(data['groups']),'groups proposed',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
