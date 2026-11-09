"""Prepare a local source-only ITP pilot; --execute explicitly runs Codex CLI Astra."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import re

ROOT=Path('/tmp/auditmanager_requirement_scope_astra_20261110')
SOURCE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/ITP/versions/v003/02_work/document.md')

def prepare():
    ROOT.mkdir(exist_ok=True)
    md=SOURCE.read_text()
    headers=list(re.finditer(r'^##[ \t]+Page[ \t]+(\d+)\b[^\n]*$',md,re.M))
    pages=[(int(h.group(1)),md[h.start():headers[i+1].start() if i+1<len(headers) else len(md)]) for i,h in enumerate(headers)]
    selected={i for i,(_,text) in enumerate(pages) if any(q in text for q in ('5.3.1.4','FC033E','FC042E','FC048E','FC060E'))}
    expanded={j for i in selected for j in range(max(0,i-1),min(len(pages),i+2))}|{0}
    fragment='\n'.join(pages[i][1] for i in sorted(expanded))
    contract=Path('prompts/pipeline/en/text_analysis_task.md').read_text().split('## Requirement applicability records',1)[1]
    instructions='''Review supplied requirements and specified components. Determine permitted
comparisons, their exact scope and remaining uncertainty. MD TEXT is authoritative;
no images or external documents are supplied. GRAPHIC summaries are not proof.
Do not assume nominal/outside diameter or system ownership from a catalogue code.
Return JSON with object_registry (id, identity, scope, kind, source_refs),
requirement_registry, requirement_applicability_checks, and findings with evidence
and report_section technical_findings/questions. No historical answers supplied.
'''+contract
    request={'messages':[{'role':'system','content':instructions},{'role':'user','content':json.dumps({
        'scope':'Known ITP control fragments, not blind project audit.',
        'selected_pdf_pages':[pages[i][0] for i in sorted(expanded)],'authoritative_md':fragment},ensure_ascii=False)}],
        'model':'codex/gpt-6-astra','source_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest()}
    (ROOT/'request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2))
    print('PREPARED pages=',[pages[i][0] for i in sorted(expanded)],'chars=',len(fragment),flush=True)
    return request,fragment

async def execute(request,fragment):
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks,reconcile_requirement_checks
    target=ROOT/'result.json'
    assert not target.exists(), 'Review existing result before another call'
    assert substitute_codex_model('gpt-6-astra')=='gpt-6-astra'
    print('START Codex CLI Astra applicability pilot',flush=True)
    result=await run_codex_json_messages(request['messages'],model='codex/gpt-6-astra',timeout=900,
        stage='requirement_scope_pilot',project_id='ITP-known-fragments')
    receipt=asdict(result)
    if not result.is_error and isinstance(result.json_data,dict):
        receipt['applicability_receipt']=reconcile_requirement_checks(result.json_data,authoritative_text_blocks(fragment))
    target.write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
    print('DONE is_error=',result.is_error,'duration_ms=',result.duration_ms,flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true')
    args=parser.parse_args();request,fragment=prepare()
    if args.execute:asyncio.run(execute(request,fragment))
