"""Replay exact real requests/responses through the general release phase; no inference."""
import asyncio
import hashlib
import json
from pathlib import Path
from .run_km_final_check_pair import BASE,CANDIDATES
from .run_km_citation_completion import ROOT as COMPLETION
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_phase

ROOT=Path('/tmp/auditmanager_km_automatic_citation_release_20261111')

async def main():
    entry=json.loads(CANDIDATES['version_b'].read_text())['entry']
    baseline=json.loads((BASE/'snapshots/KM/findings_verify.json').read_text())
    original=next(p['finding_snapshot'] for p in entry['queue']['packets'] if p['finding_id']=='F-005')
    document={**baseline,'findings':[original if f['id']=='F-005' else f for f in baseline['findings']]}
    source=BASE/'inputs/KM/versions/v001/02_work/document.md'
    saved=[Path('/tmp/auditmanager_km_final_check_pair_20261111/version_b/workflow'),COMPLETION,COMPLETION/'final_check']
    answers={evidence_digest(json.loads((p/'request.json').read_text())):json.loads((p/'result.json').read_text()) for p in saved}
    assert len(answers)==3
    calls=[]
    async def replay(request,phase):
        digest=evidence_digest(request)
        if digest not in answers:raise ValueError('Request differs from real recorded model input: '+digest)
        calls.append({'phase':phase,'request_sha256':digest});return answers[digest]
    ROOT.mkdir(exist_ok=True)
    report_path=ROOT/'03_findings.json'
    if not report_path.exists():report_path.write_text(json.dumps(document,ensure_ascii=False,indent=2)+'\n')
    candidates=ROOT/'03_source_review_candidates.json'
    if not candidates.exists():candidates.write_text(json.dumps({'reports':[{'status':'candidate_ready','entry':entry}]},ensure_ascii=False,indent=2)+'\n')
    first=await run_final_release_phase(ROOT,source,invoke=replay,executor_id='exact-recorded-astra-control',max_findings=1,complete_citations=True)
    assert first['applied']+first['already_applied']==1 and not first['not_completed']
    sealed=report_path.read_bytes()
    second=await run_final_release_phase(ROOT,source,invoke=replay,executor_id='exact-recorded-astra-control',max_findings=1,complete_citations=True)
    assert second['already_applied']==1 and report_path.read_bytes()==sealed
    output=json.loads(sealed);expected=json.loads((COMPLETION/'output_preview.json').read_text())
    assert output['findings']==expected['findings']
    assert [f for f in output['findings'] if f['id']!='F-005']==[f for f in baseline['findings'] if f['id']!='F-005']
    summary={'status':'general_release_phase_matches_real_control','source_md_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'new_model_calls':0,'recorded_request_payloads_matched_exactly':len(answers),'calls_this_replay':calls,
        'whole_findings_equal_to_independently_checked_output':True,'other_classic_findings_preserved':25,'final_rows':26,
        'rerun_idempotent':True,'first_run':first,'second_run':second,
        'output_sha256':hashlib.sha256(sealed).hexdigest(),'full_project_acceptance':False,
        'boundary':'Known target snapshot adapter and replay of exact recorded requests. This proves general orchestration on this control, not new blind discovery or evaluator routing agreement.'}
    path=Path(__file__).parent/'KM_automatic_citation_release_replay.json'
    if not path.exists():path.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in summary.items() if k not in ('first_run','second_run')},ensure_ascii=False))

if __name__=='__main__':asyncio.run(main())
