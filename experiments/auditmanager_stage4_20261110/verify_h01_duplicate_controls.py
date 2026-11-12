"""Replay completed H01 requests through actual merge/link reconciliation."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage
from backend.app.pipeline.stages.findings_merge.candidate_decisions import reconcile_candidate_decisions, effective_candidate_decisions
from experiments.auditmanager_stage4_20261110.run_h01_duplicate_controls import ROOT, TARGETS


def verify():
    results = []
    for disc, ids in TARGETS.items():
        folder = ROOT / disc
        base = json.loads((folder / '03_findings.json').read_text())
        original = deepcopy(base)
        text = json.loads((folder / '02_text_analysis.json').read_text())
        blocks = json.loads((folder / '01_blocks_analysis.json').read_text())
        combined = base
        for sid in sorted(ids):
            sub = folder / sid
            if not (sub / 'validated.json').exists():
                results.append({'discipline': disc, 'source_id': sid, 'status': 'awaiting_response'})
                continue
            req = json.loads((sub / 'request.json').read_text())
            packet = json.loads(req['messages'][-1]['content'])
            payload = json.loads((sub / 'validated.json').read_text())
            prior = deepcopy(combined)
            combined = combine_findings_with_targeted(combined, [('candidate_recovery_' + sid, payload)])
            assert base == original
            assert len(combined['findings']) == len(prior['findings'])
            allowed = {'source_finding_ids', 'recovery_link_receipts'}
            for before, after in zip(prior['findings'], combined['findings']):
                assert {k:v for k,v in before.items() if k not in allowed} == {k:v for k,v in after.items() if k not in allowed}
            coverage = build_candidate_coverage(text, blocks, combined)
            decisions = effective_candidate_decisions(combined, coverage)
            rec = reconcile_candidate_decisions(coverage, decisions, set(packet['allowed_source_refs']))
            row, = [x for x in rec['decisions'] if x['origin']==packet['candidate']['origin'] and x['pointer']==packet['candidate']['pointer']]
            links = [f['id'] for f in combined['findings'] if sid in f.get('source_finding_ids', [])]
            results.append({'discipline':disc,'source_id':sid,'status':row['status'],
                'reason':row.get('reason'),'target_ids':links,'new_findings':len(payload['findings']),
                'original_finding_texts_preserved':len(prior['findings']),
                'recovery_pending':payload.get('recovery_pending', []),
                'model_decision':payload.get('candidate_decisions'),
                'source_result_sha256':hashlib.sha256((sub/'result.json').read_bytes()).hexdigest(),
                'semantic_verified':False})
        (folder / 'duplicate_link_replay.json').write_text(json.dumps(combined,ensure_ascii=False,indent=2)+'\n')
    report={'controls':results,'all_responses_present':all(x['status']!='awaiting_response' for x in results),
        'scope':'Actual provenance merge and preservation replay; not certification of the original engineering claims or new independent detection.',
        'stage4_complete':False,'production_accepted':False}
    (ROOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps([{k:v for k,v in x.items() if k not in ['model_decision','recovery_pending']} for x in results],ensure_ascii=False,indent=2))


if __name__=='__main__':verify()
