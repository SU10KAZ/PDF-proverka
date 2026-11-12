"""Resolve two known fragment-context requests locally; no model calls or admission."""
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_context import expand_source_review_context
from .run_itp_pipe_operations import immutable

BASE = Path(__file__).resolve().parent
SOURCE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/ITP/versions/v003/02_work/document.md')
ROOT = Path('/tmp/auditmanager_itp_requirement_context_recovery_20261112')


def main():
    md = SOURCE.read_text()
    original_path = BASE/'ITP_requirement_claude_result.json'
    raw = json.loads(original_path.read_text())['json_data']
    cases = []
    for fid, queries in [('F-02',['ТЕХНИЧЕСКОЕ ЗАДАНИЕ']),('F-03',['5.5','5.4.1.9'])]:
        original = next(f for f in raw['findings'] if f['id'] == fid)
        item = {'id':fid, 'problem':original['title'], 'description':original['statement'],
                'source_evidence_review_status':'pending','evidence':original['evidence']}
        queue = build_source_review_queue([item], md)
        expanded = expand_source_review_context(queue, fid, md, queries)
        packet = expanded['packets'][0]
        sources = {s['block_id']:s['authoritative_text'] for s in packet['sources']}
        if fid == 'F-02':
            title = sources['blk_35ff8683b1cf4be2abdef61740ddda2e']
            assert 'ТЕХНИЧЕСКОЕ ЗАДАНИЕ' in title and '15-09/25-РД' in title and '15.09.2025' in title
            assert 'Рабочая документация' in title and '77:07:0013002:4706' in title
            disposition = 'Title, contract reference, RD stage and site identity retrieved; current governing revision, approval and exact linkage remain unverified. Do not claim the full document has no title or project identity.'
            assertions = {'title_retrieved':True,'contract_reference_retrieved':True,'site_identity_retrieved':True,'governing_revision_verified':False}
        else:
            assert '5.5.1' in sources['blk_fba6f821ad1d450ebfbc5fc330bbc2b2']
            assert '5.5.3' in sources['blk_fba6f821ad1d450ebfbc5fc330bbc2b2']
            assert '5.5.4' in sources['blk_f8a0fda6c9be4e8287fa10e1870e1e77']
            assert '5.6.' in sources['blk_f8a0fda6c9be4e8287fa10e1870e1e77']
            disposition = 'Section 5.5 continuation retrieved through the start of 5.6. Re-review against existing source instead of asking an engineer to provide it. This does not map coupling rows to systems or establish a technical defect.'
            assertions = {'heating_section_continuation_retrieved':True,'next_section_boundary_retrieved':True,'coupling_requirement_applicability_verified':False}
        directory = ROOT/fid
        directory.mkdir(parents=True,exist_ok=True)
        immutable(directory/'original_model_finding.json',original)
        immutable(directory/'expanded_queue.json',expanded)
        cases.append({'finding_id':fid,'query_origin':'Local query derived from the original model request; not demonstrated autonomous model query selection.',
            'queries':queries,'receipt':packet['context_search_receipt'],
            'assertions':assertions,'disposition':disposition,
            'artifacts':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [directory/'original_model_finding.json',directory/'expanded_queue.json']}})
    result = {'status':'known_fragment_context_recovered_locally','cases':cases,'new_model_calls':0,
        'source':{'path':str(SOURCE),'sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest()},
        'original_model_result_sha256':hashlib.sha256(original_path.read_bytes()).hexdigest(),
        'stage4_control_closed':False,'production_acceptance':False,'evaluator_modified':False,
        'remaining':['Semantic re-review with recovered context; original model questions remain immutable.','Match actual coupling rows to applicable systems/materials and diameter convention.','819 remains a quantity sum, not a proven violation count.'],
        'method':'Before emitting a question for missing requirements, search the authoritative whole-document TEXT for the named heading and its continuation. Preserve remaining applicability uncertainty.'}
    (BASE/'ITP_requirement_context_recovery.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'status':result['status'],'cases':[{k:c[k] for k in ['finding_id','assertions']} for c in cases]},ensure_ascii=False))

if __name__ == '__main__':main()
