"""Exercise recovered provenance through verifier; expose unfinished semantic work."""
import copy
import json
import tempfile
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.prepare.graph_builder import build_block_page_index


if __name__ == '__main__':
    source = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/EOM/versions/v002/03_analysis/runs/control')
    original = json.loads((source/'03_findings.json').read_text())
    candidate = copy.deepcopy(original)
    graph = json.loads((source/'document_graph.json').read_text())
    text_candidate_lineage(candidate['findings'],json.loads((source/'02_text_analysis.json').read_text()),set(build_block_page_index(graph)))
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory)
        (output/'02_work').mkdir()
        (output/'02_work'/'document.md').write_bytes((source.parents[2]/'02_work'/'document.md').read_bytes())
        for name in ('document_graph.json','01_blocks_analysis.json','02_text_analysis.json'):
            (output/name).write_bytes((source/name).read_bytes())
        (output/'03_findings.json').write_text(json.dumps(candidate,ensure_ascii=False))
        stats = _run_deterministic_phase(output,'EOM-source-review-replay')
        assert not stats['critic_error'] and not stats['corrector_error']
        assert not stats['source_review_queue_error']
        queue = json.loads((output/'03_source_review_queue.json').read_text())
        after = json.loads((output/'03_findings.json').read_text())
    before = {f['id']:f for f in original['findings']}
    assert set(before) == {f['id'] for f in after['findings']}
    for finding in after['findings']:
        for field in ('problem','description','solution','severity'):
            assert finding.get(field) == before[finding['id']].get(field)
    pending = [{'finding_id':f['id'],'claim':f.get('problem'),'requests':f['source_review_requests']}
               for f in after['findings'] if f.get('source_evidence_review_status') == 'pending']
    assert queue['pending_count'] == len(pending)
    assert {p['finding_id'] for p in queue['packets']} == {p['finding_id'] for p in pending}
    assert all(p['finding_snapshot'] == next(f for f in after['findings'] if f['id'] == p['finding_id']) for p in queue['packets'])
    Path(__file__).with_name('EOM_source_review_queue.json').write_text(json.dumps(queue,ensure_ascii=False,indent=2)+'\n')
    result = {'scope':'Actual EOM saved findings with recovered text-candidate provenance through deterministic verifier',
              'stats':stats,'pending_count':len(pending),'pending_requests':pending,
              'claims_and_severity_unchanged':True,'semantic_review_completed':False,
              'automatic_dispatch_implemented':False,
              'gap':'The verifier now prepares source-bounded review packets. Semantic review dispatch and validated decisions remain unimplemented.',
              'required_next_behavior':'Review packets and record supported/refuted/needs_data separately. Validate source relevance before any release or removal.',
              'known_negative_control':'F-040 is source-refuted by EOM_F040_semantic_refutation.json; do not treat its pending status as support.',
              'new_model_calls':0}
    Path(__file__).with_name('EOM_pending_source_review.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print('Pending semantic reviews:',len(pending),'claims preserved:',len(before))
