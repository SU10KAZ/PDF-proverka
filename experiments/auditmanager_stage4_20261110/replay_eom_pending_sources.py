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
        for name in ('document_graph.json','01_blocks_analysis.json','02_text_analysis.json'):
            (output/name).write_bytes((source/name).read_bytes())
        (output/'03_findings.json').write_text(json.dumps(candidate,ensure_ascii=False))
        stats = _run_deterministic_phase(output,'EOM-source-review-replay')
        assert not stats['critic_error'] and not stats['corrector_error']
        after = json.loads((output/'03_findings.json').read_text())
    before = {f['id']:f for f in original['findings']}
    assert set(before) == {f['id'] for f in after['findings']}
    for finding in after['findings']:
        for field in ('problem','description','solution','severity'):
            assert finding.get(field) == before[finding['id']].get(field)
    pending = [{'finding_id':f['id'],'claim':f.get('problem'),'requests':f['source_review_requests']}
               for f in after['findings'] if f.get('source_evidence_review_status') == 'pending']
    result = {'scope':'Actual EOM saved findings with recovered text-candidate provenance through deterministic verifier',
              'stats':stats,'pending_count':len(pending),'pending_requests':pending,
              'claims_and_severity_unchanged':True,'semantic_review_completed':False,
              'automatic_dispatch_implemented':False,
              'gap':'The current corrector flags human review and stores source_review_requests. Recovery pass selection handles unlinked/deferred candidates, not these linked pending final claims.',
              'required_next_behavior':'Build source-bounded final-claim review packets from these requests, preserve original and merged claims, and record supported/refuted/needs_data separately. Validate source relevance before any release or removal.',
              'known_negative_control':'F-040 is source-refuted by EOM_F040_semantic_refutation.json; do not treat its pending status as support.',
              'new_model_calls':0}
    Path(__file__).with_name('EOM_pending_source_review.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print('Pending semantic reviews:',len(pending),'claims preserved:',len(before))
