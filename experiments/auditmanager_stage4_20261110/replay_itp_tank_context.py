"""Known-case local context retrieval, queries from a prior visual review."""
from copy import deepcopy
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.findings_review.deterministic_critic import build_index
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews, build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_context import expand_source_review_context
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts


def main():
    source = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/ITP/versions/v003')
    out = source/'03_analysis/runs/control'
    md = (source/'02_work/document.md').read_text()
    finding = next(f for f in json.loads((out/'03_findings.json').read_text())['findings'] if f['id'] == 'F-042')
    before = deepcopy(finding)
    index = build_index(json.loads((out/'01_blocks_analysis.json').read_text()), json.loads((out/'document_graph.json').read_text()))
    known = index.image_block_ids | index.text_block_ids
    text_candidate_lineage([finding], json.loads((out/'02_text_analysis.json').read_text()), known)
    attach_pending_source_reviews([finding], known)
    queue = build_source_review_queue([finding], md)
    queries = ['K22', 'К22', 'K23', 'К23', 'RM 1500', 'RM 200']
    expanded = expand_source_review_context(queue, 'F-042', md, queries)
    attach_graphic_receipts(expanded, md, out/'blocks_stage02_100')
    packet = expanded['packets'][0]
    expected = {'blk_11981549aceb463ba84c7c0de619d2fc', 'blk_5c7cb0c7b68c4f78ac019a2e7535195f',
                'blk_72bd74f01146432880a289b071f70238', 'blk_7e913f6c32cb4db7aa9d9b8688b6e979'}
    assert {s['block_id'] for s in packet['sources']} == expected
    assert packet['graphic_context_status'] == 'ready_for_attachment'
    assert len(packet['graphic_attachments']) == 6
    for key in ('problem', 'description', 'solution', 'severity'):
        assert finding[key] == before[key]
    report = {'finding_id': 'F-042', 'source_md_sha256': expanded['source_md_sha256'],
              'context_sha256': packet['context_sha256'], 'queries': queries,
              'query_origin': 'Manual known-case graphic review ITP_F042_graphic_text_control.json; not independent model recovery.',
              'search_receipt': packet['context_search_receipt'],
              'text_block_ids': [s['block_id'] for s in packet['sources']],
              'graphic_receipts': packet['graphic_attachments'], 'tests_passed': 28,
              'finding_core_unchanged': True, 'new_model_calls': 0, 'automatic_dispatch': False,
              'boundary': 'Retrieval and attachment preparation only. Actual model requests for context and response quality remain untested.'}
    Path(__file__).with_name('ITP_tank_context_expansion.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print('Recovered 4 authoritative text blocks; 6 graphic files ready; no model calls')


if __name__ == '__main__':
    main()
