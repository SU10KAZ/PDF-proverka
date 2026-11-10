"""Validate actual pilot receipts locally; semantic acceptance remains manual."""
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.findings_review.deterministic_critic import build_index
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews, build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_context import expand_source_review_context
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review
from experiments.auditmanager_stage4_20261110.run_itp_graphic_review_pilot import ROOT, MD


def main():
    request = json.loads((ROOT/'request.json').read_text())
    result = json.loads((ROOT/'result.json').read_text())
    attempt = json.loads((ROOT/'attempt.json').read_text())
    if result['is_error'] or attempt['status'] != 'finished':
        raise ValueError('No successful completed response')
    run = MD.parents[1]/'03_analysis/runs/control'
    md = MD.read_text()
    finding = next(f for f in json.loads((run/'03_findings.json').read_text())['findings'] if f['id'] == 'F-042')
    index = build_index(json.loads((run/'01_blocks_analysis.json').read_text()), json.loads((run/'document_graph.json').read_text()))
    known = index.image_block_ids | index.text_block_ids
    text_candidate_lineage([finding], json.loads((run/'02_text_analysis.json').read_text()), known)
    attach_pending_source_reviews([finding], known)
    queue = build_source_review_queue([finding], md)
    queue = expand_source_review_context(queue, 'F-042', md, ['K22', 'К22', 'K23', 'К23', 'RM 1500', 'RM 200'])
    attach_graphic_receipts(queue, md, run/'blocks_stage02_100')
    reconstructed = prepare_source_review_request(queue, finding, md)
    if reconstructed != request or request['request_sha256'] != attempt['request_sha256']:
        raise ValueError('Executed packet differs from reconstructed source packet')
    for receipt in queue['packets'][0]['graphic_attachments']:
        receipt['attached_to_model'] = True
    validation = validate_source_review(result['json_data'], queue, [finding], md)
    validation['transport_boundary'] = 'All six paths passed to Codex CLI --image; this does not prove visual attention to each image.'
    validation['result_file_sha256'] = hashlib.sha256((ROOT/'result.json').read_bytes()).hexdigest()
    target = ROOT/'validation.json'
    target.write_text(json.dumps(validation, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({k:v for k,v in validation.items() if k not in ('raw_response','claims')}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
