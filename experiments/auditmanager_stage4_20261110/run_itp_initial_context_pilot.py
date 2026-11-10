"""Prepare original candidate context to test model-requested source retrieval.

Queries and answers from prior reviews are deliberately excluded. Known case,
not a blind discovery experiment. Dispatch requires this payload's approval.
"""
import argparse
import asyncio
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.findings_review.deterministic_critic import build_index
from backend.app.pipeline.stages.findings_verify.source_review_queue import attach_pending_source_reviews, build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from experiments.auditmanager_stage4_20261110 import run_itp_graphic_review_pilot as runner

ROOT=Path('/tmp/auditmanager_itp_initial_context_astra_20261110')
PREPARED=Path('/tmp/auditmanager_itp_initial_context_prepared.json')


def prepare():
    if (ROOT/'attempt.json').exists():
        raise RuntimeError('Existing attempt; do not overwrite or restart')
    md=runner.MD.read_text(); run=runner.MD.parents[1]/'03_analysis/runs/control'
    finding=next(f for f in json.loads((run/'03_findings.json').read_text())['findings'] if f['id']=='F-042')
    index=build_index(json.loads((run/'01_blocks_analysis.json').read_text()),json.loads((run/'document_graph.json').read_text()))
    known=index.image_block_ids | index.text_block_ids
    text_candidate_lineage([finding],json.loads((run/'02_text_analysis.json').read_text()),known)
    attach_pending_source_reviews([finding],known)
    queue=build_source_review_queue([finding],md)
    attach_graphic_receipts(queue,md,run/'blocks_stage02_100')
    request=prepare_source_review_request(queue,finding,md)
    PREPARED.write_text(json.dumps(request,ensure_ascii=False,indent=2)+'\n')
    ROOT.mkdir(exist_ok=True)
    (ROOT/'queue.json').write_text(json.dumps(queue,ensure_ascii=False,indent=2)+'\n')
    (ROOT/'finding.json').write_text(json.dumps(finding,ensure_ascii=False,indent=2)+'\n')
    configure()
    _,manifest=runner.preflight()
    manifest['scope']='Known original-candidate context; evaluate whether model requests missing linked sources without manually supplied search queries. Not blind discovery.'
    Path(__file__).with_name('ITP_initial_context_pilot_preflight.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(manifest,ensure_ascii=False,indent=2))


def configure():
    runner.PREPARED=PREPARED
    runner.ROOT=ROOT
    runner.EXPECTED_IMAGES=4
    runner.SCOPE='Known original-candidate context; evaluate model-generated missing-source queries; not blind discovery.'


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',metavar='APPROVED_REQUEST_SHA256');args=parser.parse_args()
    if args.execute:
        configure();asyncio.run(runner.execute(args.execute))
    else:prepare()
