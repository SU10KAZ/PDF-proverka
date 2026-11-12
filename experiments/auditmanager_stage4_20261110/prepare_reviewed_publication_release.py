"""Prepare a final-check request from a completed, source-bound review.

No provider call and no mutation of the sealed source report. This adapter keeps
the original visible fields exact while adding the review queue metadata.
"""
import argparse
from copy import deepcopy
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check
from .run_primary_text_runtime_probe import sha, write


def prepare(review_root, destination, candidate_path=None):
    manifest = json.loads((review_root / 'manifest.json').read_text())
    report = Path(manifest['report_path'])
    source = Path(manifest['md_path'])
    assert sha(report) == manifest['report_sha256']
    assert sha(source) == manifest['md_sha256']
    candidate_path = candidate_path or review_root / 'workflow/candidate.json'
    candidate = json.loads(candidate_path.read_text())
    receipts = candidate.get('receipts', [])
    ready_pair = (candidate_path != review_root / 'workflow/candidate.json'
                  and len(receipts) == 1 and receipts[0].get('status') == 'applied')
    if candidate.get('status') != 'candidate_ready' and not ready_pair:
        raise ValueError('source_review_not_ready')
    entry = candidate['entry']
    finding = json.loads((review_root / 'finding.json').read_text())
    assert entry['queue']['packets'][0]['finding_snapshot'] == finding
    sealed = json.loads(report.read_text())
    originals = [f for f in sealed['findings'] if f['id'] == finding['id']]
    assert len(originals) == 1
    # Only review metadata was added by the source-review preparation.
    assert {k: v for k, v in finding.items() if k not in (
        'source_evidence_review_status', 'source_review_requests')} == {
        k: v for k, v in originals[0].items() if k not in (
            'source_evidence_review_status', 'source_review_requests')}
    document = deepcopy(sealed)
    document['findings'] = [deepcopy(finding) if f['id'] == finding['id'] else f
                            for f in document['findings']]
    md = source.read_text()
    preview, receipts = apply_review_admissions(document, [entry], md)
    assert len(receipts) == 1 and receipts[0]['status'] == 'applied'
    untouched = [f for f in sealed['findings'] if f['id'] != finding['id']]
    assert [f for f in preview['findings'] if f['id'] != finding['id']] == untouched
    request = prepare_final_check(entry, document, md)['request']
    destination.mkdir(parents=True, exist_ok=False)
    for name, value in [('entry', entry), ('input_document', document),
                        ('local_preview', preview), ('request', request)]:
        write(destination / (name + '.json'), value)
    result = {'finding_id': finding['id'], 'source_report_path': str(report),
              'source_report_sha256': sha(report), 'source_md_path': str(source),
              'source_md_sha256': sha(source), 'entry_sha256': sha(destination / 'entry.json'),
              'request_sha256': sha(destination / 'request.json'),
              'source_candidate_path': str(candidate_path), 'source_candidate_sha256': sha(candidate_path),
              'other_findings_unchanged': len(untouched), 'local_admission': receipts[0],
              'message_chars': sum(len(m['content']) for m in request['messages']),
              'image_count': len(request['image_paths']),
              'final_check_completed': False, 'production_acceptance': False}
    write(destination / 'preparation.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('review_root', type=Path)
    parser.add_argument('destination', type=Path)
    parser.add_argument('--candidate', type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.review_root.resolve(), args.destination.resolve(),
                            args.candidate.resolve() if args.candidate else None), ensure_ascii=False))
