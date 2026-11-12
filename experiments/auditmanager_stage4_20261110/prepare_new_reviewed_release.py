"""Prepare final review for a new source-reviewed candidate beside existing rows.

This does not perform semantic grouping or change the baseline report.
"""
import argparse
from copy import deepcopy
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check
from .run_primary_text_runtime_probe import sha, write


def prepare(review_root, baseline, md_path, destination, project_id):
    candidate_path = review_root/'workflow/candidate.json'
    candidate = json.loads(candidate_path.read_text())
    assert candidate['status'] == 'candidate_ready'
    entry = candidate['entry']
    finding = json.loads((review_root/'finding.json').read_text())
    assert entry['queue']['source_md_sha256'] == sha(md_path)
    assert entry['queue']['packets'][0]['finding_snapshot'] == finding
    base = json.loads(baseline.read_text())
    assert all(f['id'] != finding['id'] for f in base['findings'])
    document = deepcopy(base)
    document['findings'].append(finding)
    md = md_path.read_text()
    preview, receipts = apply_review_admissions(document, [entry], md)
    assert len(receipts) == 1 and receipts[0]['status'] == 'applied', receipts
    assert preview['findings'][:-1] == base['findings']
    request = prepare_final_check(entry, document, md)['request']
    destination.mkdir(parents=True, exist_ok=False)
    for name, data in [('entry', entry), ('input_document', document),
                       ('local_preview', preview), ('request', request)]:
        write(destination/(name+'.json'), data)
    manifest = {'project_id': project_id, 'finding_id': finding['id'],
                'source_report_path': str(baseline), 'source_report_sha256': sha(baseline),
                'source_md_path': str(md_path), 'source_md_sha256': sha(md_path),
                'source_candidate_path': str(candidate_path), 'source_candidate_sha256': sha(candidate_path),
                'entry_sha256': sha(destination/'entry.json'), 'request_sha256': sha(destination/'request.json'),
                'other_findings_unchanged': len(base['findings']),
                'message_chars': sum(len(m['content']) for m in request['messages']),
                'image_count': len(request['image_paths']),
                'semantic_grouping_completed': False, 'production_acceptance': False}
    write(destination/'preparation.json', manifest)
    return manifest


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('review_root', 'baseline', 'md_path', 'destination'):
        p.add_argument(name, type=Path)
    p.add_argument('--project-id', required=True)
    a = p.parse_args()
    print(json.dumps(prepare(a.review_root.resolve(), a.baseline.resolve(), a.md_path.resolve(),
                             a.destination.resolve(), a.project_id), ensure_ascii=False))
