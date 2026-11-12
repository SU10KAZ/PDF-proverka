"""Verify an actual F071 release and bind publication parts to its Excel output."""
from copy import deepcopy
import argparse
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook

from backend.app.pipeline.stages.findings_merge.publication_reconciliation import fingerprint
from backend.app.pipeline.stages.findings_merge.publication_delivery import check_document_publication_delivery
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_final_check import validate_final_check
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_primary_text_runtime_probe import sha, write

ROOT = Path('/tmp/auditmanager_publication_f071_final_check_20261112')
SOURCE = Path('/tmp/auditmanager_publication_f071_source_review_20261112')
PROJECTION = Path('/tmp/auditmanager_publication_identity_scope_20261112_run/evaluations/publication_identity_071/publication_projection.json')
DEST = Path(__file__).parent / 'publication_f071_released_evidence'


def main(root=ROOT, destination=DEST):
    ROOT, DEST = root, destination
    manifest = json.loads((ROOT / 'preparation.json').read_text())
    for kind in ('source_report', 'source_md'):
        assert sha(Path(manifest[kind + '_path'])) == manifest[kind + '_sha256']
    entry = json.loads((ROOT / 'entry.json').read_text())
    original = json.loads((ROOT / 'input_document.json').read_text())
    md = Path(manifest['source_md_path']).read_text()
    result = json.loads((ROOT / 'workflow/result.json').read_text())
    assert validate_final_check(entry, original, md, result['json_data'])['status'] == 'release_candidate'
    receipt = json.loads((ROOT / 'receipt.json').read_text())
    assert receipt['status'] == 'applied' and receipt['result_sha256'] == evidence_digest(result)
    output = json.loads((ROOT / 'output_preview.json').read_text())
    finding, = [f for f in output['findings'] if f['id'] == 'F-071']
    assert evidence_digest(finding) == receipt['applied_finding_sha256']
    sealed = json.loads(Path(manifest['source_report_path']).read_text())
    assert len(output['findings']) == 85
    assert [f for f in output['findings'] if f['id'] != 'F-071'] == [f for f in sealed['findings'] if f['id'] != 'F-071']
    projection = json.loads(PROJECTION.read_text())
    assert projection['archive'] == next(f for f in sealed['findings'] if f['id'] == 'F-071')
    decisions = []
    for route in ('technical_findings', 'pending', 'history_only'):
        for part in projection[route]:
            state = {'technical_findings': 'emitted', 'pending': 'deferred', 'history_only': 'history'}[route]
            targets = [] if state != 'emitted' else [{
                'finding_id': 'F-071', 'finding_sha256': fingerprint(finding),
                'field_quotes': {k: finding[k] for k in ('problem', 'description', 'solution', 'risk')}}]
            decisions.append({'publication_id': part['publication_id'], 'part_sha256': fingerprint(part),
                'state': state, 'targets': targets, 'reviewer': 'coordinator_part_mapping_after_source_and_final_checks',
                'reason': 'Local M2 conflict retained; unresolved reserve role remains deferred; processing history is archived.'})
    document = deepcopy(output)
    document['publication_delivery_checks'] = [{'projection': projection,
        'projection_sha256': fingerprint(projection), 'decisions': decisions}]
    checked, = check_document_publication_delivery(document)
    assert len(checked['pending_parts']) == 1
    history = document['source_review_history'][-1]
    assert next(c for c in history['decision']['claim_reviews'] if c['claim_id'] == 'F-071-C3')['verdict'] == 'needs_data'
    DEST.mkdir(exist_ok=True)
    write(DEST / '03_findings.json', document)
    write(DEST / 'delivery_receipt.json', checked)
    wb = Workbook()
    build_project_sheet(wb, {'project_id': 'AK', 'sheet_name': 'AK', 'findings_json': document})
    wb.save(DEST / 'report.xlsx'); wb.close()
    wb = load_workbook(DEST / 'report.xlsx', read_only=True)
    rows = list(wb['AK'].values)
    columns = {key: i for i, (key, *_) in enumerate(PROJ_COLUMNS)}
    ids = {f['id'] for f in document['findings']}
    exported = [r[columns['finding_id']] for r in rows if r[columns['finding_id']] in ids]
    assert len(ids) == len(exported) == len(set(exported)) == 85
    row, = [r for r in rows if r[columns['finding_id']] == 'F-071']
    assert finding['description'] in row[columns['description']]
    wb.close()
    for name in ('entry.json', 'preparation.json', 'receipt.json'):
        (DEST / name).write_bytes((ROOT / name).read_bytes())
    for label, source in [('final_result', ROOT / 'workflow/result.json'),
                          ('final_request', ROOT / 'workflow/request.json'),
                          ('source_candidate', Path(manifest['source_candidate_path'])),
                          ('source_manifest', SOURCE / 'manifest.json')]:
        (DEST / (label + '.json')).write_bytes(source.read_bytes())
    assessment = {'status': 'source_review_final_check_and_delivery_export_passed_bounded_case',
        'finding_id': 'F-071', 'other_findings_unchanged': 84, 'exported_findings': 85,
        'pending_reserve_role_retained': True, 'f003_complementary_basis_preserved': True,
        'model': 'codex/gpt-6-astra', 'known_dev_control_not_blind': True,
        'limits': ['Part-to-final mapping is explicit coordinator review, not autonomous grouping.',
                   'One case does not establish whole-project improvement or engineering acceptance.'],
        'files': {p.name: sha(p) for p in sorted(DEST.iterdir()) if p.is_file() and p.name != 'assessment.json'},
        'stage4_complete': False, 'production_acceptance': False}
    write(DEST / 'assessment.json', assessment)
    print(assessment['status'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--destination', type=Path, default=DEST)
    args = parser.parse_args()
    main(args.root, args.destination)
