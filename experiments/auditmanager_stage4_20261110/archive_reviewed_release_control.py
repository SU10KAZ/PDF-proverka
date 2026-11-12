"""Verify and archive one actual source/final-check release, including real Excel.

Does not certify engineering truth or synthesize publication part mappings.
"""
import argparse
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_final_check import validate_final_check
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_primary_text_runtime_probe import sha, write


def archive(root, destination):
    manifest = json.loads((root / 'preparation.json').read_text())
    for kind in ('source_report', 'source_md'):
        assert sha(Path(manifest[kind + '_path'])) == manifest[kind + '_sha256']
    assert sha(root / 'entry.json') == manifest['entry_sha256']
    assert sha(root / 'request.json') == manifest['request_sha256']
    assert sha(Path(manifest['source_candidate_path'])) == manifest['source_candidate_sha256']
    entry = json.loads((root / 'entry.json').read_text())
    original = json.loads((root / 'input_document.json').read_text())
    sealed = json.loads(Path(manifest['source_report_path']).read_text())
    md = Path(manifest['source_md_path']).read_text()
    result = json.loads((root / 'workflow/result.json').read_text())
    assert validate_final_check(entry, original, md, result['json_data'])['status'] == 'release_candidate'
    receipt = json.loads((root / 'receipt.json').read_text())
    assert receipt['status'] == 'applied' and receipt['result_sha256'] == evidence_digest(result)
    output = json.loads((root / 'output_preview.json').read_text())
    fid = manifest['finding_id']
    finding, = [f for f in output['findings'] if f['id'] == fid]
    assert evidence_digest(finding) == receipt['applied_finding_sha256']
    assert len(output['findings']) == len(sealed['findings'])
    assert [f for f in output['findings'] if f['id'] != fid] == [f for f in sealed['findings'] if f['id'] != fid]
    destination.mkdir(parents=True, exist_ok=False)
    write(destination / '03_findings.json', output)
    wb = Workbook()
    build_project_sheet(wb, {'project_id': 'AK', 'sheet_name': 'AK', 'findings_json': output})
    wb.save(destination / 'report.xlsx'); wb.close()
    wb = load_workbook(destination / 'report.xlsx', read_only=True)
    rows = list(wb['AK'].values)
    columns = {key: i for i, (key, *_) in enumerate(PROJ_COLUMNS)}
    ids = {f['id'] for f in output['findings']}
    exported = [r[columns['finding_id']] for r in rows if r[columns['finding_id']] in ids]
    assert len(ids) == len(exported) == len(set(exported)) == len(output['findings'])
    row, = [r for r in rows if r[columns['finding_id']] == fid]
    assert finding['description'] in row[columns['description']]
    wb.close()
    for name in ('entry.json', 'preparation.json', 'receipt.json'):
        (destination / name).write_bytes((root / name).read_bytes())
    for label, source in [('final_result', root / 'workflow/result.json'),
                          ('final_request', root / 'workflow/request.json'),
                          ('source_candidate', Path(manifest['source_candidate_path']))]:
        (destination / (label + '.json')).write_bytes(source.read_bytes())
    assessment = {'status': 'source_and_final_check_release_export_verified_bounded_control',
        'finding_id': fid, 'other_findings_unchanged': len(output['findings']) - 1,
        'exported_findings': len(exported), 'model': 'codex/gpt-6-astra',
        'known_dev_control_not_blind': True,
        'limits': ['Actual release and Excel are verified, not engineering acceptance.',
                   'No claim that every semantic subclaim of the whole report is accounted for.',
                   'No autonomous publication-part mapping or whole-project improvement claimed.'],
        'files': {p.name: sha(p) for p in sorted(destination.iterdir()) if p.is_file()},
        'stage4_complete': False, 'production_acceptance': False}
    write(destination / 'assessment.json', assessment)
    print(json.dumps({k: assessment[k] for k in ('status', 'finding_id', 'other_findings_unchanged', 'exported_findings')}))


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('root', type=Path); p.add_argument('destination', type=Path)
    a = p.parse_args(); archive(a.root, a.destination)
