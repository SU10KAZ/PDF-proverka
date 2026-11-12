"""Verify and export a source-reviewed new finding without semantic regrouping."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import shutil
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check, validate_final_check
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_primary_text_runtime_probe import sha, write


def read(p):
    return json.loads(p.read_text())


def archive(root, destination):
    manifest = read(root/'preparation.json')
    for name in ('source_report', 'source_md', 'source_candidate'):
        assert sha(Path(manifest[name+'_path'])) == manifest[name+'_sha256']
    assert sha(root/'entry.json') == manifest['entry_sha256']
    assert sha(root/'request.json') == manifest['request_sha256']
    entry = read(root/'entry.json')
    document = read(root/'input_document.json')
    md = Path(manifest['source_md_path']).read_text()
    assert prepare_final_check(entry, document, md)['request'] == read(root/'request.json')
    checked = validate_final_check(entry, document, md, read(root/'workflow/result.json')['json_data'])
    assert checked['status'] == 'release_candidate', checked
    receipt = read(root/'receipt.json')
    assert receipt['status'] == 'applied'
    assert receipt['result_sha256'] == evidence_digest(read(root/'workflow/result.json'))
    assert receipt['request_sha256'] == evidence_digest(read(root/'request.json'))
    after = read(root/'output_preview.json')
    expected, admissions = apply_review_admissions(document, [entry], md)
    assert admissions[0]['status'] == 'applied'
    assert after['findings'] == expected['findings']
    base = read(Path(manifest['source_report_path']))
    assert after['findings'][:-1] == base['findings']
    item = deepcopy(after['findings'][-1])
    assert item['id'] == manifest['finding_id']
    item['source_review_original_finding_id'] = item['id']
    item['source_review_history'] = after['source_review_history']
    item['source_finding_ids'] = list(dict.fromkeys([*item.get('source_finding_ids', []), item['id']]))
    combined = combine_findings_with_targeted(base, [('source_review', {'findings': [item]})])
    assert combined['findings'][:-1] == base['findings']
    assert len(combined['findings']) == len(base['findings'])+1
    new = combined['findings'][-1]
    for key in ('problem', 'description', 'solution', 'severity', 'report_section'):
        assert new.get(key) == item.get(key)
    write(root/'03_findings.json', combined)
    wb = Workbook()
    project = manifest['project_id']
    build_project_sheet(wb, {'project_id': project, 'sheet_name': project, 'findings_json': combined})
    wb.save(root/'report.xlsx'); wb.close()
    wb = load_workbook(root/'report.xlsx')
    cols = {key: i for i, (key, *_) in enumerate(PROJ_COLUMNS)}
    ids = {f['id'] for f in combined['findings']}
    rows = [r for r in wb[project].values if r[cols['finding_id']] in ids]
    assert len(rows) == len({r[cols['finding_id']] for r in rows}) == len(ids)
    row = next(r for r in rows if r[cols['finding_id']] == new['id'])
    for key in ('problem', 'solution'):
        assert row[cols[key]] == new[key]
    assert new['description'] in row[cols['description']]
    wb.close()
    destination.mkdir(parents=True, exist_ok=False)
    for p in sorted(root.rglob('*')):
        if p.is_file() and p.suffix in ('.json', '.xlsx'):
            dest = destination/p.relative_to(root)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(p, dest)
    assessment = {'status': 'bounded_new_finding_release_and_excel_verified',
                  'final_id': new['id'], 'original_id': item['id'],
                  'prior_findings_unchanged': len(base['findings']), 'final_unique_rows': len(ids),
                  'semantic_grouping_completed': False, 'engineer_acceptance': False,
                  'production_acceptance': False,
                  'files': [{'path': str(p), 'sha256': sha(p)} for p in sorted(destination.rglob('*')) if p.is_file()]}
    write(destination/'assessment.json', assessment)
    return assessment


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('root', type=Path); p.add_argument('destination', type=Path)
    a = p.parse_args()
    result = archive(a.root.resolve(), a.destination.resolve())
    print(json.dumps({k: v for k, v in result.items() if k != 'files'}, ensure_ascii=False))
