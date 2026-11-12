"""Apply current, unmodified model decisions to the complete EOM control report."""
from copy import deepcopy
import json
from pathlib import Path
import shutil
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_eom_instance_operations import BASE
from .run_primary_text_runtime_probe import sha, write

PRIOR = Path('/tmp/auditmanager_eom_f032_output_20261111/03_findings.json')
OUT = Path(__file__).parent/'eom_current_profile_evidence'


def read(p):
    return json.loads(p.read_text())


def main():
    OUT.mkdir(exist_ok=False)
    before = read(PRIOR)
    document = deepcopy(before)
    md_path = BASE/'02_work/document.md'
    md = md_path.read_text()
    records = []
    for fid in ('F-001', 'F-030'):
        root = Path('/tmp/auditmanager_eom_f'+fid[-3:]+'_current_profile_20261112')
        candidate = read(root/'candidate.json')
        entry = candidate['entry']
        assert entry['queue']['source_md_sha256'] == sha(md_path)
        snapshot = next(p['finding_snapshot'] for p in entry['queue']['packets'] if p['finding_id'] == fid)
        index = next(i for i, f in enumerate(document['findings']) if f['id'] == fid)
        added = {'source_review_requests', 'source_evidence_review_status'}
        assert {k: v for k, v in snapshot.items() if k not in added} == document['findings'][index]
        document['findings'][index] = deepcopy(snapshot)
        document, receipts = apply_review_admissions(document, [entry], md)
        assert receipts == candidate['admission_receipts']
        assert receipts[0]['status'] == 'applied'
        item = document['findings'][index]
        for k, v in entry['decision']['approved_fields'].items():
            assert item[k] == v
        records.append({'finding_id': fid, 'receipt': receipts[0],
                        'actual_section': item['report_section'], 'severity': item['severity'],
                        'manual_override': False, 'model_response_sha256': sha(root/'result.json')})
        dest = OUT/fid
        dest.mkdir()
        for name in ('binding.json', 'request.json', 'result.json', 'candidate.json', 'attempt.json'):
            shutil.copyfile(root/name, dest/name)
    changed = {'F-001', 'F-030'}
    assert len(document['findings']) == len(before['findings']) == 51
    assert [f for f in document['findings'] if f['id'] not in changed] == [f for f in before['findings'] if f['id'] not in changed]
    write(OUT/'03_findings.json', document)
    wb = Workbook()
    build_project_sheet(wb, {'project_id': 'EOM', 'sheet_name': 'EOM', 'findings_json': document})
    wb.save(OUT/'report.xlsx'); wb.close()
    wb = load_workbook(OUT/'report.xlsx')
    rows = list(wb['EOM'].values)
    cols = {key: i for i, (key, *_) in enumerate(PROJ_COLUMNS)}
    ids = {f['id'] for f in document['findings']}
    exported = [r[cols['finding_id']] for r in rows if r[cols['finding_id']] in ids]
    assert len(exported) == len(set(exported)) == 51
    for fid in changed:
        item = next(f for f in document['findings'] if f['id'] == fid)
        row = next(r for r in rows if r[cols['finding_id']] == fid)
        for key in ('problem', 'solution', 'risk', 'norm'):
            assert row[cols[key]] == item[key]
        assert item['description'] in row[cols['description']]
    wb.close()
    assessment = {'status': 'automatic_admission_and_export_verified_profile_disagreement_retained',
                  'baseline': {'path': str(PRIOR), 'sha256': sha(PRIOR)},
                  'md': {'path': str(md_path), 'sha256': sha(md_path)},
                  'records': records, 'other_findings_unchanged': 49, 'unique_excel_rows': 51,
                  'final_release_check_completed': False, 'production_acceptance': False,
                  'limits': ['Known-source development control, not independent discovery.',
                             'Model preserves technical routing, unlike prior local questions override; neither is declared engineering truth.',
                             'No equipment fault, wrong supply or normative breach is established.',
                             'Severity and section agreement are not accepted from model self-assessment.'],
                  'files': [{'path': str(p), 'sha256': sha(p)} for p in sorted(OUT.rglob('*')) if p.is_file()]}
    write(OUT/'assessment.json', assessment)
    print(json.dumps({k: v for k, v in assessment.items() if k != 'files'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
