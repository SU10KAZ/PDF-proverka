"""Replay sealed multi-document source review, merge and export; no model calls."""
from pathlib import Path
import hashlib
import json
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS

SOURCE = Path('/tmp/auditmanager_eom_manufacturer_bundle_review_20261111')
BASE = Path('/tmp/auditmanager_eom_f032_output_20261111/03_findings.json')
ROOT = Path('/tmp/auditmanager_eom_manufacturer_question_output_20261111')
OUT = Path(__file__).parent

def read(path):
    return json.loads(path.read_text())

def write(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2)+'\n')

def main():
    ROOT.mkdir(exist_ok=True)
    candidate = read(SOURCE/'workflow/candidate.json')
    bundle = read(SOURCE/'bundle.json')
    finding = read(SOURCE/'finding.json')
    reviewed, receipts = apply_review_admissions({'findings': [finding]}, [candidate['entry']], bundle['md'])
    assert receipts == [{'finding_id': finding['id'], 'status': 'applied', 'action': 'question'}], receipts
    item = reviewed['findings'][0]
    item['source_finding_ids'] = [finding['id']]
    item['source_review_bundle'] = {key: bundle[key] for key in ('source_md_sha256', 'source_catalogue', 'source_origins')}
    # Keep the original review ID and complete evidence history through F-ID assignment.
    item['source_review_history'] = reviewed.get('source_review_history', [])
    base = read(BASE)
    combined = combine_findings_with_targeted(base, [('source_review_cross_document', reviewed)])
    n = len(base['findings'])
    assert n == 51
    assert not any('QF3.20' in str(f.get('problem',''))+str(f.get('description','')) for f in base['findings'])
    assert combined['findings'][:n] == base['findings']
    assert len(combined['findings']) == n+1
    question = combined['findings'][-1]
    assert question['source_finding_ids'] == [finding['id']]
    assert question['source_review_bundle'] == item['source_review_bundle']
    assert question['report_section'] == 'questions'
    assert question['source_evidence_review_status'] == 'reviewed_question'
    write(ROOT/'03_findings.json', combined)
    write(ROOT/'reviewed_source.json', reviewed)
    wb = Workbook()
    build_project_sheet(wb, {'project_id': 'EOM', 'sheet_name': 'EOM', 'findings_json': combined})
    wb.save(ROOT/'report.xlsx'); wb.close()
    wb = load_workbook(ROOT/'report.xlsx'); rows = list(wb['EOM'].values)
    col = next(i for i, (key, *_) in enumerate(PROJ_COLUMNS) if key == 'finding_id')
    heading = next(i for i, row in enumerate(rows) if 'Вопросы и уточнения' in row)
    positions = [i for i, row in enumerate(rows) if row[col] == question['id']]
    assert len(positions) == 1 and positions[0] > heading
    description_column = next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='description')
    assert question['description'] in rows[positions[0]][description_column]
    wb.close()
    (OUT/'EOM_manufacturer_question_control.xlsx').write_bytes((ROOT/'report.xlsx').read_bytes())
    assessment = {
        'sealed_candidate_sha256': hashlib.sha256((SOURCE/'workflow/candidate.json').read_bytes()).hexdigest(),
        'baseline_sha256': hashlib.sha256(BASE.read_bytes()).hexdigest(),
        'raw_candidate_status': candidate['status'], 'raw_receipts': candidate['admission_receipts'],
        'local_admission_receipts': receipts, 'new_model_calls': 0,
        'prior_findings_unchanged': n, 'final_findings': n+1,
        'final_question_id': question['id'], 'question_exported_separately': True,
        'source_catalogue_preserved': True, 'full_acceptance_proven': False,
        'limitations': [
            'Known-source bounded control, not a blind whole-project audit.',
            'Manufacturer restriction is scoped to the supplied DEKraft accessory; actual project article and EKF compatibility remain unresolved.',
            'No physical incompatibility or project-wide missing selection is established.',
            'Experimental composition of real admission, merge and export functions; production dispatcher integration remains pending.']}
    write(OUT/'EOM_manufacturer_question_output_replay.json', assessment)
    print(json.dumps(assessment, ensure_ascii=False))

if __name__ == '__main__': main()
