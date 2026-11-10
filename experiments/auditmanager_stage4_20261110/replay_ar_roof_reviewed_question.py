"""Local admission replay of the sealed Astra decision; no new model calls."""
from pathlib import Path
import hashlib
import json
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS

ROOT = Path('/tmp/auditmanager_ar_roof_reviewed_question_20261111')
SOURCE = Path('/tmp/auditmanager_ar_roof_handoff_v2_source_workflow_20261111/F-024/candidate.json')
OUTPUT = Path('/tmp/auditmanager_ar_roof_handoff_v2_output_20261111')


def main():
    ROOT.mkdir(exist_ok=True)
    document = json.loads((OUTPUT/'03_findings.json').read_text())
    candidate = json.loads(SOURCE.read_text())
    md = (OUTPUT/'02_work/document.md').read_text()
    revised, receipts = apply_review_admissions(document, [candidate['entry']], md)
    assert receipts == [{'finding_id': 'F-024', 'status': 'applied', 'action': 'question'}], receipts
    untouched = [f for f in document['findings'] if f['id'] != 'F-024']
    assert [f for f in revised['findings'] if f['id'] != 'F-024'] == untouched
    question = next(f for f in revised['findings'] if f['id'] == 'F-024')
    assert question['report_section'] == 'questions'
    assert question['source_evidence_review_status'] == 'reviewed_question'
    (ROOT/'03_findings.json').write_text(json.dumps(revised, ensure_ascii=False, indent=2))
    wb = Workbook();build_project_sheet(wb, {'project_id': 'AR', 'sheet_name': 'AR', 'findings_json': revised})
    wb.save(ROOT/'report.xlsx');wb.close()
    wb = load_workbook(ROOT/'report.xlsx');rows = list(wb['AR'].values)
    col = next(i for i, (key, *_) in enumerate(PROJ_COLUMNS) if key == 'finding_id')
    heading = next(i for i, row in enumerate(rows) if 'Вопросы и уточнения' in row)
    positions = [i for i, row in enumerate(rows) if row[col] == 'F-024']
    assert len(positions) == 1 and positions[0] > heading
    wb.close()
    report = {'model': 'codex/gpt-6-astra', 'sealed_candidate_sha256': hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        'raw_candidate_status': candidate['status'], 'raw_rejection_reason': candidate['admission_receipts'][0]['reason'],
        'local_contract_replay': receipts, 'other_findings_unchanged': len(untouched), 'final_findings': len(revised['findings']),
        'question_exported_separately': True, 'new_model_calls': 0, 'full_acceptance_proven': False,
        'boundary': 'Actual source review and adjudication preserve a conditional engineering question. Structured search-boundary contract repaired locally; raw model response unchanged. Not full pipeline or whole-project acceptance.'}
    Path(__file__).with_name('AR_roof_reviewed_question_replay.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(report, ensure_ascii=False))

if __name__ == '__main__':main()
