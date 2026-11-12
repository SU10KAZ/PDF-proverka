"""Check automatic AR question admission through final review, merge and Excel.

Known-source development control, not an independent project audit.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check
from backend.app.pipeline.stages.findings_verify.source_review_release_workflow import run_final_release_check
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_ar_rope_current_profile import SOURCE
from .run_primary_text_runtime_probe import sha, write

REVIEW = Path('/tmp/auditmanager_ar_rope_current_profile_20261112')
BASE = Path('/tmp/auditmanager_ar_roof_reviewed_question_20261111/03_findings.json')
ROOT = Path('/tmp/auditmanager_ar_rope_current_release_20261112')


def read(path):
    return json.loads(path.read_text())


async def run(execute):
    ROOT.mkdir(exist_ok=True)
    candidate = read(REVIEW/'candidate.json')
    assert candidate['receipts'] == [{'finding_id': 'AR-O27-crossdoc', 'status': 'applied', 'action': 'question'}]
    entry = candidate['entry']
    finding = read(SOURCE/'finding.json')
    bundle = read(SOURCE/'bundle.json')
    base = read(BASE)
    # Final review evaluates the source-bound candidate before merge assigns F IDs.
    document = {**base, 'findings': [*base['findings'], finding]}
    request = prepare_final_check(entry, document, bundle['md'])['request']
    binding = {'inputs': {str(p): sha(p) for p in [REVIEW/'candidate.json', SOURCE/'bundle.json', SOURCE/'finding.json', BASE]},
               'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
               'message_chars': sum(len(m['content']) for m in request['messages']),
               'image_count': len(request['image_paths']), 'known_case': True,
               'engineer_answers_supplied': False, 'automatic_retries': 0}
    for name, data in [('binding', binding), ('request', request), ('input_document', document)]:
        path = ROOT/(name+'.json')
        if path.exists():
            assert read(path) == data
        else:
            write(path, data)
    print(json.dumps(binding, ensure_ascii=False), flush=True)
    if not execute:
        return
    os.environ['CODEX_TRANSIENT_RETRIES'] = '0'
    after, receipt = await run_final_release_check(entry, document, bundle['md'], ROOT/'workflow',
        invoke=direct_cli_executor('codex/gpt-6-astra', project_id='AR-rope-current-final'),
        executor_id='codex/gpt-6-astra:high:AR-rope-current-final')
    write(ROOT/'receipt.json', receipt)
    for path, digest in binding['inputs'].items():
        assert sha(Path(path)) == digest
    assert receipt['status'] == 'applied', receipt
    n = len(base['findings'])
    assert after['findings'][:n] == base['findings']
    item = after['findings'][-1]
    assert item['id'] == finding['id'] and item['report_section'] == 'questions'
    item['source_finding_ids'] = [finding['id']]
    item['source_review_history'] = after.get('source_review_history', [])
    item['source_review_bundle'] = {k: bundle[k] for k in ('source_md_sha256', 'source_catalogue', 'source_origins')}
    combined = combine_findings_with_targeted(base, [('source_review_cross_document', {'findings': [item]})])
    assert combined['findings'][:n] == base['findings']
    assert len(combined['findings']) == n+1
    write(ROOT/'03_findings.json', combined)
    wb = Workbook()
    build_project_sheet(wb, {'project_id': 'AR', 'sheet_name': 'AR', 'findings_json': combined})
    wb.save(ROOT/'report.xlsx'); wb.close()
    wb = load_workbook(ROOT/'report.xlsx')
    rows = list(wb['AR'].values)
    col = next(i for i, (key, *_) in enumerate(PROJ_COLUMNS) if key == 'finding_id')
    heading = next(i for i, row in enumerate(rows) if 'Вопросы и уточнения' in row)
    question = combined['findings'][-1]
    positions = [i for i, row in enumerate(rows) if row[col] == question['id']]
    assert len(positions) == 1 and positions[0] > heading
    wb.close()
    assessment = {'final_receipt': receipt['status'], 'prior_findings_unchanged': n,
                  'final_findings': n+1, 'question_id': question['id'],
                  'question_exported_separately': True, 'manual_verdict_override': False,
                  'full_audit_acceptance': False, 'production_acceptance': False,
                  'limitations': ['Known-source control, not blind discovery.',
                                  'Applicable KM9.2/KM8 content remains unverified; no physical defect established.',
                                  'Internal O27 editorial wording still needs product-level assessment.']}
    write(ROOT/'assessment.json', assessment)
    print(json.dumps(assessment, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    asyncio.run(run(parser.parse_args().execute))
