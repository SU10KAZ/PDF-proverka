"""Local, explicitly reviewed replacement of the narrower T411 pilot by its broader claim.

This is a known-source output control, not an automatic semantic deduplicator.
The independent 53-row baseline is retained; both pilot versions are archived.
"""
import hashlib
import json
from pathlib import Path
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS
from .run_itp_pipe_operations import BASE, immutable
from .replay_itp_t411_output import BASELINE

SOURCE = Path('/tmp/auditmanager_itp_gvs_circulation_source_review_20261111')
NARROW = Path('/tmp/auditmanager_itp_t411_output_20261111/03_findings.json')
ROOT = Path('/tmp/auditmanager_itp_gvs_circulation_output_20261111')
OUT = Path(__file__).parent


def main():
    ROOT.mkdir(exist_ok=True)
    md = (BASE / '02_work/document.md').read_text()
    candidate = json.loads((SOURCE / 'workflow/candidate.json').read_text())
    finding = json.loads((SOURCE / 'finding.json').read_text())
    assert candidate['status'] == 'candidate_ready'
    revised, receipts = apply_review_admissions(
        {'findings': [finding]}, [candidate['entry']], md)
    assert len(receipts) == 1 and receipts[0]['status'] == 'applied', receipts
    item, = revised['findings']
    assert item['report_section'] == 'technical_findings'
    before = json.loads(BASELINE.read_text())
    narrow_document = json.loads(NARROW.read_text())
    assert narrow_document['findings'][:-1] == before['findings']
    narrow = narrow_document['findings'][-1]
    assert narrow['id'] == 'F-054'
    # Assertions protect this reviewed control against changed source candidates;
    # substring matching is not offered as a general semantic coverage algorithm.
    for term in ('2,73', '5,89'):
        assert term in narrow['description'] and term in item['description']
    for term in ('Т411', 'Т412', '1,57', '3,13', '4,30', '9,02', '4,31'):
        assert term in item['description']
    coverage = {
        'origin': 'explicit_local_source_claim_review',
        'reviewer': 'root_audit_agent',
        'narrow_snapshot': narrow,
        'narrow_document_sha256': hashlib.sha256(NARROW.read_bytes()).hexdigest(),
        'decision': 'superseded_by_broader_reviewed_circulation_claim',
        'reason': 'Both candidates concern the same T411 flow annotations on the same scheme and section. The broader candidate retains 2.73 versus 5.89 and adds T412 plus the traced common K15 contour. One corrective action reconciles branch-flow inputs. No separate T411 row is appended.',
        'severity_change': {'prior': narrow['severity'], 'current': item['severity'],
                            'basis': candidate['entry']['decision']['display_field_reviews']['severity']},
        'boundaries': ['Not an automatic deduplication acceptance test.',
                       'Pump insufficiency remains unproved.',
                       'F-011 concerns separate PZ arithmetic/units and remains intact.'],
    }
    item['source_review_history'] = revised['source_review_history']
    item['source_review_original_finding_id'] = finding['id']
    item['pilot_supersession_review'] = coverage
    item['source_finding_ids'] = list(dict.fromkeys(
        item.get('source_finding_ids', []) + narrow.get('source_finding_ids', [])))
    after = combine_findings_with_targeted(before, [('GVS_circulation_source_review', revised)])
    assert after['findings'][:-1] == before['findings']
    assert len(before['findings']) == 53 and len(after['findings']) == 54
    new = after['findings'][-1]
    assert new['id'] == 'F-054' and new['pilot_supersession_review'] == coverage
    immutable(ROOT / '03_findings.json', after)
    wb = Workbook()
    build_project_sheet(wb, {'project_id': 'ITP', 'sheet_name': 'ITP', 'findings_json': after})
    wb.save(ROOT / 'report.xlsx'); wb.close()
    wb = load_workbook(ROOT / 'report.xlsx')
    cols = {key: i for i, (key, *_) in enumerate(PROJ_COLUMNS)}
    rows = list(wb['ITP'].values)
    selected = [r for r in rows if r[cols['finding_id']] == new['id']]
    assert len(selected) == 1
    for key in ('problem', 'solution'):
        assert selected[0][cols[key]] == new[key]
    assert new['description'] in selected[0][cols['description']]
    ids = {f['id'] for f in after['findings']}
    exported = [r[cols['finding_id']] for r in rows if r[cols['finding_id']] in ids]
    assert len(exported) == len(set(exported)) == 54
    wb.close()
    evidence = OUT / 'itp_gvs_circulation_source_evidence'
    evidence.mkdir(exist_ok=True)
    for name in ('finding.json', 'queue.json', 'initial_request.json', 'manifest.json'):
        immutable(evidence / name, json.loads((SOURCE / name).read_text()))
    for path in sorted((SOURCE / 'workflow').rglob('*.json')):
        dest = evidence / 'workflow' / path.relative_to(SOURCE / 'workflow')
        dest.parent.mkdir(parents=True, exist_ok=True)
        immutable(dest, json.loads(path.read_text()))
    immutable(evidence / 'supersession_review.json', coverage)
    immutable(evidence / '03_findings.json', after)
    report = {
        'scope': 'Known-source broader circulation finding through source admission, merge and Excel; narrow pilot superseded with explicit provenance.',
        'source_candidate_sha256': hashlib.sha256((SOURCE / 'workflow/candidate.json').read_bytes()).hexdigest(),
        'baseline_sha256': hashlib.sha256(BASELINE.read_bytes()).hexdigest(),
        'source_md_sha256': hashlib.sha256(md.encode()).hexdigest(),
        'receipts': receipts, 'prior_findings_preserved_exactly': 53,
        'final_findings': 54, 'new_finding_id': new['id'],
        'narrow_pilot_emitted_as_duplicate': False,
        'historical_compound_control_closed': False, 'full_acceptance': False,
        'limitations': ['Known-source context was executor-selected; not an independent full audit.',
                       'Supply-flow discrepancy and pipe thickness scope remain separate pending controls.',
                       'Preserving baseline rows does not certify their truth.',
                       'Other ITP corrections have not all been composed into this snapshot.'],
    }
    immutable(OUT / 'ITP_GVS_circulation_output_replay.json', report)
    (OUT / 'ITP_GVS_circulation_control.xlsx').write_bytes((ROOT / 'report.xlsx').read_bytes())
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
