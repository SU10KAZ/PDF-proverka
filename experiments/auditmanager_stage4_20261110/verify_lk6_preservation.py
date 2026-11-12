"""Read-only DEV lineage check; no model calls or automatic duplicate suppression."""
import hashlib
import json
from pathlib import Path

from openpyxl import load_workbook
from backend.app.pipeline.stages.report.generate_excel_report import PROJ_COLUMNS
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions

HERE = Path(__file__).parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify():
    overlap = json.loads((HERE / 'KM_LK6_classic_overlap_review.json').read_text())
    baseline_path = Path(overlap['baseline'])
    assert digest(baseline_path) == overlap['baseline_sha256']
    candidate_path = HERE / 'km_lk6_quantity_full_context_evidence/workflow/candidate.json'
    assert digest(candidate_path) == overlap['source_candidate_sha256']
    candidate = json.loads(candidate_path.read_text())
    entry = candidate['entry']
    original = entry['queue']['packets'][0]['finding_snapshot']
    # Resolve against the actual classic_control root (snapshots/KM is two levels down).
    md = baseline_path.parents[2] / 'inputs/KM/versions/v001/02_work/document.md'
    local, receipts = apply_review_admissions({'findings': [original]}, [entry], md.read_text())
    assert receipts == [{'finding_id': original['id'], 'status': 'applied', 'action': 'revise'}]
    assert len(local['findings']) == 1
    evidence = HERE / 'km_f005_output_evidence'
    assessment = json.loads((evidence / 'assessment.json').read_text())
    for artifact in assessment['files']:
        assert digest(Path(artifact['path'])) == artifact['sha256']
    before = json.loads(baseline_path.read_text())['findings']
    after = json.loads((evidence / '03_findings.json').read_text())['findings']
    assert len(after) == len(before) == 26
    assert len({f['id'] for f in after}) == 26
    assert [f for f in after if f['id'] != 'F-005'] == [f for f in before if f['id'] != 'F-005']
    indexed = {f['id']: f for f in after}
    wb = load_workbook(evidence / 'report.xlsx', read_only=True)
    try:
        rows = list(wb['KM'].values)
        id_column = next(i for i, (key, *_) in enumerate(PROJ_COLUMNS) if key == 'finding_id')
        description_column = next(i for i, (key, *_) in enumerate(PROJ_COLUMNS) if key == 'description')
        for target in ('F-005', 'F-017'):
            matches = [row for row in rows if row[id_column] == target]
            assert len(matches) == 1
            assert indexed[target]['description'] in matches[0][description_column]
    finally:
        wb.close()
    return {
        'status': 'bounded_existing_output_and_local_admission_verified',
        'candidate_sha256': digest(candidate_path),
        'local_admission_receipts': receipts,
        'output_sha256': digest(evidence / '03_findings.json'),
        'xlsx_sha256': digest(evidence / 'report.xlsx'),
        'findings': 26, 'other_than_F005_unchanged': 25,
        'exported_once': ['F-005', 'F-017'],
        'lineage_review': {
            'quantity_basis': 'F-005 preserves 172 known anchors and conditional 228/40, plus separately reviewed plate claims.',
            'marking_basis': 'F-017 already retains OGM26 missing from the specification; no additional row required for that shared basis.',
            'unresolved_new_detail': 'The proposed exact OGM26-to-OGM25 physical correspondence is not certified by unchanged F-017.'},
        'limits': [
            'Coordinator-known case mapping; not an autonomous semantic merge.',
            'Unchanged rows demonstrate preservation, not source correctness.',
            'Local candidate admission is not final release of that candidate.',
            'F005 profile disagreement remains held under benchmark_errata.json.'],
        'new_findings_claimed': 0, 'new_model_calls': 0,
        'stage4_complete': False, 'production_acceptance': False,
    }


if __name__ == '__main__':
    result = verify()
    print(json.dumps(result, ensure_ascii=False, indent=2))
