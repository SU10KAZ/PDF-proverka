"""Reproduce a known-control omission; never mutate frozen trial inputs or results."""
import hashlib
import json
import re
from pathlib import Path

BASE = Path(__file__).resolve().parent
FIELDS = ('problem', 'description', 'solution')

def main():
    paths = {
        'source': BASE / 'ar_rope_bundle_source_evidence/bundle.json',
        'candidate': BASE / 'ar_rope_current_release_evidence/adjudication/candidate.json',
        'adjudicated': BASE / 'ar_rope_current_release_evidence/adjudication/output.json',
        'released': BASE / 'ar_rope_current_release_evidence/release/03_findings.json',
        'matrix': BASE / 'stage4_original_control_matrix.json',
    }
    data = {k: json.loads(p.read_text()) for k, p in paths.items()}
    entry = data['candidate']['entry']
    claim = next(x for x in entry['response']['claims'] if x['claim_id'] == 'C8')
    review = next(x for x in entry['decision']['claim_reviews'] if x['claim_id'] == 'C8')
    adjudicated = next(x for x in data['adjudicated']['findings'] if x['id'] == 'AR-O27-crossdoc')
    released = next(x for x in data['released']['findings'] if x['id'] == 'F-025')
    approved = entry['decision']['approved_fields']
    quote = next(x for x in claim['evidence'] if x['type'] == 'text' and 'КМ 8' in x.get('quote', ''))
    assert quote['quote'] in data['source']['md']
    assert quote['block_id'] in data['source']['md']
    has_reference = lambda s: bool(re.search(r'КМ\s*8\b', s))
    stages = {
        'source_review_C8': has_reference(claim['reason']),
        'adjudication_review_C8': has_reference(review['reason']),
        'approved_user_fields': has_reference('\n'.join(approved[k] for k in FIELDS)),
        'adjudicated_user_fields': has_reference('\n'.join(adjudicated[k] for k in FIELDS)),
        'released_user_fields': has_reference('\n'.join(released[k] for k in FIELDS)),
    }
    assert list(stages.values()) == [True, True, False, False, False]
    assert all(approved[k] == adjudicated[k] == released[k] for k in FIELDS)
    assert released['report_section'] == 'questions'
    result = {
        'control': 'H06-P03',
        'finding': 'AR-O27-crossdoc -> F-025',
        'status': 'confirmed_reference_omission_in_approved_user_fields',
        'scope': 'Historical known-source pilot only; no live comparison outputs inspected.',
        'source_quote': quote,
        'source_review_claim': claim,
        'adjudication_claim_review': review,
        'approved_user_fields': approved,
        'reference_present_by_stage': stages,
        'localization': 'Source review and adjudication reasoning explicitly retain KM8; approved user fields omit it. Export preserves approved fields unchanged.',
        'not_established': [
            'Contents or applicability of the actual KM8 calculation.',
            'Physical defect or missing calculation in the full project.',
            'General omission rate or improvement from a fix.',
        ],
        'proposed_behavior': 'Keep a source-bound calculation reference in the addressed question when the accepted reasoning identifies it as necessary missing context. Distinguish calculation KM8 from design KM9.2; do not infer their contents or applicability.',
        'acceptance_checks_for_future_candidate': [
            'Final question explicitly requests applicable calculation via the existing KM8 reference and address-matches the node.',
            'Keep the separate KM9.2 design-context request and conditional applicability.',
            'Do not turn an unavailable calculation into an absent-calculation or insufficient-strength defect.',
            'Keep all 24 prior findings unchanged and export the question separately.',
            'Negative control needed: an unrelated catalogue reference must not be copied solely because it occurs in the source; no confirmed matched pair supplied by this trace.',
        ],
        'stage4_complete': False,
        'production_acceptance': False,
        'frozen_matrix_modified': False,
        'source_files': {k: {'path': str(p.relative_to(BASE.parent.parent)), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for k, p in paths.items()},
    }
    out = BASE / 'AR_calculation_reference_trace.json'
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'artifact': str(out), 'reference_present_by_stage': stages}, ensure_ascii=False))

if __name__ == '__main__':
    main()
