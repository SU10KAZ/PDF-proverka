"""Publish DEV results and freeze the tested candidate before holdout selection."""
from collections import Counter
from pathlib import Path
import subprocess

from experiments.semantic_foundation_v3 import frozen_v1 as v1
from experiments.semantic_foundation_v3.run import read, receipt
from .prepare import ROOT, FINAL, FINAL_SHA, immutable

CONTRACTS = {
 'TABLE_V3_ARCHITECTURE.md': '''# Table V3 architecture

Frozen Foundation -> source table features -> unordered boundary evidence ->
stratum promotion gate -> logical table components -> retained row materialization.
The existing ledger, semantics and decisions files are byte-identical to Foundation.
The new tables.json is a reference view, not a second owner registry. A REVIEW edge
closes automatic assembly and publishes a candidate relation. SAME is the only
operation that extends a component. One component can span N pages; multiple
components can share a page. Pairwise candidate edges connect consecutive source
segments, including non-adjacent pages. Interleaved-table retrieval is not a Relation
Matcher and is not implemented. Explicit same title + schema + row progression +
container/title-block evidence can support non-adjacent continuation; its current
human denominator is zero, so the frozen candidate abstains in that stratum.

No model calls, numerical score thresholds, production integration, case-specific
exceptions or Section V3 changes. Review components have deterministic keys but
do not assert that their unresolved external boundaries are complete.
''',
 'TABLE_BOUNDARY_EVIDENCE_CONTRACT.md': '''# Boundary evidence contract

Each boundary retains ledger left/right anchors, join_evidence, split_evidence,
neutral_evidence, conflict, candidate_join, candidate_split, source witnesses and
rule_strata. No ordered rule chain decides a boundary. Every predicate runs.

Admitted JOIN only -> SAME/PROVEN. Admitted SPLIT only -> NEW/PROVEN.
Contradictory candidate JOIN and SPLIT -> REVIEW/CONFLICT, even if one predicate
has insufficient calibration. No admitted evidence -> REVIEW/NO_EVIDENCE.
Unpromoted candidates are retained as neutral observations; they never prove a
decision, and calibration cannot erase a source conflict.

Width, repeated first row, same page, same container and shared title-block context
are observations, never standalone continuation proof. First-row kinds come from
Foundation. Semantic column roles require independently named columns, never the
Markdown separator position. All DATA_LIKE first rows remain DATA in row output.

Ordinal continuation needs actual +1 record progression; both observed numeric
sequences must progress or consist of a single record, and at least one must have
two records. Decimal measurements and constant first-column values are not resets.
Reset requires real increasing runs on both sides, left final ordinal >1, right
restart at 1. Specification group resets remain neutral. Column enumeration rows
are recognized only after an independently supported schema.

Headerless joins require edge adjacency, compatible edge columns and source-level
row continuation. Their own DEV gate applies. Repeated exact table notes with a
note marker on multiple source pages are transparent only in the table view; their
original ledger owners and text remain unchanged. Other narrative supports SPLIT
as a candidate, but its poor DEV precision prevents promotion. Distinct explicit
titles/containers and semantic column/type changes are independently collected.
''',
 'TABLE_IDENTITY_CONTRACT.md': '''# Table identity contract

TableKey is a SHA-256-derived semantic key over DocumentVersion, normalized frozen
section/container identity, explicit table title, semantic table type, column
contract and occurrence among components with that semantic signature. Unknown
schemas retain width plus an explicit UNRESOLVED marker. No page number, bbox,
run/session/pair identifier or filesystem timestamp enters the key.

Pages and segment references remain provenance. Local semantic ordering resolves
otherwise identical components; inserting an indistinguishable earlier component
can change later occurrence suffixes. This is not a cross-version Relation Matcher.
Tests move unchanged semantic content between page numbers and preserve TableKeys.
Equipment groups are row content, not automatic table identity boundaries.
Cable journal, specification, automation modules, fan performance and noise data
have independent structural contracts. Physical co-location proves no identity.
''',
 'TABLE_ROW_CONTRACT.md': '''# Row contract

Rows are ordered parallel columns: source ledger line, content_key, explicit_number,
and role. Stable row identity is (TableKey, normalized-cell-content fingerprint,
occurrence among identical rows), not absolute row position. Source page, block and
Markdown locator resolve through the ledger. Every content row survives, including
schema rows, duplicate headers, blanks and DATA_LIKE first rows. Markdown separators
remain in the ledger. Column enumeration does not become an explicit data-row number.

row_values resolves the pinned source and returns sequence, all raw cells, retained
nonempty key/label cells, explicit number, page and source line refs. Parsing removes
exactly two outer pipes and respects escaped pipes; empty boundary cells survive.
This is lazy cell materialization to avoid duplicating long specification text.
Inserted distinct rows do not change later content identities; indistinguishable
duplicate rows use a local occurrence discriminator. Row text is never copied into
a second ownership registry.
''',
 'OLD_EVAL_DIAGNOSTIC.md': '''# Old frozen EVAL — diagnostic only

NOT FINAL RELEASE GATE. Not run in this task. No old EVAL answers, Checkpoint 1,
Phase 2, Checkpoint 3, V002 Sheet truth or production human decisions were used for
tuning. The 52-document source manifest was used solely for Foundation conservation,
SHEET compatibility, replay and source-only structural checks. Holdout exclusion
uses source identities and hashes, never evaluation answers.
''',
}


def percent(v):
    return 'N/A (no PROVEN cases)' if v is None else f'{100*v:.2f}%'


def main():
    reports = ROOT / 'reports'
    assert not (reports / 'TABLE_V3_CANDIDATE_MANIFEST.json').exists()
    assert v1.file_sha(FINAL) == FINAL_SHA
    assert read(reports / 'TABLE_V3_REPLAY.json')['pass']
    optimized = read(reports / 'TABLE_V3_OPTIMIZED_REPLAY.json')
    assert all(v['pass'] for v in optimized.values())
    assert read(reports / 'TABLE_V3_TESTS.json')['returncode'] == 0
    cases = read(ROOT / 'TABLE_V3_DEV_TRUTH.json')['cases']
    baseline = read(reports / 'TABLE_V3_BASELINE.json')
    current = read(ROOT / 'iterations/05_validated_candidate/score.json')
    predictions = read(ROOT / 'iterations/05_validated_candidate/predictions.json')
    audit = read(ROOT / 'iterations/05_validated_candidate/rule_audit.json')
    policy = read(Path(__file__).with_name('policy.json'))
    assert policy['promoted_rules'] == sorted(k for k, v in audit.items() if v['promoted'])
    for k in policy['promoted_rules']:
        assert audit[k]['precision'] >= .9 and audit[k]['denominator'] >= 5 and audit[k]['documents'] >= 2
    for name, body in CONTRACTS.items():
        (reports / name).write_text(body)
    false_join = sum(predictions[c['case_id']]['decision'] == 'SAME' and c['answer'] == 'NEW' for c in cases)
    false_split = sum(predictions[c['case_id']]['decision'] == 'NEW' and c['answer'] == 'SAME' for c in cases)
    current.update(false_joins=false_join, false_splits=false_split)
    immutable(reports / 'TABLE_V3_DEV_SCORECARD.json', current)
    lines = ['# TABLE DEV scorecard', '', '| Metric | Foundation | Table V3 |', '|---|---:|---:|']
    for k in ('accuracy_on_proven', 'coverage', 'legacy_review_incorrect'):
        lines.append(f'| {k} | {percent(baseline[k])} | {percent(current[k])} |')
    lines += ['', f'Human TABLE cases: 44; SAME 33 / NEW 11. False joins {false_join}; false splits {false_split}.',
              'PROVEN: 13/13 (5 SAME, 8 NEW); REVIEW: 31. DEV only, not an independent release gate.', '',
              '| Source stratum | N | PROVEN correct / N | REVIEW |', '|---|---:|---:|---:|']
    for stratum in sorted({c['stratum'] for c in cases}):
        subset = [c for c in cases if c['stratum'] == stratum]
        correct = sum(predictions[c['case_id']]['decision'] == c['answer'] for c in subset)
        review = sum(predictions[c['case_id']]['decision'] == 'REVIEW' for c in subset)
        lines.append(f'| {stratum} | {len(subset)} | {correct}/{len(subset)} | {review} |')
    (reports / 'TABLE_V3_DEV_SCORECARD.md').write_text('\n'.join(lines) + '\n')
    log = ['# Rule promotion log', '', 'Gate declared before implementation: precision >=90%, >=5 human cases, >=2 documents.',
           'Automatic controls never enlarge denominators. Every rule is checked in its own source-structure stratum.', '',
           '| Iteration | Change | Active PROVEN | Coverage / legacy |', '|---|---|---:|---:|',
           '| Baseline | Frozen Foundation | 0 | 0% / 0% |',
           '| 01 | Collect all evidence; no rules admitted | 0 | 0% / 0% |',
           '| 02 | Preserve punctuation and surface case for real ordinals and continuation | 0 | 0% / 0% |',
           '| 03 | Lossless cells; strict two-sided runs; equipment contracts; conflict veto | 0 | 0% / 0% |',
           '| 04 | Separate geometry/header strata; exact repeated table notes; meter column roles | 0 | 0% / 0% |',
           '| 05 | Admit only two passing rule strata | 13/13 | 29.55% / 29.55% |',
           '| Execution optimization | Combined role regex, no witness deep copy; byte-equivalent on all 73 documents | 13/13 | unchanged |', '',
           'Iterations 01–04 are observation-only runs, not silently deployed rule versions. Each directory retains raw candidates, '
           'per-case predictions, precision denominators, proposed policy and scorer output. Producer receipts in each run pin the code hashes.', '',
           '| Rule / structural stratum | Correct / N | Documents | Precision | Admitted |', '|---|---:|---:|---:|---|']
    for name, record in audit.items():
        log.append(f"| {name} | {record['correct']}/{record['denominator']} | {record['documents']} | {percent(record['precision'])} | {record['promoted']} |")
    (reports / 'TABLE_RULE_PROMOTION_LOG.md').write_text('\n'.join(log) + '\n')
    categories = ['FALSE_JOIN', 'FALSE_SPLIT', 'MISSED_CONTINUATION', 'WRONG_RESET_SIGNAL', 'HEADER_AS_DATA',
                  'CAPTION_CONTAINER_ERROR', 'NARRATIVE_GAP_ERROR', 'SAME_PAGE_MULTI_TABLE_ERROR',
                  'SPECIFICATION_GROUP_ERROR', 'PASSPORT_MULTI_TABLE_ERROR', 'REVIEW_TOO_CONSERVATIVE', 'OTHER']
    errors = []
    for c in cases:
        p = predictions[c['case_id']]
        if p['decision'] == c['answer']:
            continue
        codes = sorted({code for e in p.get('edges', []) for code in e['candidate_join'] + e['candidate_split']})
        primary = ('FALSE_JOIN' if p['decision'] == 'SAME' else 'FALSE_SPLIT' if p['decision'] == 'NEW' else 'REVIEW_TOO_CONSERVATIVE')
        tags = [primary] + (['MISSED_CONTINUATION'] if c['answer'] == 'SAME' and p['decision'] == 'REVIEW' else [])
        errors.append({'case_id': c['case_id'], 'stratum': c['stratum'], 'document_version': c['document_version'],
                       'anchors': c['anchors'], 'human_answer': c['answer'], 'prediction': p['decision'],
                       'categories': tags, 'observed_rule_candidates': codes,
                       'explanation': 'Abstention caused by insufficient calibrated evidence or source conflict; no claim of a proven semantic error.'})
    immutable(reports / 'TABLE_V3_ERROR_TAXONOMY.json', {'allowed_categories': categories, 'errors': errors,
        'counts': dict(Counter(t for e in errors for t in e['categories'])), 'false_joins': false_join, 'false_splits': false_split})
    perf = read(reports / 'FINAL_PERFORMANCE_MEASUREMENTS.json')
    plines = ['# Table V3 performance', '', perf['method'], '', '| Metric | Foundation | V3 | Ratio | Target |', '|---|---:|---:|---:|---:|']
    for k in perf['ratios']:
        plines.append(f"| {k} | {perf['foundation'][k]:.4f} | {perf['candidate'][k]:.4f} | {perf['ratios'][k]:.4f} | {perf['targets'][k]} |")
    plines += ['', 'Initial and optimized raw trials are retained; no best-run cherry-picking. Source-only 52-document timings are retained separately.',
               'The V3 view adds parsing/identity/evidence for 9,208 DEV table rows while preserving all Foundation artifacts byte-for-byte. '
               'Any missed target remains visible and is not waived as a release gate. Profiling attributes overhead to the additional table view; '
               'Foundation itself is unchanged. RSS uses equally launched fresh processes, avoiding the larger audit-parent floor in the preparation run.']
    (reports / 'TABLE_V3_PERFORMANCE.md').write_text('\n'.join(plines) + '\n')
    core = [Path(__file__).with_name(name) for name in ('model.py', 'run.py', 'policy.json')]
    foundation = read(reports / 'FOUNDATION_INPUT_FREEZE.json')
    files = [receipt(p) for p in core] + foundation['code']
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    manifest = {'schema': 'table-v3-candidate-freeze.v1', 'commit': commit, 'code': files,
                'candidate_input_hash': v1.digest({'code': files, 'foundation': foundation, 'policy': policy}),
                'truth': receipt(ROOT / 'TABLE_V3_DEV_TRUTH.json'), 'policy': policy,
                'foundation_input_hash': foundation['input_sha256'], 'human_truth_sha256': FINAL_SHA,
                'scorecard': receipt(reports / 'TABLE_V3_DEV_SCORECARD.json'),
                'semantic_artifact_manifests': [receipt(ROOT / name / 'index.json') for name in
                    ('optimized_benchmark_candidate_1', 'optimized_benchmark_candidate_2', 'candidate_controls_optimized_run1', 'candidate_controls_optimized_run2')],
                'replay': receipt(reports / 'TABLE_V3_OPTIMIZED_REPLAY.json'),
                'frozen_before_holdout_selection': True, 'fresh_holdout_answers_seen': 0, 'old_eval_answers_seen': 0,
                'verdict': 'B — TABLE V3 PROMISING BUT NEEDS MORE DEV TRUTH',
                'reason': '31/44 abstentions; specification item flow 4/4, headerless key flow 4/4, same-page contract change 1/1 and non-adjacent identity 0 cannot clear predeclared human support gates.',
                'production_release': str(Path('/home/coder/auditmanager/current').resolve()),
                'production_modified': False, 'section_v3_modified': False, 'relation_matcher_implemented': False,
                'human_truth_modified': False, 'push': 0, 'deploy': 0}
    print(immutable(reports / 'TABLE_V3_CANDIDATE_MANIFEST.json', manifest))


if __name__ == '__main__':
    main()
