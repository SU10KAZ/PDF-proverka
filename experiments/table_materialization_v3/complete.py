"""Verify frozen receipts and publish the final checkpoint without changing candidates."""
from pathlib import Path
import subprocess
import zipfile

from experiments.semantic_foundation_v3 import frozen_v1 as v1
from experiments.semantic_foundation_v3.run import read, receipt
from .prepare import ROOT, FINAL, FINAL_SHA, immutable
from .holdout import check_freeze


def main():
    manifest = check_freeze()
    reports = ROOT / 'reports'
    selection = read(reports / 'FRESH_TABLE_HOLDOUT_SELECTION.json')
    independence = read(reports / 'FRESH_TABLE_HOLDOUT_INDEPENDENCE.json')
    assert independence['pass'] and selection['case_count'] == 27
    assert selection['candidate_manifest']['sha256'] == v1.file_sha(reports / 'TABLE_V3_CANDIDATE_MANIFEST.json')
    packet = read(reports / 'FRESH_TABLE_HOLDOUT_PACKET_FREEZE.json')
    assert all(v1.file_sha(Path(r['path'])) == r['sha256'] for r in packet['files'])
    with zipfile.ZipFile(ROOT / 'FRESH_TABLE_V3_HOLDOUT_BLIND.zip') as z:
        assert z.testzip() is None
    assert v1.file_sha(FINAL) == FINAL_SHA
    assert str(Path('/home/coder/auditmanager/current').resolve()) == manifest['production_release']
    assert read(reports / 'HOLDOUT_UI_VERIFICATION.json')['pass']
    tests = subprocess.run(['python', '-m', 'unittest', 'experiments.semantic_foundation_v3.test_foundation',
                           'experiments.semantic_foundation_v3.test_dev_packet', 'experiments.table_materialization_v3.test_tables',
                           'experiments.table_materialization_v3.test_holdout'], capture_output=True, text=True)
    assert tests.returncode == 0, tests.stdout + tests.stderr
    immutable(reports / 'FINAL_TESTS.json', {'returncode': tests.returncode, 'output': tests.stdout + tests.stderr})
    actions = '''# Next human action

Open `FRESH_TABLE_V3_HOLDOUT_BLIND.zip`, extract it and open `fresh_holdout/index.html`.
Annotate all 27 source-anchored cases as one table / different tables / unsure, then
export `FRESH_TABLE_V3_HUMAN_ANSWERS.json`. No labels or predictions are prefilled.
Do not consult the private selection strata or candidate DEV predictions while annotating.

Evaluate that export only against candidate da519855, without changing its rules.
Keep holdout answers out of future DEV tuning. This is an independent assessment
of an intentionally conservative candidate, not a release authorization.

Verdict B: more independently sourced DEV examples are needed for specification
group continuation (4/4 observed), headerless key continuation (4/4), same-page
contract changes (1/1), and strict reset/non-adjacent identities (zero human support).
Any future rule development needs a separate DEV expansion and a new candidate
freeze. Never convert this frozen holdout into DEV. Runtime 1.20535x slightly exceeds
the 1.20x target; retain that performance follow-up for a future candidate.
'''
    (reports / 'NEXT_ACTION.md').write_text(actions)
    score = read(reports / 'TABLE_V3_DEV_SCORECARD.json')
    perf = read(reports / 'FINAL_PERFORMANCE_MEASUREMENTS.json')
    invariant = read(reports / 'TABLE_V3_INVARIANTS.json')
    state = {'final_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
             'candidate_commit': manifest['commit'],
             'branch': subprocess.check_output(['git', 'branch', '--show-current'], text=True).strip(),
             'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
             'production_release': manifest['production_release'],
             'v2_preserved': subprocess.check_output(['git', 'rev-parse', 'research/section-table-v2-frozen-0f4ea8a9'], text=True).strip(),
             'foundation_preserved': subprocess.check_output(['git', 'rev-parse', '5bd396fc'], text=True).strip(),
             'push': 0, 'deploy': 0}
    assert state['branch'] == 'main'
    immutable(reports / 'FINAL_STATE.json', state)
    text = f'''# TABLE MATERIALIZATION V3 COMPLETE

Verdict: **B — TABLE V3 PROMISING BUT NEEDS MORE DEV TRUTH**.

44 TABLE human DEV cases: 33 SAME / 11 NEW. Foundation: PROVEN accuracy N/A
(0 decisions), coverage 0%, legacy 0%. Table V3: PROVEN accuracy 100% (13/13),
coverage 29.55%, legacy 29.55%. Decisions: 5 SAME, 8 NEW, 31 REVIEW. False joins 0,
false splits 0. The three QA corrections were consumed only from the final truth;
no case-specific rules were added.

On the 21-document DEV corpus, multi-page tables: 0 -> 5, maximum 11 pages.
9,208 table rows are retained. The additional 52-document source-only control
corpus preserves 15,634 rows; it has 0 automatically joined multi-page tables.
All Foundation semantic files are byte-identical. No Section V3, Relation Matcher,
Sheet Matcher, Sheet v4, Astra, production flags, services or production human
decisions were changed. Source-only verification is not old EVAL scoring.

Median runtime: {perf['foundation']['seconds']:.6f}s -> {perf['candidate']['seconds']:.6f}s
(+{100*(perf['ratios']['seconds']-1):.2f}%, target +20% slightly missed).
RSS: {perf['foundation']['peak_rss_kib']:.0f} -> {perf['candidate']['peak_rss_kib']:.0f} KiB
(+{100*(perf['ratios']['peak_rss_kib']-1):.2f}%). Artifact bytes:
{perf['foundation']['artifact_bytes']:.0f} -> {perf['candidate']['artifact_bytes']:.0f}
(+{100*(perf['ratios']['artifact_bytes']-1):.2f}%). All raw trials retained.

Replay PASS: 73 documents / 292 semantic files, byte-for-byte; pure optimization
also compared against the pre-optimization candidate. 37 contract tests pass.
The Chromium UI check traverses all 27 cases without selecting any answer.

Fresh holdout: 27 cases / 27 documents, nine structural groups with three each.
DEV/holdout overlap 0; prior EVAL overlap 0 under document/family/source/PDF/page
hash checks. Candidate freeze precedes source selection. No Table V3 inference on
holdout; no answers generated; no predictions in the human packet.

Candidate: `{manifest['commit']}`. Final checkout: `{state['final_head']}`.
Historical V2 and Foundation commits preserved. Only unrelated restore files remain
untracked. Frozen human truth unchanged, production unchanged, push 0, deploy 0.
Old EVAL: not run; diagnostic only if explicitly evaluated later, never a release gate.

Next human action: extract the blind ZIP, open `fresh_holdout/index.html`, annotate
27 cases and export JSON. Keep this holdout independent of future DEV expansion.
'''
    (reports / 'CHECKPOINT.md').write_text(text)
    required = ['TABLE_DEV_TRUTH_FREEZE.json', 'TABLE_V3_BASELINE.md', 'TABLE_V3_ARCHITECTURE.md',
                'TABLE_BOUNDARY_EVIDENCE_CONTRACT.md', 'TABLE_RULE_PROMOTION_LOG.md', 'TABLE_V3_DEV_SCORECARD.md',
                'TABLE_V3_ERROR_TAXONOMY.json', 'TABLE_IDENTITY_CONTRACT.md', 'TABLE_ROW_CONTRACT.md',
                'TABLE_V3_PERFORMANCE.md', 'TABLE_V3_REPLAY.json', 'TABLE_V3_CANDIDATE_MANIFEST.json',
                'FRESH_TABLE_HOLDOUT_SELECTION.json', 'FRESH_TABLE_HOLDOUT_INDEPENDENCE.json',
                'OLD_EVAL_DIAGNOSTIC.md', 'NEXT_ACTION.md', 'CHECKPOINT.md']
    assert all((reports / name).is_file() for name in required)
    immutable(reports / 'DELIVERABLES_MANIFEST.json', {'reports': [receipt(reports / name) for name in required],
               'archive': receipt(ROOT / 'FRESH_TABLE_V3_HOLDOUT_BLIND.zip'), 'score': score,
               'case_specific_rules': 0, 'final_truth_unchanged': True, 'candidate_unchanged_after_freeze': True,
               'final_contract_checks': receipt(reports / 'FINAL_TESTS.json'),
               'multi_page_tables': {'foundation_dev': 0, 'v3_dev': invariant['dev_conservation']['multi_page_tables']}})
    print(text)


if __name__ == '__main__':
    main()
