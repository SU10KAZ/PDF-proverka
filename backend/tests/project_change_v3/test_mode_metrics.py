from copy import deepcopy
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import engine, mode_metrics, run_storage
from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401
from backend.tests.project_change_v3.test_subject_search import SubjectFake, handlers, run


def test_offline_comparison_checks_source_identity_and_does_not_choose_winner(env, monkeypatch):
    from scripts.compare_projectchange_modes import load
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, '0')
    set_test_provider(FakeProvider(handlers=gf.fake_handlers()))
    standard = engine.run_v3_pipeline(session_id=env['session_id'], pair_id=gf.PAIR_ID)
    assert standard['reason_code'] == 'v3_completed'
    set_test_provider(SubjectFake(handlers=handlers()))
    subject = run(env)
    assert subject['reason_code'] == 'v3_completed'
    admitted = {}
    for side, key in [('old', 'left'), ('new', 'right')]:
        pdf = Path(env[key]['pdf_path'])
        admitted[side] = {'artifacts': {'pdf': {'sha256': run_storage.sha(pdf)},
                                       'work_md': {'path': str(pdf.with_suffix('.md'))}}}
    a, a_hash = load(run_storage.run_dir(env['session_id'], gf.PAIR_ID, standard['run_id']), admitted)
    b, b_hash = load(run_storage.run_dir(env['session_id'], gf.PAIR_ID, subject['run_id']), admitted)
    assert a_hash == b_hash  # run-specific absolute crop paths do not change evidence identity
    report = mode_metrics.compare(a, b, source_equal=True)
    assert report['comparable'] and report['winner'] is None
    assert report['quality_adjudication'] == 'not_evaluated'
    assert len({row['run_id'] for row in report['runs']}) == 2
    changed = deepcopy(b)
    changed[0]['provenance']['model'] = 'other'
    report = mode_metrics.compare(a, changed, source_equal=False)
    assert not report['comparable']
    assert {'different_model', 'different_sources'} <= set(report['limitations'])
    changed = deepcopy(b)
    changed[0]['provenance']['subject_recovery'] = {'run_id': 'previous'}
    assert not mode_metrics.compare(a, changed, source_equal=True)['comparable']
    admitted['new']['artifacts']['pdf']['sha256'] = 'other'
    with pytest.raises(PermissionError, match='source'):
        load(run_storage.run_dir(env['session_id'], gf.PAIR_ID, subject['run_id']), admitted)
