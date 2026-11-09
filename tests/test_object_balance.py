from backend.app.pipeline.stages.text_analysis.object_balance import evaluate_object_balances


def item(key, role='active'):
    return {'key': key, 'role': role, 'source_refs': ['b']}


def operation(left, right):
    return {'id': 'O1', 'kind': 'set_balance', 'scope': 'one board',
            'left': {'scope_refs': ['b'], 'items': left}, 'right': {'scope_refs': ['b'], 'items': right}}


def test_two_way_difference_preserves_execution_and_excludes_reserve():
    op = operation([item(['cable', 'FRHF', '5x35']), item(['spare'], 'reserve')],
                   [item(['cable', 'HF', '5x35'])])
    r = evaluate_object_balances([op], {'b'})['operations'][0]
    assert r['status'] == 'computed' and r['matched'] == 0
    assert r['only_left'][0]['key'][1] == 'FRHF'
    assert r['only_right'][0]['key'][1] == 'HF'
    assert r['reserves']['left'] == [['spare']]
    assert r['source_interpretation_verified'] is False


def test_missing_scope_unknown_source_and_duplicates_stay_unchecked():
    op = operation([item(['a']), item(['a'])], [])
    assert evaluate_object_balances([op], {'b'})['not_checked'] == 1
    op['left']['items'].pop()
    assert evaluate_object_balances([op], set())['not_checked'] == 1
    assert evaluate_object_balances([op, op], {'b'})['not_checked'] == 2
    assert evaluate_object_balances([], {'b'})['coverage_complete'] is False
