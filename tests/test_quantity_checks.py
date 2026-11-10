from backend.app.pipeline.stages.text_analysis.quantity_checks import evaluate_quantity_checks


def value(n, **extra):
    return dict(object='board', parameter='load', unit='kW', basis='active installed',
                regime='normal', value=n, source_refs=['b'], **extra)


def test_exact_sum_no_binary_float_error():
    op = {'id': 'Q1', 'kind': 'sum', 'terms': [value('0.1'), value('0.2')], 'target': value('0.3')}
    row = evaluate_quantity_checks([op], {'b'})['operations'][0]
    assert row['numerically_equal'] and row['source_interpretation_verified'] is False
    op['target']['value'] = '0.4'
    assert evaluate_quantity_checks([op], {'b'})['operations'][0]['difference'] == '-0.1'


def test_different_basis_unknown_source_and_nonfinite_are_not_computed():
    op = {'id': 'Q1', 'kind': 'compare', 'terms': [value('3')], 'target': value('3.5')}
    op['terms'][0]['basis'] = 'different physical surface'
    assert evaluate_quantity_checks([op], {'b'})['not_checked'] == 1
    op['terms'][0]['basis'] = 'active installed'
    assert evaluate_quantity_checks([op], set())['not_checked'] == 1
    op['target']['value'] = 'NaN'
    assert evaluate_quantity_checks([op], {'b'})['not_checked'] == 1
    assert evaluate_quantity_checks([op, op], {'b'})['not_checked'] == 2


def test_widely_separated_exponents_preserve_small_difference():
    tiny = '1.' + '1' * 65 + 'e-100'
    op = {'id': 'Q2', 'kind': 'sum', 'terms': [value('1e100'), value(tiny)],
          'target': value('1e100')}
    row = evaluate_quantity_checks([op], {'b'})['operations'][0]
    from decimal import Decimal
    assert Decimal(row['difference']) == Decimal(tiny)
    assert row['term_sources'] == [['b'], ['b']]
    assert row['target_sources'] == ['b']


def test_empty_operation_id_is_not_a_traceable_check():
    op = {'id': ' ', 'kind': 'compare', 'terms': [value('3')], 'target': value('3')}
    assert evaluate_quantity_checks([op], {'b'})['not_checked'] == 1


def test_declared_inventory_detects_omitted_and_repeated_rows():
    op = {'id':'sum', 'kind':'sum', 'expected_term_ids':['row1','row2'],
          'terms':[value('2',term_id='row1'),value('3',term_id='row2')], 'target':value('5')}
    row = evaluate_quantity_checks([op], {'b'})['operations'][0]
    assert row['numerically_equal']
    assert row['term_ids'] == ['row1','row2'] and row['term_values'] == ['2','3']
    assert not row['source_interpretation_verified']
    op['terms'].pop()
    row = evaluate_quantity_checks([op], {'b'})['operations'][0]
    assert row['status'] == 'not_checked' and row['missing_term_ids'] == ['row2']
    op['terms'].append(value('3',term_id='row1'))
    assert evaluate_quantity_checks([op], {'b'})['operations'][0]['reason'] == 'duplicate_term_identity'


def test_invalid_or_extra_inventory_members_do_not_pass():
    op = {'id':'sum','kind':'sum','terms':[value('2',term_id='unexpected')],
          'expected_term_ids':['row1'],'target':value('2')}
    assert evaluate_quantity_checks([op], {'b'})['operations'][0]['unexpected_term_ids'] == ['unexpected']
    op['expected_term_ids'] = ['row1', {}]
    assert evaluate_quantity_checks([op], {'b'})['operations'][0]['reason'] == 'invalid_term_inventory'
