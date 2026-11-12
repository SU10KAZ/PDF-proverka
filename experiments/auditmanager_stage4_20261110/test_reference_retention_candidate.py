"""Known-source regression plus synthetic boundaries, not a historical control pair."""
import copy
import hashlib
import json
from pathlib import Path
import importlib.util
import pytest

BASE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('reference_retention_candidate', BASE / 'reference_retention_candidate.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

@pytest.fixture
def case():
    trace = json.loads((BASE / 'AR_calculation_reference_trace.json').read_text())
    bundle = json.loads((BASE / 'ar_rope_bundle_source_evidence/bundle.json').read_text())
    evidence = trace['source_quote']
    # Exact authoritative quote is a bounded TEXT fragment, not a re-extraction.
    text = evidence['quote']
    assert text in bundle['md']
    sha = hashlib.sha256(bundle['md'].encode()).hexdigest()
    obligation = dict(reference='КМ8', role='calculation_context',
        block_id=evidence['block_id'], quote=text,
        block_sha256=hashlib.sha256(text.encode()).hexdigest(),
        source_md_sha256=sha,
        selection_reason='Known-control C8 explicitly requests calculation from existing KM8 reference.')
    return trace['approved_user_fields'], [obligation], dict(
        source_blocks={evidence['block_id']: {'type': 'TEXT', 'text': text}}, source_md_sha256=sha)


def test_real_omission_requires_repair_without_suppression(case):
    fields, obligations, kwargs = case
    before = copy.deepcopy(case)
    result = module.check_reference_retention(fields, obligations, **kwargs)
    assert result['status'] == 'repair_required'
    assert result['issues'][0]['reference'] == 'КМ8'
    assert not result['suppress_finding'] and not result['automatic_rewrite']
    assert case == before

@pytest.mark.parametrize('reference', ['КМ8', 'КМ 8'])
def test_explicit_question_passes_only_literal_check(case, reference):
    fields, obligations, kwargs = case
    fields['solution'] += f' Проверить применимый расчёт по ссылке {reference}.'
    result = module.check_reference_retention(fields, obligations, **kwargs)
    assert result['status'] == 'literal_retention_verified'
    assert not result['semantic_acceptance']

@pytest.mark.parametrize('reference', ['КМ80', 'КМ8.1', 'КМ8 . 1', 'КМ9.2'])
def test_other_document_does_not_satisfy_reference(case, reference):
    fields, obligations, kwargs = case
    fields['solution'] += f' Проверить {reference}.'
    assert module.check_reference_retention(fields, obligations, **kwargs)['status'] == 'repair_required'


def test_unselected_catalogue_references_are_not_automatically_required(case):
    fields, _, kwargs = case
    assert module.check_reference_retention(fields, [], **kwargs)['status'] == 'not_assessed'

@pytest.mark.parametrize('mutation', ['revision', 'quote', 'block', 'graphic'])
def test_unbound_source_cannot_supply_obligation(case, mutation):
    fields, obligations, kwargs = case
    if mutation == 'revision': obligations[0]['source_md_sha256'] = 'different'
    if mutation == 'quote': obligations[0]['quote'] = 'См. КМ8 без подтверждения источником'
    if mutation == 'block': obligations[0]['block_sha256'] = 'different'
    if mutation == 'graphic': kwargs['source_blocks'][obligations[0]['block_id']]['type'] = 'GRAPHIC'
    with pytest.raises(ValueError): module.check_reference_retention(fields, obligations, **kwargs)
