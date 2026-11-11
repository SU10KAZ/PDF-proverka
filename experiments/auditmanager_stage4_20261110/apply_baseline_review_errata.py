"""Build a corrected evaluation view without rewriting historical review receipts."""
import copy
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = Path('/tmp/auditmanager_comparison_evaluation_20261111')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build():
    source = ROOT / 'baseline_r1/output_review_queue.json'
    assert digest(source) == '71662c80401cae97f4e29d34052c5d820cda17e14f4c40119a660435c65d5979'
    md = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111/itp_r1_classic/classic_control/inputs/ITP/versions/v001/02_work/document.md')
    assert digest(md) == 'e788ede36ab49e8129950f8967018d477b29e13e3cea9b122fbf6d4986212a35'
    block = md.read_text().split('### BLOCK #16 [TEXT]: blk_30789714316b48908c5addc9bfb135d2', 1)
    if len(block) != 2:
        # Block ordinal is presentation metadata; bind by stable block identifier.
        import re
        block_text = next(b for b in re.split(r'(?=### BLOCK #)', md.read_text())
                          if 'blk_30789714316b48908c5addc9bfb135d2' in b.splitlines()[0])
    else:
        block_text = block[1].split('### BLOCK #', 1)[0]
    position = block_text.split('| 33 |', 1)[1].split('| 34 |', 1)[0]
    assert 'Kvs=40 м3/ч, DN50 мм, PN25' in position
    original = json.loads(source.read_text())
    corrected = copy.deepcopy(original)
    row = next(r for r in corrected['rows'] if r['discipline'] == 'ITP' and r['finding_id'] == 'F-018')
    claim = next(c for c in row['claim_reviews'] if c['verdict'] == 'not_explicit_in_checked_row')
    before = copy.deepcopy(claim)
    claim.update(verdict='confirmed_authoritative_TEXT_continuation_row',
                 limits='В продолжении позиции33 прямо указано Kvs=40 м3/ч, DN50 мм, PN25. Прежняя оценка ошибочно учитывала только первую строку. Это исправление проверяющего, не новая находка модели; фактическое исполнение клапана не установлено.')
    row['evidence'].append({'path': str(md), 'sha256': digest(md),
                            'route': 'authoritative_TEXT_review_erratum',
                            'block_id': 'blk_30789714316b48908c5addc9bfb135d2',
                            'quote': '| 33 |' + position})
    for a, b in zip(original['rows'], corrected['rows']):
        for field in ('finding', 'target_links', 'triage'):
            assert a[field] == b[field]
        if a is not None and (a['discipline'], a['finding_id']) != ('ITP', 'F-018'):
            assert a == b
    output = ROOT / 'baseline_r1/output_review_queue_corrected_v1.json'
    output.write_text(json.dumps(corrected, ensure_ascii=False, indent=2) + '\n')
    receipt = {
        'status': 'derived_review_view_with_one_evaluator_correction',
        'historical_queue_path': str(source), 'historical_queue_sha256': digest(source),
        'corrected_view_path': str(output), 'corrected_view_sha256': digest(output),
        'corrections': [{'discipline': 'ITP', 'repeat': 1, 'finding_id': 'F-018',
                         'before': before, 'after': claim,
                         'source_path': str(md), 'source_sha256': digest(md)}],
        'metric_policy': 'Use corrected view for repeat1 factual evaluation; apply the same source fact to all arms. Do not count evaluator-discovered context as model recall.',
        'historical_sources_mutated': False, 'frozen_evaluator_mutated': False,
        'runtime_mutated': False, 'model_calls': 0, 'production_acceptance': False,
    }
    assert digest(source) == receipt['historical_queue_sha256']
    return receipt


if __name__ == '__main__':
    result = build()
    target = HERE / 'baseline_r1_review_erratum_Kvs40.json'
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, ensure_ascii=False))
