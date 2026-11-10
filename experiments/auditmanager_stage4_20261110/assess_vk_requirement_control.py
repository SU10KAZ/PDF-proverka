"""Evaluator-only known-source checks, never imported by the model executor."""
import hashlib
import json
import shutil
from pathlib import Path
from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def assess():
    src = Path('/tmp/auditmanager_vk_requirement_control_20261110')
    out = Path(__file__).parent
    result = json.loads((src / 'result.json').read_text())
    data = result['json_data']
    manifest = json.loads((src / 'manifest.json').read_text())
    md = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/VK/versions/v002/02_work/document.md')
    assert hashlib.sha256(md.read_bytes()).hexdigest() == manifest['source_md_sha256']
    blocks = authoritative_text_blocks(md.read_text())
    quotes = []

    def walk(value):
        if isinstance(value, dict):
            if 'quote' in value and 'block_id' in value:
                quote = value['quote']
                quotes.append({'block_id': value['block_id'], 'quote': quote,
                               'exact_match': isinstance(quote, str) and quote in blocks.get(value['block_id'], '')})
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(data)
    finding = next(f for f in data['findings'] if f['id'] == 'F_K2_CLAMP_BALANCE')
    arithmetic = [(v['diameter'], v['rapid'], v['kralle'], v['difference'])
                  for v in finding['arithmetic']['by_diameter']]
    assert arithmetic == [('DN100', 7, 4, 3), ('DN150', 33, 18, 15), ('DN200', 26, 26, 0)]
    checks = [c for c in data['requirement_applicability_checks'] if c['requirement_id'] == 'R_K2_SML']
    assert {('O_K2_JOINTS', 'applies'), ('O_K21_JOINTS', 'does_not_apply')} <= {
        (c['object_id'], c['applicability']) for c in checks}
    validation = json.loads((src / 'validation.json').read_text())
    report = {
        'status': 'source_scope_control_pass_full_output_not_accepted',
        'historical_case': 'B-VK-F-003',
        'engineer_decision': 'Отдельное решение инженера по классической записи не установлено.',
        'model': 'codex/gpt-6-astra',
        **{k: result.get(k) for k in ('duration_ms', 'input_tokens', 'output_tokens', 'cost_source')},
        'new_independent_project_discovery': False, 'history_or_answers_supplied': False,
        'exact_quotes_checked': len(quotes),
        'exact_quotes_match': all(q['exact_match'] for q in quotes),
        'quote_failures': [q for q in quotes if not q['exact_match']],
        'positive_control': {
            'source_fact': 'K2 Rapid/Kralle 7/4 and33/18 with project all-SML-joints reinforcement instruction.',
            'conditional_differences': [3, 15], 'model_preserved': True,
            'physical_shortage_proven': False},
        'negative_controls': {
            'DN200_26_26_not_reported_as_shortage': True,
            'K21_requirement_not_transferred_from_K2': True,
            'K21_six_clamps_not_used_to_offset_K2': True},
        'scope_receipts': checks,
        'requirement_gate': {
            'checks': len(validation['checks']),
            'all_needs_data': all(c['status'] == 'needs_data' for c in validation['checks']),
            'reason': 'Unknown revisions and other context leave complete applicability unresolved; scope matching and quoted quantities remain separate facts.',
            'no_forced_metadata_repair': True},
        'findings_admission': {
            'F_K2_CLAMP_BALANCE': 'Retain bounded document quantity mismatch and assumptions; overlaps classic F-003, not a new unique defect.',
            'other_five_rows': 'Not accepted as final questions: internal retrieval and mixed claims require further context.'},
        'control_pair_kind': 'Real source component controls, not engineer-confirmed independent historical false/true pair.',
        'automatic_model_application': False,
        'full_six_project_acceptance': False, 'production_changed': False,
    }
    for source, target in [('result.json', 'VK_requirement_astra_result.json'),
                           ('manifest.json', 'VK_requirement_astra_manifest.json'),
                           ('validation.json', 'VK_requirement_astra_validation.json')]:
        shutil.copyfile(src / source, out / target)
    (out / 'VK_requirement_astra_assessment.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('status', 'exact_quotes_checked', 'exact_quotes_match')}))


if __name__ == '__main__':
    assess()
