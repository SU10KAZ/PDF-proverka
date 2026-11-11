"""Seal evaluator-only targets and explicit holds before comparison outputs exist.

This does not freeze runtime, establish input availability, or score any run.
Never mount the resulting file in an independent investigator's environment.
"""
import argparse
import hashlib
import json
from pathlib import Path

from experiments.auditmanager_stage4_20261110.build_evaluator_errata_view import build_view

TARGETS = Path('experiments/auditmanager_stage1_20261109/consolidated/evaluation_targets.json')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def assemble(view, release, targets, errata, *, version="AM-SIX-EVALUATOR-ERRATA-1"):
    """Validate the complete target inventory before creating a snapshot."""
    rows = targets['targets']
    by_id = {row['target_id']: row for row in rows}
    if len(by_id) != len(rows):
        raise ValueError('Duplicate canonical target ID')
    original = view['original_denominators']
    working = view['working_truth_denominators']
    original_ids = [i for ids in original.values() for i in ids]
    if len(set(original_ids)) != len(original_ids) or set(original_ids) != set(by_id):
        raise ValueError('Release denominator does not exactly cover canonical targets')
    if original != release['denominators']:
        raise ValueError('View does not match frozen release denominators')
    if set(working) != set(original):
        raise ValueError('Changed route inventory')
    for route, ids in original.items():
        if any(by_id[i]['route'] != route for i in ids):
            raise ValueError('Target route disagrees with release')
        if len(set(working[route])) != len(working[route]) or not set(working[route]) <= set(ids):
            raise ValueError('Working denominator adds, duplicates, or moves targets')
    held_truth = {i for h in view['holds'] if 'truth_scoring' in h['held_dimensions'] for i in h['target_ids']}
    excluded = set(original_ids) - {i for ids in working.values() for i in ids}
    if excluded != held_truth:
        raise ValueError('Excluded targets must have an explicit truth-scoring hold')
    for alias, canonical in targets['aliases'].items():
        if alias in by_id or canonical not in by_id:
            raise ValueError('Alias is ambiguous or refers to a missing target')
    return {
        'version': version,
        'status': 'frozen_evaluator_only_not_trial_ready',
        'original_release': view['original_release'],
        'errata_source': view['errata'],
        'errata': errata,
        'targets': rows,
        'aliases': targets['aliases'],
        'theme_index': targets['theme_index'],
        'original_denominators': original,
        'truth_denominators': working,
        'holds': view['holds'],
        'counts': {route: len(ids) for route, ids in working.items()},
        'evaluation_contract': release['evaluation_contract'],
        'unscored_themes': release['unscored_themes'],
        'matching_policy': targets['matching_policy'],
        'required_before_scoring': [
            'Seal run inputs, code, prompts, model configuration and output hashes.',
            'Verify full input availability per target; unavailable is not a model miss.',
            'Adjudicate semantic matches with mandatory object, relation and location context.',
            'Keep partial matches, unsupported additions and new findings separate.',
            'Report factual coverage separately from section accuracy; apply dimension-specific holds.',
            'Prove candidate arrival and reasoned removal before giving filter credit.',
            'Measure engineer effort and transport usage; unknown cost is not zero.',
        ],
        'exposure': 'Six historical projects are DEV-known regression, never a blind portability test.',
        'observed_quality_metrics': None,
        'production_acceptance': False,
    }


def freeze(output, *, version="AM-SIX-EVALUATOR-ERRATA-1"):
    # build_view verifies all files in the original release, then reapplies errata.
    view = build_view()
    release_path = Path(view['original_release']['path'])
    release = json.loads(release_path.read_text())
    if not any(f['path'] == str(TARGETS) and f['sha256'] == sha(TARGETS) for f in release['files']):
        raise ValueError('Target inventory is not sealed by release')
    errata = json.loads(Path(view['errata']['path']).read_text())
    snapshot = assemble(view, release, json.loads(TARGETS.read_text()), errata, version=version)
    snapshot['target_source'] = {'path': str(TARGETS), 'sha256': sha(TARGETS)}
    output.parent.mkdir(parents=True, exist_ok=True)
    # Never overwrite a version after results have been observed.
    with output.open('x') as f:
        f.write(json.dumps(snapshot, ensure_ascii=False, indent=2) + '\n')
    return {'path': str(output), 'sha256': sha(output), 'counts': snapshot['counts']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--version', required=True)
    args=parser.parse_args()
    print(json.dumps(freeze(args.output, version=args.version), ensure_ascii=False))
