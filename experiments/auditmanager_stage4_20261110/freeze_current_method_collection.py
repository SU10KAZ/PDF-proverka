"""Freeze a new 54-run separated-method measurement on complete source inputs."""
import argparse
import json
from pathlib import Path
import subprocess
from .materialize_trial_inputs import sha, write
from .paired_runner import load_harness, method_profile, REPOSITORY
from .paired_protocol import shared_manifest, verify_shared_protocol, verify_trial_binding

HERE = Path(__file__).resolve().parent
ARMS = ('classic_current', 'inventory', 'source_review')
DISCIPLINES = ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM')
BASELINE = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')


def ref(p):
    return {'path': str(p.resolve()), 'sha256': sha(p)}


def freeze(root):
    root = root.resolve()
    trial_path = root/'experimental_collection_trial.json'
    assert not trial_path.exists(), 'Immutable trial already exists'
    snapshot_path = HERE/'current_candidate_snapshot_provenance.json'
    snapshot = json.loads(snapshot_path.read_text())
    decisions_path = HERE/'experimental_arm_decisions_current.json'
    decisions = json.loads(decisions_path.read_text())
    assert decisions['arms']['candidate']['decision'] == 'held'
    for binding in decisions['bindings']:
        assert sha(Path(binding['path'])) == binding['sha256']
    inputs_path = HERE/'current_method_trial_input_checks.json'
    inputs = json.loads(inputs_path.read_text())
    assert inputs['runs'] == 54 and inputs['equal_inputs_across_arms_and_repeats']
    layout = json.loads((root/'layout.json').read_text())
    index = {(r['repeat'], r['discipline'], r['variant']): r for r in layout['runs']}
    runs = []
    for repeat in (1, 2, 3):
        order = ARMS[repeat-1:]+ARMS[:repeat-1]
        for disc in DISCIPLINES:
            for arm in order:
                assert decisions['arms'][arm]['decision'].startswith('eligible_for_')
                row = index[(repeat, disc, arm)]; control = Path(row['work'])/'classic_control'
                assert not (control/'protocol.json').exists() and not (control/'status').exists()
                manifest_path = control/'input_manifest.json'
                item, = json.loads(manifest_path.read_text())
                for name, digest in item['files'].items():
                    assert sha(Path(item['version_dir'])/name) == digest
                runs.append({'work': row['work'], 'variant': arm, 'discipline': disc,
                             'repeat': repeat, 'input_manifest_sha256': sha(manifest_path)})
    configs = {arm: {'method_config': method_profile(arm)[0],
                     'method_environment': method_profile(arm)[1]} for arm in ARMS}
    trial = {'status': 'frozen_comparative_execution',
        'scope': 'separate_experimental_methods_current_sources_not_combined_acceptance',
        'evaluator': ref(HERE/'evaluator_release_current_methods_20261112.json'),
        'profile_disagreements': ref(HERE/'EOM_F001_F030_current_profile_disagreement.json'),
        'acceptance_protocol': ref(HERE/'original_trial_protocol.json'),
        'source_snapshot': ref(snapshot_path), 'experimental_decisions': ref(decisions_path),
        'preflight_checks': ref(HERE/'current_snapshot_preflight.json'),
        'source_input_checks': ref(inputs_path),
        'historical_baseline_trial': ref(BASELINE/'baseline_collection_trial.json'),
        'historical_baseline_root': str(BASELINE),
        'freeze_script': ref(Path(__file__)),
        'dispatcher': ref(HERE/'run_experimental_collection.py'),
        'shared_executor_files': shared_manifest(REPOSITORY),
        'codex_cli_version': subprocess.check_output(['codex','--version'], text=True).strip(),
        'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
        'max_request_attempts': 3, 'retry_delay_seconds': 10,
        'project_parallelism': 1, 'graphic_parallelism': 2,
        'arm_configuration': configs, 'runs': runs,
        'dispatch_order': 'Three repeats; AK/ITP/AR/KM/VK/EOM; arm order rotated by repeat.',
        'failure_policy': 'Stop after nonzero exit or incomplete execution. No automatic whole-audit restart.',
        'isolation': 'Fresh model contexts, inline sources/images only; shell, image tools, apps, skills, memories and subagents disabled. No historical findings or engineer answers supplied.',
        'analysis_policy': 'Seal every output before semantic matching. Do not tune this frozen code from intermediate results. Retain all failures and pending obligations.',
        'profile_scoring': 'Keep original section labels and report disagreement sensitivity for EOM F001/F030 separately; factual preservation and unsupported consequences remain scored. No hidden relabeling.',
        'comparisons': ['inventory vs classic_current: inventory/operations/context bundle',
                        'source_review vs classic_current: primary TEXT handoff/source review/final check bundle'],
        'historical_baseline_limit': '18 prior successful audits are DEV-known references, not a paired timing baseline for changed input preparation or overlapping historical workloads.',
        'cost_policy': 'Capture every attempt and token/cache counts; wall time per audit. Subscription cost and quota percentage unknown, not zero.',
        'remaining_acceptance': ['All original 37 control outcomes remain explicit; partial is not passed.',
            'Combined candidate remains held pending constituent acceptance.',
            'New-project portability, independent engineering workload and production gates remain required.'],
        'stage4_complete': False, 'production_acceptance': False}
    write(trial_path, trial)
    harness = load_harness(snapshot['source_root'], 'classic_current')
    for row in runs:
        arm = row['variant']; work = Path(row['work'])
        harness.METHOD_CONFIG, env = method_profile(arm); harness.ENV.update(env)
        harness.freeze(work, source_snapshot=snapshot)
        path = work/'classic_control/protocol.json'; protocol = json.loads(path.read_text())
        protocol.update(shared_execution={'variant': arm, 'source_root': snapshot['source_root'],
                                         'source_files': trial['shared_executor_files']},
                        comparison_trial=ref(trial_path),
                        scope='Current source snapshot; separated method bundle: '+arm,
                        excluded='External normative vault, optimization, automatic grouping and combined candidate.')
        path.write_text(json.dumps(protocol, ensure_ascii=False, indent=2)+'\n')
        verify_shared_protocol(REPOSITORY, snapshot['source_root'], work, arm)
        verify_trial_binding(work, arm, row['discipline'])
    receipt = {'status': '54_current_method_runs_frozen_not_started', 'trial': ref(trial_path),
               'runs': 54, 'model_calls': 0, 'stage4_complete': False, 'production_acceptance': False}
    write(HERE/'current_method_collection_freeze.json', receipt)
    print(json.dumps(receipt))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    freeze(parser.parse_args().root)
