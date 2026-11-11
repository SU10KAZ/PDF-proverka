"""Freeze separate measurement arms; combined method remains ineligible."""
import json
from pathlib import Path
import subprocess
from .materialize_trial_inputs import sha
from .paired_runner import load_harness, method_profile, REPOSITORY
from .paired_protocol import shared_manifest, verify_shared_protocol, verify_trial_binding

HERE = Path(__file__).resolve().parent
ROOT = Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
ARMS = ('classic_current', 'inventory', 'source_review')
DISCIPLINES = ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM')


def ref(p):
    return {'path': str(p.resolve()), 'sha256': sha(p)}


def main():
    trial_path = ROOT / 'experimental_collection_trial.json'
    if trial_path.exists():
        raise ValueError('Existing trial is immutable; no silent overwrite')
    snapshot_path = HERE / 'candidate_retrieval_v2_snapshot_provenance.json'
    snapshot = json.loads(snapshot_path.read_text())
    decisions_path = HERE / 'experimental_arm_decisions_v2.json'
    decisions = json.loads(decisions_path.read_text())
    assert decisions['arms']['candidate']['decision'] == 'held'
    for arm in ARMS:
        assert decisions['arms'][arm]['decision'].startswith('eligible_for_')
    for item in decisions['bindings']:
        assert sha(Path(item['path'])) == item['sha256']
    layout = json.loads((ROOT / 'layout.json').read_text())
    index = {(r['repeat'], r['discipline'], r['variant']): r for r in layout['runs']}
    runs = []
    for repeat in (1, 2, 3):
        # Rotate the within-project arm order across repeats, then keep all runs serial.
        order = ARMS[repeat-1:] + ARMS[:repeat-1]
        for discipline in DISCIPLINES:
            for arm in order:
                r = index[(repeat, discipline, arm)]
                control = Path(r['work']) / 'classic_control'
                assert not (control / 'protocol.json').exists() and not (control / 'status').exists()
                manifest_path = control / 'input_manifest.json'
                m, = json.loads(manifest_path.read_text())
                for name, digest in m['files'].items():
                    assert sha(Path(m['version_dir']) / name) == digest
                runs.append({'work': r['work'], 'variant': arm, 'discipline': discipline,
                             'repeat': repeat, 'input_manifest_sha256': sha(manifest_path)})
    assert len(runs) == 54
    configs = {arm: {'method_config': method_profile(arm)[0], 'method_environment': method_profile(arm)[1]}
               for arm in ARMS}
    trial = {
        'status': 'frozen_comparative_execution', 'scope': 'separate_experimental_methods_not_combined_acceptance',
        'evaluator': ref(HERE / 'evaluator_release_errata_v2.json'),
        'acceptance_protocol': ref(HERE / 'original_trial_protocol.json'),
        'source_snapshot': ref(snapshot_path), 'experimental_decisions': ref(decisions_path),
        'preflight_checks': ref(HERE / 'retrieval_v2_preflight_checks.json'),
        'source_input_checks': ref(HERE / 'experimental_input_copies_v2_check.json'),
        'historical_baseline_trial': ref(ROOT / 'baseline_collection_trial.json'),
        'freeze_script': ref(Path(__file__)),
        'shared_executor_files': shared_manifest(REPOSITORY),
        'model': 'codex/gpt-6-astra', 'reasoning_effort': 'high',
        'codex_cli_version': subprocess.check_output(['codex', '--version'], text=True).strip(),
        'max_request_attempts': 3, 'retry_delay_seconds': 10,
        'project_parallelism': 1, 'graphic_parallelism': 2,
        'arm_configuration': configs,
        'dispatch_order': 'For each repeat AK ITP AR KM VK EOM. Within each project: r1 classic_current/inventory/source_review; r2 inventory/source_review/classic_current; r3 source_review/classic_current/inventory.',
        'start_condition': 'Historical collection must finish all 18 runs successfully; verify process exit, completion receipts and seals before first experimental dispatch.',
        'failure_policy': 'Stop the collection on any nonzero exit or non-completed run. Preserve all evidence. No automatic whole-audit retry or restart.',
        'load_policy': 'One full audit at a time, no overlapping model pilots. Log unrelated concurrency if observed. Historical baseline overlapped pilots and is not valid alone for the p95 timing acceptance gate.',
        'comparisons': ['classic_current vs historical classic: common foundation changes',
                        'inventory vs classic_current: inventory/operations/context bundle',
                        'source_review vs classic_current: source review plus independent final check'],
        'source_access': 'Only inline project sources and supplied graphics; tool-less direct Codex CLI, no answers/history, Claude or OpenRouter. External-source availability remains explicit.',
        'results_policy': 'Seal candidates, final output, coverage and request receipts before semantic evaluation; never use findings to tune this frozen candidate mid-series.',
        'cost_policy': 'Record request/token/cache/retry counts and wall time; subscription monetary price remains unknown. Draft per-method bounds are fixed in arm_configuration, not proof of acceptable cost.',
        'known_limits': ['Known six projects are DEV regressions, not new blind validation.',
            'All 37 control outcomes and disputes remain as recorded in experimental_decisions.',
            'F014 invalid negative control remains excluded by frozen evaluator v2.',
            'Source-only access and omitted optimization/external norms limit the supported comparison scope.',
            'Combination, new-project portability, independent engineer workload and production acceptance are not established.'],
        'runs': runs, 'stage4_complete': False, 'production_acceptance': False,
    }
    with trial_path.open('x') as f:
        f.write(json.dumps(trial, ensure_ascii=False, indent=2) + '\n')
    harness = load_harness(snapshot['source_root'], 'classic_current')
    for r in runs:
        arm = r['variant']; work = Path(r['work'])
        harness.METHOD_CONFIG, env = method_profile(arm)
        harness.ENV.update(env)
        harness.freeze(work, source_snapshot=snapshot)
        path = work / 'classic_control/protocol.json'
        p = json.loads(path.read_text())
        p['shared_execution'] = {'variant': arm, 'source_root': snapshot['source_root'],
                                 'source_files': trial['shared_executor_files']}
        p['comparison_trial'] = ref(trial_path)
        p['scope'] = 'Frozen candidate foundation plus explicitly selected optional method family: ' + arm
        p['excluded'] = 'Optimization, external normative vault, automatic grouping, combined candidate.'
        p['limits'] = 'Experimental measurement only; preserve unresolved control and source-availability limits.'
        assert p['method_config'] == configs[arm]['method_config']
        path.write_text(json.dumps(p, ensure_ascii=False, indent=2) + '\n')
        verify_shared_protocol(REPOSITORY, snapshot['source_root'], work, arm)
        verify_trial_binding(work, arm, r['discipline'])
    receipt = {'status': '54_experimental_runs_frozen_not_started', 'trial': ref(trial_path),
               'runs': 54, 'model_calls': 0, 'combined_candidate_admitted': False,
               'stage4_complete': False, 'production_acceptance': False}
    (HERE / 'experimental_collection_v2_freeze_receipt.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == '__main__':
    main()
