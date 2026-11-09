"""Read-only input preflight; writes an evaluator manifest, never starts models."""
import hashlib
import json
from pathlib import Path

CONTROL = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare():
    protocol = json.loads((CONTROL / 'protocol.json').read_text())
    manifest = json.loads((CONTROL / 'input_manifest.json').read_text())
    if digest(CONTROL / 'input_manifest.json') != protocol['input_manifest_sha256']:
        raise ValueError('Baseline input manifest changed')
    projects = []
    for project in manifest:
        version = Path(project['version_dir'])
        selected = {}
        for relative, expected in project['files'].items():
            # No findings, summaries, receipts, review decisions or vector graphs.
            if relative not in ('02_work/document.md', '02_work/document.pdf', '02_work/blocks.json') and not (
                relative.startswith('03_analysis/runs/control/blocks_stage02_100/')
                and (relative.endswith('.png') or relative.endswith('/index.json'))
            ):
                continue
            path = version / relative
            actual = digest(path)
            if actual != expected:
                raise ValueError(f'Input changed: {path}')
            selected[relative] = {'path': str(path), 'sha256': actual, 'bytes': path.stat().st_size}
        projects.append({'discipline': project['discipline'], 'project': project['project'],
                         'graphics_declared': project['graphics'], 'inputs': selected})
    return {
        'status': 'local_preflight_only_not_frozen_or_started',
        'projects': projects,
        'source_version_hashes_verified': True,
        'payload_content_and_index_isolation_verified': False,
        'baseline_commit': protocol['commit'],
        'baseline_models': protocol['stage_models'],
        'baseline_graphic_models': protocol['environment']['STAGE01_CODEX_ONLY_LEG_MODELS'],
        'candidate_model_policy': 'codex/gpt-6-astra only; Claude and OpenRouter disabled',
        'comparison_design': {
            'historical_control': 'Retention and regression reference; model topology differs, no causal attribution to methods.',
            'method_effect': 'Requires classic and candidate arms with identical Astra topology, effort, source inputs and output evaluation. Run neither arm until prepared and authorized.',
            'known_six': 'Development regression, never newly blind validation.',
            'new_projects': 'Separate transfer evaluation after candidate freeze, without prior answer exposure.',
        },
        'required_before_launch': [
            'Complete stage4 real controls and freeze candidate implementation/prompts.',
            'Inspect selected block metadata/index contents for answer leakage; rebuild clean graph from source.',
            'Create isolated input/output directories; never resume into the historical control output.',
            'Enforce Astra on every detector, judge, retry and targeted pass, without provider fallback.',
            'Resolve external payload approval; this manifest is not transmission authorization.',
            'Keep evaluator history and answer artifacts inaccessible to audit executors until outputs are sealed.',
            'Record actual process handle, per-stage status, request usage, retries, duration and final output hashes.',
        ],
        'metrics': ['confirmed technical recall', 'prior useful finding retention',
                    'repeated false claims', 'questions separately from technical findings',
                    'engineer review workload', 'tokens and monetary cost when available',
                    'elapsed time and retry count'],
        'new_model_calls': 0,
    }


if __name__ == '__main__':
    result = prepare()
    output = Path(__file__).with_name('six_project_comparison_preflight.json')
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'projects': len(result['projects']),
                      'graphics': sum(p['graphics_declared'] for p in result['projects']),
                      'files_verified': sum(len(p['inputs']) for p in result['projects']),
                      'status': result['status']}))
