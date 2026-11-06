"""Prepare an additive downstream retention probe from a saved source-only finding.

This deliberately bypasses detector adjudication: it tests merge/verify retention,
not the full acceptance pipeline and not an independent audit.
"""
import argparse
import copy
from pathlib import Path
import shutil
from source_contract import read, write, sha
import run_operation_arm as executor


def append_candidate(graph, compact, block_id, candidate):
    graph, compact = copy.deepcopy(graph), copy.deepcopy(compact)
    if not candidate.get('id') or not candidate.get('finding'):
        raise ValueError('Candidate identity/text required')
    for data, key in ((graph, 'block_analyses'), (compact, 'blocks')):
        if any(f.get('id') == candidate['id'] for b in data[key] for f in b.get('findings', [])):
            raise ValueError('Candidate ID already exists')
        matches = [b for b in data[key] if b.get('block_id') == block_id]
        if len(matches) != 1:
            raise ValueError('Target block must exist exactly once')
        matches[0]['findings'].append(copy.deepcopy(candidate))
    return graph, compact


def prepare(work, disc, response, block_id):
    raw = read(response)
    findings = raw.get('json', {}).get('findings', [])
    if raw.get('error') or len(findings) != 1:
        raise ValueError('Probe requires exactly one saved successful detector finding')
    project = next(p for p in read(work/'classic_control/input_manifest.json') if p['discipline'] == disc)
    source_version, source_out = Path(project['version_dir']), Path(project['output_dir'])
    graph, compact = read(source_out/'01_blocks_analysis.json'), read(source_out/'01_blocks_for_text.json')
    numbers = [int(f['id'][2:]) for b in graph['block_analyses'] for f in b.get('findings', []) if f['id'].startswith('G-')]
    model_finding = findings[0]
    candidate = {'id': f'G-{max(numbers, default=0)+1:03d}',
                 'severity': model_finding['severity'], 'category': model_finding['category'],
                 'finding': model_finding['finding'] + '\n\nРекомендация: ' + model_finding.get('recommendation', ''),
                 'norm': None, 'norm_quote': model_finding.get('norm_quote'),
                 'value_found': model_finding.get('value_found'), 'block_evidence': block_id,
                 'highlight_regions': [], 'provenance': {'found_by': ['codex'],
                 'detector_summary': 'saved source-only context replay; downstream retention probe',
                 'model': raw.get('usage', {}).get('model'), 'response_sha256': sha(response)}}
    graph, compact = append_candidate(graph, compact, block_id, candidate)
    arm = work/'operation_arm'/disc
    arm.mkdir(parents=True, exist_ok=False)
    root = arm/'classic_control'
    version = root/'inputs'/disc/'versions'/source_version.name
    out = version/'03_analysis/runs/control'
    for name, digest in project['files'].items():
        src = source_version/name
        if sha(src) != digest:
            raise ValueError(f'Frozen source changed: {name}')
        dst = version/name; dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
    out.mkdir(parents=True, exist_ok=True)
    write(out/'01_blocks_analysis.json', graph)
    write(out/'01_blocks_for_text.json', compact)
    shutil.copyfile(source_out/'02_text_analysis.json', out/'02_text_analysis.json')
    for path in version.rglob('*.json'):
        path.write_text(path.read_text().replace(str(source_version), str(version)))
    write(arm/'injection.json', {'kind': 'saved_graphic_candidate', 'candidate': candidate,
          'source_response': str(response), 'source_response_sha256': sha(response),
          'baseline_inputs': {n: sha(source_out/n) for n in
          ['01_blocks_analysis.json', '01_blocks_for_text.json', '02_text_analysis.json']},
          'judge_and_evidence_gate_replayed': False})
    (arm/'STOP_REQUESTED.json').symlink_to(work/'STOP_REQUESTED.json')
    baseline = read(work/'classic_control/protocol.json')
    write(root/'protocol.json', {'discipline': disc, 'version_dir': str(version), 'output_dir': str(out),
          'files': {str(p.relative_to(version)): sha(p) for p in version.rglob('*') if p.is_file()},
          'code_sha256': sha(executor.__file__), 'preparer_sha256': sha(__file__),
          'baseline_code_files': baseline['source_files'], 'stage_models': baseline['stage_models'],
          'injection_sha256': sha(arm/'injection.json'),
          'scope': 'Same finished classic candidates plus one actual saved source-only graphic finding; replay merge/verify only.',
          'limits': 'Known development control. Judge/evidence gate bypassed intentionally. No whole-project discovery credit or end-to-end acceptance. Check retention of all baseline useful atoms separately.',
          'source_history_supplied': False, 'production_changed': False})
    print(disc, candidate['id'], 'prepared downstream-only graphic retention probe')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--discipline', required=True)
    p.add_argument('--response', type=Path, required=True)
    p.add_argument('--block-id', required=True)
    a = p.parse_args()
    prepare(a.work, a.discipline, a.response, a.block_id)
