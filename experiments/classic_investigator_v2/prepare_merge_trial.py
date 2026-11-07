"""Freeze a paired, source-identical merge experiment across six disciplines.

Saved classic merge requests only; no historical answers or corrected outputs.
Model calls are made separately by run_context_trial.py after local preparation.
"""
import argparse
import copy
from pathlib import Path
from source_contract import read, write, sha


def prepare(work, trial, instruction):
    if trial.exists():
        raise ValueError('Existing trial cannot be overwritten')
    suffix = instruction.read_text()
    plans = []
    for index, disc in enumerate(('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM')):
        matches = []
        for path in sorted((work/'classic_control/requests'/disc).glob('*/request.json')):
            data = read(path)
            if data['options'].get('stage') == 'findings_merge':
                matches.append((path, data))
        if len(matches) != 1:
            raise ValueError(f'{disc}: expected exactly one frozen merge request')
        path, original = matches[0]
        if original['options'].get('model') != 'codex/gpt-6-sol':
            raise ValueError('Unexpected baseline model')
        if original['messages'][0]['role'] != 'system':
            raise ValueError('Missing original system instruction')
        # Alternate fixed order; one pair still cannot establish statistical reliability.
        order = ('control', 'candidate') if index % 2 == 0 else ('candidate', 'control')
        for arm in order:
            request = copy.deepcopy(original)
            request['options']['allowed_tools'] = ''
            if arm == 'candidate':
                request['messages'][0]['content'] += '\n\n' + suffix
            assert request['messages'][1:] == original['messages'][1:]
            plans.append((disc, arm, path, request))
    trial.mkdir(parents=True)
    payload = {'instruction_path': str(instruction.resolve()), 'instruction_sha256': sha(instruction),
               'instruction': suffix, 'disciplines': ['AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'],
               'design': 'One paired replay per discipline, alternating control/candidate order. '
                         'Same source messages and model/options except appended instruction. '
                         'Native tools disabled in both arms. Not an independent discovery audit.'}
    write(trial/'payload.json', payload)
    requests = []
    for index, (disc, arm, source, request) in enumerate(plans):
        path = trial/f'{index:02d}_request.json'
        write(path, request)
        requests.append({'path': str(path.resolve()), 'sha256': sha(path),
                         'model': request['options']['model'], 'discipline': disc, 'arm': arm,
                         'source_request': str(source.resolve()), 'source_request_sha256': sha(source)})
    write(trial/'protocol.json', {'graph_file': str((trial/'payload.json').resolve()),
          'graph_sha256': sha(trial/'payload.json'), 'requests': requests,
          'preparer_sha256': sha(__file__), 'scope': 'Merge-only comparison of completed classic inputs. '
          'No targeted passes, atomic guard or final verification in this replay. '
          'Known development projects; no historical engineer answers supplied. '
          'Earlier KM probe used augmented inputs and is not the KM control for this experiment.',
          'production_changed': False, 'historical_answers_supplied': False})
    print('Prepared', len(requests), 'requests; no model calls made')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--trial', type=Path, required=True)
    parser.add_argument('--instruction', type=Path, required=True)
    args = parser.parse_args()
    prepare(args.work, args.trial, args.instruction)
