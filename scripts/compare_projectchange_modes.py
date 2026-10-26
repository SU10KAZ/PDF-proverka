#!/usr/bin/env python3
"""Offline metrics of two frozen runs on one admitted 272 DEV pair; zero model calls."""
import argparse
import copy
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.app.services.project_change_v3 import mode_metrics, run_storage
from backend.app.services.project_change_v3.subject_search import digest
from backend.app.services.project_change_v3.source_prep import parse_md_blocks
from experiments.project_change_272 import policy


def load(directory, admitted):
    manifest = run_storage.read(directory / 'run_manifest.json') or {}
    if manifest.get('state') not in run_storage.TERMINAL:
        raise ValueError('only completed frozen runs can be compared')
    # Bind metadata to the admitted pair before reading its results or source text.
    for side in ('old', 'new'):
        if manifest.get(side + '_pdf_sha256') != admitted[side]['artifacts']['pdf']['sha256']:
            raise PermissionError('run belongs to a different source or version')
    for artifact in manifest['artifacts'].values():
        if Path(artifact['ref']).name != artifact['ref']:
            raise ValueError('unsafe artifact name')
        if run_storage.sha(directory / artifact['ref']) != artifact['sha256']:
            raise ValueError('frozen artifact changed')
    result = run_storage.read(directory / 'project_change_v3_result.json')
    state = run_storage.read(directory / 'state.json')
    structure_path = directory / 'project_change_v3' / 'DOCUMENT_STRUCTURE.json'
    source_manifest = run_storage.read(directory / 'project_change_v3_source_manifest.json')
    if run_storage.sha(structure_path) != source_manifest['structure_sha256']:
        raise ValueError('prepared structure changed')
    sources = copy.deepcopy(run_storage.read(structure_path))
    markdown = {side.upper(): parse_md_blocks(Path(admitted[side]['artifacts']['work_md']['path']))
                for side in ('old', 'new')}
    for page in sources:
        for block in page['blocks']:
            if block.get('text_source') == 'STRUCTURED_MD' and block['structured_md'] != markdown[page['side']].get(block['block_id']):
                raise ValueError('run Markdown differs from admitted baseline')
            if not block.get('graphic_crop_ref'):
                continue
            # Physical source layout is deterministic; never follow an arbitrary
            # path from an imported response to hash unrelated files.
            record_path = directory / 'project_change_v3' / 'source' / page['side'].lower() / f"p{page['physical_page']:03d}" / 'page.json'
            record = run_storage.read(record_path)
            if record is None:
                raise ValueError('source record missing: ' + str(record_path))
            source = next(b for b in record['blocks'] if b['block_id'] == block['block_id'])
            crop = record_path.parent / (block['block_id'] + '.png')
            if not crop.resolve().is_relative_to(record_path.parent.resolve()):
                raise ValueError('unsafe block ID')
            if run_storage.sha(crop) != source['graphic_crop_sha256']:
                raise ValueError('graphic crop changed')
            block['graphic_crop_ref'] = source['graphic_crop_sha256']
    return (result, state), digest(sources)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dev-pair', type=int, required=True)
    parser.add_argument('--corpus-root', type=Path)
    parser.add_argument('--standard-run', type=Path, required=True)
    parser.add_argument('--subjects-run', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    # No validation/final reserve access or foreign-project tuning.
    pairs = policy.admitted_pairs('DEV', indices=[args.dev_pair],
                                  **({'root': args.corpus_root} if args.corpus_root else {}))
    [pair] = pairs
    if any(pair['embargo_pages'].values()):
        raise PermissionError('whole-pair comparison contains embargoed pages')
    left, left_sha = load(args.standard_run, pair)
    right, right_sha = load(args.subjects_run, pair)
    report = mode_metrics.compare(left, right, source_equal=left_sha == right_sha)
    report['admission'] = {'partition': 'DEV', 'pair_index': args.dev_pair,
                           'historically_known': True, 'model_calls_by_this_command': 0}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'comparable': report['comparable'], 'limitations': report['limitations'],
                      'quality_adjudication': report['quality_adjudication'], 'output': str(args.output)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
