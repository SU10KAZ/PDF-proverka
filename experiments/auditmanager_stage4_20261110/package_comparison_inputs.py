"""Stage source-only comparison inputs locally. Does not invoke any model."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

from experiments.auditmanager_stage4_20261110.check_six_project_indexes import check


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def package(destination):
    root = Path(__file__).resolve().parent
    manifest_path = root / 'six_project_comparison_preflight.json'
    manifest = json.loads(manifest_path.read_text())
    destination = destination.resolve()
    # Refuse to resume or overwrite either a completed or interrupted package.
    destination.mkdir(parents=True, exist_ok=False)
    receipt = {'status': 'building', 'root': str(destination),
               'preflight_sha256': sha(manifest_path), 'projects': [],
               'new_model_calls': 0, 'execution_authorized': False,
               'filesystem_access_isolation_verified': False}
    receipt_path = root / 'six_project_input_package.json'
    try:
        for project in manifest['projects']:
            validation = check(project)
            if validation['status'] != 'passed':
                raise ValueError('Crop validation failed: ' + project['discipline'])
            target = destination / project['discipline']
            work = target / '02_work'
            crops = target / '03_analysis' / 'runs' / 'candidate' / 'blocks_stage02_100'
            work.mkdir(parents=True)
            crops.mkdir(parents=True)
            copied = {}
            def copy(source_entry, dest):
                source = Path(source_entry['path'])
                if sha(source) != source_entry['sha256']:
                    raise ValueError('Changed source: ' + str(source))
                shutil.copyfile(source, dest)
                actual = sha(dest)
                if actual != source_entry['sha256']:
                    raise ValueError('Copy mismatch: ' + str(dest))
                copied[str(dest.relative_to(target))] = actual
            for name in ('document.md', 'document.pdf'):
                copy(project['inputs']['02_work/' + name], work / name)
            entry = project['inputs']['02_work/blocks.json']
            if sha(Path(entry['path'])) != entry['sha256']:
                raise ValueError('Changed source geometry')
            metadata = json.loads(Path(entry['path']).read_text())
            # Geometry only; avoid exporting old absolute paths or crop URLs.
            clean = {'coordinate_space': metadata['coordinate_space'],
                     'pages': [{k: p[k] for k in ('page_index','width_px','height_px','rotation') if k in p}
                               for p in metadata['pages']],
                     'blocks': [{k: b[k] for k in ('block_id','ordinal','page_index','page_label',
                                                  'block_type','shape_type','coords_norm','polygon_points') if k in b}
                                for b in metadata['blocks']]}
            (work / 'blocks.json').write_text(json.dumps(clean, ensure_ascii=False, indent=2) + '\n')
            copied['02_work/blocks.json'] = sha(work / 'blocks.json')
            for block in validation['candidate_index']['blocks']:
                copy(project['inputs']['03_analysis/runs/control/blocks_stage02_100/' + block['file']],
                     crops / block['file'])
            index_path = crops / 'index.json'
            index_path.write_text(json.dumps(validation['candidate_index'], ensure_ascii=False, indent=2) + '\n')
            copied[str(index_path.relative_to(target))] = sha(index_path)
            actual_paths = {str(p.relative_to(target)) for p in target.rglob('*') if p.is_file()}
            if actual_paths != set(copied) or any(p.is_symlink() for p in target.rglob('*')):
                raise ValueError('Unexpected package contents')
            receipt['projects'].append({'discipline': project['discipline'],
                                        'root': str(target), 'files': copied})
        receipt['status'] = 'source_package_verified_not_runnable_yet'
        receipt['remaining'] = [
            'Build document graph and project metadata from this source package.',
            'Verify executor filesystem restrictions prevent access to evaluator/history.',
            'Freeze implementation and Astra-only settings; resolve external transmission approval.',
        ]
    except Exception as exc:
        receipt.update(status='failed', error=str(exc))
        raise
    finally:
        receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    result = package(args.destination)
    print(result['status'], len(result['projects']), 'projects')
