"""Prepare complete source-only inputs, preserving every authoritative source byte."""
import asyncio
import json
import os
from pathlib import Path
import shutil
import sys
from .materialize_trial_inputs import sha, write

HERE = Path(__file__).parent
DEST = Path('/tmp/auditmanager_current_trial_inputs_context_20261112')
PREVIOUS = Path('/tmp/auditmanager_six_candidate_inputs_context_v3_20261111')


def deny_network(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
        raise RuntimeError('SOURCE_PREPARATION_FORBIDS_NETWORK_AND_SUBPROCESSES')


async def main():
    manifest_path = HERE/'current_candidate_snapshot_provenance.json'
    snapshot = json.loads(manifest_path.read_text())
    capsule = Path(snapshot['source_root'])
    for name, digest in snapshot['source_files'].items():
        assert sha(capsule/name) == digest
    sys.path.insert(0, str(capsule))
    os.environ['BLOCK_CONTEXT_WORKERS'] = '1'
    from .build_source_graphs import build
    from .enrich_source_graph_sheets import sheet_metadata
    from backend.app.pipeline.stages.block_context.builder import build_block_context
    from backend.app.pipeline.stages.block_context.contract import block_context_up_to_date, crops_materialized
    from backend.app.pipeline.stages.block_grounding.block_profile_registry import load_prepared_package
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import crop_index_matches_policy, stage02_crop_policy
    package_path = HERE/'six_corrected_source_input_package.json'
    package = json.loads(package_path.read_text())
    sys.addaudithook(deny_network)
    DEST.mkdir(exist_ok=False)
    receipt = {'status': 'building', 'model_calls': 0,
               'input_package': {'path': str(package_path), 'sha256': sha(package_path)},
               'source_snapshot': {'path': str(manifest_path), 'sha256': sha(manifest_path)},
               'projects': []}
    def save():
        (DEST/'build_receipt.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2)+'\n')
    save()
    try:
        for project in package['projects']:
            disc = project['discipline']; source = Path(project['root']); target = DEST/disc
            assert {str(p.relative_to(source)) for p in source.rglob('*') if p.is_file()} == set(project['files'])
            for name, digest in project['files'].items():
                assert sha(source/name) == digest
            shutil.copytree(source, target)
            out = target/'03_analysis/runs/candidate'
            index = out/'blocks_stage02_100/index.json'
            old = PREVIOUS/disc
            for name in ('02_work/document.md','02_work/document.pdf','02_work/blocks.json'):
                assert sha(source/name) == sha(old/name)
            old_index = old/'03_analysis/runs/candidate/blocks_stage02_100/index.json'
            old_rows = json.loads(old_index.read_text())['blocks']
            rows = json.loads(index.read_text())['blocks']
            assert {(r['block_id'],r['file'],r['sha256']) for r in rows} == {(r['block_id'],r['file'],r['sha256']) for r in old_rows}
            for row in old_rows:
                assert sha(index.parent/row['file']) == row['sha256']
            # Reuse policy/index annotations only after proving the source and
            # all crop bytes identical. Never copy prior model/audit outputs.
            shutil.copyfile(old_index, index)
            entry = build(target); graph_path = Path(entry['path'])
            graph = json.loads(graph_path.read_text())
            for page in graph['pages']:
                page.update(sheet_metadata(page))
            graph_path.write_text(json.dumps(graph, ensure_ascii=False, indent=2)+'\n')
            async def progress(event):
                if event.get('type') == 'block_done':
                    print(disc, event.get('completed'), event.get('total'), flush=True)
            summary = await build_block_context(target, output_dir=out, blocks_index_path=index, progress_cb=progress)
            assert block_context_up_to_date(out)['ready'] and not summary['blocks_failed']
            assert crops_materialized(index.parent)[0]
            assert crop_index_matches_policy(index, stage02_crop_policy())
            assert summary['blocks_total'] == len(rows)
            for block in summary['blocks']:
                loaded = load_prepared_package(out, block['block_id'])
                assert loaded is not None and loaded['source_kind'] == block['source_kind']
            for name, digest in project['files'].items():
                if name != '03_analysis/runs/candidate/blocks_stage02_100/index.json':
                    assert sha(target/name) == digest
            receipt['projects'].append({'discipline': disc, 'root': str(target),
                'blocks': len(rows), 'source_counts': summary['source_counts'],
                'original_sources_unchanged': True, 'index_policy_source': {'path': str(old_index), 'sha256': sha(old_index)},
                'files': {str(p.relative_to(target)):sha(p) for p in target.rglob('*') if p.is_file()}})
            save()
        receipt['status'] = 'completed_source_context_only'
    except BaseException as exc:
        receipt.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        save()
    write(HERE/'current_trial_context_receipt.json', receipt)


if __name__ == '__main__':
    asyncio.run(main())
