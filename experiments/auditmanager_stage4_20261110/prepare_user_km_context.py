"""Prepare the user KM package with the same source-only context builder as baseline."""
import asyncio
import json
import os
from pathlib import Path
import shutil
import sys
from .materialize_trial_inputs import sha

HERE = Path(__file__).parent
SOURCE = Path('/tmp/auditmanager_user_km_214_v002_20261111/source_only')
DEST = SOURCE.with_name('prepared_context')


def deny_network(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
        raise RuntimeError('SOURCE_PREPARATION_FORBIDS_NETWORK_AND_SUBPROCESSES')


async def main():
    snapshot_path = HERE / 'classic_catalog_snapshot_provenance.json'
    snapshot = json.loads(snapshot_path.read_text())
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
    sys.addaudithook(deny_network)
    assert not DEST.exists()
    original = {str(p.relative_to(SOURCE)): sha(p) for p in SOURCE.rglob('*') if p.is_file()}
    shutil.copytree(SOURCE, DEST)
    info = {'project_id': 'calibration-km-214-v002', 'name': 'KM', 'section': 'KM', 'description': '',
            'md_file': '02_work/document.md', 'pdf_file': '02_work/document.pdf', 'blocks_json_file': '02_work/blocks.json'}
    (DEST/'01_input/project_info.json').write_text(json.dumps(info, ensure_ascii=False, indent=2)+'\n')
    receipt = {'status': 'building_source_context', 'model_calls': 0, 'root': str(DEST),
               'context_source_snapshot_sha256': sha(snapshot_path)}
    receipt_path = DEST.parent/'context_build_receipt.json'
    def save():
        receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2)+'\n')
    save()
    try:
        entry = build(DEST); graph_path = Path(entry['path'])
        graph = json.loads(graph_path.read_text()); sheet_counts = {}
        for page in graph['pages']:
            fields = sheet_metadata(page); page.update(fields)
            state = fields['sheet_confidence']; sheet_counts[state] = sheet_counts.get(state, 0)+1
        graph['boundary'] = 'Unchanged authoritative MD; sheet labels only from its explicit stamps. Graphics descriptions remain navigation, not verified evidence.'
        graph_path.write_text(json.dumps(graph, ensure_ascii=False, indent=2)+'\n')
        out = graph_path.parent; index = out/'blocks_stage02_100/index.json'
        async def progress(event):
            if event.get('type') == 'block_done':
                print(json.dumps({k:event[k] for k in ('type','completed','total') if k in event}), flush=True)
        summary = await build_block_context(DEST, output_dir=out, blocks_index_path=index, progress_cb=progress)
        ready = block_context_up_to_date(out)
        assert ready['ready'] and not summary['blocks_failed']
        assert crops_materialized(index.parent)[0]
        assert crop_index_matches_policy(index, stage02_crop_policy())
        assert summary['blocks_total'] == 9
        for block in summary['blocks']:
            package = load_prepared_package(out, block['block_id'])
            assert package is not None and package['source_kind'] == block['source_kind']
        for name, digest in original.items():
            if name != '01_input/project_info.json':
                assert sha(DEST/name) == digest
        assert graph['source_md_sha256'] == sha(DEST/'02_work/document.md')
        receipt.update(status='prepared_source_contracts_passed', graphic_blocks=9,
            text_blocks=graph['total_text_blocks'], sheet_status_counts=sheet_counts,
            source_counts=summary['source_counts'],
            metadata_change={'before':original['01_input/project_info.json'], 'after':sha(DEST/'01_input/project_info.json'),
                             'reason':'Local runner paths and fixed discipline only; original intake preserved separately.'},
            files={str(p.relative_to(DEST)):sha(p) for p in DEST.rglob('*') if p.is_file()},
            source_text_pdf_graphic_bytes_unchanged=True, historical_answers_read=False,
            new_independent_object_claimed=False, trial_started=False)
    except Exception as exc:
        receipt.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        save()
    (HERE/'user_KM_214_context_receipt.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({'status':receipt['status'],'graphics':9,'texts':receipt['text_blocks'],'model_calls':0}))


if __name__ == '__main__':
    asyncio.run(main())
