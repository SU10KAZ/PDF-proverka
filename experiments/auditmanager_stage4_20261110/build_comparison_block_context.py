"""Build missing baseline graphic context from sources, with network forbidden."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

SOURCE = Path('/tmp/auditmanager_paired_comparison_sources_20261111/classic_astra_with_catalog')
INPUTS = Path('/tmp/auditmanager_six_candidate_inputs_policy_v2_20261111')
DEST = Path('/tmp/auditmanager_six_candidate_inputs_context_v3_20261111')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def deny_network(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
        raise RuntimeError('SOURCE_CONTEXT_BUILD_FORBIDS_NETWORK_AND_SUBPROCESSES')


async def main():
    # Import only the frozen historical implementation, never current backend.
    sys.path.insert(0, str(SOURCE))
    os.environ['BLOCK_CONTEXT_WORKERS'] = '1'
    from backend.app.pipeline.stages.block_context.builder import build_block_context
    from backend.app.pipeline.stages.block_context.contract import block_context_up_to_date, crops_materialized
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import crop_index_matches_policy, stage02_crop_policy
    sys.addaudithook(deny_network)
    DEST.mkdir(exist_ok=False)
    report = {'status': 'building', 'model_calls': 0, 'source_root': str(SOURCE), 'projects': []}
    def save():
        (DEST/'build_receipt.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    save()
    try:
        for disc in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
            source=INPUTS/disc; target=DEST/disc
            original={str(p.relative_to(source)):sha(p) for p in source.rglob('*') if p.is_file()}
            shutil.copytree(source,target)
            out=target/'03_analysis/runs/candidate'; index=out/'blocks_stage02_100/index.json'
            async def progress(event):
                if event.get('type') == 'block_done':
                    print(json.dumps({'discipline':disc, **event},ensure_ascii=False), flush=True)
            summary=await build_block_context(target,output_dir=out,blocks_index_path=index,progress_cb=progress)
            ready=block_context_up_to_date(out)
            assert ready['ready'], ready
            assert crops_materialized(index.parent)[0]
            assert crop_index_matches_policy(index,stage02_crop_policy())
            assert all(sha(target/name)==digest for name,digest in original.items())
            added={str(p.relative_to(target)):sha(p) for p in target.rglob('*') if p.is_file() and str(p.relative_to(target)) not in original}
            assert all(n=='03_analysis/runs/candidate/block_context_summary.json' or n.startswith('03_analysis/runs/candidate/block_vector_graphs/') for n in added)
            report['projects'].append({'discipline':disc,'blocks':summary['blocks_total'],'sources':summary['source_counts'],'added_files':added,'original_bytes_unchanged':True,'ready':True})
            save()
        report['status']='completed_source_context_only'
    except Exception as exc:
        report.update(status='failed',error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        save()

if __name__=='__main__':asyncio.run(main())
