"""Check actual classic input contracts before any model dispatch."""
import json
from pathlib import Path
import sys
from .materialize_trial_inputs import sha

HERE=Path(__file__).parent
INPUTS=Path('/tmp/auditmanager_six_candidate_inputs_context_v3_20261111')

def main():
    receipt=json.loads((INPUTS/'build_receipt.json').read_text())
    assert receipt['status']=='completed_source_context_only',receipt['status']
    manifest=json.loads((HERE/'classic_catalog_snapshot_provenance.json').read_text())
    source=Path(manifest['source_root'])
    for name,digest in manifest['source_files'].items():assert sha(source/name)==digest,name
    sys.path.insert(0,str(source))
    from backend.app.pipeline.stages.block_context.contract import block_context_up_to_date,crops_materialized
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import crop_index_matches_policy,stage02_crop_policy
    from backend.app.pipeline.stages.block_grounding.block_profile_registry import load_prepared_package
    rows=[]
    for disc in ('AK','ITP','AR','KM','VK','EOM'):
        root=INPUTS/disc;out=root/'03_analysis/runs/candidate';index=out/'blocks_stage02_100/index.json'
        assert crop_index_matches_policy(index,stage02_crop_policy()),disc
        assert crops_materialized(index.parent)[0],disc
        ready=block_context_up_to_date(out);assert ready['ready'],(disc,ready)
        summary=ready['summary'];entries=json.loads(index.read_text())['blocks']
        assert len(entries)==summary['blocks_total'] and not summary['blocks_failed'],disc
        expected={str(b['block_id']) for b in entries}
        assert expected=={str(b['block_id']) for b in summary['blocks']},disc
        for b in summary['blocks']:
            package=load_prepared_package(out,b['block_id'])
            assert package is not None and package['source_kind']==b['source_kind'],b['block_id']
        graph=json.loads((out/'document_graph.json').read_text())
        assert graph['source_md_sha256']==sha(root/'02_work/document.md'),disc
        origin=Path('/tmp/auditmanager_six_candidate_inputs_policy_v2_20261111')/disc
        for p in origin.rglob('*'):
            if p.is_file():assert sha(p)==sha(root/p.relative_to(origin)),str(p)
        rows.append({'discipline':disc,'graphic_blocks':len(entries),'source_counts':summary['source_counts'],'ready':True})
    report={'status':'actual_input_contracts_passed','projects':rows,'model_calls':0,
            'boundary':'Checks source preparation and package readability; runtime and audit quality remain to be measured.'}
    (HERE/'comparison_context_readiness.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report))

if __name__=='__main__':main()
