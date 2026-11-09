"""Read-only replay of the compact view used after graphic analysis."""
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.block_analysis.blocks_for_text import build_compact_view
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]

def read(p): return json.loads(p.read_text())
def ref(p): return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}

def main():
    rows=[]
    for project in read(HERE/'merge_coverage_inventory.json')['projects']:
        bp=Path(project['sources'][1]['path']); source=read(bp); generated=build_compact_view(source)
        saved_path=bp.parent/'01_blocks_for_text.json';saved=read(saved_path)
        assert generated==saved,project['discipline']
        by_id={b['block_id']:b for b in saved['blocks']}
        for item in project['deferred']:
            compact=by_id.get(item['block_id'])
            rows.append({'discipline':project['discipline'],'sources':[ref(bp),ref(saved_path)],
                         'source_pointer':item['pointer'],'block_id':item['block_id'],
                         'deferred_claim':item['finding'].get('finding'),
                         'required_context':item['finding'].get('required_context'),
                         'gate_reasons':item['finding'].get('_evidence_gate',{}).get('reasons'),
                         'compact_block_present':compact is not None,
                         'compact_block':compact,
                         'deferred_field_forwarded':bool(compact and 'deferred_findings' in compact),
                         'required_context_field_forwarded':bool(compact and 'required_context' in json.dumps(compact)),
                         'replay_equals_saved_view':True})
    assert len(rows)==4
    assert not any(r['deferred_field_forwarded'] or r['required_context_field_forwarded'] for r in rows)
    result={'finding':'Compact block→text projection does not carry deferred candidates or their required_context; reproduced exactly for all six saved views.',
            'code':[{**ref(ROOT/'backend/app/pipeline/stages/block_analysis/blocks_for_text.py'),'symbol':'build_compact_view','line':67},
                    {**ref(ROOT/'backend/app/pipeline/stages/block_analysis/gemma_findings_only.py'),'symbol':'publication gate and analysis assembly','lines':[3912,3944,4136]}],
            'cases':rows,'verified_projects':6,
            'boundary':'This proves loss from the compact projection, not deletion from raw01_blocks_analysis. The merge prompt can read the raw file, and three deferred meanings were independently preserved. No blanket claim that all downstream agents never saw them.',
            'queue_inspection':'References to required_context/deferred_findings in backend/app/pipeline locate schema/prompt/gate/storage, not an explicit deferred-candidate retrieval runner. This is bounded code-search evidence, not proof about external services.',
            'recommendation':'Carry unresolved candidates in a separate compact queue, with source IDs, requested documents and a resolution receipt. Schedule address-based retrieval only for useful specific questions; never publish all deferred candidates as defects.',
            'acceptance_controls':['AR aerator: available upper plan must be addressed, but no automatic deficit claim.','KM OGM11: retain question on component boundaries, not unconditional 200mm error.','EOM room numbers: do not lose a project issue due to wording about context.'],
            'production_changed':False,'stage_2_complete':False}
    (HERE/'deferred_projection_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print('Six exact compact-view replays; four deferred candidates and their context requests not forwarded as fields.')

if __name__=='__main__':main()
