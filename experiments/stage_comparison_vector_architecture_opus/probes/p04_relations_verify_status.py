import json,sys
from pathlib import Path
ROOT=Path('/home/coder/projects/PDF-proverka')
P=ROOT/'experiments/stage_comparison_vector_architecture_opus/probes'
sys.path.insert(0,str(ROOT)); sys.path.insert(0,str(P))
from experiments.stage_comparison_vector_blocks import comparator as C
from relgraph_rotfix import extract_rotation_correct
pairs={p['pair_id']:p for p in json.load(open(ROOT/'experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))['pairs']}
for pid in ('eom_singleline_changed','vk_plan','vk_nodes','vk_node_plan'):
    p=pairs[pid]; sides={}
    for sn in ('left','right'):
        s=p[sn]
        d,_,_=extract_rotation_correct(s['pdf'],int(s['page_index']),s['bbox_norm'],f'{pid}_{sn}')
        sides[sn]=d
    r=C.compare_descriptions(sides['left'],sides['right'])
    old=json.load(open(ROOT/f'experiments/stage_comparison_vector_blocks/artifacts/comparisons/{pid}/comparison.json'))
    hv={'eom_singleline_changed':'STRUCTURE_CHANGED','vk_plan':'NEAR_IDENTICAL','vk_nodes':'STRUCTURE_SAME_VALUES_CHANGED','vk_node_plan':'NEAR_IDENTICAL'}[pid]
    print(f"{pid:24s} human={hv:30s} status_before={old['status']:28s} status_after={r['status']:28s} "
          f"geom_before={old['geometry']['similarity']:.4f}@{old['geometry']['selected_tolerance']} "
          f"geom_after={r['geometry']['similarity']:.4f}@{r['geometry']['selected_tolerance']} "
          f"text_before={old['text']['effective_similarity']:.4f} text_after={r['text']['effective_similarity']:.4f}")
