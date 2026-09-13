"""Post-freeze descriptive project experiments for the two rejected approaches.

Read frozen local-unit artifacts; no candidate tuning or new source parsing.
Timings exclude materialization and must not be compared as end-to-end speedups.
"""
import argparse
from collections import Counter
from pathlib import Path
import time
from experiments.text_comparison_v1.common import read,write,file_hash
from .alignment import align
from .facts import compare
from .run import verify


def run(root):
    root=Path(root);manifest=verify(root);out=root/"postfreeze_alternatives"
    if out.exists():raise ValueError("Immutable experiment already exists")
    metrics=[];timings={}
    for approach in ("sequence","engineering_anchor"):
        tick=time.perf_counter();rows=[];all_changes=[]
        for pair in read(root/"FROZEN_PROJECT_INPUTS.json")["pairs"]:
            us={s:read(root/"run1/documents"/pair[s]["document_version"]/"units.json")["units"] for s in ("old","new")}
            by={s:{u["unit_id"]:u for u in v} for s,v in us.items()}
            alignment=align(us["old"],us["new"],approach)
            changes=[{**c,"pair_key":pair["pair_key"]} for r in alignment["relations"] for c in compare(r,[by["old"][i] for i in r["old_unit_ids"]],[by["new"][i] for i in r["new_unit_ids"]])]
            rows.append({"pair_key":pair["pair_key"],**alignment["quality"],"changes":dict(Counter(c["category"] for c in changes)),"candidate_characters":sum(p["local_input_chars"] for p in alignment["retrieval"])})
            all_changes.extend(changes)
            write(out/approach/pair["pair_key"]/"alignment.json",alignment)
            write(out/approach/pair["pair_key"]/"changes.json",changes)
        metrics.append({"approach":approach,"aligned_relations":sum(x["aligned_relations"] for x in rows),"aligned_old_units":sum(x["aligned_old_units"] for x in rows),"aligned_new_units":sum(x["aligned_new_units"] for x in rows),
                        "review_units":sum(x["review_units"] for x in rows),"changes":dict(Counter(c["category"] for c in all_changes)),"pairs":rows,
                        "engineering_proposals":[c for c in all_changes if c["category"]=="ENGINEERING_CHANGE"],"candidate_characters":sum(x["candidate_characters"] for x in rows),
                        "model_calls":0,"model_input_tokens":0,"model_output_tokens":0,"human_precision":None})
        timings[approach]=time.perf_counter()-tick
        print({k:metrics[-1][k] for k in ("approach","aligned_relations","review_units","changes")},flush=True)
    write(out/"metrics.json",{"candidate_content_hash":manifest["candidate_content_hash"],"post_freeze":True,"used_for_tuning":False,"metrics":metrics})
    write(out/"performance.json",{"seconds":timings,"materialization_included":False})

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",required=True);run(p.parse_args().root)
