"""Frozen offline project execution, byte replay, and durable local evidence."""
import argparse
from collections import Counter
from pathlib import Path
import resource
import subprocess
import time
from datetime import datetime, timezone

from experiments.text_comparison_v1.common import read,write,file_hash,digest
from .alignment import align, TOP_K, MAX_POSTING_CANDIDATES
from .facts import compare
from .units import build, MAX_LOCAL_CHARS

REPO=Path(__file__).resolve().parents[2]


def freeze(root):
    root=Path(root)
    manifest=root/"reports/CANDIDATE_MANIFEST.json"
    if manifest.exists():raise ValueError("Candidate already frozen")
    files={str(p.relative_to(REPO)):file_hash(p) for name in ("semantic_foundation_v3","text_comparison_v1","text_safe_coverage","text_alignment_v2")
           for p in sorted((REPO/"experiments"/name).glob("*.py"))}
    config={"approach":"hybrid","top_k":TOP_K,"max_posting_candidates":MAX_POSTING_CANDIDATES,
            "max_variant_units":3,"max_variant_characters":MAX_LOCAL_CHARS,"max_paragraph_before_natural_sentence_split":1800,
            "identity":"unique exact assertion or complete typed template","automatic_fuzzy_alignment":False,
            "fact_comparison":"complete typed assertion template with independent local scope support",
            "section_relation_required":False,"absence_without_positive_evidence":"REVIEW",
            "ai_enabled":False,"model":None,"prompt":None,"temperature":None,
            "table_content_compared":False,"graphic_content_compared":False}
    value={"schema":"text-alignment-candidate.v2","repository":str(REPO),"candidate_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=REPO,text=True).strip(),
           "frozen_at":datetime.now(timezone.utc).isoformat(),"code_files":files,"configuration":config,
           "input_manifest_sha256":file_hash(root/"FROZEN_PROJECT_INPUTS.json"),
           "dev_experiment_sha256":file_hash(root/"dev_experiment/scorecard.json"),
           "dev_truth_sha256":"71ed12f693442665a64c073a56f3d13df321bfdd0303f4a378286437616906d7",
           "project_content_inspected_before_freeze":False,"historical_diagnostics_used_for_tuning":False,
           "old_eval_or_holdout_answers_read":False,"no_production_changes":True,
           "audit_policy":{"seed":"text-v2-quality-audit-20260913","engineering_n":30,"alignment_n":24,
                           "strata":["change_type","document_pair","alignment_shape","certificate","section_ownership"]},
           "holdout_policy":{"seed":"text-v2-fresh-source-holdout-20260913","target_n":24,"predictions_in_packet":False,"annotations":0,
                             "selection_basis":"source-only structural strata and stable hash, independent of candidate correctness"}}
    value["candidate_content_hash"]=digest([files,config])
    write(manifest,value)
    print({k:value[k] for k in ("candidate_commit","candidate_content_hash","frozen_at")},flush=True)


def verify(root):
    root=Path(root);m=read(root/"reports/CANDIDATE_MANIFEST.json")
    for p,h in m["code_files"].items():
        if file_hash(REPO/p)!=h:raise ValueError("Frozen candidate changed: "+p)
    if file_hash(root/"FROZEN_PROJECT_INPUTS.json")!=m["input_manifest_sha256"]:raise ValueError("Project inputs changed")
    return m


def run(root,name):
    root=Path(root);freeze=verify(root);output=root/name
    if output.exists():raise ValueError("Immutable run already exists")
    started=time.perf_counter();summaries=[];all_changes=[];perf=[]
    for pair in read(root/"FROZEN_PROJECT_INPUTS.json")["pairs"]:
        tick=time.perf_counter();mats={}
        for side in ("old","new"):
            d=pair[side]
            for artifact in d["artifacts"].values():
                if isinstance(artifact,dict) and "sha256" in artifact and file_hash(artifact["path"])!=artifact["sha256"]:
                    raise ValueError("Source receipt mismatch: "+artifact["path"])
            mats[side]=build(d)
            write(output/"documents"/d["document_version"]/"units.json",mats[side])
        materialize_seconds=time.perf_counter()-tick
        a,b=mats["old"]["units"],mats["new"]["units"]
        at=time.perf_counter();result=align(a,b)
        align_seconds=time.perf_counter()-at
        aa={u["unit_id"]:u for u in a};bb={u["unit_id"]:u for u in b}
        ct=time.perf_counter();changes=[]
        for rel in result["relations"]:
            for c in compare(rel,[aa[i] for i in rel["old_unit_ids"]],[bb[i] for i in rel["new_unit_ids"]]):
                c.update(pair_key=pair["pair_key"],old_document=pair["old"]["document_code"],new_document=pair["new"]["document_code"])
                changes.append(c)
        compare_seconds=time.perf_counter()-ct
        write(output/"pairs"/pair["pair_key"]/"alignment.json",result)
        write(output/"pairs"/pair["pair_key"]/"changes.json",changes)
        summary={"pair_key":pair["pair_key"],"source_pair_id":pair["source_pair_id"],"old_document":pair["old"]["document_code"],"new_document":pair["new"]["document_code"],
                 "old_version":pair["old"]["document_version"],"new_version":pair["new"]["document_version"],
                 **result["quality"],"changes":dict(Counter(c["category"] for c in changes)),
                 "change_types":dict(Counter(c["type"] for c in changes)),
                 "pages":sum(m["foundation_quality"]["pages"] for m in mats.values()),
                 "eligible_old_units":mats["old"]["quality"]["eligible"],"eligible_new_units":mats["new"]["quality"]["eligible"],
                 "full_narrative_characters":sum(len(u["text"]) for u in a+b),
                 "retrieval_candidate_characters":sum(p["local_input_chars"] for p in result["retrieval"])}
        summaries.append(summary);all_changes.extend(changes)
        perf.append({"pair_key":pair["pair_key"],"seconds":time.perf_counter()-tick,"materialize_seconds":materialize_seconds,"alignment_seconds":align_seconds,"comparison_seconds":compare_seconds})
        print({"pair":pair["source_pair_id"],"units":[len(a),len(b)],"aligned":result["quality"]["aligned_relations"],"changes":summary["changes"],"seconds":round(perf[-1]["seconds"],3)},flush=True)
    write(output/"project.json",{"candidate_content_hash":freeze["candidate_content_hash"],"input_sha256":file_hash(root/"FROZEN_PROJECT_INPUTS.json"),
                                 "pairs":summaries,"changes":all_changes,"table_content_compared":False,"graphic_content_compared":False,
                                 "model_calls":0,"model_input_tokens":0,"model_output_tokens":0})
    write(output/"performance.json",{"seconds":time.perf_counter()-started,"pairs":perf,
                                     "peak_rss_kib":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                                     "artifact_bytes":sum(p.stat().st_size for p in output.rglob("*.json")),"model_calls":0,"input_tokens":0,"output_tokens":0})


def replay(root,left,right):
    root=Path(root);verify(root)
    hashes=[]
    for name in (left,right):
        hashes.append({str(p.relative_to(root/name)):file_hash(p) for p in sorted((root/name).rglob("*.json")) if p.name!="performance.json"})
    a,b=hashes;diff=sorted(p for p in a.keys()|b.keys() if a.get(p)!=b.get(p))
    result={"pass":bool(a) and not diff,"files_compared":len(a),"differences":diff,"sha256_by_path":a,
            "excluded":["performance.json: observed runtime and RSS"],"ai_replay":"No pipeline model calls; no model re-query","candidate_unchanged":True}
    write(root/"reports/REPLAY_AUDIT.json",result)
    print({k:result[k] for k in ("pass","files_compared","differences")},flush=True)

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("command",choices=["freeze","run","replay"]);p.add_argument("--root",required=True);p.add_argument("--name");p.add_argument("--left",default="run1");p.add_argument("--right",default="run2")
    a=p.parse_args()
    if a.command=="freeze":freeze(a.root)
    elif a.command=="run":run(a.root,a.name)
    else:replay(a.root,a.left,a.right)
