"""Read-only, post-freeze audit selection and exact source/context accounting."""
import argparse
from collections import Counter,defaultdict
from pathlib import Path
import os
import statistics
import sys

from experiments.text_comparison_v1.common import read,write,digest,file_hash,canonical
from .run import verify


def select(root):
    root=Path(root);manifest=verify(root)
    seed=manifest["audit_policy"]["seed"]
    project=read(root/"run1/project.json")
    engineering=sorted([c for c in project["changes"] if c["category"]=="ENGINEERING_CHANGE"],key=lambda c:(c["type"],c["pair_key"],digest([seed,c["change_id"]])))
    pool=[]
    for pair in project["pairs"]:
        a={u["unit_id"]:u for u in read(root/"run1/documents"/pair["old_version"]/"units.json")["units"]}
        b={u["unit_id"]:u for u in read(root/"run1/documents"/pair["new_version"]/"units.json")["units"]}
        for r in read(root/"run1/pairs"/pair["pair_key"]/"alignment.json")["relations"]:
            if r["decision"]!="SAME_ENGINEERING_SUBJECT":continue
            old=[a[i] for i in r["old_unit_ids"]];new=[b[i] for i in r["new_unit_ids"]]
            owned=all(u["section_ownership_proven"] for u in old+new)
            pool.append({"pair_key":pair["pair_key"],"relation":r,"old":old,"new":new,
                         "stratum":str((r["kind"],r["evidence"][0],owned)),"rank":digest([seed,r["alignment_id"]])})
    strata=Counter(x["stratum"] for x in pool);used_strata=Counter();used_pairs=Counter();chosen=[]
    while pool and len(chosen)<manifest["audit_policy"]["alignment_n"]:
        x=min(pool,key=lambda x:(used_strata[x["stratum"]],used_pairs[x["pair_key"]],strata[x["stratum"]],x["rank"]))
        chosen.append(x);pool.remove(x);used_strata[x["stratum"]]+=1;used_pairs[x["pair_key"]]+=1
    write(root/"quality_audit/selection.json",{"policy":manifest["audit_policy"],"engineering_population":len(engineering),"engineering_sample":engineering[:30],
                                             "alignment_sample":chosen,"alignment_strata":dict(strata),"selection_used_correctness":False,"human_truth":False})
    print({"engineering":len(engineering[:30]),"alignment":len(chosen),"strata":dict(strata)},flush=True)


def measure(root):
    root=Path(root);verify(root)
    deps=root.parent/"20260913_text_comparison_v1/tokenizer_deps"
    sys.path.insert(0,str(deps));os.environ["TIKTOKEN_CACHE_DIR"]=str(root.parent/"20260913_text_comparison_v1/tokenizer_cache")
    import tiktoken
    enc=tiktoken.get_encoding("o200k_base");count=lambda s:len(enc.encode(s,disallowed_special=()))
    rows=[];invariant=Counter();provenance_bad=[]
    baseline_root=root.parent/"20260913_text_comparison_goal_driven"
    inputs=read(root/"FROZEN_PROJECT_INPUTS.json")
    for pair in inputs["pairs"]:
        mats={};byid={};full={};raw_total=0
        for side in ("old","new"):
            d=pair[side];m=read(root/"run1/documents"/d["document_version"]/"units.json");mats[side]=m
            byid[side]={u["unit_id"]:u for u in m["units"]}
            source=Path(d["artifacts"]["work_md"]["path"]).read_text();lines=source.splitlines()
            full[side]=count("\n".join(u["text"] for u in m["units"]));raw_total+=count(source)
            allowed=set();disallowed={r["line_id"] for r in m["exclusions"]}
            for u in m["units"]:
                if u["source_route"]!="TEXT":invariant["non_text_unit"]+=1
                for ref in u["source_refs"]:
                    allowed.add(ref["line_id"])
                    if ref["line_id"] in disallowed:invariant["excluded_line_used"]+=1
                    import hashlib
                    if hashlib.sha256(lines[ref["markdown_line"]-1].encode()).hexdigest()!=ref["line_sha256"]:
                        provenance_bad.append({"unit_id":u["unit_id"],"source_ref":ref})
            invariant["narrative_lines_checked"]+=len(allowed)
            invariant["units_checked"]+=len(m["units"])
        alignment=read(root/"run1/pairs"/pair["pair_key"]/"alignment.json")
        packages=[];narrative_total=sum(full.values())
        token_by_unit={u["unit_id"]:count(u["text"]) for m in mats.values() for u in m["units"]}
        for p in alignment["retrieval"]:
            # Count actual compact potential local package body, including titles
            # and stable candidate/line ids, never document-wide prompts.
            def package_unit(u):
                return {"unit_id":u["unit_id"],"text":u["text"],"heading":[s["title"] for s in u["section_context"]],
                        "source_refs":[{"line_id":r["line_id"],"page":r["page"],"block_id":r["block_id"]} for r in u["source_refs"]]}
            body={"old":[package_unit(byid["old"][i]) for i in p["old_unit_ids"]],
                  "new_candidates":[[package_unit(byid["new"][i]) for i in c["new_unit_ids"]] for c in p["candidates"]]}
            content="\n".join(byid["old"][i]["text"] for i in p["old_unit_ids"])+"\n"+"\n".join(byid["new"][i]["text"] for c in p["candidates"] for i in c["new_unit_ids"])
            packages.append({"input_hash":p["input_hash"],"old_unit_ids":p["old_unit_ids"],"candidate_count":len(p["candidates"]),
                             "content_tokens":count(content),"local_package_tokens":count(canonical(body).decode()),"full_pair_narrative_tokens":narrative_total})
        seen_old=Counter(i for r in alignment["relations"] if r["decision"]=="SAME_ENGINEERING_SUBJECT" for i in r["old_unit_ids"])
        seen_new=Counter(i for r in alignment["relations"] if r["decision"]=="SAME_ENGINEERING_SUBJECT" for i in r["new_unit_ids"])
        invariant["duplicate_aligned_unit_ownership"]+=sum(v>1 for v in list(seen_old.values())+list(seen_new.values()))
        baseline=read(baseline_root/"run1/pairs"/pair["pair_key"]/"relations.json")
        baseline_mats={s:read(baseline_root/"run1/documents"/pair[s]["document_version"]/"sections.json") for s in ("old","new")}
        baseline_lines={s:{ref["line_id"] for r in baseline if r["status"]=="PROVEN" for sec in baseline_mats[s]["sections"] if sec["instance_id"] in r[s+"_sections"] for block in sec["ordered_text_blocks"] for ref in block["source_refs"]} for s in ("old","new")}
        narrative_line_sets={s:{r["line_id"] for u in mats[s]["units"] for r in u["source_refs"]} for s in ("old","new")}
        aligned_line_sets={s:{ref["line_id"] for r in alignment["relations"] if r["decision"]=="SAME_ENGINEERING_SUBJECT" for i in r[s+"_unit_ids"] for ref in byid[s][i]["source_refs"]} for s in ("old","new")}
        expanded={s:len(aligned_line_sets[s]-baseline_lines[s]) for s in ("old","new")}
        baseline_scope={s:len(baseline_lines[s]&narrative_line_sets[s]) for s in ("old","new")}
        rows.append({"pair_key":pair["pair_key"],"full_narrative_tokens":full,"raw_markdown_tokens_count_only":raw_total,
                     "retrieval_packages":packages,"aligned_local_unit_tokens":sum(token_by_unit[i] for i in list(seen_old)+list(seen_new)),
                     "narrative_lines":{s:len(x) for s,x in narrative_line_sets.items()},"aligned_lines":{s:len(x) for s,x in aligned_line_sets.items()},
                     "baseline_matched_lines_on_same_denominator":baseline_scope,"aligned_lines_outside_baseline_matched_scope":expanded,
                     "retrieval_no_candidate_old_singletons":sum(len(p["old_unit_ids"])==1 and not p["candidates"] for p in alignment["retrieval"]),
                     "retrieval_unproven_old_singletons":sum(len(p["old_unit_ids"])==1 and bool(p["candidates"]) and p["old_unit_ids"][0] not in seen_old for p in alignment["retrieval"])})
    ps=[p for row in rows for p in row["retrieval_packages"]];nonempty=[p for p in ps if p["candidate_count"]]
    summary={"full_narrative_once_tokens":sum(sum(r["full_narrative_tokens"].values()) for r in rows),
             "raw_markdown_once_tokens_count_only":sum(r["raw_markdown_tokens_count_only"] for r in rows),
             "retrieval_packages":len(ps),"retrieval_packages_with_candidates":len(nonempty),
             "retrieval_content_tokens":sum(p["content_tokens"] for p in ps),"retrieval_package_tokens":sum(p["local_package_tokens"] for p in ps),
             "median_local_package_tokens":statistics.median(p["local_package_tokens"] for p in ps),
             "max_local_package_tokens":max(p["local_package_tokens"] for p in ps),
             "median_content_reduction_per_nonempty_package":statistics.median(1-p["content_tokens"]/p["full_pair_narrative_tokens"] for p in nonempty),
             "median_package_reduction_per_nonempty_package":statistics.median(1-p["local_package_tokens"]/p["full_pair_narrative_tokens"] for p in nonempty),
             "aligned_local_unit_tokens":sum(r["aligned_local_unit_tokens"] for r in rows),
             "actual_model_calls":0,"actual_model_input_tokens":0,"actual_model_output_tokens":0,"average_actual_model_context":None}
    write(root/"reports/CONTEXT_AND_SCOPE.json",{"tokenizer":"tiktoken","version":tiktoken.__version__,"encoding":"o200k_base","summary":summary,"pairs":rows,
                                             "caveat":"Exact tokenizer counts of local potential context, not actual model consumption or cost savings. Overlapping retrieval packages are counted separately. Research assistant reasoning/audit overhead is not provider telemetry."})
    write(root/"reports/SOURCE_INVARIANTS.json",{"pass":not provenance_bad and not any(invariant[k] for k in ("non_text_unit","excluded_line_used","duplicate_aligned_unit_ownership")),
                                             "counts":dict(invariant),"provenance_errors":provenance_bad,"scope":"Ledger routing and source-line hashes, not manual proof of upstream routing accuracy"})
    print(summary,flush=True)

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("command",choices=["select","measure"]);p.add_argument("--root",required=True);a=p.parse_args()
    (select if a.command=="select" else measure)(a.root)
