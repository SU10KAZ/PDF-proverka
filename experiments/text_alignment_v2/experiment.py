"""Source-derived DEV contract experiments; never score these as human truth."""
import argparse
from collections import Counter
import copy
from decimal import Decimal
from pathlib import Path
import re
import time

from experiments.text_comparison_v1.common import read,write,digest,file_hash
from .units import from_materialization, SENTENCE, canonical_text
from .facts import analysis,compare
from .alignment import align,APPROACHES


def clone(u,side,i,text=None):
    v=copy.deepcopy(u)
    v["unit_id"]=side+"_"+digest([u["unit_id"],i,text])[:24]
    v["text"]=text if text is not None else u["text"]
    v["document_version"]="CONSTRUCTED_DEV_"+side
    v["constructed_probe"]=True
    v["original_source_refs"]=v["source_refs"]
    v["source_refs"]=[{"line_id":i,"page":1,"block_id":str(i),"markdown_line":i+1,"synthetic_locator":True}]
    v["ordinal"]=i
    return v


def probes(units):
    count=Counter(canonical_text(u["text"]) for u in units)
    base=sorted([u for u in units if u["eligibility"]=="ELIGIBLE" and count[canonical_text(u["text"])]==1],key=lambda u:digest(["v2-dev-probe-selection",u["unit_id"]]))[:60]
    old=[clone(u,"old",i) for i,u in enumerate(base)]
    new=[clone(u,"new",i) for i,u in enumerate(base)]
    expected=[([a["unit_id"]],[b["unit_id"]]) for a,b in zip(old,new)]
    yield "identity",old,new,expected,0
    moved=[clone(u,"move",i) for i,u in enumerate(reversed(base))]
    yield "paragraph_or_section_movement",old,moved,[([a["unit_id"]],[b["unit_id"]]) for a,b in zip(old,reversed(moved))],0
    edited=[clone(u,"edit",i,"  "+u["text"].replace(" ","\n ").replace("ё","е")+"  ") for i,u in enumerate(base)]
    yield "wrapping_editorial",old,edited,[([a["unit_id"]],[b["unit_id"]]) for a,b in zip(old,edited)],0
    numeric=[]
    for u in base:
        f=analysis(u["text"])
        slots=[s for s in f["slots"] if s["kind"]=="VALUE_CHANGED" and not s["ambiguous"] and re.fullmatch(r"\d+(?:\.\d+)?",s["value"])]
        if not slots:continue
        s=slots[0];a,b=s["span"]
        raw=f["canonical"]
        m=re.search(r"\d+(?:[.,]\d+)?",raw[a:b])
        if not m:continue
        value=str(Decimal(m.group().replace(",","."))+Decimal("7"))
        changed=raw[:a+m.start()]+value+raw[a+m.end():]
        numeric.append((u,raw,changed))
    a=[clone(u,"value_old",i,x) for i,(u,x,y) in enumerate(numeric)]
    b=[clone(u,"value_new",i,y) for i,(u,x,y) in enumerate(numeric)]
    yield "explicit_value_intervention",a,b,[([x["unit_id"]],[y["unit_id"]]) for x,y in zip(a,b)],len(a)
    split=[]
    for u in base:
        raw=u["text"]
        cuts=[m.end() for m in SENTENCE.finditer(raw)]
        possible=[k for k in cuts if min(len(re.findall(r"[а-яa-z]{2,}",t.casefold())) for t in (raw[:k],raw[k:]))>=7]
        if possible:split.append((u,raw[:possible[0]].strip(),raw[possible[0]:].strip()))
    a=[clone(u,"split_old",i) for i,(u,x,y) in enumerate(split)]
    b=[clone(u,"split_new",2*i+j,t) for i,(u,x,y) in enumerate(split) for j,t in enumerate((x,y))]
    ex=[([x["unit_id"]],[y["unit_id"] for y in b[2*i:2*i+2]]) for i,x in enumerate(a)]
    yield "one_to_many",a,b,ex,0
    yield "many_to_one",b,a,[(y,x) for x,y in ex],0
    # Both versions contain duplicate assertion templates; no forced identity.
    a=[clone(u,"dup_old",i) for i,u in enumerate(base[:8]*2)]
    b=[clone(u,"dup_new",i) for i,u in enumerate(base[:8]*2)]
    yield "duplicate_ambiguity",a,b,[],0


def run(root,foundation_root):
    root,foundation_root=Path(root),Path(foundation_root)
    source=read(root/"DEV_TEXT_ONLY.json")
    if source["source_sha256"]!="71ed12f693442665a64c073a56f3d13df321bfdd0303f4a378286437616906d7" or file_hash(source["source"])!=source["source_sha256"]:
        raise ValueError("DEV truth pin mismatch")
    if any(c["kind"] not in {"SECTION","OWNER"} for c in source["cases"]):raise ValueError("Non-TEXT truth")
    output=root/"dev_experiment"
    if output.exists():raise ValueError("Immutable experiment exists")
    rows=[];elapsed=Counter()
    for d in read(root/"DEV_DOCUMENTS.json"):
        mat=read(foundation_root/"dev_final"/d["document_version"]/"sections.json")
        units=from_materialization(d,mat)
        write(output/"documents"/d["document_version"]/"units.json",units)
        cases=list(probes(units["units"]))
        for label,a,b,expected,changed in cases:
            key=digest([d["document_version"],label])[:24]
            write(output/"probes"/(key+".json"),{"label":label,"human_truth":False,"purpose":"CONSTRUCTED_SOFTWARE_CONTRACT","old":a,"new":b,"expected_correspondence_from_construction":expected,"interventions":changed})
            for approach in APPROACHES:
                tick=time.perf_counter();result=align(a,b,approach)
                aa={x["unit_id"]:x for x in a};bb={x["unit_id"]:x for x in b}
                changes=[c for r in result["relations"] for c in compare(r,[aa[i] for i in r["old_unit_ids"]],[bb[i] for i in r["new_unit_ids"]])]
                elapsed[approach]+=time.perf_counter()-tick
                expected_set={(tuple(x),tuple(y)) for x,y in expected}
                accepted=[r for r in result["relations"] if r["decision"]=="SAME_ENGINEERING_SUBJECT"]
                correct=sum((tuple(r["old_unit_ids"]),tuple(r["new_unit_ids"])) in expected_set for r in accepted)
                engineering=[c for c in changes if c["category"]=="ENGINEERING_CHANGE"]
                false_changes=sum(label!="explicit_value_intervention" or (tuple(c["old_unit_ids"]),tuple(c["new_unit_ids"])) not in expected_set for c in engineering)
                row={"document_version":d["document_version"],"probe":label,"probe_id":key,"approach":approach,
                     "expected_relations":len(expected),"accepted":len(accepted),"correct":correct,"false_alignments":len(accepted)-correct,
                     "engineering":len(engineering),"false_changes":false_changes,"changed_by_construction":changed,
                     "review_units":result["quality"]["review_units"],"units":len(a)+len(b),
                     "local_candidate_chars":sum(p["local_input_chars"] for p in result["retrieval"])}
                rows.append(row)
                write(output/approach/(key+".json"),{"alignment":result,"changes":changes,"score":row})
        print({"dev_document":d["document_code"],"units":units["quality"]["units"]},flush=True)
    summary=[]
    for approach in APPROACHES:
        rs=[r for r in rows if r["approach"]==approach]
        totals={k:sum(r[k] for r in rs) for k in ("expected_relations","accepted","correct","false_alignments","engineering","false_changes","changed_by_construction","review_units","units","local_candidate_chars")}
        summary.append({"approach":approach,**totals,"contract_precision":totals["correct"]/totals["accepted"] if totals["accepted"] else None,
                        "contract_relation_coverage":totals["correct"]/totals["expected_relations"] if totals["expected_relations"] else None,
                        "model_calls":0,"input_tokens":0,"output_tokens":0})
    write(output/"scorecard.json",{"human_truth":False,"limitations":"Source-derived deterministic contracts, not human TextAlignment/TextChange labels or natural cross-version precision","summary":summary,"rows":rows})
    write(output/"performance.json",dict(elapsed))
    print(summary,flush=True)

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",required=True);p.add_argument("--foundation-root",required=True)
    a=p.parse_args();run(a.root,a.foundation_root)
