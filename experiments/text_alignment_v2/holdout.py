"""Post-freeze source-only blind TextAlignment/TextChange packet.

No import of the candidate unit builder, aligner or fact comparator. No candidate
outputs, truth answers, or previous holdout cases are read. Prior source manifest
metadata is read exclusively to exclude documents and duplicate source pages.
"""
import argparse
from collections import Counter,defaultdict
from pathlib import Path
import re

from experiments.text_comparison_v1.common import read,write,digest,file_hash,canonical
from experiments.semantic_foundation_v3.dev_packet import prepare_source,page_hashes
from experiments.semantic_foundation_v3.ledger import LineLedger
from experiments.semantic_foundation_v3.page_model import PageModel
from experiments.semantic_foundation_v3.models import HeadingModel,FurnitureModel,CaptionModel

SEED="text-v2-fresh-source-holdout-20260913"
STRATA=("NUMERIC_SOURCE","MODAL_SOURCE","MULTI_SENTENCE_SOURCE","EARLY_SOURCE","LATE_SOURCE","GENERAL_SOURCE")


def source_chunks(doc):
    ledger,raw=LineLedger.read(doc);pages=PageModel(ledger,raw);headings=HeadingModel(ledger,pages)
    furniture=FurnitureModel(ledger,headings);captions=CaptionModel(ledger,pages,headings,furniture)
    chunks=[];pending=[];heading=None;table=False
    def flush():
        nonlocal pending
        if pending:
            text=" ".join(ledger.lines[i].text for i in pending)
            if len(re.findall(r"[а-яa-z]{2,}",text.casefold()))>=6 and len(text)<=4000:
                chunks.append({"text":text,"source_refs":[{"document_version":doc["document_version"],**ledger.anchor(i,"LINE")} for i in pending],
                               "heading":heading,"ordinal":len(chunks)})
        pending=[]
    for i,line in enumerate(ledger.lines):
        if re.search(r"<table\b",line.text,re.I):table=True
        inside=table
        if re.search(r"</table>",line.text,re.I):table=False
        if (not pages.eligible(line.page) or line.block_type.casefold()!="text" or i in furniture.records or i in captions.records
                or inside or line.text.count("|")>=2 or re.search(r"<t[rdh]\b|^\s*!\[",line.text,re.I)):
            flush();continue
        if i in headings.records:
            flush();heading=line.text;continue
        if pending:
            prev=ledger.lines[pending[-1]]
            if line.block_id!=prev.block_id or line.markdown_line>prev.markdown_line+1:
                flush()
        pending.append(i)
    flush()
    return chunks


def terms(x):
    return set(re.findall(r"[а-яa-z]{3,}",(x["text"]+" "+(x["heading"] or "")).casefold().replace("ё","е")))


def exclusion_documents(root,audits):
    manifests=[root/"DEV_DOCUMENTS.json",
               audits/"20260910_section_table_materialization_v2/baseline_documents.json",
               audits/"20260913_text_comparison_v1/holdout/pairs.json",
               audits/"20260913_text_comparison_goal_driven/holdout/pairs.json",
               audits/"20260913_table_materialization_v3/reports/FRESH_TABLE_HOLDOUT_SELECTION.json"]
    docs=[];receipts=[]
    for p in manifests:
        value=read(p)
        if p.name=="pairs.json":selected=[row[s] for row in value for s in ("old","new")]
        elif p.name=="FRESH_TABLE_HOLDOUT_SELECTION.json":selected=value["documents"]
        else:selected=value
        docs.extend(selected);receipts.append({"path":str(p),"sha256":file_hash(p),"use":"SOURCE_IDENTITIES_ONLY_NO_CASES_OR_ANSWERS"})
    return list({d["document_version"]:d for d in docs}.values()),receipts


def build(root,repo,*,replay_only=False):
    root,repo=Path(root),Path(repo);out=root/"fresh_holdout"
    if out.exists() and not replay_only:raise ValueError("Fresh holdout already exists")
    replay_receipts=[]
    before_sources={str(p):file_hash(p) for p in (out/"sources").rglob("*") if p.is_file()} if replay_only else {}
    def emit(path,value):
        if replay_only:
            if not Path(path).exists() or Path(path).read_bytes()!=canonical(value):
                raise ValueError("Holdout replay difference: "+str(path))
            replay_receipts.append({"path":str(path),"sha256":file_hash(path)})
        else:
            write(path,value)
    freeze=read(root/"reports/CANDIDATE_MANIFEST.json")
    for p,h in freeze["code_files"].items():
        if file_hash(repo/p)!=h:raise ValueError("Frozen candidate changed")
    excluded,receipts=exclusion_documents(root,root.parent)
    codes={d["document_code"] for d in excluded}
    hashes={a["sha256"] for d in excluded for a in d["artifacts"].values() if isinstance(a,dict) and "sha256" in a}
    excluded_pages=set()
    for d in excluded:
        excluded_pages.update(page_hashes(Path(d["artifacts"]["work_md"]["path"]).read_text()).values())
        hashes.update(a["sha256"] for a in d.get("source_refs",{}).get("original_sources",[]))
    grouped=defaultdict(list)
    for p in repo.glob("projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/document.md"):
        if p.parents[3].name not in codes and "/272_" not in str(p):grouped[str(p.parents[2])].append(p)
    universe=[];rejects=Counter()
    for family,paths in sorted(grouped.items()):
        if len(paths)<2:continue
        a,b=sorted(paths)[0],sorted(paths)[-1]
        if not all(p.with_name("document.pdf").exists() and (p.with_name("blocks.json").exists() or p.with_name("result.json").exists()) for p in (a,b)):
            rejects["missing_source"]+=1;continue
        hs=[file_hash(p) for p in (a,b)]
        if hs[0]==hs[1] or set(hs)&hashes:
            rejects["identical_or_previously_used_document"]+=1;continue
        texts=[p.read_text(errors="replace") for p in (a,b)]
        if any(len(t)>800000 or len(re.findall(r"^#{4,6}\s+",t,re.M))<3 for t in texts):
            rejects["source_size_or_narrative_heading_quota"]+=1;continue
        if any(set(page_hashes(t).values())&excluded_pages for t in texts):
            rejects["previously_used_source_page"]+=1;continue
        universe.append({"family":family,"old_path":str(a),"new_path":str(b),"md_hashes":hs,"rank":digest([SEED,family,hs])})
    packets=[];pairs=[];selection=[];selected_hashes=set();selected_page_hashes=set()
    for source in sorted(universe,key=lambda x:x["rank"]):
        paths=[Path(source[k]) for k in ("old_path","new_path")]
        pdf_hashes=[file_hash(p.with_name("document.pdf")) for p in paths]
        if set(pdf_hashes)&(hashes|selected_hashes):
            rejects["previously_used_or_duplicate_pdf"]+=1;continue
        docs=[prepare_source(p,out/"sources") for p in paths]
        for d,h in zip(docs,pdf_hashes):
            d["stage"]="FRESH_BLIND_TEXT_ALIGNMENT_V2"
            d["artifacts"]["pdf"]={"path":d["source_refs"]["source_pdf"],"sha256":h}
        chunks=[source_chunks(d) for d in docs]
        if min(map(len,chunks))<12:
            rejects["source_narrative_chunk_quota"]+=1;continue
        pair_id="fresh_text_pair_"+digest(source)[:20]
        chosen=[];used=[set(),set()]
        for si,stratum in enumerate(STRATA):
            # Alternate anchoring sides. NEW-anchored windows allow the blind
            # annotator to discover added facts; OLD anchoring allows removals.
            side=si%2;xs,ys=chunks[side],chunks[1-side]
            candidates=[]
            for i,x in enumerate(xs):
                if i in used[side]:continue
                numeric=bool(re.search(r"\d+(?:[.,]\d+)?\s*(?:кВт|Вт|мм|м[²³23]|Па|°)",x["text"],re.I))
                modal=bool(re.search(r"не допускается|долж|необходимо|требуется|запрещ|следует",x["text"],re.I))
                multi=len(re.findall(r"[.;]\s+[А-ЯA-Z]",x["text"]))>=1
                satisfies={"NUMERIC_SOURCE":numeric,"MODAL_SOURCE":modal,"MULTI_SENTENCE_SOURCE":multi,
                           "EARLY_SOURCE":i<len(xs)/3,"LATE_SOURCE":i>=2*len(xs)/3,"GENERAL_SOURCE":True}[stratum]
                if satisfies:candidates.append(i)
            if not candidates:candidates=[i for i in range(len(xs)) if i not in used[side]]
            i=min(candidates,key=lambda i:digest([SEED,pair_id,stratum,xs[i]["source_refs"]]))
            used[side].add(i);x=xs[i];xt=terms(x)
            def rank(j):
                yt=terms(ys[j]);score=len(xt&yt)/len(xt|yt) if xt|yt else 0
                return (-score,digest([SEED,ys[j]["source_refs"]]))
            js=sorted(range(len(ys)),key=rank)[:3]
            def window(cs,j):
                result=[];length=0
                for k in range(max(0,j-1),min(len(cs),j+2)):
                    z=cs[k]
                    if length+len(z["text"])>8000:continue
                    result.append({"role":"FOCUS" if k==j else "CONTEXT","text":z["text"],"source_refs":z["source_refs"],"heading":z["heading"]});length+=len(z["text"])
                return {"paragraphs":result,"context_is_complete_scope":False}
            case={"case_id":"fresh_text_"+digest([pair_id,side,x["source_refs"]])[:24],"pair_id":pair_id,
                  "anchor_side":"OLD" if side==0 else "NEW","anchor":window(xs,i),
                  "opposite_source_windows":[window(ys,j) for j in js],
                  "old_document":docs[0],"new_document":docs[1],
                  "alignment_answer":None,"change_answer":None,"human_notes":None,
                  "allowed_alignment_answers":["SAME_ENGINEERING_SUBJECT","RELATED_BUT_DIFFERENT","NO_MATCH","UNSURE"],
                  "allowed_shapes":["ONE_TO_ONE","ONE_TO_N","N_TO_ONE","NO_MATCH","REVIEW"],
                  "allowed_change_answers":["VALUE_CHANGED","PROPERTY_CHANGED","SYSTEM_CHANGED","REQUIREMENT_CHANGED","FACT_ADDED","FACT_REMOVED","NO_SEMANTIC_CHANGE","EDITORIAL_CHANGE","REVIEW"],
                  "may_add_missing_counterpart_or_context":True,"absence_requires_positive_scope_evidence":True}
            packets.append(case);chosen.append({"case_id":case["case_id"],"source_stratum":stratum,"anchor_side":case["anchor_side"]})
        pairs.append({"pair_id":pair_id,"old":docs[0],"new":docs[1]});selection.extend(chosen)
        selected_hashes.update(pdf_hashes)
        for p in paths:selected_page_hashes.update(page_hashes(p.read_text(errors="replace")).values())
        if len(packets)>=24:break
    packet={"schema":"fresh-blind-text-alignment.v2","candidate_commit":freeze["candidate_commit"],"candidate_manifest_sha256":file_hash(root/"reports/CANDIDATE_MANIFEST.json"),
            "N":len(packets),"annotations":0,"predictions_included":False,
            "instructions":"Read local OLD/NEW source windows, establish subject and shape first, then compare facts. Windows are source suggestions, not predictions. Add missing context when needed; absence alone is not removal. No answers have been provided.","cases":packets}
    emit(root/"reports/FRESH_TEXT_ALIGNMENT_HOLDOUT.json",packet)
    emit(out/"pairs.json",pairs);emit(out/"source_universe.json",universe)
    emit(out/"selection.json",{"seed":SEED,"N":len(packets),"pairs":len(pairs),"source_universe_count":len(universe),"source_universe_sha256":digest(universe),
                                 "exclusion_receipts":receipts,"excluded_documents":len(excluded),"rejections":dict(rejects),"case_selection":selection,
                                 "candidate_outputs_read":False,"correctness_used_for_selection":False,"human_truth_or_predictions_read":False,
                                 "selected_vs_excluded_page_overlap":len(selected_page_hashes&excluded_pages),
                                 "semantic_class_coverage":"UNVERIFIED: source strata are not labels; true addition/removal and semantic rewrite cannot be guaranteed without annotation"})
    if replay_only:
        after_sources={str(p):file_hash(p) for p in (out/"sources").rglob("*") if p.is_file()}
        if before_sources!=after_sources:raise ValueError("Holdout prepared source replay differs")
        write(root/"reports/HOLDOUT_REPLAY.json",{"pass":True,"deterministic_packet_files":replay_receipts,"source_files_compared":len(before_sources),"model_calls":0,"predictions_read":False})
    print({"N":len(packets),"pairs":len(pairs),"source_universe":len(universe),"rejections":dict(rejects)},flush=True)

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",required=True);p.add_argument("--repo",required=True);p.add_argument("--replay",action="store_true");a=p.parse_args();build(a.root,a.repo,replay_only=a.replay)
