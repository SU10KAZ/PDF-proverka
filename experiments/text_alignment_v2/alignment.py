"""Three cheap retrieval experiments with explicit, local identity certificates."""
from collections import Counter, defaultdict
from difflib import SequenceMatcher
import math
import re

from experiments.text_comparison_v1.common import digest
from .facts import analysis, joined
from .units import canonical_text, MAX_LOCAL_CHARS

TOP_K = 3
MAX_POSTING_CANDIDATES = 256
APPROACHES = ("sequence", "engineering_anchor", "hybrid")


def variants(units, grouped=True):
    result=[]
    for i,u in enumerate(units):
        if u["eligibility"]!="ELIGIBLE":
            continue
        for size in range(1,4 if grouped else 2):
            group=units[i:i+size]
            if len(group)!=size or any(x["eligibility"]!="ELIGIBLE" for x in group):
                break
            if any(x["source_refs"][-1]["line_id"]+1 != y["source_refs"][0]["line_id"] for x,y in zip(group,group[1:])):
                break
            text=joined(group)
            if len(text)>MAX_LOCAL_CHARS:
                break
            f=analysis(text)
            result.append({"id":digest([x["unit_id"] for x in group])[:24],"units":group,
                           "ids":[x["unit_id"] for x in group],"start":i,"size":size,"f":f,
                           "terms":set(f["words"]),"anchors":set(f["marks"]+([s["property"] for s in f["slots"]] if f["engineering"] else []))})
    return result


def section_titles(v):
    return {canonical_text(s["title"]) for u in v["units"] for s in u.get("section_context",[]) if len(s["title"].split())>=2}


def scope_support(a,b,stable):
    titles_a,titles_b=section_titles(a),section_titles(b)
    same_heading=bool(titles_a & titles_b)
    # Immediate content witnesses are unique exact singleton matches in both
    # documents. Position does not establish the witness; content does.
    before=(a["start"]-1,b["start"]-1) in stable
    after=(a["start"]+a["size"],b["start"]+b["size"]) in stable
    marks=bool(a["f"]["marks"]) and a["f"]["marks"]==b["f"]["marks"] and min(len(a["f"]["words"]),len(b["f"]["words"]))>=10
    evidence=[]
    if same_heading: evidence.append("SAME_HEADING_TEXT_PRIOR")
    if before: evidence.append("UNIQUE_EXACT_PRECEDING_CONTENT")
    if after: evidence.append("UNIQUE_EXACT_FOLLOWING_CONTENT")
    if marks: evidence.append("SAME_EXPLICIT_ENGINEERING_MARKS")
    return evidence


def similarity(a,b,idf):
    common=a["terms"]&b["terms"]
    union=a["terms"]|b["terms"]
    return sum(idf.get(t,1) for t in common)/sum(idf.get(t,1) for t in union) if union else 0


def certificate(a,b):
    x,y=a["f"],b["f"]
    if x["canonical"].rstrip(".; ")==y["canonical"].rstrip(".; "):
        return "EXACT_LOCAL_ASSERTION"
    if (x["template"]==y["template"] and x["slots"] and y["slots"] and
            x["marks"]==y["marks"] and x["engineering"] and y["engineering"] and
            min(len(x["words"]),len(y["words"]))>=7):
        return "EXACT_TYPED_ASSERTION_TEMPLATE"
    return None


def align(old,new,approach="hybrid"):
    if approach not in APPROACHES:
        raise ValueError(approach)
    aa,bb=variants(old,approach!="sequence"),variants(new,approach!="sequence")
    ai={v["id"]:v for v in aa}; bi={v["id"]:v for v in bb}
    exact_a,exact_b=defaultdict(list),defaultdict(list)
    templates_a,templates_b=defaultdict(list),defaultdict(list)
    postings,anchors=defaultdict(set),defaultdict(set)
    for v in aa:
        exact_a[v["f"]["canonical"].rstrip(".; ")].append(v)
        templates_a[v["f"]["template"]].append(v)
    for v in bb:
        exact_b[v["f"]["canonical"].rstrip(".; ")].append(v)
        templates_b[v["f"]["template"]].append(v)
        for t in v["terms"]:postings[t].add(v["id"])
        for t in v["anchors"]:anchors[t].add(v["id"])
    idf={t:1+math.log((len(bb)+1)/(len(ids)+1)) for t,ids in postings.items()}
    stable={(x[0]["start"],exact_b[k][0]["start"]) for k,x in exact_a.items()
            if len(x)==1 and len(exact_b[k])==1 and x[0]["size"]==exact_b[k][0]["size"]==1}
    seq_candidates=defaultdict(set)
    if approach=="sequence":
        seq=SequenceMatcher(None,[v["f"]["template"] for v in aa],[v["f"]["template"] for v in bb],autojunk=False)
        for tag,i,j,k,l in seq.get_opcodes():
            if tag=="equal":
                for x,y in zip(aa[i:j],bb[k:l]):seq_candidates[x["id"]].add(y["id"])
            elif tag=="replace":
                for offset,x in enumerate(aa[i:j]):
                    for y in bb[k+max(0,offset-1):min(l,k+offset+2)]:seq_candidates[x["id"]].add(y["id"])
    proposals, packages=[],[]
    for a in aa:
        cheap=Counter()
        if approach=="sequence":
            pool=seq_candidates[a["id"]]
        elif approach=="engineering_anchor":
            for t in a["anchors"]:
                for vid in anchors[t]:cheap[vid]+=1
            pool={k for k,_ in sorted(cheap.items(),key=lambda x:(-x[1],x[0]))[:MAX_POSTING_CANDIDATES]}
        else:
            for t in a["terms"]:
                for vid in postings[t]:cheap[vid]+=idf[t]
            pool={k for k,_ in sorted(cheap.items(),key=lambda x:(-x[1],x[0]))[:MAX_POSTING_CANDIDATES]}
            pool.update(v["id"] for v in exact_b[a["f"]["canonical"].rstrip(".; ")])
            pool.update(v["id"] for v in templates_b[a["f"]["template"]])
        ranks=[]
        for vid in sorted(pool):
            b=bi[vid]
            if a["size"]>1 and b["size"]>1:
                continue
            cert=certificate(a,b)
            score=similarity(a,b,idf)
            if score<.18 and not cert:
                continue
            scope=scope_support(a,b,stable)
            rank=(2 if cert=="EXACT_LOCAL_ASSERTION" else 1.8 if cert else score)+(.03 if scope else 0)
            # Duplicate assertions anywhere in either document prohibit a unique
            # identity certificate, including those outside a capped posting list.
            if cert=="EXACT_LOCAL_ASSERTION":
                key=a["f"]["canonical"].rstrip(".; ")
                unique=len(exact_a[key])==len(exact_b[key])==1
            elif cert:
                key=a["f"]["template"]
                unique=len(templates_a[key])==len(templates_b[key])==1
            else:
                unique=False
            ranks.append({"new_variant_id":vid,"score":round(rank,8),"lexical_score":round(score,8),
                          "certificate":cert,"globally_unique":unique,"scope_evidence":scope})
        ranks.sort(key=lambda x:(-x["score"],x["new_variant_id"]))
        top=ranks[:TOP_K]
        package={"old_variant_id":a["id"],"old_unit_ids":a["ids"],"candidates":[{**x,"new_unit_ids":bi[x["new_variant_id"]]["ids"]} for x in top],
                 "retrieval_pool_size":len(pool),"local_input_chars":len(joined(a["units"]))+sum(len(joined(bi[x["new_variant_id"]]["units"])) for x in top)}
        package["input_hash"]=digest({"old":joined(a["units"]),"candidates":[joined(bi[x["new_variant_id"]]["units"]) for x in top],"metadata":package})
        packages.append(package)
        if top and top[0]["certificate"] and top[0]["globally_unique"]:
            # Distinct competing complete certificates are never resolved by rank.
            complete=[x for x in ranks if x["certificate"] and x["score"]>=1.8]
            if len(complete)==1:
                proposals.append((a,bi[top[0]["new_variant_id"]],top[0]))
    target_proposals=Counter(b["id"] for a,b,r in proposals)
    used_a,used_b=set(),set()
    relations=[]

    def emit(a,b,decision,evidence,scope=False):
        ids_a=[u["unit_id"] for u in a];ids_b=[u["unit_id"] for u in b]
        kind="ONE_TO_ONE" if len(a)==len(b)==1 else "ONE_TO_N" if len(a)==1 and len(b)>1 else "N_TO_ONE" if len(a)>1 and len(b)==1 else "NO_MATCH" if not a or not b else "REVIEW"
        if decision!="SAME_ENGINEERING_SUBJECT" and kind!="NO_MATCH":kind="REVIEW"
        rel={"old_unit_ids":ids_a,"new_unit_ids":ids_b,"kind":kind,"decision":decision,
             "evidence":evidence,"scope_supported":bool(scope),"section_relation_required":False}
        rel["alignment_id"]="alignment_"+digest([approach,ids_a,ids_b,decision,evidence])[:24]
        relations.append(rel)

    # Complete exact group identity takes priority over a partial typed-template
    # proposal; disjoint ownership follows. No inferred many-to-many relationship.
    proposals.sort(key=lambda x:(-x[2]["score"],-(x[0]["size"]+x[1]["size"]),x[0]["id"],x[1]["id"]))
    for a,b,r in proposals:
        if target_proposals[b["id"]]!=1 or used_a.intersection(a["ids"]) or used_b.intersection(b["ids"]):
            continue
        used_a.update(a["ids"]);used_b.update(b["ids"])
        emit(a["units"],b["units"],"SAME_ENGINEERING_SUBJECT",[r["certificate"],"RECIPROCAL_UNIQUE_COMPLETE_CERTIFICATE"]+r["scope_evidence"],r["scope_evidence"])
    by_old={p["old_unit_ids"][0]:p for p in packages if len(p["old_unit_ids"])==1}
    # Unaligned units each remain separate review work items. Candidate metadata
    # is not promoted to a match or to FACT_ADDED/FACT_REMOVED.
    for side,units,used in (("OLD",old,used_a),("NEW",new,used_b)):
        for u in units:
            if u["unit_id"] in used:continue
            p=by_old.get(u["unit_id"],{}) if side=="OLD" else {}
            reason="CANDIDATES_UNPROVEN" if p.get("candidates") else "NO_CERTIFIED_LOCAL_MATCH"
            emit([u] if side=="OLD" else [],[u] if side=="NEW" else [],"UNSURE",[reason]+u["review_reasons"])
    return {"approach":approach,"relations":relations,"retrieval":packages,
            "quality":{"old_units":len(old),"new_units":len(new),"aligned_old_units":len(used_a),"aligned_new_units":len(used_b),
                       "aligned_relations":sum(r["decision"]=="SAME_ENGINEERING_SUBJECT" for r in relations),
                       "review_units":len(old)+len(new)-len(used_a)-len(used_b),
                       "shapes":dict(Counter(r["kind"] for r in relations)),"model_calls":0}}
