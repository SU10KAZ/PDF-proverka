import sys, json
sys.path.insert(0,"experiments/stage_comparison_vector_objects_v03_opus/probes")
import neg_common as N, grp_match as M, v03_objects as O, v03_counterfactual as CF
import neg_textcf as T, neg_tablecf as TB
out=[]
cs=N.carriers()
for c in cs:
    ex=N.carrier_extract(c)
    if len(ex.segments)>4000 or len(ex.texts)<3: continue
    la=O.build_objects(ex)
    for cid,prm in (("D1_text_edit",{}),("D2_text_move",{}),("C1_remove_object",{"bucket":"small"}),("C3_move_object",{"bucket":"small","frac":0.02})):
        try: ex2,man=CF.apply(ex,la,cid,key=N.carrier_key(c),**prm)
        except Exception as e: continue
        lb=O.build_objects(ex2, S_override=la.S)
        laa=O.build_objects(ex, S_override=la.S)
        off,share,how=N.offset(ex,ex2)
        ref=M.churn_rows(laa,ex.segments,lb,ex2.segments,off,tol=0.8)
        fst=N.rows_fast(laa,ex.segments,lb,ex2.segments,off,tol=0.8)
        assert len(ref)==len(fst), (len(ref),len(fst))
        dif=0; worst=0.0
        for a,b in zip(ref,fst):
            d=abs(a["unmatched_share"]-b["unmatched_share"])
            worst=max(worst,d)
            if d>1e-9: dif+=1
        out.append({"carrier":N.carrier_key(c)[:36],"cf":cid,"rows":len(ref),
                    "rows_differing_unmatched_share":dif,"max_abs_diff":round(worst,9)})
    if len(out)>=24: break
print(json.dumps({"n_checks":len(out),
  "checks_with_any_difference":sum(1 for r in out if r["rows_differing_unmatched_share"]),
  "max_abs_diff_overall":max(r["max_abs_diff"] for r in out) if out else None,
  "rows":out}, ensure_ascii=False, indent=1))
