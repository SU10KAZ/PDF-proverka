# -*- coding: utf-8 -*-
"""Text-only negative controls that the cf engine does NOT provide.

D1..D8 keep the text-line INVENTORY intact (same count, same font sizes, same
boxes).  A real text-only revision does not: notes get deleted, added, re-typeset at
another height, or the text layer breaks entirely (mine M10).  That matters because
the object layer's ONLY channel from text is the characteristic scale S (median font
size over >=5 lines) and the label anchor.  These controls exercise exactly that
channel while leaving every inked segment byte-identical.
"""
from __future__ import annotations
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_counterfactual as CF   # noqa: E402


def _man(cf_id, ex, ex2, params, note=""):
    return {"cf_class": "N", "cf_id": cf_id, "params": params, "note": note,
            "expected_verdict": "NO_GRAPHIC_CHANGE",
            "n_text_before": len(ex.texts), "n_text_after": len(ex2.texts),
            "geometry_touched": False}


def text_delete(ex, key, frac):
    rng = random.Random(CF._seed_for("NT1_text_delete", key, str(frac)))
    n = len(ex.texts)
    if n < 2:
        raise CF.CFNotApplicable("fewer than 2 text lines")
    k = max(1, int(round(n * frac)))
    ix = list(range(n))
    rng.shuffle(ix)
    drop = set(ix[:k])
    texts = [dict(t) for i, t in enumerate(ex.texts) if i not in drop]
    ex2 = CF._clone(ex, texts=texts, prov={"cf": f"NT1_text_delete_{frac}"})
    return ex2, _man(f"NT1_text_delete_{frac}", ex, ex2,
                     {"frac": frac, "deleted": k}, "notes deleted, geometry untouched")


def text_wipe(ex, key):
    if not ex.texts:
        raise CF.CFNotApplicable("no text")
    ex2 = CF._clone(ex, texts=[], prov={"cf": "NT3_text_wipe"})
    return ex2, _man("NT3_text_wipe", ex, ex2, {"deleted": len(ex.texts)},
                     "whole text layer lost (broken font / raster caption)")


def text_resize(ex, key, k):
    if len(ex.texts) < 2:
        raise CF.CFNotApplicable("fewer than 2 text lines")
    texts = []
    for t in ex.texts:
        u = dict(t)
        u["size"] = float(t.get("size") or 0.0) * k
        bb = t["bbox"]
        cy = (bb[1] + bb[3]) / 2
        cx = (bb[0] + bb[2]) / 2
        w = (bb[2] - bb[0]) * k / 2
        h = (bb[3] - bb[1]) * k / 2
        u["bbox"] = [cx - w, cy - h, cx + w, cy + h]
        texts.append(u)
    ex2 = CF._clone(ex, texts=texts, prov={"cf": f"NT2_text_resize_{k}"})
    return ex2, _man(f"NT2_text_resize_{k}", ex, ex2, {"k": k},
                     "notes re-typeset at another height, geometry untouched")


def text_add(ex, key, frac):
    rng = random.Random(CF._seed_for("NT4_text_add", key, str(frac)))
    n = len(ex.texts)
    if n < 2:
        raise CF.CFNotApplicable("fewer than 2 text lines")
    k = max(1, int(round(n * frac)))
    ix = list(range(n))
    rng.shuffle(ix)
    geom = CF._block_geom(ex)
    texts = [dict(t) for t in ex.texts]
    for i in ix[:k]:
        u = dict(ex.texts[i])
        dx = rng.uniform(-0.2, 0.2) * geom["w"]
        dy = rng.uniform(-0.2, 0.2) * geom["h"]
        bb = u["bbox"]
        u["bbox"] = [bb[0] + dx, bb[1] + dy, bb[2] + dx, bb[3] + dy]
        u["cx"] = u["cx"] + dx
        u["cy"] = u["cy"] + dy
        u["text"] = "ПРИМ. " + str(u["text"])
        texts.append(u)
    ex2 = CF._clone(ex, texts=texts, prov={"cf": f"NT4_text_add_{frac}"})
    return ex2, _man(f"NT4_text_add_{frac}", ex, ex2, {"frac": frac, "added": k},
                     "new notes added, geometry untouched")


VARIANTS = (
    [("NT1_text_delete", {"frac": f}) for f in (0.1, 0.25, 0.5, 0.9)]
    + [("NT3_text_wipe", {})]
    + [("NT2_text_resize", {"k": k}) for k in (0.75, 1.35)]
    + [("NT4_text_add", {"frac": f}) for f in (0.25, 1.0)]
)


def apply(ex, cf_id, key, **p):
    if cf_id == "NT1_text_delete":
        return text_delete(ex, key, p["frac"])
    if cf_id == "NT2_text_resize":
        return text_resize(ex, key, p["k"])
    if cf_id == "NT3_text_wipe":
        return text_wipe(ex, key)
    if cf_id == "NT4_text_add":
        return text_add(ex, key, p["frac"])
    raise ValueError(cf_id)
