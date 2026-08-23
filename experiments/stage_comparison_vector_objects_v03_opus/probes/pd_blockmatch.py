# -*- coding: utf-8 -*-
"""pd_blockmatch — кандидаты пар подготовленных графических блоков П ↔ РД.

Кандидат порождается ТЕКСТОВЫМ якорем (zone_name + content_summary из ocr_text блока)
— это разрешено брифом только как object identity / relation anchor.
Подтверждение пары — ВИЗУАЛЬНОЕ (рендер обоих кропов и просмотр глазами).
"""
from __future__ import annotations
import json, math, re, sys, collections
from pathlib import Path
BASE = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import pd_cropfetch as C  # noqa

STOP = set("и в на не с по для из до от а также при или что это как том тома лист листов чертеже показаны показано представлен изображен фрагмент".split())


def anchor_text(b):
    t = b.get("ocr_text") or ""
    try:
        j = json.loads(t)
        a = j.get("analysis") or j
        parts = [str(a.get("location", {}).get("zone_name") or ""),
                 str(a.get("content_summary") or ""),
                 " ".join(str(x) for x in (a.get("elements") or [])[:20]) if isinstance(a.get("elements"), list) else ""]
        return " ".join(parts)
    except Exception:
        return t


def toks(s):
    s = s.lower().replace("ё", "е")
    ws = re.findall(r"[a-zа-я0-9]+", s)
    return [w for w in ws if w not in STOP and len(w) > 2]


def load(rj, tag):
    out = []
    for b in C.blocks_of(ROOT / rj):
        if not b.get("crop_url"):
            continue
        a = anchor_text(b)
        if not a.strip():
            continue
        b["doc_tag"] = tag
        b["anchor"] = a
        b["toks"] = collections.Counter(toks(a))
        out.append(b)
    return out


def idf(docs):
    df = collections.Counter()
    for d in docs:
        for w in d["toks"]:
            df[w] += 1
    n = len(docs)
    return {w: math.log(1 + n / c) for w, c in df.items()}


def vec(d, I):
    v = {w: (1 + math.log(c)) * I.get(w, 0.0) for w, c in d["toks"].items()}
    nrm = math.sqrt(sum(x * x for x in v.values())) or 1.0
    return {w: x / nrm for w, x in v.items()}


def cos(a, b):
    if len(a) > len(b):
        a, b = b, a
    return sum(x * b.get(w, 0.0) for w, x in a.items())
