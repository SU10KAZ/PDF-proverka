#!/usr/bin/env python3
"""G2 research PoC: компаратор двух SYSTEM_GRAPH.

RESEARCH ONLY. Отделён от экстрактора намеренно (§27): экстрактор знает
дисциплину, компаратор — нет. Он оперирует только контрактом SYSTEM_GRAPH
(типы узлов, рёбра, уровни, canonical_identity, evidence), поэтому пригоден
для ВК/ОВ, как только у них появится свой экстрактор.
"""
from __future__ import annotations

import collections
from typing import Any, Optional

CHANGE_TYPES = {
    "SYSTEM_BACKBONE_CHANGED",
    "FUNCTIONAL_GROUP_CHANGED",
    "NODE_ADDED",
    "NODE_REMOVED",
    "NODE_TYPE_CHANGED",
    "CONNECTION_CHANGED",
    "GROUP_COUNT_CHANGED",
    "DETAIL_LEVEL_INCREASED",
    "UNCERTAIN_STRUCTURAL_CHANGE",
}


def _by_type(graph, *types):
    return [n for n in graph["nodes"] if n["type"] in types]


def _ident_set(node) -> set:
    s = set((node.get("attrs") or {}).get("identity_set") or [])
    if node.get("canonical_identity"):
        s.add(node["canonical_identity"])
    return s


def _region(graph, node) -> Optional[dict]:
    bb = None
    for e in node.get("evidence") or []:
        if e.get("bbox_visual_pt"):
            bb = e["bbox_visual_pt"]
            break
    if bb is None:
        return None
    return {"block_id": graph["block"]["block_id"], "page_index": graph["block"]["page_index"],
            "bbox_visual_pt": bb}


def _source_path(graph, section_id):
    """Упорядоченная цепочка узлов пути питания секции: источник → … → ввод."""
    idx = {n["id"]: n for n in graph["nodes"]}
    inputs = [n for n in graph["nodes"] if n["type"] == "INPUT_DEVICE"
              and n["canonical_identity"] == f"INPUT#{section_id}"]
    if not inputs:
        return []
    target = inputs[0]["id"]
    incoming = collections.defaultdict(list)
    for e in graph["edges"]:
        if e["type"] == "FEEDS":
            incoming[e["to"]].append(e["from"])
    chain, cur, guard = [target], target, 0
    while incoming.get(cur) and guard < 12:
        cur = incoming[cur][0]
        chain.append(cur)
        guard += 1
    return [idx[i] for i in reversed(chain) if i in idx]


# ═══════════════════════════════ LEVEL A ═══════════════════════════════════

def compare_backbone(left, right):
    changes, facts = [], {}
    ls = _by_type(left, "BUS_SECTION")
    rs = _by_type(right, "BUS_SECTION")
    lt = _by_type(left, "SECTION_DEVICE")
    rt = _by_type(right, "SECTION_DEVICE")
    li = _by_type(left, "INPUT_DEVICE")
    ri = _by_type(right, "INPUT_DEVICE")
    facts["bus_sections"] = [len(ls), len(rs)]
    facts["section_ties"] = [len(lt), len(rt)]
    facts["inputs"] = [len(li), len(ri)]

    if len(ls) != len(rs):
        changes.append({
            "type": "SYSTEM_BACKBONE_CHANGED", "level": "A", "subject": "bus_sections",
            "summary": f"число секций шин изменилось: {len(ls)} → {len(rs)}",
            "left_region": _region(left, ls[0]) if ls else None,
            "right_region": _region(right, rs[0]) if rs else None,
            "evidence": [{"side": "left", "value": [n["display_label"] for n in ls]},
                         {"side": "right", "value": [n["display_label"] for n in rs]}],
            "confidence": "HIGH", "provenance": ["VECTOR"],
        })
    if len(li) != len(ri):
        changes.append({
            "type": "SYSTEM_BACKBONE_CHANGED", "level": "A", "subject": "inputs",
            "summary": f"число вводов изменилось: {len(li)} → {len(ri)}",
            "left_region": _region(left, li[0]) if li else None,
            "right_region": _region(right, ri[0]) if ri else None,
            "evidence": [{"side": "left", "value": [n["display_label"] for n in li]},
                         {"side": "right", "value": [n["display_label"] for n in ri]}],
            "confidence": "HIGH", "provenance": ["VECTOR"],
        })

    # ── пути питания: раскрытие детализации vs смена источника (§14/§15/§22) ──
    path_facts = []
    for i, (a, b) in enumerate(zip(ls, rs), 1):
        lp = _source_path(left, a["id"])
        rp = _source_path(right, b["id"])
        lsrc = next((n for n in lp if n["type"] == "SOURCE"), None)
        rsrc = next((n for n in rp if n["type"] == "SOURCE"), None)
        lmid = [n.get("subclass") for n in lp if n["type"] == "SERVICE_NODE"]
        rmid = [n.get("subclass") for n in rp if n["type"] == "SERVICE_NODE"]
        rec = {
            "section": [a["display_label"], b["display_label"]],
            "left_path": [n.get("display_label") for n in lp],
            "right_path": [n.get("display_label") for n in rp],
            "left_source_subclass": (lsrc or {}).get("subclass"),
            "right_source_subclass": (rsrc or {}).get("subclass"),
            "intermediates": [lmid, rmid],
        }
        path_facts.append(rec)
        lab = ((lsrc or {}).get("attrs") or {}).get("abstraction")
        rab = ((rsrc or {}).get("attrs") or {}).get("abstraction")
        if lsrc and rsrc and lab != rab and lmid == rmid:
            changes.append({
                "type": "DETAIL_LEVEL_INCREASED", "level": "A", "subject": "source_path",
                "summary": (f"источник секции {a['display_label']} показан подробнее: "
                            f"{lsrc['display_label']} ({lsrc.get('subclass')}) → "
                            f"{rsrc['display_label']} ({rsrc.get('subclass')}); "
                            "остальная цепочка питания совпадает"),
                "left_region": _region(left, lsrc), "right_region": _region(right, rsrc),
                "evidence": [{"side": "left", "value": rec["left_path"]},
                             {"side": "right", "value": rec["right_path"]}],
                "confidence": "MEDIUM", "provenance": ["VECTOR"],
                "note": "НЕ трактуется как появление нового источника",
            })
        elif lsrc and rsrc and len(lp) != len(rp):
            changes.append({
                "type": "DETAIL_LEVEL_INCREASED" if len(rp) > len(lp) else "CONNECTION_CHANGED",
                "level": "A", "subject": "source_path",
                "summary": (f"цепочка питания секции {a['display_label']}: "
                            f"{' → '.join(map(str, rec['left_path']))} ⇒ "
                            f"{' → '.join(map(str, rec['right_path']))}"),
                "left_region": _region(left, lsrc), "right_region": _region(right, rsrc),
                "evidence": [{"side": "left", "value": rec["left_path"]},
                             {"side": "right", "value": rec["right_path"]}],
                "confidence": "MEDIUM", "provenance": ["VECTOR"],
            })
    facts["source_paths"] = path_facts

    # ── секционирование ──
    if lt and rt:
        a, b = lt[0], rt[0]
        if a.get("subclass") != b.get("subclass"):
            changes.append({
                "type": "NODE_TYPE_CHANGED", "level": "A", "subject": "section_tie",
                "summary": (f"секционный аппарат сменил тип: {a['display_label']} "
                            f"({a.get('subclass')}) → {b['display_label']} ({b.get('subclass')})"),
                "left_region": _region(left, a), "right_region": _region(right, b),
                "evidence": [{"side": "left", "value": a["display_label"],
                              "attrs": a.get("attrs")},
                             {"side": "right", "value": b["display_label"],
                              "attrs": b.get("attrs")}],
                "confidence": "HIGH", "provenance": ["VECTOR"],
            })
        lc = set((a.get("attrs") or {}).get("control") or [])
        rc = set((b.get("attrs") or {}).get("control") or [])
        if lc != rc:
            changes.append({
                "type": "FUNCTIONAL_GROUP_CHANGED", "level": "A", "subject": "section_tie_control",
                "summary": (f"управление секционным аппаратом: {sorted(lc) or '—'} → "
                            f"{sorted(rc) or '—'}"),
                "left_region": _region(left, a), "right_region": _region(right, b),
                "evidence": [{"side": "left", "value": sorted(lc)},
                             {"side": "right", "value": sorted(rc)}],
                "confidence": "MEDIUM", "provenance": ["VECTOR"],
            })
    elif bool(lt) != bool(rt):
        changes.append({
            "type": "SYSTEM_BACKBONE_CHANGED", "level": "A", "subject": "section_tie",
            "summary": "секционная связь есть только на одной стадии",
            "left_region": _region(left, lt[0]) if lt else None,
            "right_region": _region(right, rt[0]) if rt else None,
            "evidence": [{"side": "left", "value": [n["display_label"] for n in lt]},
                         {"side": "right", "value": [n["display_label"] for n in rt]}],
            "confidence": "MEDIUM", "provenance": ["VECTOR"],
        })

    backbone_same = (len(ls) == len(rs) and len(li) == len(ri) and bool(lt) == bool(rt))
    verdict = ("BACKBONE_PRESERVED" if backbone_same else "BACKBONE_CHANGED")
    if not ls or not rs:
        verdict = "UNCERTAIN"
    return verdict, changes, facts


# ═══════════════════════════════ LEVEL B ═══════════════════════════════════

import re as _re


def _device_family(token: str) -> str:
    """Марка прибора → семейство: «1ТТ1...1ТТ3»→ТТ, «ТА7...ТА9»→ТА, «Wh1»→WH.

    Нужна, чтобы «учёт сохранён, приборы переобозначены» не выглядело как
    «группа учёта исчезла»: в П и РД одни и те же функции подписаны по-разному.
    """
    t = (token or "").upper().replace("Ё", "Е")
    t = _re.sub(r"^\d+", "", t)
    t = _re.split(r"[.\s]", t)[0]
    t = _re.sub(r"[\d\-–_]+$", "", t)
    return t or token


def compare_groups(left, right):
    changes = []
    def snap(g):
        out = collections.defaultdict(dict)
        for n in g["nodes"]:
            if n["type"].endswith("_GROUP"):
                out[n["type"]][n.get("section")] = n
        return out
    L, R = snap(left), snap(right)
    facts = {}
    for gtype in sorted(set(L) | set(R)):
        ls, rs = L.get(gtype, {}), R.get(gtype, {})
        facts[gtype] = {"left_sections": sorted(ls), "right_sections": sorted(rs)}
        if len(ls) != len(rs):
            changes.append({
                "type": "GROUP_COUNT_CHANGED", "level": "B", "subject": gtype,
                "summary": f"{gtype}: групп было {len(ls)}, стало {len(rs)}",
                "left_region": _region(left, next(iter(ls.values()))) if ls else None,
                "right_region": _region(right, next(iter(rs.values()))) if rs else None,
                "evidence": [{"side": "left", "value": sorted(ls)},
                             {"side": "right", "value": sorted(rs)}],
                "confidence": "MEDIUM", "provenance": ["VECTOR"],
            })
        for sec in sorted(set(ls) & set(rs)):
            a, b = ls[sec], rs[sec]
            la = {_device_family(t) for t in ((a.get("attrs") or {}).get("member_tokens") or [])}
            rb = {_device_family(t) for t in ((b.get("attrs") or {}).get("member_tokens") or [])}
            if not la and not rb:
                continue
            jac = len(la & rb) / max(len(la | rb), 1)
            facts.setdefault(gtype + ":overlap", {})[sec] = round(jac, 3)
            if jac < 0.2:
                changes.append({
                    "type": "FUNCTIONAL_GROUP_CHANGED", "level": "B",
                    "subject": f"{gtype}@{sec}",
                    "summary": (f"{gtype} секции {sec}: функция присутствует на обеих "
                                f"стадиях, но набор приборов пересекается на {jac:.0%} — "
                                "исполнение переработано"),
                    "left_region": _region(left, a), "right_region": _region(right, b),
                    "evidence": [{"side": "left", "value": sorted(la)[:10]},
                                 {"side": "right", "value": sorted(rb)[:10]}],
                    "confidence": "LOW", "provenance": ["VECTOR"],
                    "note": "функция присутствует с обеих сторон; изменилось её исполнение",
                })
    return changes, facts


# ═══════════════════════════════ LEVEL C ═══════════════════════════════════

def compare_feeders(left, right):
    changes = []
    lo = [n for n in left["nodes"] if n["type"] == "OUTGOING_DEVICE"]
    ro = [n for n in right["nodes"] if n["type"] == "OUTGOING_DEVICE"]
    lsec = [n["id"] for n in _by_type(left, "BUS_SECTION")]
    rsec = [n["id"] for n in _by_type(right, "BUS_SECTION")]
    sec_map = dict(zip(lsec, rsec))

    def _x(node):
        for e in node.get("evidence") or []:
            if e.get("kind") == "geometry" and isinstance(e.get("value"), dict):
                return e["value"].get("x", 0.0)
        return 0.0

    def order_index(nodes):
        by = collections.defaultdict(list)
        for n in nodes:
            by[n.get("section")].append(n)
        idx = {}
        for sec, group in by.items():
            for i, n in enumerate(sorted(group, key=_x)):
                idx[n["id"]] = i
        return idx
    li, ri = order_index(lo), order_index(ro)

    used_r = set()
    matched, uncertain = [], []

    def try_match(pool_l, pool_r, same_section):
        out = []
        for a in pool_l:
            sa = _ident_set(a)
            if not sa:
                continue
            best = None
            for b in pool_r:
                if b["id"] in used_r:
                    continue
                if same_section and sec_map.get(a.get("section")) != b.get("section"):
                    continue
                sb = _ident_set(b)
                if not sb:
                    continue
                if sa & sb:
                    score = 2
                elif any(x in y or y in x for x in sa for y in sb):
                    score = 1
                else:
                    continue
                if best is None or score > best[0]:
                    best = (score, b)
            if best:
                used_r.add(best[1]["id"])
                out.append((a, best[1], best[0], same_section))
        return out

    pending = list(lo)
    for pair in try_match(pending, ro, True):
        matched.append(pair)
    done_l = {a["id"] for a, _, _, _ in matched}
    for pair in try_match([a for a in lo if a["id"] not in done_l], ro, False):
        matched.append(pair)
    done_l = {a["id"] for a, _, _, _ in matched}

    reorders = 0
    for a, b, score, same_sec in matched:
        conf = "HIGH" if score == 2 and a["confidence"] != "LOW" and b["confidence"] != "LOW" \
            else ("MEDIUM" if score == 2 else "LOW")
        if not same_sec:
            changes.append({
                "type": "CONNECTION_CHANGED", "level": "C", "subject": "feeder_section",
                "summary": (f"ветвь «{a['display_label']}» перенесена: секция "
                            f"{a.get('section')} → {b.get('section')}"),
                "left_region": _region(left, a), "right_region": _region(right, b),
                "evidence": [{"side": "left", "value": a["display_label"]},
                             {"side": "right", "value": b["display_label"]}],
                "confidence": conf, "provenance": ["VECTOR"],
            })
        elif li.get(a["id"]) != ri.get(b["id"]):
            reorders += 1
        if conf == "LOW":
            uncertain.append((a, b))
    if reorders:
        changes.append({
            "type": "CONNECTION_CHANGED", "level": "C", "subject": "branch_order",
            "summary": (f"порядок ветвей в секциях перестроен: у {reorders} из "
                        f"{len(matched)} сопоставленных ветвей изменилась позиция"),
            "left_region": None, "right_region": None,
            "evidence": [{"side": "both", "value": {"reordered": reorders,
                                                    "matched": len(matched)}}],
            "confidence": "HIGH", "provenance": ["VECTOR"],
            "note": "позиция на листе не является идентичностью ветви",
        })

    removed = [a for a in lo if a["id"] not in done_l]
    added = [b for b in ro if b["id"] not in used_r]

    # ── раскрытие детализации: один грубый узел ↔ несколько уточнённых (§31) ──
    detail_pairs = []
    for a in list(removed):
        sa = _ident_set(a)
        kin = [b for b in added if any(x and (x in y or y in x) for x in sa for y in _ident_set(b))]
        if len(kin) >= 2:
            detail_pairs.append((a, kin))
            removed.remove(a)
            for b in kin:
                added.remove(b)
    for a, kin in detail_pairs:
        changes.append({
            "type": "DETAIL_LEVEL_INCREASED", "level": "C", "subject": "feeder_expansion",
            "summary": (f"ветвь «{a['display_label']}» раскрыта в {len(kin)} отдельных: "
                        + ", ".join(str(b["display_label"]) for b in kin)),
            "left_region": _region(left, a), "right_region": _region(right, kin[0]),
            "evidence": [{"side": "left", "value": a["display_label"]},
                         {"side": "right", "value": [b["display_label"] for b in kin]}],
            "confidence": "LOW", "provenance": ["VECTOR"],
        })

    # ── Честная развязка (§18, §33). Пока в ОДНОЙ секции не сопоставлены ветви и
    #    слева, и справа, утверждать «удалено»/«добавлено» нельзя: скорее всего это
    #    одна и та же функция под другим обозначением (ВРУ-НСТ ↔ ШУ.ХП). Такое
    #    выдаётся как неустановленное соответствие. Уверенное NODE_REMOVED /
    #    NODE_ADDED остаётся только там, где другая сторона пуста.
    rev_map = {v: k for k, v in sec_map.items()}
    by_sec_l = collections.defaultdict(list)
    by_sec_r = collections.defaultdict(list)
    for a in removed:
        by_sec_l[a.get("section")].append(a)
    for b in added:
        by_sec_r[rev_map.get(b.get("section"), b.get("section"))].append(b)

    def _name(n):
        return f"{n['display_label'] or n['label']} ({n['label']})"

    for sec in sorted(set(by_sec_l) | set(by_sec_r), key=lambda x: str(x)):
        ul, ur = by_sec_l.get(sec, []), by_sec_r.get(sec, [])
        if ul and ur:
            changes.append({
                "type": "UNCERTAIN_STRUCTURAL_CHANGE", "level": "C",
                "subject": "unresolved_correspondence",
                "summary": (f"секция {sec}: соответствие не установлено для "
                            f"{len(ul)} ветвей слева и {len(ur)} справа — "
                            "утверждать удаление/добавление нельзя"),
                "left_region": _region(left, ul[0]), "right_region": _region(right, ur[0]),
                "evidence": [{"side": "left", "value": [_name(n) for n in ul],
                              "identity": [sorted(_ident_set(n)) for n in ul]},
                             {"side": "right", "value": [_name(n) for n in ur],
                              "identity": [sorted(_ident_set(n)) for n in ur]}],
                "confidence": "LOW", "provenance": ["VECTOR"],
                "note": "кандидаты на переименование семейства щита либо на перенос функции",
            })
            if len(ul) != len(ur):
                changes.append({
                    "type": "GROUP_COUNT_CHANGED", "level": "C",
                    "subject": f"outgoing_devices@{sec}",
                    "summary": (f"секция {sec}: несопоставленных ветвей слева {len(ul)}, "
                                f"справа {len(ur)} — нетто-изменение "
                                f"{len(ur) - len(ul):+d} отходящих"),
                    "left_region": None, "right_region": None,
                    "evidence": [{"side": "left", "value": [_name(n) for n in ul]},
                                 {"side": "right", "value": [_name(n) for n in ur]}],
                    "confidence": "MEDIUM", "provenance": ["VECTOR"],
                })
            continue
        for a in ul:
            ctype = ("UNCERTAIN_STRUCTURAL_CHANGE" if not _ident_set(a) else "NODE_REMOVED")
            changes.append({
                "type": ctype, "level": "C", "subject": "outgoing_device",
                "summary": (f"ветвь «{_name(a)}» секции {sec} не сопоставлена справа"
                            + ("" if ctype == "NODE_REMOVED"
                               else " — идентичность не восстановлена")),
                "left_region": _region(left, a), "right_region": None,
                "evidence": [{"side": "left", "value": a["display_label"],
                              "identity": sorted(_ident_set(a)),
                              "status": (a.get("attrs") or {}).get("status")}],
                "confidence": "MEDIUM" if ctype == "NODE_REMOVED" else "LOW",
                "provenance": ["VECTOR"],
            })
        for b in ur:
            ctype = ("UNCERTAIN_STRUCTURAL_CHANGE" if not _ident_set(b) else "NODE_ADDED")
            changes.append({
                "type": ctype, "level": "C", "subject": "outgoing_device",
                "summary": (f"ветвь «{_name(b)}» секции {sec} не сопоставлена слева"
                            + ("" if ctype == "NODE_ADDED"
                               else " — идентичность не восстановлена")),
                "left_region": None, "right_region": _region(right, b),
                "evidence": [{"side": "right", "value": b["display_label"],
                              "identity": sorted(_ident_set(b)),
                              "status": (b.get("attrs") or {}).get("status")}],
                "confidence": "MEDIUM" if ctype == "NODE_ADDED" else "LOW",
                "provenance": ["VECTOR"],
            })

    if len(lo) != len(ro):
        changes.insert(0, {
            "type": "GROUP_COUNT_CHANGED", "level": "C", "subject": "outgoing_devices",
            "summary": f"число отходящих аппаратов изменилось: {len(lo)} → {len(ro)}",
            "left_region": None, "right_region": None,
            "evidence": [{"side": "left", "value": collections.Counter(
                              n.get("section") for n in lo)},
                         {"side": "right", "value": collections.Counter(
                              n.get("section") for n in ro)}],
            "confidence": "HIGH", "provenance": ["VECTOR"],
        })

    facts = {
        "left_outgoing": len(lo), "right_outgoing": len(ro),
        "matched": len(matched),
        "matched_high": sum(1 for a, b, sc, ss in matched
                            if sc == 2 and a["confidence"] != "LOW" and b["confidence"] != "LOW"),
        "matched_in_other_section": sum(1 for _, _, _, ss in matched if not ss),
        "reordered": reorders,
        "unmatched_left": len(removed), "unmatched_right": len(added),
        "uncertain_pairs": len(uncertain),
        "matches": [{"left": a["display_label"], "left_device": a["label"],
                     "right": b["display_label"], "right_device": b["label"],
                     "left_section": a.get("section"), "right_section": b.get("section"),
                     "left_pos": li.get(a["id"]), "right_pos": ri.get(b["id"]),
                     "score": sc} for a, b, sc, ss in matched],
    }
    return changes, facts


def compare_system_graphs(left, right) -> dict:
    if not left.get("quality", {}).get("backbone_recovered") or \
       not right.get("quality", {}).get("backbone_recovered"):
        return {"route": "MODE_2_REQUIRED", "verdict": "UNCERTAIN",
                "reason": "остов не восстановлен хотя бы с одной стороны",
                "changes": [], "facts": {}}
    verdict, ch_a, facts_a = compare_backbone(left, right)
    ch_b, facts_b = compare_groups(left, right)
    ch_c, facts_c = compare_feeders(left, right)
    changes = ch_a + ch_b + ch_c
    for i, c in enumerate(changes, 1):
        c["change_id"] = f"M2-{i:03d}"
    return {
        "route": "MODE_2_REQUIRED",
        "mode": "MODE_2",
        "backbone_verdict": verdict,
        "levels": {"A": facts_a, "B": facts_b, "C": facts_c},
        "changes": changes,
        "quality": {
            "left": left["quality"], "right": right["quality"],
            "identity_match_rate": round(facts_c["matched"] / max(facts_c["left_outgoing"], 1), 3),
            "high_confidence_match_rate": round(
                facts_c["matched_high"] / max(facts_c["left_outgoing"], 1), 3),
        },
    }


__all__ = ["compare_system_graphs", "CHANGE_TYPES"]
