"""Сверка результатов «Сравнения версий» с реестром GPT (план §11).

Запуск: python gpt_reconcile.py <register.json> <series.json> <api_base> <out.json>

* register.json — реестр GPT из архива EM2_наложения_и_полный_текст.rar;
* series.json — серия «все соседние» ({"series": <ответ GET /series/{id}>});
* api_base — http://<хост>/api/version-comparison/objects/<object_id>.

Ключ записи — (переход, стр. A, стр. B). Категории: согласны; статус разный;
сопоставление разное; у нас «Нужна проверка»; есть только у нас; есть только
у GPT. Для каждого расхождения — первичная причина по правилам (поле ``why``):

* «а» — цветовой артефакт GPT: у нас основная область совпадает, отличие —
  отделённое предупреждение;
* «г» — перепагинация СО: страницы СО сопоставлены иначе, или текст страницы
  изменился, а позиции нет (строки переехали на другую страницу);
* «г-дубль» — страница — дубль листа (⧉ в матрице), отдельной пары у нас нет;
* «з» — разная политика статуса листа СО: у нас — по позициям, у GPT — по
  картинке страницы;
* «и» — у нас «Нужна проверка» (причина в ``reasons``), у GPT — определённый
  статус.

Первичная причина — отправная точка ручного разбора по PDF, а не вывод:
итог разбора и найденные ошибки — в docs/version_comparison_em2_verification.md.
"""
import json
import sys
import urllib.request
from collections import Counter, defaultdict

GPT_SAME = {"Основное содержание совпадает", "Только PDF-пометки", "Удалено предупреждение согласования"}
GPT_DIFF = {"Изменен текст и изображение", "Визуальные различия; текст совпадает"}
GPT_ADDED = {"Добавлен", "Добавлен повтор СО л.1"}
GPT_REMOVED = {"Удален", "Удалена пустая форма СО"}


def gpt_class(status: str) -> str:
    if status in GPT_SAME:
        return "same"
    if status in GPT_DIFF:
        return "different"
    if status in GPT_ADDED:
        return "added"
    if status in GPT_REMOVED:
        return "removed"
    return "other"


def our_class(status: str) -> str:
    return {"same": "same", "same_zones_changed": "same", "different": "different", "added": "added",
            "removed": "removed", "needs_review": "needs_review", "unmatched": "needs_review"}.get(status, status)


def _get(url: str) -> dict:
    return json.load(urllib.request.urlopen(url))


def main(register: str, series_file: str, api: str, out: str) -> None:
    reg = json.load(open(register, encoding="utf-8"))["records"]
    ser = json.load(open(series_file, encoding="utf-8"))["series"]
    mx = _get(f"{api}/projects/{ser['project_id']}/matrix")
    order = [v["version_id"] if isinstance(v, dict) else v for v in mx["versions"]]
    dups = {(order.index(vid), int(p)) for vid, d in (mx.get("duplicates") or {}).items() for p in d}
    gpt = {}
    for r in reg:
        gpt[(r["v_old"], r["v_new"], r.get("old_page"), r.get("new_page"))] = r
    ours, results = {}, {}
    for k, pair in enumerate(ser["pairs"]):
        run = _get(f"{api}/runs/{pair['run_id']}")
        for it in run["items"]:
            key = (k, k + 1, it.get("a_page"), it.get("b_page"))
            ours[key] = it
            if it.get("result_dir"):
                results[key] = _get(f"{api}/runs/{pair['run_id']}/sheets/{it['spid']}")["result"]
    rows = []
    for key in sorted(set(gpt) | set(ours), key=lambda t: (t[0], t[2] or 0, t[3] or 0)):
        g, o = gpt.get(key), ours.get(key)
        if g and o:
            gc, oc = gpt_class(g["status"]), our_class(o.get("result_status") or o["status"])
            cat = "agree" if gc == oc else ("ours_needs_review" if oc == "needs_review" else "status_differs")
        elif o:
            cat = "only_ours"
        else:
            cat = "only_gpt"
        res = results.get(key) or {}
        rows.append({"key": key, "category": cat,
                     "reasons": [x for x in (res.get("reasons") or (o or {}).get("reasons") or [])
                                 if not x.startswith("UNCHECKED_CHANNEL")],
                     "gpt": g and {k: g.get(k) for k in ("status", "title", "visual_status", "text_status", "note")},
                     "ours": o and {k: o.get(k) for k in ("result_status", "title", "part", "spec", "overlay_status",
                                                          "regions", "style_regions")},
                     "_channels": res.get("channels") or {}})
    # «Только у GPT» и «только у нас» по одной стороне — это разное сопоставление листов.
    by_side = defaultdict(list)
    for r in rows:
        if r["category"] in ("only_ours", "only_gpt"):
            v0, v1, a, b = r["key"]
            if a:
                by_side[(v0, "a", a)].append(r)
            if b:
                by_side[(v0, "b", b)].append(r)
    for lst in by_side.values():
        cats = {r["category"] for r in lst}
        if cats == {"only_ours", "only_gpt"}:
            for r in lst:
                r["category"] = "matching_differs"
    for r in rows:
        r["why"] = _why(r, dups)
        del r["_channels"]
    summary = defaultdict(Counter)
    for r in rows:
        summary[f"({r['key'][0]})→({r['key'][1]})"][r["category"]] += 1
    causes = Counter(f"{r['category']}:{r['why']}" for r in rows if r["category"] != "agree")
    json.dump({"rows": rows, "summary": {k: dict(v) for k, v in summary.items()}, "causes": dict(causes)},
              open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    for k, v in summary.items():
        print(k, dict(v))
    for k, v in sorted(causes.items()):
        print(" ", k, v)


def _why(r: dict, dups: set) -> str:
    cat = r["category"]
    k, _, a, b = r["key"]
    g, o = r["gpt"] or {}, r["ours"] or {}
    if cat == "agree":
        return ""
    if cat in ("matching_differs", "only_gpt", "only_ours"):
        return "г-дубль" if (k, a) in dups or (k + 1, b) in dups else "г"
    if cat == "ours_needs_review":
        return "и"
    spec = o.get("spec") if isinstance(o.get("spec"), dict) else {}
    if spec.get("basis") == "positions":
        return "г" if g.get("status") == "Изменен текст и изображение" else "з"
    ch = r["_channels"]
    if (ch.get("warnings") or {}).get("diffs") and not (ch.get("geometry") or {}).get("diffs"):
        return "а"
    return "?"


if __name__ == "__main__":
    main(*sys.argv[1:5])
