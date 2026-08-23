# -*- coding: utf-8 -*-
"""mine · step 8 — human-readable table of the benchmark (artifacts/mine_corpus.md)."""
import json
from pathlib import Path
ART = Path(__file__).resolve().parents[1] / "artifacts"


def main():
    d = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))
    L = []
    L.append("# mine — набор трудных пар подготовленных графических блоков (VECTOR 0.3)\n")
    L.append(f"Пар: **{d['n_pairs']}** (из них помечены `uncertain` и НЕ являются эталоном: "
             f"{sum(1 for p in d['pairs'] if 'uncertain' in p['classes'])}).  "
             "Ось: кросс-ревизионная (один документ, соседние версии). "
             "Разметку делал ОДИН размечающий.\n")
    L.append("Каждая пара подтверждена глазами по `artifacts/mine_crops/<pair_id>.png` "
             "(A | B после регистрации | наложение: красное — только A, синее — только B).\n")
    L.append("**Проверка «файл сам с собой»**: для каждой пары сверены sha256 обоих PDF, "
             "совпадений нет (см. поля `side_a.sha256` / `side_b.sha256` в `mine_pairs.json`).\n")

    L.append("\n## Сводка по классам\n")
    cnt = {}
    for p in d["pairs"]:
        for c in p["classes"]:
            cnt[c] = cnt.get(c, 0) + 1
    L.append("| класс | пар |")
    L.append("|---|---|")
    for c, n in sorted(cnt.items(), key=lambda x: -x[1]):
        L.append(f"| `{c}` | {n} |")

    L.append("\n## Пары\n")
    L.append("| # | pair_id | классы | вердикт | объектов | дисц. | документ | версии | стр. A/B | /Rotate | сегментов A/B | diff (равный масштаб) | уверенность |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, p in enumerate(d["pairs"], 1):
        a, b = p["side_a"], p["side_b"]
        s = p["screen_signals"]
        L.append("| {} | `{}` | {} | **{}** | {} | {} | {} | {}→{} | {}/{} | {}/{} | {}/{} | {} | {} |".format(
            i, p["pair_id"], ", ".join(p["classes"]), p["expected_verdict"],
            "—" if p["expected_changed_objects"] is None else p["expected_changed_objects"],
            p["discipline"], p["doc_id"], a["version"], b["version"],
            a["page_number"], b["page_number"], a["rotation"], b["rotation"],
            a["segments"] if a["segments"] is not None else "?",
            b["segments"] if b["segments"] is not None else "?",
            s["diff_frac_block_equal_scale"], p["label_confidence"]))

    L.append("\n## Что именно изменилось (глазами)\n")
    for p in d["pairs"]:
        a, b = p["side_a"], p["side_b"]
        L.append(f"### `{p['pair_id']}` — {', '.join(p['classes'])} → **{p['expected_verdict']}**"
                 + (f", объектов: {p['expected_changed_objects']}" if p["expected_changed_objects"] is not None else ""))
        L.append("")
        L.append(f"* **Чем трудна:** {p['why_hard_ru']}")
        L.append(f"* **Ожидаемое (человек):** {p['human_expected_ru']}")
        L.append(f"* **A:** `{a['pdf']}` стр. {a['page_number']} (page_index {a['page_index']}), "
                 f"блок `{a['block_id']}`, coords_px {a['coords_px']}, page_px {a['page_px']}, /Rotate {a['rotation']}, "
                 f"sha256 `{a['sha256'][:16]}…`")
        L.append(f"* **B:** `{b['pdf']}` стр. {b['page_number']} (page_index {b['page_index']}), "
                 f"блок `{b['block_id']}`, coords_px {b['coords_px']}, page_px {b['page_px']}, /Rotate {b['rotation']}, "
                 f"sha256 `{b['sha256'][:16]}…`")
        L.append(f"* **Картинка:** `{p['crop_png']}`")
        L.append("")
    (ART / "mine_corpus.md").write_text("\n".join(L), encoding="utf-8")
    print("written", ART / "mine_corpus.md")


if __name__ == "__main__":
    main()
