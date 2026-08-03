#!/usr/bin/env python3
"""Агрегатор полнокорпусного зонда: corpus_results.jsonl → ЭМПИРИКА_полный_корпус.md.

Сводит метрики по дисциплинам и профилям (маппинг block→profile из
DISCIPLINE_COVERAGE.json), выделяет выбросы-кандидаты на новые режимы привязки.
"""
import json
import statistics as st
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
RESULTS = HERE / "corpus_results.jsonl"
COVERAGE = ROOT / "experiments" / "блоки разных дисциплин" / "DISCIPLINE_COVERAGE.json"
OUT = HERE.parent / "ЭМПИРИКА_полный_корпус.md"


def pct(x, n):
    return f"{100.0 * x / n:.0f}%" if n else "—"


def med(vals, digits=2):
    return round(st.median(vals), digits) if vals else None


def q(vals, p, digits=2):
    if not vals:
        return None
    vs = sorted(vals)
    return round(vs[min(len(vs) - 1, int(p * len(vs)))], digits)


def main():
    rows = [json.loads(l) for l in open(RESULTS, encoding="utf-8")]
    ok = [r for r in rows if r.get("ok")]
    bad = [r for r in rows if not r.get("ok")]

    cov = json.load(open(COVERAGE, encoding="utf-8"))
    prof = {}
    for d in cov["disciplines"]:
        for rec in d["records"]:
            prof[rec["block_id"]] = (d["discipline"], rec["profile_id"], rec.get("subtype"), rec["status"])

    for r in ok:
        p = prof.get(r.get("block_id"))
        r["profile_id"] = p[1] if p else None
        r["status"] = p[3] if p else "unmapped"

    by_d = defaultdict(list)
    for r in ok:
        by_d[r["discipline"]].append(r)

    L = []
    L.append("# Эмпирика полного корпуса: все 1187 уникальных блоков галереи\n")
    L.append("Автогенерация: `пробы/corpus_probe.py` → `пробы/corpus_aggregate.py`; сырьё —")
    L.append("`пробы/corpus_results.jsonl` (по строке на блок). Заменяет выборочную эмпирику")
    L.append("дизайн-отчётов (60+ PDF) полнокорпусными числами.\n")
    L.append(f"Всего блоков: **{len(rows)}** (ok {len(ok)}, ошибок/таймаутов {len(bad)});")
    L.append(f"сматчено с профилями каталога: {sum(1 for r in ok if r['profile_id'])} из {len(ok)}.\n")

    # ── 1. Базовая таблица по дисциплинам ──
    L.append("## 1. База по дисциплинам\n")
    L.append("| Дисц | Блоков | rot≠0 | текст<40 | dashes>0 | drawings за кропом (медиана доли) | сегм p90 | >SEG_CAP | derot выиграл |")
    L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for d in sorted(by_d):
        rs = by_d[d]
        n = len(rs)
        L.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            d, n,
            pct(sum(1 for r in rs if r.get("rotation")), n),
            pct(sum(1 for r in rs if r.get("text_chars", 0) < 40), n),
            pct(sum(1 for r in rs if r.get("dashes_nontrivial", 0) > 0), n),
            med([r.get("off_crop_share", 0) for r in rs], 2),
            q([r.get("segs_total", 0) for r in rs], 0.9, 0),
            sum(1 for r in rs if r.get("capped")),
            sum(1 for r in rs if r.get("space_mode") == "derot"),
        ))
    L.append("")

    # ── 2. Размеры ──
    L.append("## 2. Размеры: засечки и привязка чисел\n")
    L.append("Учитываются блоки с ≥5 голыми числами (2–5 цифр). Режимы: break = число в разрыве")
    L.append("линии; near = линия вплотную под/над/сбоку; tickpair = между парой засечек.\n")
    L.append("| Дисц | Блоков с ≥5 чисел | link-rate медиана | p25 | p75 | доля блоков link≥0.5 | break/near/tickpair | засечки-на-линии ≥6 (доля блоков) | топ длин засечек | масштаб-инлайеры медиана |")
    L.append("|---|---:|---:|---:|---:|---:|---|---:|---|---:|")
    for d in sorted(by_d):
        rs = [r for r in by_d[d] if (r.get("bare_nums") or 0) >= 5]
        n = len(rs)
        lr = [r["link_rate"] for r in rs if r.get("link_rate") is not None]
        modes = Counter()
        for r in rs:
            for k, v in (r.get("link_modes") or {}).items():
                modes[k] += v
        tl = Counter()
        for r in by_d[d]:
            for ln, c in (r.get("tick_len_top") or []):
                tl[ln] += c
        scale = [r["scale_inlier_share"] for r in rs if r.get("scale_inlier_share") is not None]
        L.append("| {} | {} | {} | {} | {} | {} | {}/{}/{} | {} | {} | {} |".format(
            d, n, med(lr), q(lr, 0.25), q(lr, 0.75),
            pct(sum(1 for v in lr if v >= 0.5), n) if n else "—",
            modes.get("break", 0), modes.get("near", 0), modes.get("tickpair", 0),
            pct(sum(1 for r in by_d[d] if r.get("ticks_on_line", 0) >= 6), len(by_d[d])),
            " ".join(f"{k}:{v}" for k, v in tl.most_common(3)),
            med(scale),
        ))
    L.append("")

    # ── 3. Полочки/лидеры, отметки, уклоны ──
    L.append("## 3. Выноски, отметки, уклоны\n")
    L.append("| Дисц | блоков с ≥3 полочек | полочек всего | из них с лидером | отметок всего | с полочкой | с галочкой | уклон-чисел | со стрелкой-заливкой |")
    L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for d in sorted(by_d):
        rs = by_d[d]
        sh = sum(r.get("shelf_texts", 0) for r in rs)
        ld = sum(r.get("leader_shelf_texts", 0) for r in rs)
        ev = sum(r.get("elev_nums", 0) for r in rs)
        evs = sum(r.get("elev_with_shelf", 0) for r in rs)
        evf = sum(r.get("elev_with_flag", 0) for r in rs)
        sn = sum(r.get("slope_nums", 0) for r in rs)
        sa = sum(r.get("slope_with_arrow", 0) for r in rs)
        L.append(f"| {d} | {sum(1 for r in rs if r.get('shelf_texts',0)>=3)} | {sh} | {ld} | {ev} | {evs} | {evf} | {sn} | {sa} |")
    L.append("")

    # ── 4. Кружки и мелкие заливки ──
    L.append("## 4. Кружки и мелкие заливки\n")
    L.append("| Дисц | блоков с кружками | кружков всего | с 1 токеном внутри | полу-пары | концентрич. пары | топ Ø | стрелки-заливки | junction-dots | квадратики |")
    L.append("|---|---:|---:|---:|---:|---:|---|---:|---:|---:|")
    for d in sorted(by_d):
        rs = by_d[d]
        dia = Counter()
        for r in rs:
            for dd, c in (r.get("circle_dia_top") or []):
                dia[dd] += c
        sf = Counter()
        for r in rs:
            for k, v in (r.get("small_fills") or {}).items():
                sf[k] += v
        L.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            d,
            sum(1 for r in rs if r.get("circles_full", 0) > 0),
            sum(r.get("circles_full", 0) for r in rs),
            sum(r.get("circles_with_token", 0) for r in rs),
            sum(r.get("circles_half_pairs", 0) for r in rs),
            sum(r.get("circles_concentric_pairs", 0) for r in rs),
            " ".join(f"Ø{k}:{v}" for k, v in dia.most_common(4)),
            sf.get("arrow_triangle", 0), sf.get("junction_dot", 0), sf.get("square_marker", 0),
        ))
    L.append("")

    # ── 5. Таблицы и глиф-слова ──
    L.append("## 5. Сетки таблиц и глиф-слова\n")
    L.append("| Дисц | блоков с ≥1 регионом сетки | лучший регион ≥6 строк и ≥3 V | text<40 блоков | из них с ≥50 глифами | глиф-словных боксов всего |")
    L.append("|---|---:|---:|---:|---:|---:|")
    for d in sorted(by_d):
        rs = by_d[d]
        t0 = [r for r in rs if r.get("text_chars", 0) < 40]
        L.append("| {} | {} | {} | {} | {} | {} |".format(
            d,
            sum(1 for r in rs if r.get("table_regions", 0) >= 1),
            sum(1 for r in rs if (r.get("table_best") or {}).get("rows", 0) >= 6
                and (r.get("table_best") or {}).get("v", 0) >= 3),
            len(t0),
            sum(1 for r in t0 if r.get("glyphs", 0) >= 50),
            sum(r.get("glyph_word_boxes", 0) for r in t0),
        ))
    L.append("")

    # ── 6. Профили: топ по link-rate и полочкам ──
    L.append("## 6. Срез по профилям (блоки с ≥5 чисел; профили с ≥4 такими блоками)\n")
    by_p = defaultdict(list)
    for r in ok:
        if r.get("profile_id") and (r.get("bare_nums") or 0) >= 5:
            by_p[(r["discipline"], r["profile_id"])].append(r)
    L.append("| Дисц/профиль | блоков | link медиана | полочек/блок медиана | засечек-на-линии медиана |")
    L.append("|---|---:|---:|---:|---:|")
    for (d, p), rs in sorted(by_p.items(), key=lambda kv: (-len(kv[1]),)):
        if len(rs) < 4:
            continue
        lr = [r["link_rate"] for r in rs if r.get("link_rate") is not None]
        L.append("| {}/{} | {} | {} | {} | {} |".format(
            d, p, len(rs), med(lr),
            med([r.get("shelf_texts", 0) for r in rs], 0),
            med([r.get("ticks_on_line", 0) for r in rs], 0)))
    L.append("")

    # ── 7. Выбросы ──
    L.append("## 7. Выбросы — кандидаты на новые режимы/проверку\n")
    o1 = [r for r in ok if (r.get("bare_nums") or 0) >= 15 and (r.get("link_rate") or 0) < 0.15]
    L.append(f"**≥15 чисел и link<0.15 ({len(o1)} блоков)** — числа не о размерах (ряды кладки,")
    L.append("таблицы, координаты) либо непокрытый режим привязки:\n")
    for r in sorted(o1, key=lambda r: -(r.get("bare_nums") or 0))[:40]:
        L.append(f"- {r['discipline']}/{r.get('profile_id')} `{r['block_id']}` — чисел {r['bare_nums']}, link {r.get('link_rate')}, засечек {r.get('ticks_on_line')}, полочек {r.get('shelf_texts')}")
    L.append("")
    o2 = [r for r in ok if r.get("space_mode") == "derot"]
    L.append(f"**Derot-пространство выиграло: {len(o2)} блоков** (rotation-хелпер обязателен):")
    dd = Counter(r["discipline"] for r in o2)
    L.append(", ".join(f"{k} {v}" for k, v in dd.most_common()) + "\n")
    o3 = [r for r in ok if r.get("capped")]
    L.append(f"**Сверхтяжёлые (> SEG_CAP сегментов): {len(o3)}**: " + ", ".join(
        f"{r['discipline']}`{r['block_id']}`({r['segs_total']})" for r in sorted(o3, key=lambda r: -r['segs_total'])[:12]) + "\n")
    if bad:
        L.append(f"**Ошибки/таймауты ({len(bad)}):** " + ", ".join(
            f"{r['discipline']}/{r.get('pdf','?')}: {r.get('error')}" for r in bad[:20]) + "\n")

    # ── 8. Перф ──
    times = [r.get("time_total", 0) for r in ok]
    L.append("## 8. Производительность\n")
    L.append(f"Время на блок: медиана {med(times)}с, p90 {q(times,0.9)}с, max {max(times)}с;")
    L.append(f"суммарно однопоточно ≈{sum(times):.0f}с на {len(ok)} блоков.\n")

    OUT.write_text("\n".join(L), encoding="utf-8")
    print(f"Записано: {OUT} ({len(L)} строк)")


if __name__ == "__main__":
    main()
