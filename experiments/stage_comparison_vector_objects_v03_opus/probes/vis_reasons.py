#!/usr/bin/env python3
"""visscore S3 — разбор ОБОСНОВАНИЙ слепых ответов (не только вердикта).

Разметка обоснований — моя, один наблюдатель, тот же, что ставил истину.
Она названа явно и лежит рядом с числами, чтобы её можно было оспорить построчно.
"""
import json, os
from collections import Counter

ART = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "artifacts")

# localization: HIT — обоснование называет ровно тот элемент/механизм, который в истине;
#               PARTIAL — вердикт верен, но механизм назван неточно; MISS — назван не тот элемент.
# naming: exact — объект назван так же, как в истине; other — тот же объект под другим именем.
ADJ = {
 "vis01": ("HIT", "exact", "", "верно свёл разницу к тексту подписи"),
 "vis02": ("HIT", "exact", "", "заливка и оранжевая полоса — ровно предмет записи"),
 "vis03": ("HIT", "exact", "", "назван и механизм: иная оцифровка кружка"),
 "vis04": ("HIT", "exact", "", ""),
 "vis05": ("MISS", "n/a", "", "выдуман переход «залитый клин → тонкая линия»; на деле округление 0.5 pt"),
 "vis06": ("HIT", "exact", "", ""),
 "vis07": ("HIT", "exact", "", "оба чертежа названы своими заголовками"),
 "vis08": ("HIT", "exact", "стрелка уклона переехала к тройнику", "добавленный фитинг назван верно; про стрелку в истине ничего нет"),
 "vis09": ("HIT", "exact", "", ""),
 "vis10": ("HIT", "exact", "", ""),
 "vis11": ("HIT", "exact", "", ""),
 "vis12": ("PARTIAL", "other", "", "объект назван «кружок с выноской» вместо знака дерева; механизм — «число сегментов» вместо округления координат, но категория («иная оцифровка») верна"),
 "vis13": ("HIT", "exact", "", "разное разложение на примитивы (×8.3) не создало видимой разницы — агент это и сказал"),
 "vis14": ("HIT", "exact", "", ""),
 "vis15": ("HIT", "exact", "", "прочитал заголовок и оси на растровом кропе"),
 "vis16": ("HIT", "exact", "", ""),
 "vis17": ("HIT", "exact", "", ""),
 "vis18": ("HIT", "exact", "", ""),
 "vis19": ("HIT", "exact", "", ""),
 "vis20": ("HIT", "exact", "", ""),
 "vis21": ("PARTIAL", "exact", "", "вердикт верен, но механизм назван неверно: «заливка/контур» вместо ступенчатого хвоста от округления"),
 "vis22": ("HIT", "other", "", "«знак обрыва с двумя полками» = символ арматурного стыка"),
 "vis23": ("HIT", "exact", "", ""),
 "vis24": ("HIT", "exact", "", ""),
 "vis25": ("HIT", "other", "", "«символ разрыва» = тот же символ арматурного стыка"),
 "vis26": ("HIT", "exact", "", "разницу отнёс к растрированию краёв — это и есть антиалиасинг"),
 "vis27": ("HIT", "exact", "", ""),
 "vis28": ("HIT", "exact", "", "запись реестра 15 582 pt — фантом; агент сказал «совпадают»"),
 "vis29": ("HIT", "exact", "", ""),
 "vis30": ("HIT", "exact", "", ""),
}


def main():
    score = {r["case_id"]: r for r in json.load(open(os.path.join(ART, "vis_score.json"), encoding="utf-8"))["rows"]}
    rows = []
    for cid, (loc, nam, extra, note) in ADJ.items():
        r = score[cid]
        rows.append(dict(case_id=cid, truth=r["truth"], pred=r["pred"], correct=r["correct"],
                         localization=loc, naming=nam, unverified_extra_claim=extra,
                         note_ru=note, why=r["why"]))
    diff = [x for x in rows if x["truth"] == "DIFFERENT"]
    same = [x for x in rows if x["truth"] == "SAME"]
    summ = dict(
        n=len(rows),
        localization_all=dict(Counter(x["localization"] for x in rows)),
        localization_DIFFERENT=dict(Counter(x["localization"] for x in diff)),
        localization_SAME=dict(Counter(x["localization"] for x in same)),
        naming_DIFFERENT=dict(Counter(x["naming"] for x in diff)),
        hit_share_DIFFERENT=round(sum(1 for x in diff if x["localization"] == "HIT") / len(diff), 3),
        unverified_extra_claims=[x["case_id"] for x in rows if x["unverified_extra_claim"]],
        adjudicator="один наблюдатель (тот же, что ставил истину); второго разметчика нет",
    )
    json.dump(dict(summary=summ, rows=rows), open(os.path.join(ART, "vis_reasons.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(summ, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
