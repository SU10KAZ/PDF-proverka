"""Текст основной надписи и таблицы изменений; аннотации SHX/облака/FreeText в (1), (3), (8), (10).

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, fitz
P=sys.argv[1]
def stamp_words(pg, wmm=185, hmm=60):
    r=pg.rect; k=72/25.4
    box=fitz.Rect(r.x1-wmm*k-10, r.y1-hmm*k-10, r.x1, r.y1)
    ws=[w for w in pg.get_text("words") if fitz.Rect(w[:4]).intersects(box)]
    ws.sort(key=lambda w:(round(w[1]/4),w[0]))
    return " | ".join(w[4] for w in ws)
for f,pages in [("(10)",[1,3,11,12,13,50,51]),("(11)",[12,13,50,52]),("(0)",[3,4,23])]:
    d=fitz.open(f"{P}/133_23-ГК-ЭМ2 {f}.pdf")
    for pn in pages:
        pg=d[pn-1]
        print(f"{f} p{pn} {pg.rect.width:.0f}x{pg.rect.height:.0f} rot={pg.rotation}:", stamp_words(pg)[:420])
d=fitz.open(f"{P}/133_23-ГК-ЭМ2 (10).pdf")
pg=d[46]
an=list(pg.annots()); print("(10) p47 annots:", len(an), an[0].type, an[0].info, an[0].rect, an[0].colors, "opacity", an[0].opacity)
print("   text head:", pg.get_text()[:300].replace("\n"," / "))
d=fitz.open(f"{P}/133_23-ГК-ЭМ2 (8).pdf")
for pn in [55,72]:
    for a in d[pn-1].annots(): print(f"(8) p{pn}", a.type, a.info.get('content'), a.info.get('title'), a.rect, a.colors)
d=fitz.open(f"{P}/133_23-ГК-ЭМ2 (3).pdf")
for pn in [5,32]:
    for a in d[pn-1].annots(): print(f"(3) p{pn}", a.type, repr(a.info.get('content'))[:200], a.info.get('title'), a.rect)
d=fitz.open(f"{P}/133_23-ГК-ЭМ2 (1).pdf")
for a in d[22].annots(): print("(1) p23", a.type, repr(a.info.get('content'))[:200], a.rect)
