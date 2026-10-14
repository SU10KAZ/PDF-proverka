"""Утверждения GPT по (10)→(11): Рр 323.55 → 324.26, марки ШУН с «ПЧ» и без на схеме и в СО.

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, fitz, re
P=sys.argv[1]
def op(i): return fitz.open(f"{P}/133_23-ГК-ЭМ2 ({i}).pdf")
d10, d11 = op(10), op(11)
def find(doc, pat, pages=None):
    out=[]
    for pg in doc:
        if pages and pg.number+1 not in pages: continue
        for w in pg.get_text("words"):
            if re.search(pat, w[4]): out.append((pg.number+1, w[4], tuple(round(v) for v in w[:2])))
    return out
print("Pp=323.55 in (10):", find(d10, r"Рр=323", [12])[:3]); print("Pp=324.26 in (11):", find(d11, r"Рр=324", [13])[:3])
print("ШУН.*ПЧ (10) p13:", find(d10, r"ШУН", [13])[:12])
print("ШУН (11) p14:", find(d11, r"ШУН", [14])[:12])
print("ШУН SO (11):", [x for x in find(d11, r"ШУН") if x[0]>40][:8])
# SO sheet numbers: last word near bottom-right 'Лист' cell; approximate: words inside 20x15mm box at right-bottom above lower edge
def so_sheet(pg):
    r=pg.rect; k=72/25.4
    box=fitz.Rect(r.x1-(5+10)*k-2, r.y1-(5+15)*k, r.x1-5*k+1, r.y1-(5+7)*k)
    return [w[4] for w in pg.get_text("words") if fitz.Rect(w[:4]).intersects(box)]
for i in [7,8,11]:
    d=op(i); print(f"({i}) SO 'Лист' cell guesses:", [(pg.number+1, so_sheet(pg)) for pg in d if pg.number+1>=44][:40])
