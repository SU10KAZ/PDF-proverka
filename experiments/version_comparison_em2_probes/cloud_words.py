"""Листы с облаками (9)→(10): слова с координатами — сдвиги надписей на 1–3 pt и настоящее добавление «ШУ-кв.114».

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, fitz
from collections import Counter
P=sys.argv[1]
def op(i): return fitz.open(f"{P}/133_23-ГК-ЭМ2 ({i}).pdf")
A,B=op(9),op(10)
k=72/25.4
for a,b in [(67,69),(82,84)]:
    pa,pb=A[a-1],B[b-1]
    def words(pg):
        r=pg.rect; stamp=fitz.Rect(r.x1-192*k, r.y1-62*k, r.x1, r.y1)
        return [(w[4], round(w[0]), round(w[1])) for w in pg.get_text("words") if not fitz.Rect(w[:4]).intersects(stamp)]
    wa,wb=words(pa),words(pb)
    ca,cb=Counter(x[0] for x in wa),Counter(x[0] for x in wb)
    print(f"(9)p{a}->(10)p{b}: words {len(wa)}->{len(wb)}; bag equal={ca==cb}; only_old={list((ca-cb).items())[:8]} only_new={list((cb-ca).items())[:8]}")
    sa,sb=set(wa),set(wb)
    moved=[x for x in wa if x not in sb][:6]
    print("   same word, different position (sample old):", moved)
    print("   ... new:", [x for x in wb if x not in sa][:6])
