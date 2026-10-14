"""Номера листов СО по ячейке «Лист» штампа в (7), (8), (9), (11): дубли, пропуски, пустые формы.

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, fitz, re
P=sys.argv[1]
def op(i): return fitz.open(f"{P}/133_23-ГК-ЭМ2 ({i}).pdf")
def sheet_no(pg):
    r=pg.rect; k=72/25.4
    ws=pg.get_text("words")
    # anchor: word 'Лист' within 200mm x 60mm of bottom-right corner; value = numeric word directly below/above within its column
    br=[w for w in ws if w[4]=='Лист' and w[0]>r.x1-200*k and w[1]>r.y1-60*k]
    cands=[]
    for a in br:
        cx=(a[0]+a[2])/2
        for w in ws:
            if re.fullmatch(r'\d{1,3}(\.\d)?', w[4]) and abs((w[0]+w[2])/2-cx)<6*k and 0<abs(w[1]-a[1])<9*k:
                cands.append((w[4], round(w[1]-a[1])))
    code=[w[4] for w in ws if '133/23' in w[4] and w[1]>r.y1-60*k]
    return cands, code
for i in [7,8,9,11]:
    d=op(i); rows=[]
    for pg in d:
        if pg.number+1 < 44: continue
        c,code=sheet_no(pg)
        if any('.СО' in x for x in code) or 'ЭМ2' in ''.join(code) and pg.rect.width<1200 and pg.rect.height<900:
            rows.append(f"{pg.number+1}:{'/'.join(v for v,_ in c) or '?'}")
    print(f"({i})", " ".join(rows))
