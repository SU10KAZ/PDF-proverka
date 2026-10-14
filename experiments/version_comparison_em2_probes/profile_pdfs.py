"""Профиль 12 PDF: SHA-256, страницы, размеры, поворот, текстовый слой, аннотации, изображения. Аргументы: <папка PDF> <выход.json>.

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, os, json, hashlib, re
from collections import Counter
import fitz
d = sys.argv[1]; out = sys.argv[2]
files = sorted(os.listdir(d), key=lambda n: int(re.search(r'\((\d+)\)', n).group(1)))
res = []
for fn in files:
    p = os.path.join(d, fn)
    h = hashlib.sha256(open(p,'rb').read()).hexdigest()
    doc = fitz.open(p)
    pages = []
    for pg in doc:
        txt = pg.get_text("text")
        annots = [(a.type[1], (a.info.get('content') or '')[:60]) for a in pg.annots()] if pg.annots() else []
        imgs = pg.get_images(full=True)
        try: ndraw = len(pg.get_drawings())
        except Exception: ndraw = -1
        fonts = sorted({f[3] for f in pg.get_fonts()})
        pages.append(dict(n=pg.number+1, w=round(pg.rect.width,1), h=round(pg.rect.height,1),
            rot=pg.rotation, mb=[round(x,1) for x in pg.mediabox], chars=len(txt.strip()),
            annots=annots, nimg=len(imgs), ndraw=ndraw, fonts=fonts[:8], widgets=len(list(pg.widgets()))))
    res.append(dict(file=fn, sha=h, pages=len(doc), meta=doc.metadata, toc=len(doc.get_toc()), pg=pages))
json.dump(res, open(out,'w'), ensure_ascii=False)
for r in res:
    P = r['pg']
    sizes = Counter((x['w'],x['h'],x['rot']) for x in P)
    low = [x['n'] for x in P if x['chars'] < 50]
    an = [(x['n'], x['annots']) for x in P if x['annots']]
    print(f"{r['file']}: pages={r['pages']} sha={r['sha'][:12]} producer={r['meta'].get('producer')!r} creator={r['meta'].get('creator')!r} mod={r['meta'].get('modDate')}")
    print("   sizes:", dict(sizes.most_common(6)))
    print("   low-text pages(<50ch):", low[:30])
    print("   annot pages:", [(n, Counter(t for t,_ in a)) for n,a in an][:12])
    print("   imgs/page max:", max(x['nimg'] for x in P), " draw/page median:", sorted(x['ndraw'] for x in P)[len(P)//2])
