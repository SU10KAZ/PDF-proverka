"""Текст аннотаций FreeText попадает в page.get_text(): (0) и (1) PDF 23.

После удаления аннотаций в КОПИИ В ПАМЯТИ текст совпадает с (0). Копия не сохраняется,
исходный файл не меняется (SHA-256 проверяется до и после).
Запуск: python -I annot_text_leak.py <папка с PDF корпуса ЭМ2>. См. docs/version_comparison_plan.md §16.
"""
import difflib
import hashlib
import sys

import fitz

P = sys.argv[1]
src = f"{P}/133_23-ГК-ЭМ2 (1).pdf"
sha_before = hashlib.sha256(open(src, "rb").read()).hexdigest()
a = fitz.open(f"{P}/133_23-ГК-ЭМ2 (0).pdf")[22]
b = fitz.open(src)[22]
diff = [l for l in difflib.unified_diff(a.get_text().splitlines(), b.get_text().splitlines(), lineterm="", n=0)]
print("diff (0)->(1) p23 get_text():", diff[:12])
mem = fitz.open(stream=open(src, "rb").read(), filetype="pdf")
pg = mem[22]
for an in list(pg.annots()):
    pg.delete_annot(an)
print("text equal after dropping annots in memory copy:", pg.get_text() == a.get_text())
print("source sha unchanged:", hashlib.sha256(open(src, "rb").read()).hexdigest() == sha_before)
