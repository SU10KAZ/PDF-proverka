"""Страница «Разрешение на внесение изменений» (PDF 3) в (7) и (11).

Таблица «Изм. / Лист / Содержание изменения / Код» — заявленный перечень изменений по листам.
Запуск: python -I permit_probe.py <папка с PDF корпуса ЭМ2>. См. docs/version_comparison_plan.md §15 п.3, §16.
"""
import sys

import fitz

P = sys.argv[1]
for i, pn in [(11, 3), (7, 3)]:
    pg = fitz.open(f"{P}/133_23-ГК-ЭМ2 ({i}).pdf")[pn - 1]
    t = pg.get_text()
    print(f"=== ({i}) PDF {pn}: {len(t)} chars")
    print(t[:1500])
