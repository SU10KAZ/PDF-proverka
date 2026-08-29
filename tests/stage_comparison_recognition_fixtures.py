"""Общая фикстура нативного слоя для тестов, которым он нужен лишь как фон.

Полнота распознавания проверяет прочитанный Markdown против нативного текста
PDF. Тесты, которые проверяют СОВСЕМ ДРУГОЕ — разбор экспликации, форму факта,
маршрутизацию синтеза, — не должны заодно доказывать, что лист распознан: им
нужен фон, на котором распознавание в порядке.

Эта фикстура строит ровно такой фон: нативный слой, содержащий то же, что
прочитал Markdown. Расхождение слоя и Markdown — предмет собственных тестов
полноты, а не побочный эффект чужих.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from backend.app.services.stage_comparison import recognition_coverage

#: Столько «прочего текста листа» добавляется к странице, чтобы её нативный
#: слой заведомо считался пригодным (MIN_NATIVE_CHARS).
_FILLER = " ".join("текст чертежа" for _ in range(20))


def native_layer_index(
    left: Iterable[Mapping[str, Any]],
    right: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Индекс нативного слоя, подтверждающий ровно то, что прочитал Markdown."""
    output: dict[str, dict[str, Any]] = {"LEFT": {}, "RIGHT": {}}
    for side, fragments in (("LEFT", left), ("RIGHT", right)):
        by_page: dict[str, list[str]] = {}
        for fragment in fragments or ():
            page = str(int(fragment.get("pdf_page") or 0))
            by_page.setdefault(page, []).append(str(fragment.get("text") or ""))
        for page, texts in by_page.items():
            output[side][page] = recognition_coverage.build_page_index(
                " ".join([*texts, _FILLER])
            )
    return output


__all__ = ["native_layer_index"]
