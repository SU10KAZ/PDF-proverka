"""
test_block_captions.py
----------------------
Гуманизация ссылок на блоки в текстах замечаний (block_captions.py).

Контекст задачи:
- Merge-LLM вставляла внутренние block_id в видимые тексты замечаний
  («Текстовое замечание подтверждено блоками 6L97-3VTH-XTC и 3C6E-3QEP-D39») —
  сторонний эксперт не знает, что такое «блок».
- Замена: ID → подпись «Название» (лист N, стр. PDF M) из уже существующих
  артефактов; слово «блок*» перед ID → «фрагмент*» (с поправкой падежа
  «блоки»→«фрагменты»); ID из problem/description переносятся в структуру
  (image → related_block_ids, text → selected_text_block_ids), иначе критик и
  UI-привязка кропов теряют fallback.
- Контракты по итогам адверсариальной ревизии: санация имён (ID-эхо в label,
  JSON-огрызки, generic-метки, хедж-хвосты), компактный рендер при >4 ID,
  защита псевдотаблиц (не трогаем множественные пробелы), U+0000 во входе,
  различимость подписей в _normalize_problem_pattern, симметричный
  фингерпринт verdict_preservation.

Run:
    python -m pytest tests/test_block_captions.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.findings.block_captions import (  # noqa: E402
    BlockCaption,
    build_block_caption_map,
    humanize_findings,
    humanize_findings_file,
    humanize_text,
)


# ─── Фикстуры артефактов ─────────────────────────────────────────────────────

TEXT_BLOCK_ID = "6L97-3VTH-XTC"
IMAGE_BLOCK_ID = "3C6E-3QEP-D39"
GARBAGE_LABEL_ID = "4QTH-Q9QR-AW4"
UNKNOWN_ID = "ZZZZ-ZZZZ-ZZZ"


def _write_artifacts(output_dir: Path) -> None:
    """Мини-версия document_graph.json + 01_blocks_analysis.json (v2-формат)."""
    graph = {
        "pages": [
            {
                "page": 5,
                "sheet_no_raw": "1 (из 2)",
                "sheet_name": None,
                "text_blocks": [
                    {
                        "id": TEXT_BLOCK_ID,
                        # первая содержательная строка — 3-я (короткие — шапка)
                        "text": "№ п/п\nПримечание\nПеречень отклонений проектных решений РД от ПД\n1\t13AB-РД-ДК-K2",
                    }
                ],
                "image_blocks": [],
            },
            {
                "page": 24,
                "sheet_no_raw": "18",
                "sheet_name": "Схемы систем К2",
                "text_blocks": [],
                "image_blocks": [
                    {
                        "id": IMAGE_BLOCK_ID,
                        # page строкой — как в живых v2-графах
                        "page": "24",
                        "ocr_text_normalized": "Примечание к схемам К2: ревизии на 1, 11, 21 этажах",
                    },
                    {"id": GARBAGE_LABEL_ID, "ocr_text_normalized": ""},
                ],
            },
        ]
    }
    blocks_analysis = {
        "block_analyses": [
            {
                "block_id": IMAGE_BLOCK_ID,
                "page": 24,
                "sheet": "Ливневая канализация и дренаж. Корпус 2. Схемы систем К2",
                "label": "Схема стояков системы К2 с отметками ревизий",
            },
            {
                "block_id": GARBAGE_LABEL_ID,
                "page": 24,
                # битый label — огрызок JSON-ответа OCR (живой кейс)
                "label": '{\n  "location": {\n    "grid_lines": "Не определены",\n    "zone_name": "Схема"\n  ',
            },
        ]
    }
    (output_dir / "document_graph.json").write_text(
        json.dumps(graph, ensure_ascii=False), encoding="utf-8"
    )
    (output_dir / "01_blocks_analysis.json").write_text(
        json.dumps(blocks_analysis, ensure_ascii=False), encoding="utf-8"
    )


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    _write_artifacts(tmp_path)
    return tmp_path


@pytest.fixture
def captions(output_dir: Path):
    return build_block_caption_map(output_dir)


# ─── Карта подписей ──────────────────────────────────────────────────────────

def test_text_block_caption_from_meaningful_line(captions):
    cap = captions[TEXT_BLOCK_ID]
    assert cap.name.startswith("Перечень отклонений")
    assert cap.sheet_no == "1 (из 2)"
    assert cap.page == 5
    assert cap.kind == "text"
    rendered = cap.render()
    assert "лист 1 (из 2)" in rendered
    assert "стр. PDF 5" in rendered


def test_image_block_prefers_stage02_label_over_ocr(captions):
    cap = captions[IMAGE_BLOCK_ID]
    assert cap.name == "Схема стояков системы К2 с отметками ревизий"
    assert cap.sheet_no == "18"
    assert cap.page == 24
    assert cap.kind == "image"


def test_garbage_json_label_degrades_to_positional(captions):
    # label-огрызок JSON: zone_name «Схема» — generic-метка, не название;
    # подпись деградирует до позиционной, но НЕ вырождается
    cap = captions[GARBAGE_LABEL_ID]
    assert cap.name == ""
    assert not cap.is_degenerate
    assert cap.render() == "(лист 18, стр. PDF 24)"


def test_name_with_embedded_id_is_sanitized(tmp_path):
    # эхо-label со своим block_id («Блок 3C6E-… — схема») вернул бы сырой ID
    # в текст и сломал идемпотентность повторного прогона
    graph = {"pages": [{"page": 3, "sheet_no_raw": "2", "text_blocks": [], "image_blocks": [
        {"id": IMAGE_BLOCK_ID, "ocr_text_normalized": f"Блок {IMAGE_BLOCK_ID} — схема стояков канализации"},
    ]}]}
    (tmp_path / "document_graph.json").write_text(json.dumps(graph, ensure_ascii=False), encoding="utf-8")
    caps = build_block_caption_map(tmp_path)
    cap = caps[IMAGE_BLOCK_ID]
    assert IMAGE_BLOCK_ID not in cap.render()

    text = f"Подтверждено блоком {IMAGE_BLOCK_ID}."
    once, replaced = humanize_text(text, caps)
    assert replaced == [IMAGE_BLOCK_ID]
    assert IMAGE_BLOCK_ID not in once
    twice, replaced2 = humanize_text(once, caps)
    assert twice == once and replaced2 == []


def test_hedge_tail_and_nested_quotes_cleaned(tmp_path):
    graph = {"pages": [{"page": 7, "sheet_no_raw": "4", "text_blocks": [], "image_blocks": [
        {"id": IMAGE_BLOCK_ID,
         "ocr_text_normalized": "Чертеж изделия «Анкерная пластина С1», вероятно с указанием размеров и сварных швов"},
    ]}]}
    (tmp_path / "document_graph.json").write_text(json.dumps(graph, ensure_ascii=False), encoding="utf-8")
    cap = build_block_caption_map(tmp_path)[IMAGE_BLOCK_ID]
    # хедж-хвост отрезан, внутренние «ёлочки» заменены на „лапки“
    assert "вероятно" not in cap.name
    assert "«" not in cap.name and "»" not in cap.name
    assert cap.name.startswith("Чертеж изделия „Анкерная пластина")


def test_shorten_does_not_cut_at_abbreviation(tmp_path):
    # «Развертка Пом. 123…» — точка после сокращения, не конец предложения
    long_tail = " со сложным перечислением осей и отметок высот по всем этажам"
    graph = {"pages": [{"page": 9, "sheet_no_raw": "6", "text_blocks": [], "image_blocks": [
        {"id": IMAGE_BLOCK_ID, "ocr_text_normalized": "Развертка Пом. 123 стены в осях 1-5" + long_tail},
    ]}]}
    (tmp_path / "document_graph.json").write_text(json.dumps(graph, ensure_ascii=False), encoding="utf-8")
    cap = build_block_caption_map(tmp_path)[IMAGE_BLOCK_ID]
    assert cap.name != "Развертка Пом"
    assert cap.name.startswith("Развертка Пом. 123")


# ─── Замена в тексте ─────────────────────────────────────────────────────────

def test_humanize_text_replaces_ids_and_block_word(captions):
    text = (
        "Текстовое замечание подтверждено блоками "
        f"{TEXT_BLOCK_ID} и {IMAGE_BLOCK_ID}."
    )
    new_text, replaced = humanize_text(text, captions)
    assert sorted(replaced) == sorted([TEXT_BLOCK_ID, IMAGE_BLOCK_ID])
    assert TEXT_BLOCK_ID not in new_text
    assert IMAGE_BLOCK_ID not in new_text
    # «блоками» → «фрагментами» (то же окончание)
    assert "блоками" not in new_text
    assert "подтверждено фрагментами «Перечень отклонений" in new_text
    assert "«Схема стояков системы К2 с отметками ревизий» (лист 18, стр. PDF 24)" in new_text


def test_humanize_text_nominative_plural_ending(captions):
    # «блоки» → «фрагменты» (не «фрагменти»)
    text = f"Блоки {TEXT_BLOCK_ID} и {IMAGE_BLOCK_ID} противоречат друг другу."
    new_text, _ = humanize_text(text, captions)
    assert new_text.startswith("Фрагменты «Перечень отклонений")
    assert "Фрагменти" not in new_text


def test_humanize_text_keeps_unknown_ids(captions):
    text = f"Расхождение в блоке {UNKNOWN_ID} не подтверждено."
    new_text, replaced = humanize_text(text, captions)
    assert replaced == []
    assert new_text == text  # неизвестный ID и слово «блоке» не тронуты


def test_humanize_text_latin_block_word(captions):
    # живой паттерн из ТХ-проекта: «(BLOCK 9JEA-M6WQ-FLT стр. 4)»
    text = f"В таблицах (BLOCK {IMAGE_BLOCK_ID} стр. 24) указано E160."
    new_text, replaced = humanize_text(text, captions)
    assert replaced == [IMAGE_BLOCK_ID]
    assert "BLOCK" not in new_text
    assert "(фрагмент «Схема стояков системы К2" in new_text


def test_humanize_text_parenthetical_reference(captions):
    text = f"На плане полов (блок {IMAGE_BLOCK_ID}) указано НП5.1."
    new_text, replaced = humanize_text(text, captions)
    assert replaced == [IMAGE_BLOCK_ID]
    assert "(фрагмент «Схема стояков системы К2" in new_text


def test_humanize_text_idempotent(captions):
    text = f"Подтверждено блоком {IMAGE_BLOCK_ID}."
    once, _ = humanize_text(text, captions)
    twice, replaced_again = humanize_text(once, captions)
    assert twice == once
    assert replaced_again == []


def test_humanize_text_preserves_pseudo_tables(captions):
    # выравнивание пробелами в другом месте поля не должно схлопываться
    table = "QF1   16A   ВВГнг 3x2.5"
    text = f"{table}\nПодтверждено блоком {IMAGE_BLOCK_ID}."
    new_text, _ = humanize_text(text, captions)
    assert table in new_text


def test_humanize_text_strips_foreign_nul(captions):
    text = f"Шум\x00в тексте. Подтверждено блоком {IMAGE_BLOCK_ID}."
    new_text, replaced = humanize_text(text, captions)
    assert replaced == [IMAGE_BLOCK_ID]
    assert "\x00" not in new_text
    assert "фрагментом «Схема стояков" in new_text


def test_humanize_text_compact_mode_above_threshold(tmp_path):
    # >4 разных ID в одном поле → компактный рендер без названий,
    # текст не раздувается стеной одинаковых подписей
    ids = [f"AAA{i}-BBB{i}-CC{i}" for i in range(1, 7)]
    graph = {"pages": [{"page": 2, "sheet_no_raw": "1", "text_blocks": [], "image_blocks": [
        {"id": bid, "ocr_text_normalized": "Схема расположения элементов усиления навесного фасада"}
        for bid in ids
    ]}]}
    (tmp_path / "document_graph.json").write_text(json.dumps(graph, ensure_ascii=False), encoding="utf-8")
    caps = build_block_caption_map(tmp_path)
    text = "Подтверждено блоками " + ", ".join(ids) + "."
    new_text, replaced = humanize_text(text, caps)
    assert len(replaced) == 6
    assert "Схема расположения" not in new_text  # без названий
    assert new_text.count("(лист 1, стр. PDF 2)") == 6
    assert len(new_text) < len(text) + 6 * 40


def test_degenerate_caption_is_not_substituted():
    # подпись без названия/листа/страницы вернула бы сырой ID в текст —
    # такие ID не заменяем (и не ломаем идемпотентность)
    captions = {"AAAA-BBBB-CCC": BlockCaption(block_id="AAAA-BBBB-CCC")}
    text = "Подтверждено блоком AAAA-BBBB-CCC."
    new_text, replaced = humanize_text(text, captions)
    assert new_text == text
    assert replaced == []


def test_render_without_name_degrades_gracefully():
    cap = BlockCaption(block_id="AAAA-BBBB-CCC", page=7, sheet_no="3")
    assert cap.render() == "(лист 3, стр. PDF 7)"


# ─── Замечания целиком ───────────────────────────────────────────────────────

def test_humanize_findings_routes_ids_by_kind(captions):
    findings = [
        {
            "id": "F-004",
            "description": f"Противоречие. Подтверждено блоками {TEXT_BLOCK_ID} и {IMAGE_BLOCK_ID}.",
            "related_block_ids": [IMAGE_BLOCK_ID],
        }
    ]
    stats = humanize_findings(findings, captions)
    assert stats["findings_changed"] == 1
    assert stats["ids_replaced"] == 2
    # image-блок уже был в related (дубля нет), текст-блок ушёл в selected_text
    assert findings[0]["related_block_ids"] == [IMAGE_BLOCK_ID]
    assert findings[0]["selected_text_block_ids"] == [TEXT_BLOCK_ID]
    assert stats["related_added"] == 0
    assert stats["text_refs_added"] == 1
    assert TEXT_BLOCK_ID not in findings[0]["description"]


def test_humanize_findings_solution_ids_not_transferred(captions):
    # критик никогда не сканировал solution/risk — перенос блока-примера с
    # другой страницы в related дал бы ложный page_mismatch
    findings = [
        {
            "id": "F-001",
            "solution": f"Оформить по аналогии с узлом {IMAGE_BLOCK_ID}.",
        }
    ]
    stats = humanize_findings(findings, captions)
    assert stats["ids_replaced"] == 1
    assert IMAGE_BLOCK_ID not in findings[0]["solution"]
    assert "related_block_ids" not in findings[0]
    assert "selected_text_block_ids" not in findings[0]


def test_humanize_findings_sub_findings(captions):
    findings = [
        {
            "id": "F-002",
            "problem": "[Объединено 2 замечаний] Общая формулировка.",
            "sub_findings": [
                {"original_id": "F-002", "problem": f"Расхождение в блоке {IMAGE_BLOCK_ID}."},
                {"original_id": "F-009", "problem": "Без идентификаторов."},
            ],
        }
    ]
    stats = humanize_findings(findings, captions)
    assert stats["ids_replaced"] == 1
    assert IMAGE_BLOCK_ID not in findings[0]["sub_findings"][0]["problem"]
    assert "фрагменте «Схема стояков" in findings[0]["sub_findings"][0]["problem"]


def test_humanize_findings_untouched_without_ids(captions):
    findings = [{"id": "F-001", "description": "Обычный текст без идентификаторов."}]
    stats = humanize_findings(findings, captions)
    assert stats["findings_changed"] == 0
    assert "related_block_ids" not in findings[0]


def test_humanize_findings_file_roundtrip(output_dir):
    findings_path = output_dir / "03_findings.json"
    findings_path.write_text(
        json.dumps(
            {
                "meta": {"total_findings": 1},
                "findings": [
                    {
                        "id": "F-004",
                        "description": f"Подтверждено блоком {IMAGE_BLOCK_ID}.",
                        "solution": f"Сверить с {TEXT_BLOCK_ID}.",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    stats = humanize_findings_file(output_dir)
    assert stats is not None
    assert stats["ids_replaced"] == 2

    data = json.loads(findings_path.read_text(encoding="utf-8"))
    f = data["findings"][0]
    assert IMAGE_BLOCK_ID not in f["description"]
    assert TEXT_BLOCK_ID not in f["solution"]
    assert "фрагментом «Схема стояков системы К2" in f["description"]
    # структурная привязка: только из problem/description (solution — нет)
    assert f["related_block_ids"] == [IMAGE_BLOCK_ID]
    assert "selected_text_block_ids" not in f
    # meta не потеряна
    assert data["meta"]["total_findings"] == 1


def test_humanize_findings_file_missing_artifacts(tmp_path):
    # нет 03_findings.json → None, ничего не создаём
    assert humanize_findings_file(tmp_path) is None


# ─── Интеграция с группировкой и verdict_preservation ────────────────────────

def test_normalize_pattern_distinguishes_captions():
    from backend.app.services.findings.findings_service import _normalize_problem_pattern

    a = "Отсутствует УЗО. Подтверждено фрагментом «Схема ГРЩ» (лист 5, стр. PDF 12)."
    b = "Отсутствует УЗО. Подтверждено фрагментом «Схема ВРУ» (лист 7, стр. PDF 14)."
    assert _normalize_problem_pattern(a) != _normalize_problem_pattern(b)
    # компактные подписи тоже различимы
    c = "Отсутствует УЗО. Подтверждено фрагментом (лист 5, стр. PDF 12)."
    d = "Отсутствует УЗО. Подтверждено фрагментом (лист 7, стр. PDF 14)."
    assert _normalize_problem_pattern(c) != _normalize_problem_pattern(d)


def test_verdict_fingerprint_symmetric_raw_vs_humanized():
    from backend.app.services.findings.verdict_preservation import build_fingerprint

    raw = {
        "problem": f"Ревизии противоречивы. Подтверждено блоками {TEXT_BLOCK_ID} и {IMAGE_BLOCK_ID}.",
        "solution": "Унифицировать решение.",
        "sheet": "Лист 18",
        "category": "storm_drain",
        "severity": "ЭКОНОМИЧЕСКОЕ",
    }
    humanized = dict(raw)
    humanized["problem"] = (
        "Ревизии противоречивы. Подтверждено фрагментами "
        "«Перечень отклонений» (лист 1 (из 2), стр. PDF 5) и "
        "«Схема стояков системы К2» (лист 18, стр. PDF 24)."
    )
    fp_raw = build_fingerprint(raw, "finding")
    fp_hum = build_fingerprint(humanized, "finding")
    assert fp_raw["pattern"] == fp_hum["pattern"]
    assert fp_raw["numbers"] == fp_hum["numbers"]
    assert fp_raw["text"] == fp_hum["text"]
