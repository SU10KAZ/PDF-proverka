"""Тесты нарезчика text_analysis под лимит Codex (md_chunker)."""
from __future__ import annotations

from backend.app.pipeline.stages.text_analysis.md_chunker import (
    CODEX_TEXT_INPUT_BUDGET,
    merge_text_analysis_parts,
    plan_text_analysis_chunks,
)


def _draw_page(n: int, chars: int) -> str:
    # Чертёжный лист: есть block_id → section_class != "pz".
    return (
        f"## Страница {n}\n**Наименование листа:** План {n} этажа\n"
        f"block_id: B{n}\n" + ("чертёж линия кабель. " * (chars // 20))
    )


def _pz_page(n: int, chars: int) -> str:
    # Текстовый сводный лист без block_id → section_class == "pz".
    return (
        f"## Страница {n}\n**Наименование листа:** Пояснительная записка\n"
        + ("нагрузка таблица. " * (chars // 18))
    )


# ─── Планировщик ────────────────────────────────────────────────────────────


def test_small_md_single_pass():
    """MD в пределах бюджета → None (нарезки нет, поведение как раньше)."""
    md = _pz_page(1, 2_000)
    assert plan_text_analysis_chunks(md, system_len=5_000) is None


def test_large_md_chunks_fit_budget():
    """Большой MD режется так, что каждый проход влезает в бюджет."""
    md = _pz_page(1, 200_000) + "\n" + "\n".join(_draw_page(i, 180_000) for i in range(2, 12))
    system_len = 30_000
    plan = plan_text_analysis_chunks(md, total_budget=CODEX_TEXT_INPUT_BUDGET, system_len=system_len)
    assert plan is not None
    assert len(plan.chunks) >= 2
    assert plan.skeleton_pages == 1
    assert plan.skeleton  # скелет непустой
    for chunk in plan.chunks:
        full = system_len + len(plan.skeleton) + len(chunk) + 6_000
        assert full <= CODEX_TEXT_INPUT_BUDGET, f"чанк не влезает: {full}"


def test_no_pages_marker_single_pseudo_chunk():
    """MD без разметки страниц и больше бюджета → один псевдо-чанк (не падаем)."""
    md = "простыня без страниц " * 60_000
    plan = plan_text_analysis_chunks(md, total_budget=500_000, system_len=10_000)
    assert plan is not None
    assert len(plan.chunks) >= 1


# ─── Merge частичных ответов ────────────────────────────────────────────────


def test_merge_dedup_and_renumber():
    p1 = {
        "stage": "02_text_analysis", "text_source": "md",
        "normative_refs_found": [{"ref": "СП 256"}],
        "text_findings": [
            {"id": "T-001", "source": "MD стр.1", "finding": "Ошибка А", "norm": "СП 1"},
        ],
    }
    p2 = {
        "normative_refs_found": [{"ref": "СП 256"}, {"ref": "ГОСТ 5"}],
        "text_findings": [
            {"id": "T-001", "source": "MD стр.1", "finding": "Ошибка А", "norm": "СП 1"},  # дубль
            {"id": "T-002", "source": "MD стр.9", "finding": "Ошибка Б", "norm": "СП 2"},
        ],
    }
    m = merge_text_analysis_parts([p1, p2])
    # Дубль схлопнут, перенумерация сквозная.
    assert [f["id"] for f in m["text_findings"]] == ["T-001", "T-002"]
    assert [f["finding"] for f in m["text_findings"]] == ["Ошибка А", "Ошибка Б"]
    # Нормы объединены.
    assert {r["ref"] for r in m["normative_refs_found"]} == {"СП 256", "ГОСТ 5"}
    # Скалярные поля — из первого части.
    assert m["text_source"] == "md"


def test_merge_project_params_shallow_merge():
    p1 = {"project_params": {"object_type": "МКД", "total_load_kw": 0}, "text_findings": []}
    p2 = {"project_params": {"total_load_kw": 120, "key_equipment": ["ВРУ"]}, "text_findings": []}
    m = merge_text_analysis_parts([p1, p2])
    assert m["project_params"]["object_type"] == "МКД"
    assert m["project_params"]["total_load_kw"] == 120  # пустой 0 заменён непустым
    assert m["project_params"]["key_equipment"] == ["ВРУ"]


def test_merge_remaps_items_verified_finding_id():
    p1 = {
        "text_findings": [{"id": "T-001", "source": "s1", "finding": "A", "norm": "n1"}],
        "items_verified_from_blocks": [{"finding_id": "T-001", "block_id": "B1"}],
    }
    p2 = {
        "text_findings": [
            {"id": "T-001", "source": "s1", "finding": "A", "norm": "n1"},   # дубль → маппится на T-001
            {"id": "T-002", "source": "s9", "finding": "B", "norm": "n2"},
        ],
        "items_verified_from_blocks": [
            {"finding_id": "T-001", "block_id": "B1dup"},   # → T-001
            {"finding_id": "T-002", "block_id": "B9"},      # → T-002
        ],
    }
    m = merge_text_analysis_parts([p1, p2])
    pairs = {(it["finding_id"], it["block_id"]) for it in m["items_verified_from_blocks"]}
    assert ("T-001", "B1") in pairs
    assert ("T-001", "B1dup") in pairs
    assert ("T-002", "B9") in pairs


def test_merge_drops_dangling_item_refs():
    p = {
        "text_findings": [{"id": "T-001", "source": "s", "finding": "A", "norm": "n"}],
        "items_verified_from_blocks": [{"finding_id": "T-999", "block_id": "B"}],  # висячая ссылка
    }
    m = merge_text_analysis_parts([p])
    assert m["items_verified_from_blocks"] == []


def test_merge_empty_parts_safe():
    m = merge_text_analysis_parts([])
    assert m["text_findings"] == []
    assert m["normative_refs_found"] == []
