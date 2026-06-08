# -*- coding: utf-8 -*-
"""Тесты visual_block_equivalence (Stage 2: links-based, mark-only).

Покрывает (см. задачу Stage 2):
  1. читает связи и сравнивает ТОЛЬКО связанные пары (непросвязанные блоки
     игнорируются);
  2. stale-link НЕ сравнивается (skipped_stale_link, render не зовётся);
  3. split/merge и дубликаты 1↔many / many↔1 НЕ сравниваются
     (skipped_not_one_to_one);
  4. text/table блоки со ЗНАЧИМЫМ типом НЕ сравниваются (skipped_non_image);
  5. identical_visual → exclude_from_qwen / exclude_from_opus_md = true,
     enforced = false;
  6. changed_visual → exclude_from_qwen / exclude_from_opus_md = false;
  7. ошибка render/compare не валит batch (render_failed / uncertain);
  8. артефакт пишется атомарно и имеет ожидаемую структуру summary/pairs.

Render и визуальное сравнение замоканы — тесты быстрые, без PDF и без cv2.
"""
from __future__ import annotations

import json

from backend.app.services.stage_comparison.block_equivalence_precheck import EqBlock
from backend.app.services.stage_comparison import visual_block_equivalence as vbe


# ─── helpers ────────────────────────────────────────────────────────────────


def _img_block(bid: str, page: int = 1, btype: str = "image") -> EqBlock:
    return EqBlock(
        block_id=bid, page=page, block_type=btype,
        coords_norm=[0.1, 0.1, 0.5, 0.5], page_width=1000, page_height=1000,
    )


def _link(left: str, right: str, *, method: str = "manual", score: float = 1.0,
          left_page: int = 1, right_page: int = 1) -> dict:
    return {
        "left_block_id": left, "right_block_id": right,
        "method": method, "score": score,
        "left_page": left_page, "right_page": right_page,
    }


class _RenderRecorder:
    """render_fn-замена: возвращает не-None изображение, считает вызовы."""

    def __init__(self, fail: bool = False):
        self.calls: list[str] = []
        self.fail = fail

    def __call__(self, block, *, source_pdf_path=None, render_long_side=1000):
        self.calls.append(getattr(block, "block_id", None))
        if self.fail:
            return None, {"status": "render_failed", "error": "mock"}
        return object(), {"status": "rendered"}


class _VisRecorder:
    """visual_compare_fn-замена с заранее заданным результатом."""

    def __init__(self, result: dict | None = None, raises: bool = False):
        self.calls = 0
        self.result = result or {
            "status": "identical_visual", "total_diff_ratio": 0.004,
            "colored_overlay_diff_ratio": 0.0, "alignment_score": 0.98, "diff_bbox": None,
        }
        self.raises = raises

    def __call__(self, old_img, new_img, *, cfg=None, debug_path=None):
        self.calls += 1
        if self.raises:
            raise RuntimeError("mock compare boom")
        return dict(self.result)


def _run(links, old_blocks, new_blocks, render_fn, vis_fn, **kw):
    return vbe.run_pair_visual_block_equivalence(
        "sess", "pair",
        cfg=vbe.VisualBlockEquivalenceConfig(),
        links=links, old_blocks=old_blocks, new_blocks=new_blocks,
        old_pdf_path="/nonexistent/old.pdf", new_pdf_path="/nonexistent/new.pdf",
        write_artifact=False, write_debug=False,
        render_fn=render_fn, visual_compare_fn=vis_fn,
        generated_at="2026-06-08T00:00:00Z",
        **kw,
    )


# ─── 1. сравнивает только связанные пары ─────────────────────────────────────


def test_compares_only_linked_pairs():
    old = [_img_block("L1"), _img_block("L2"), _img_block("L_unlinked")]
    new = [_img_block("R1"), _img_block("R2"), _img_block("R_unlinked")]
    links = [_link("L1", "R1"), _link("L2", "R2")]
    render = _RenderRecorder()
    vis = _VisRecorder()

    report = _run(links, old, new, render, vis)

    assert len(report["pairs"]) == 2
    linked_left = {p["left_block_id"] for p in report["pairs"]}
    assert linked_left == {"L1", "L2"}
    assert "L_unlinked" not in linked_left
    assert vis.calls == 2  # ровно по числу связей
    assert report["summary"]["links_total"] == 2
    assert report["summary"]["links_compared"] == 2


# ─── 2. stale-link не сравнивается ───────────────────────────────────────────


def test_stale_link_not_compared():
    old = [_img_block("L1")]
    new = [_img_block("R1")]
    links = [_link("L1", "R1", method="manual_stale")]
    render = _RenderRecorder()
    vis = _VisRecorder()

    report = _run(links, old, new, render, vis)

    rec = report["pairs"][0]
    assert rec["status"] == vbe.VS_SKIP_STALE
    assert rec["exclude_from_qwen"] is False
    assert rec["exclude_from_opus_md"] is False
    assert vis.calls == 0           # сравнение не запускалось
    assert render.calls == []       # рендер не запускался
    assert report["summary"]["skipped"] == 1
    assert report["summary"]["skipped_breakdown"]["stale_link"] == 1
    assert report["summary"]["links_compared"] == 0


def test_auto_stale_link_not_compared():
    old = [_img_block("L1")]
    new = [_img_block("R1")]
    links = [_link("L1", "R1", method="auto_stale")]
    vis = _VisRecorder()
    report = _run(links, old, new, _RenderRecorder(), vis)
    assert report["pairs"][0]["status"] == vbe.VS_SKIP_STALE
    assert vis.calls == 0


# ─── 3. дубликаты 1↔many / many↔1 не сравниваются ────────────────────────────


def test_one_to_many_not_compared():
    # L1 связан с R1 и R2 → обе связи не 1↔1
    old = [_img_block("L1")]
    new = [_img_block("R1"), _img_block("R2")]
    links = [_link("L1", "R1"), _link("L1", "R2")]
    vis = _VisRecorder()
    report = _run(links, old, new, _RenderRecorder(), vis)

    statuses = [p["status"] for p in report["pairs"]]
    assert statuses == [vbe.VS_SKIP_NOT_1_1, vbe.VS_SKIP_NOT_1_1]
    assert vis.calls == 0
    assert report["summary"]["skipped_breakdown"]["not_one_to_one"] == 2


def test_many_to_one_not_compared_but_valid_link_is():
    # R_Z получает 2 связи (many↔1) → они skip; L3↔R3 валидна → сравнивается
    old = [_img_block("L1"), _img_block("L2"), _img_block("L3")]
    new = [_img_block("RZ"), _img_block("R3")]
    links = [_link("L1", "RZ"), _link("L2", "RZ"), _link("L3", "R3")]
    vis = _VisRecorder()
    report = _run(links, old, new, _RenderRecorder(), vis)

    by_left = {p["left_block_id"]: p["status"] for p in report["pairs"]}
    assert by_left["L1"] == vbe.VS_SKIP_NOT_1_1
    assert by_left["L2"] == vbe.VS_SKIP_NOT_1_1
    assert by_left["L3"] == vbe.VS_IDENTICAL          # единственная валидная сравнена
    assert vis.calls == 1
    assert report["summary"]["skipped_breakdown"]["not_one_to_one"] == 2
    assert report["summary"]["links_compared"] == 1


# ─── 4. text/table блоки не сравниваются ─────────────────────────────────────


def test_text_block_not_compared():
    old = [_img_block("L1", btype="text")]
    new = [_img_block("R1", btype="image")]
    links = [_link("L1", "R1")]
    render = _RenderRecorder()
    vis = _VisRecorder()
    report = _run(links, old, new, render, vis)

    rec = report["pairs"][0]
    assert rec["status"] == vbe.VS_SKIP_NON_IMAGE
    assert vis.calls == 0
    assert render.calls == []
    assert report["summary"]["skipped_breakdown"]["non_image"] == 1


def test_table_block_not_compared():
    old = [_img_block("L1", btype="image")]
    new = [_img_block("R1", btype="table")]
    links = [_link("L1", "R1")]
    vis = _VisRecorder()
    report = _run(links, old, new, _RenderRecorder(), vis)
    assert report["pairs"][0]["status"] == vbe.VS_SKIP_NON_IMAGE
    assert vis.calls == 0


def test_unknown_type_is_treated_as_image_and_compared():
    # тип "unknown" не является text/table → сравнивается (скоуп image/graphic)
    old = [_img_block("L1", btype="unknown")]
    new = [_img_block("R1", btype="unknown")]
    links = [_link("L1", "R1")]
    vis = _VisRecorder()
    report = _run(links, old, new, _RenderRecorder(), vis)
    assert report["pairs"][0]["status"] == vbe.VS_IDENTICAL
    assert vis.calls == 1


# ─── 5. identical_visual → исключение (информационное) ───────────────────────


def test_identical_visual_marks_exclusion():
    old = [_img_block("L1")]
    new = [_img_block("R1")]
    links = [_link("L1", "R1")]
    vis = _VisRecorder({"status": "identical_visual", "total_diff_ratio": 0.004,
                        "colored_overlay_diff_ratio": 0.0, "alignment_score": 0.98})
    report = _run(links, old, new, _RenderRecorder(), vis)

    rec = report["pairs"][0]
    assert rec["status"] == vbe.VS_IDENTICAL
    assert rec["exclude_from_qwen"] is True
    assert rec["exclude_from_opus_md"] is True
    assert rec["enforced"] is False
    assert rec["confidence"] == 0.98
    assert rec["metrics"]["total_diff_ratio"] == 0.004
    assert report["summary"]["identical_visual"] == 1
    assert report["summary"]["potential_qwen_saved"] == 1
    assert report["summary"]["potential_opus_blocks_removed"] == 1
    # mark-only: на уровне отчёта реального skip нет
    assert report["enforced"] is False
    assert report["mode"] == "mark_only"


# ─── 6. changed_visual → без исключения ──────────────────────────────────────


def test_changed_visual_no_exclusion():
    old = [_img_block("L1")]
    new = [_img_block("R1")]
    links = [_link("L1", "R1")]
    vis = _VisRecorder({"status": "changed_visual", "total_diff_ratio": 0.25,
                        "colored_overlay_diff_ratio": 0.1, "alignment_score": 0.9})
    report = _run(links, old, new, _RenderRecorder(), vis)

    rec = report["pairs"][0]
    assert rec["status"] == vbe.VS_CHANGED
    assert rec["exclude_from_qwen"] is False
    assert rec["exclude_from_opus_md"] is False
    assert report["summary"]["changed_visual"] == 1
    assert report["summary"]["potential_qwen_saved"] == 0


def test_minor_render_noise_band_no_exclusion():
    # diff в полосе (0.02, 0.05], colored низкий → minor_render_noise, не исключаем
    old = [_img_block("L1")]
    new = [_img_block("R1")]
    links = [_link("L1", "R1")]
    vis = _VisRecorder({"status": "changed_visual", "total_diff_ratio": 0.03,
                        "colored_overlay_diff_ratio": 0.0, "alignment_score": 0.97})
    report = _run(links, old, new, _RenderRecorder(), vis)

    rec = report["pairs"][0]
    assert rec["status"] == vbe.VS_MINOR
    assert rec["exclude_from_qwen"] is False
    assert rec["exclude_from_opus_md"] is False
    assert report["summary"]["minor_render_noise"] == 1


# ─── 7. ошибки render/compare не валят batch ─────────────────────────────────


def test_render_failure_yields_render_failed_not_crash():
    old = [_img_block("L1"), _img_block("L2")]
    new = [_img_block("R1"), _img_block("R2")]
    links = [_link("L1", "R1"), _link("L2", "R2")]
    render = _RenderRecorder(fail=True)
    vis = _VisRecorder()  # не должен вызваться (render отдаёт None)
    report = _run(links, old, new, render, vis)

    assert [p["status"] for p in report["pairs"]] == [vbe.VS_RENDER_FAILED, vbe.VS_RENDER_FAILED]
    assert vis.calls == 0
    for rec in report["pairs"]:
        assert rec["exclude_from_qwen"] is False
        assert rec["debug"]["old_render"] == "render_failed"
    assert report["summary"]["render_failed"] == 2


def test_compare_exception_yields_uncertain_not_crash():
    old = [_img_block("L1")]
    new = [_img_block("R1")]
    links = [_link("L1", "R1")]
    vis = _VisRecorder(raises=True)
    report = _run(links, old, new, _RenderRecorder(), vis)

    rec = report["pairs"][0]
    assert rec["status"] == vbe.VS_UNCERTAIN
    assert rec["exclude_from_qwen"] is False
    assert report["summary"]["uncertain"] == 1


def test_alignment_failed_yields_uncertain():
    old = [_img_block("L1")]
    new = [_img_block("R1")]
    links = [_link("L1", "R1")]
    vis = _VisRecorder({"status": "alignment_failed", "total_diff_ratio": None,
                        "colored_overlay_diff_ratio": None, "alignment_score": 0.2})
    report = _run(links, old, new, _RenderRecorder(), vis)
    assert report["pairs"][0]["status"] == vbe.VS_UNCERTAIN
    assert report["pairs"][0]["exclude_from_qwen"] is False


# ─── summary invariant ───────────────────────────────────────────────────────


def test_summary_invariant_total_equals_compared_plus_skipped():
    old = [_img_block("L1"), _img_block("L2", btype="text"),
           _img_block("L3"), _img_block("L4")]
    new = [_img_block("R1"), _img_block("R2"),
           _img_block("R3"), _img_block("R4")]
    links = [
        _link("L1", "R1"),                       # identical
        _link("L2", "R2"),                       # non-image (text)
        _link("L3", "R3", method="auto_stale"),  # stale
        _link("L4", "R4"),                        # identical
    ]
    vis = _VisRecorder()
    report = _run(links, old, new, _RenderRecorder(), vis)

    s = report["summary"]
    compared = (s["identical_visual"] + s["minor_render_noise"] + s["changed_visual"]
                + s["uncertain"] + s["render_failed"])
    assert s["links_compared"] == compared
    assert s["links_total"] == s["links_compared"] + s["skipped"]
    assert s["links_total"] == 4
    # все обязательные ключи summary присутствуют
    for key in ("links_total", "links_compared", "identical_visual",
                "minor_render_noise", "changed_visual", "uncertain", "skipped",
                "potential_qwen_saved", "potential_opus_blocks_removed"):
        assert key in s


# ─── 8. атомарная запись артефакта + резолв из store ─────────────────────────


def test_artifact_written_atomically_and_resolves_from_store(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))

    # реальный result.json для extract_blocks_for_equivalence
    def _result(blocks):
        return {"pages": [{"page_number": 1, "width": 1000, "height": 1000, "blocks": blocks}]}

    old_rjp = tmp_path / "old_result.json"
    new_rjp = tmp_path / "new_result.json"
    old_rjp.write_text(json.dumps(_result([
        {"id": "L1", "block_type": "image", "coords_norm": [0.1, 0.1, 0.5, 0.5]},
    ])), encoding="utf-8")
    new_rjp.write_text(json.dumps(_result([
        {"id": "R1", "block_type": "image", "coords_norm": [0.1, 0.1, 0.5, 0.5]},
    ])), encoding="utf-8")

    from backend.app.services.stage_comparison import store as store_mod
    from backend.app.services.stage_comparison import paths as paths_mod

    monkeypatch.setattr(store_mod, "_find_pair_meta", lambda s, p: {
        "left": {"result_json_path": str(old_rjp), "pdf_path": "/nonexistent/old.pdf"},
        "right": {"result_json_path": str(new_rjp), "pdf_path": "/nonexistent/new.pdf"},
    })
    monkeypatch.setattr(store_mod, "_pair_links", lambda s, p: [_link("L1", "R1")])

    vis = _VisRecorder()
    report = vbe.run_pair_visual_block_equivalence(
        "sess1", "pairA",
        cfg=vbe.VisualBlockEquivalenceConfig(),
        render_fn=_RenderRecorder(), visual_compare_fn=vis,
        write_artifact=True, write_debug=False,
        generated_at="2026-06-08T00:00:00Z",
    )

    out = paths_mod.visual_block_equivalence_report_path("sess1", "pairA")
    assert out.exists()
    # никаких висящих .tmp после атомарной замены
    assert not out.with_suffix(out.suffix + ".tmp").exists()

    on_disk = json.loads(out.read_text(encoding="utf-8"))
    assert on_disk["schema_version"] == 1
    assert on_disk["mode"] == "mark_only"
    assert on_disk["source"] == "links_json"
    assert on_disk["enforced"] is False
    assert on_disk["summary"]["links_total"] == 1
    assert on_disk["pairs"][0]["left_block_id"] == "L1"
    assert on_disk["pairs"][0]["right_block_id"] == "R1"
    assert on_disk["pairs"][0]["status"] == vbe.VS_IDENTICAL
    assert report["summary"]["identical_visual"] == 1


def test_read_pair_visual_block_equivalence_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))
    from backend.app.services.stage_comparison import paths as paths_mod

    out = paths_mod.visual_block_equivalence_report_path("s", "p")
    out.write_text(json.dumps({"schema_version": 1, "pairs": []}), encoding="utf-8")

    got = vbe.read_pair_visual_block_equivalence("s", "p")
    assert got is not None and got["schema_version"] == 1
    assert vbe.read_pair_visual_block_equivalence("s", "missing") is None
