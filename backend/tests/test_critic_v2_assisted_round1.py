"""
Tests for /api/critic-v2/assisted-round1/{files,items}.

Контекст: инженеры проверяют не весь проект, а только карточки из
assisted_round1_review/*.csv. Backend парсит CSV и отдаёт нормализованные
items, отфильтрованные по project_id.

Tests используют synthetic CSV в tmp_path + monkeypatch
CRITIC_V2_FEEDBACK_DIR, чтобы не зависеть от реальных файлов
critic v2 test/assisted_round1_review/.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


_CSV_HEADER = (
    "bucket,section,project_name,finding_id,title,original_tab,current_tab,"
    "queue,reason,taxonomy_reason,evidence_quality,score,human_decision,"
    "human_reason,explanation,reviewer_instruction"
)


def _row(
    section: str,
    project_name: str,
    finding_id: str,
    reason: str = "round1_ocr_artifact_suggested_reject",
    title: str = "Some title",
    queue: str = "suggested_reject",
    human_decision: str = "accepted",
    reviewer_instruction: str = "Открой PDF.",
) -> str:
    # Не используем csv.writer ради читаемости; кавычим только title.
    return ",".join([
        "A_risky_accepted",
        section,
        project_name,
        finding_id,
        f'"{title}"',
        "primary",
        "suggested_reject",
        queue,
        reason,
        "other",
        "valid",
        "10",
        human_decision,
        "",
        '"explanation"',
        f'"{reviewer_instruction}"',
    ])


@pytest.fixture
def review_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    fb = tmp_path / "feedback"
    fb.mkdir()
    review = fb / "assisted_round1_review"
    review.mkdir()
    monkeypatch.setenv("CRITIC_V2_FEEDBACK_DIR", str(fb))
    return review


@pytest.fixture
def client(review_dir: Path) -> TestClient:
    from backend.app.main import app
    return TestClient(app)


def _write_risky(review_dir: Path, rows: list[str]) -> None:
    (review_dir / "assisted_round1_risky_accepted_22.csv").write_text(
        _CSV_HEADER + "\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


def _write_sample(review_dir: Path, rows: list[str]) -> None:
    (review_dir / "assisted_round1_sample_60.csv").write_text(
        _CSV_HEADER + "\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


# ─── Files endpoint ─────────────────────────────────────────────────────────


class TestFilesEndpoint:
    def test_no_review_dir(self, client: TestClient, tmp_path: Path, monkeypatch):
        # Дополнительный override на абсолютно пустую папку.
        empty = tmp_path / "no-review"
        empty.mkdir()
        monkeypatch.setenv("CRITIC_V2_FEEDBACK_DIR", str(empty))
        resp = client.get("/api/critic-v2/assisted-round1/files")
        assert resp.status_code == 200
        d = resp.json()
        assert d["exists"] is False
        assert d["files"] == []

    def test_lists_both_files_with_counts(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P1", "P1:F-1")])
        _write_sample(review_dir, [_row("AR", "P1", "P1:F-2"), _row("EOM", "P2", "P2:F-3")])
        resp = client.get("/api/critic-v2/assisted-round1/files")
        d = resp.json()
        names = {f["name"]: f for f in d["files"]}
        assert "assisted_round1_risky_accepted_22.csv" in names
        assert names["assisted_round1_risky_accepted_22.csv"]["items"] == 1
        assert names["assisted_round1_risky_accepted_22.csv"]["group"] == "risky_accepted_22"
        assert names["assisted_round1_sample_60.csv"]["items"] == 2
        assert names["assisted_round1_sample_60.csv"]["group"] == "sample_60"

    def test_missing_one_file_still_lists_other(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P1", "P1:F-1")])
        resp = client.get("/api/critic-v2/assisted-round1/files")
        d = resp.json()
        for f in d["files"]:
            if f["name"] == "assisted_round1_risky_accepted_22.csv":
                assert f["exists"] is True
                assert f["items"] == 1
            else:
                assert f["exists"] is False


# ─── Items endpoint: no filter ──────────────────────────────────────────────


class TestItemsAll:
    def test_returns_normalized_items(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P1", "P1:F-1", title="T1")])
        _write_sample(review_dir, [_row("EOM", "P2.pdf", "P2.pdf:F-2",
                                        reason="round1_rd_vs_pz_suggested_reject",
                                        title="T2")])
        resp = client.get("/api/critic-v2/assisted-round1/items")
        d = resp.json()
        assert d["total"] == 2
        groups = {it["group"] for it in d["items"]}
        assert groups == {"risky_accepted_22", "sample_60"}
        first = next(it for it in d["items"] if it["finding_id"] == "P1:F-1")
        assert first["title"] == "T1"
        assert first["section"] == "AR"
        assert first["reason"] == "round1_ocr_artifact_suggested_reject"
        assert first["reason_group"] == "OCR / ошибка распознавания"
        assert first["expected_queue"] == "suggested_reject"
        assert first["source_file"] == "assisted_round1_risky_accepted_22.csv"

    def test_unknown_reason_keeps_reason_group_null(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P1", "P1:F-1",
                                       reason="suggested_reject_not_safe_to_hide")])
        resp = client.get("/api/critic-v2/assisted-round1/items")
        item = resp.json()["items"][0]
        assert item["reason"] == "suggested_reject_not_safe_to_hide"
        assert item["reason_group"] is None

    def test_group_filter_risky_only(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P1", "P1:F-1")])
        _write_sample(review_dir, [_row("AR", "P1", "P1:F-2")])
        resp = client.get("/api/critic-v2/assisted-round1/items",
                          params={"group": "risky_accepted_22"})
        d = resp.json()
        assert d["total"] == 1
        assert d["items"][0]["finding_id"] == "P1:F-1"

    def test_group_filter_invalid_400(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P1", "P1:F-1")])
        resp = client.get("/api/critic-v2/assisted-round1/items",
                          params={"group": "fake"})
        assert resp.status_code == 400


# ─── Items endpoint: project_id filter ──────────────────────────────────────


class TestItemsByProject:
    def test_exact_match(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P-alpha", "P-alpha:F-1")])
        _write_sample(review_dir, [_row("AR", "P-alpha", "P-alpha:F-2"),
                                   _row("AR", "P-beta", "P-beta:F-3")])
        resp = client.get("/api/critic-v2/assisted-round1/items",
                          params={"project_id": "P-alpha"})
        d = resp.json()
        assert d["matched_count"] == 2
        ids = {it["finding_id"] for it in d["items"]}
        assert ids == {"P-alpha:F-1", "P-alpha:F-2"}
        for it in d["items"]:
            assert it["match_quality"] == "exact"

    def test_exact_no_pdf_match(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P-alpha", "P-alpha:F-1")])
        resp = client.get("/api/critic-v2/assisted-round1/items",
                          params={"project_id": "P-alpha.pdf"})
        d = resp.json()
        assert d["matched_count"] == 1
        assert d["items"][0]["match_quality"] == "exact_no_pdf"

    def test_normalized_match_case_pdf(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "  P-Alpha.PDF  ", "P-Alpha:F-1")])
        resp = client.get("/api/critic-v2/assisted-round1/items",
                          params={"project_id": "p-alpha"})
        d = resp.json()
        assert d["matched_count"] == 1
        assert d["items"][0]["match_quality"] in ("exact_no_pdf", "normalized")

    def test_unknown_project_returns_empty(self, client: TestClient, review_dir: Path):
        _write_risky(review_dir, [_row("AR", "P-alpha", "P-alpha:F-1")])
        resp = client.get("/api/critic-v2/assisted-round1/items",
                          params={"project_id": "P-zzz"})
        assert resp.status_code == 200
        d = resp.json()
        assert d["matched_count"] == 0
        assert d["items"] == []
        # all_items_total всё ещё показывает «вообще в review-package есть карточки».
        assert d["all_items_total"] == 1

    def test_empty_review_dir_with_project_id(self, client: TestClient, review_dir: Path):
        # review_dir есть, но CSV нет
        resp = client.get("/api/critic-v2/assisted-round1/items",
                          params={"project_id": "P1"})
        assert resp.status_code == 200
        d = resp.json()
        assert d["matched_count"] == 0
        assert d["items"] == []


# ─── Broken file resilience ─────────────────────────────────────────────────


class TestBrokenInput:
    def test_broken_csv_returns_no_items_not_500(
        self, client: TestClient, review_dir: Path,
    ):
        # CSV без header вообще
        (review_dir / "assisted_round1_risky_accepted_22.csv").write_text(
            "this is not a valid csv\n", encoding="utf-8",
        )
        resp = client.get("/api/critic-v2/assisted-round1/items")
        assert resp.status_code == 200
        # Парсер тихо игнорирует строки без finding_id; результат пустой.
        assert resp.json()["total"] == 0

    def test_row_without_finding_id_skipped(self, client: TestClient, review_dir: Path):
        # Одна валидная строка + одна без finding_id.
        body = _CSV_HEADER + "\n"
        body += _row("AR", "P1", "P1:F-1") + "\n"
        # finding_id пустой
        body += _row("AR", "P1", "") + "\n"
        (review_dir / "assisted_round1_risky_accepted_22.csv").write_text(
            body, encoding="utf-8",
        )
        resp = client.get("/api/critic-v2/assisted-round1/items")
        d = resp.json()
        assert d["total"] == 1
        assert d["items"][0]["finding_id"] == "P1:F-1"


# ─── Real review-package smoke (опционально, на dev-машине) ────────────────


class TestRealReviewPackage:
    """Эти тесты НЕ используют monkeypatch, читают реальный
    critic v2 test/assisted_round1_review/. Skipped если каталога нет."""

    def setup_method(self):
        # Сбросить env-override, если был установлен другими fixture'ами.
        import os
        os.environ.pop("CRITIC_V2_FEEDBACK_DIR", None)

    def test_real_csvs_load_if_present(self):
        from backend.app.main import app
        c = TestClient(app)
        resp = c.get("/api/critic-v2/assisted-round1/files")
        d = resp.json()
        if not d.get("exists"):
            pytest.skip("real review-package не установлен на этой машине")
        names = {f["name"] for f in d["files"] if f.get("exists")}
        # На dev-машине должны быть оба CSV.
        assert "assisted_round1_risky_accepted_22.csv" in names
        assert "assisted_round1_sample_60.csv" in names

    def test_real_items_total_82(self):
        from backend.app.main import app
        c = TestClient(app)
        files = c.get("/api/critic-v2/assisted-round1/files").json()
        if not files.get("exists"):
            pytest.skip("real review-package не установлен")
        d = c.get("/api/critic-v2/assisted-round1/items").json()
        # 22 risky + 60 sample = 82 на dev-машине.
        assert d["total"] == 82

    def test_real_project_match_ozds(self):
        from backend.app.main import app
        c = TestClient(app)
        files = c.get("/api/critic-v2/assisted-round1/files").json()
        if not files.get("exists"):
            pytest.skip("real review-package не установлен")
        target = "13АВ-РД-ОЗДС (согл от 24.04.2026) (1).pdf"
        d = c.get("/api/critic-v2/assisted-round1/items",
                  params={"project_id": target}).json()
        # На реальной выборке: 1 risky F-029 + 9 sample = 10 карточек.
        assert d["matched_count"] >= 1
        for it in d["items"]:
            assert it["match_quality"] in ("exact", "exact_no_pdf", "normalized")
