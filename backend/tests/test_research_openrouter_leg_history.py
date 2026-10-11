from __future__ import annotations

from pathlib import Path

from backend.scripts.research_openrouter_leg_history import _stage01_raw_dir, classify


def _finding(detections: list[tuple[str, str]], sources: list[str]) -> dict:
    return {
        "source_finding_ids": sources,
        "provenance": {
            "detections": [
                {
                    "detector": detector,
                    "model": "openai/gpt-5.4" if detector == "gpt_openrouter" else "codex/gpt-6-astra",
                    "mode": mode,
                    "run_id": "stage01-20260906T101500Z-ensemble_gpt-codex:" + detector,
                }
                for detector, mode in detections
            ]
        },
    }


def test_classify_separates_gpt_only_from_text_and_shared_findings() -> None:
    assert classify(_finding([("gpt_openrouter", "independent")], ["G-001"]))["origin"] == "gpt_only"
    assert classify(_finding([("gpt_openrouter", "independent")], ["G-001", "T-004"]))["origin"] == "gpt_text"
    assert (
        classify(_finding([("gpt_openrouter", "independent"), ("codex", "independent")], ["G-001", "G-002"]))[
            "origin"
        ]
        == "both"
    )
    assert classify(_finding([("codex", "gap_search")], ["G-003"]))["origin"] == "codex_gap_only"
    assert classify(_finding([("codex", "independent")], ["G-003"]))["origin"] == "codex_only"
    assert classify(_finding([], ["T-001"]))["origin"] == "text_only"


def test_raw_dir_is_found_in_the_run_that_produced_stage01(tmp_path: Path) -> None:
    runs = tmp_path / "03_analysis" / "runs"
    producer = runs / "run-a" / "_stage01_findings_only_runs" / "20260906_101501__ensemble_gpt-codex_low"
    other = runs / "run-c" / "_stage01_findings_only_runs" / "20260901_080000__ensemble_gpt-codex_low"
    producer.mkdir(parents=True)
    other.mkdir(parents=True)
    (runs / "run-b").mkdir(parents=True)

    findings = [_finding([("gpt_openrouter", "independent")], ["G-001"])]

    # The reviewed run (run-b) reused Stage 01 and has no raw folder of its own.
    assert _stage01_raw_dir(tmp_path, "run-b", findings) == producer
