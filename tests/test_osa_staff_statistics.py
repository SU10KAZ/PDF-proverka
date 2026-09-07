"""Регрессии реестра статистики сотрудников OSA по Claude-транскриптам."""

import json
from datetime import datetime, timezone

from backend.app.services.common import usage_service


def _usage_record(*, input_tokens: int, output_tokens: int) -> str:
    return json.dumps({
        "type": "assistant",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": {
            "model": "claude-sonnet-5",
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
            },
        },
    })


def test_osa_target_folders_use_canonical_employee_names():
    assert usage_service._subscription_person(
        "-home-coder-projects-OSA-Maksheev"
    ) == ("maksheeva", "Макшеева П.Ю.")
    assert usage_service._subscription_person(
        "-home-coder-projects-OSA-Kulik"
    ) == ("kulik", "Кулик А.С.")


def test_target_folders_reach_subscription_statistics_once_each(tmp_path, monkeypatch):
    sessions = tmp_path / "projects"
    fixtures = {
        "-home-coder-projects-OSA-Maksheev": (1_000, 100),
        "-home-coder-projects-OSA-Kulik": (2_000, 200),
    }
    for dirname, (input_tokens, output_tokens) in fixtures.items():
        project_dir = sessions / dirname
        project_dir.mkdir(parents=True)
        (project_dir / "session.jsonl").write_text(
            _usage_record(input_tokens=input_tokens, output_tokens=output_tokens) + "\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(usage_service, "CLAUDE_SESSIONS_DIR", sessions)
    result = usage_service.scan_subscription_by_person()
    people = {person["id"]: person for person in result["people"]}

    assert set(people) == {"maksheeva", "kulik"}
    assert people["maksheeva"]["name"] == "Макшеева П.Ю."
    assert people["maksheeva"]["total_tokens"] == 1_100
    assert people["kulik"]["name"] == "Кулик А.С."
    assert people["kulik"]["total_tokens"] == 2_200
    assert result["totals"]["tokens"] == 3_300
