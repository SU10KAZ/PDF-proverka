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
    ) == ("maksheev", "Макшеев П.Ю.")
    assert usage_service._subscription_person(
        "-home-coder-projects-OSA-Kulik"
    ) == ("kulik", "Кулик А.С.")


def test_existing_osa_employee_statistics_mappings_are_unchanged():
    expected = {
        "-home-coder-projects-OSA-Kulik": ("kulik", "Кулик А.С."),
        "-home-coder-projects-OSA-Repnikov": ("repnikov", "Репников И. А."),
        "-home-coder-projects-OSA-Grivapsch": ("grivapsch", "Гривапш А. А."),
        "-home-coder-projects-OSA-Kuldiaev": ("kuldiaev", "Кульдяев Ф. С."),
        "-home-coder-projects-OSA-Alexandra": ("kalinina", "Калинина А."),
    }
    assert {
        dirname: usage_service._subscription_person(dirname)
        for dirname in expected
    } == expected


def test_target_folders_reach_subscription_statistics_once_each(tmp_path, monkeypatch):
    sessions = tmp_path / "projects"
    fixtures = {
        "-home-coder-projects-OSA-Maksheev": (1_000, 100),
        "-home-coder-projects-OSA-Maksheev-subproject": (300, 30),
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

    assert set(people) == {"maksheev", "kulik"}
    assert people["maksheev"]["name"] == "Макшеев П.Ю."
    assert people["maksheev"]["total_tokens"] == 1_430
    assert people["kulik"]["name"] == "Кулик А.С."
    assert people["kulik"]["total_tokens"] == 2_200
    assert result["totals"]["tokens"] == 3_630

    old_id = "makshee" + "va"
    old_name = "Макшее" + "ва П.Ю."
    assert old_id not in people
    assert all(person["name"] != old_name for person in result["people"])


def test_initials_folders_map_to_engineers():
    expected = {
        "-home-coder-projects-OSA-RI": ("repnikov", "Репников И. А."),
        "-home-coder-projects-OSA-MP": ("maksheev", "Макшеев П.Ю."),
        "-home-coder-projects-OSA-KA": ("kulik", "Кулик А.С."),
        "-home-coder-projects-OSA-KAE": ("kalinina", "Калинина А."),
        "-home-coder-projects-OSA-KF": ("kuldiaev", "Кульдяев Ф. С."),
        "-home-coder-projects-OSA-GA": ("grivapsch", "Гривапш А. А."),
        "-home-coder-projects-OSA-KF-subproject": ("kuldiaev", "Кульдяев Ф. С."),
        # Кириллическая «КА» до переименования в KAE.
        "-home-coder-projects-OSA---": ("kalinina", "Калинина А."),
    }
    assert {
        dirname: usage_service._subscription_person(dirname)
        for dirname in expected
    } == expected


def test_short_code_matches_whole_segment_only():
    # «KA» (Кулик) не должен поглощать «KAE» (Калинина) и наоборот.
    assert usage_service._subscription_person("-home-coder-projects-OSA-KAE")[0] == "kalinina"
    assert usage_service._subscription_person("-home-coder-projects-OSA-KA")[0] == "kulik"
    assert usage_service._subscription_person("-home-coder-projects-OSA-XY") is None
    assert usage_service._subscription_person("-home-coder-projects-OSA-----") is None


def test_canonical_names_match_employee_registry():
    from backend.app.services.common.employee_identity import identity_for_source_directory
    for marker in ("OSA-Maksheev", "OSA-Kulik"):
        identity = identity_for_source_directory(marker)
        assert usage_service._OSA_SHORT_CODES[
            {"maksheev": "MP", "kulik": "KA"}[identity.employee_id]
        ] == (identity.employee_id, identity.display_name)
