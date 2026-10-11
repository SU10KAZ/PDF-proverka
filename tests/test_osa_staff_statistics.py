"""Регрессии реестра статистики сотрудников OSA по Claude-транскриптам."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from backend.app.services.common import usage_service


@pytest.fixture(autouse=True)
def _no_real_subscription_snapshot(tmp_path, monkeypatch):
    """Снимок подписки с машины разработчика не должен влиять на тесты."""
    monkeypatch.setattr(
        usage_service, "CLAUDE_USAGE_CACHE_FILE", tmp_path / "no-claude.json",
    )


def _usage_record(*, input_tokens: int, output_tokens: int,
                  message_id=None, request_id=None, ts=None) -> str:
    return json.dumps({
        "type": "assistant",
        "timestamp": (ts or datetime.now(timezone.utc)).isoformat(),
        "requestId": request_id,
        "message": {
            "id": message_id,
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


def test_person_spend_is_split_by_source_folder(tmp_path, monkeypatch):
    sessions = tmp_path / "projects"
    fixtures = {
        "-home-coder-projects-OSA-KAE": (100, 10),
        "-home-coder-projects-OSA---": (50, 5),
        "-home-coder-projects-OSA-KA": (700, 70),
    }
    for dirname, (input_tokens, output_tokens) in fixtures.items():
        project_dir = sessions / dirname
        project_dir.mkdir(parents=True)
        (project_dir / "session.jsonl").write_text(
            _usage_record(input_tokens=input_tokens, output_tokens=output_tokens) + "\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(usage_service, "CLAUDE_SESSIONS_DIR", sessions)
    people = {
        person["id"]: person
        for person in usage_service.scan_subscription_by_person()["people"]
    }

    # Старая кириллическая «OSA/КА» переименована в KAE — одна строка-папка.
    kalinina = {f["label"]: f["total_tokens"] for f in people["kalinina"]["folders"]}
    assert kalinina == {"OSA/KAE": 165}
    assert people["kalinina"]["total_tokens"] == 165
    assert [f["label"] for f in people["kulik"]["folders"]] == ["OSA/KA"]


def test_one_request_split_into_content_blocks_is_counted_once(tmp_path, monkeypatch):
    """Claude Code пишет thinking/text/tool_use отдельными строками с usage
    всего запроса; возобновлённая сессия копирует историю в новый файл."""
    sessions = tmp_path / "projects"
    project_dir = sessions / "-home-coder-projects-OSA-KA"
    project_dir.mkdir(parents=True)
    block = _usage_record(input_tokens=1_000, output_tokens=100,
                          message_id="msg_1", request_id="req_1")
    (project_dir / "a.jsonl").write_text(block + "\n" + block + "\n" + block + "\n",
                                         encoding="utf-8")
    (project_dir / "resumed.jsonl").write_text(block + "\n", encoding="utf-8")

    monkeypatch.setattr(usage_service, "CLAUDE_SESSIONS_DIR", sessions)
    result = usage_service.scan_subscription_by_person()

    assert result["totals"]["tokens"] == 1_100


def test_subagent_transcripts_are_counted(tmp_path, monkeypatch):
    sessions = tmp_path / "projects"
    project_dir = sessions / "-home-coder-projects-OSA-RI"
    subagents = project_dir / "session-1" / "subagents"
    subagents.mkdir(parents=True)
    (project_dir / "session-1.jsonl").write_text(
        _usage_record(input_tokens=100, output_tokens=10,
                      message_id="msg_main", request_id="req_main") + "\n",
        encoding="utf-8",
    )
    (subagents / "agent-1.jsonl").write_text(
        _usage_record(input_tokens=200, output_tokens=20,
                      message_id="msg_sub", request_id="req_sub") + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(usage_service, "CLAUDE_SESSIONS_DIR", sessions)
    result = usage_service.scan_subscription_by_person()

    assert result["totals"]["tokens"] == 330


def _write_snapshot(path, *, pct, resets_at, fetched_at):
    path.write_text(json.dumps({"cachedUsageUtilization": {
        "fetchedAtMs": int(fetched_at.timestamp() * 1000),
        "utilization": {"seven_day": {
            "utilization": pct,
            "resets_at": (resets_at - timedelta(milliseconds=380)).isoformat(),
        }},
    }}), encoding="utf-8")


def test_week_and_limit_follow_subscription_snapshot(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    resets_at = (now + timedelta(days=3)).replace(second=0, microsecond=0)
    snapshot_file = tmp_path / "claude.json"
    _write_snapshot(snapshot_file, pct=30, resets_at=resets_at,
                    fetched_at=now - timedelta(minutes=1))
    monkeypatch.setattr(usage_service, "CLAUDE_USAGE_CACHE_FILE", snapshot_file)

    sessions = tmp_path / "projects"
    for dirname, mid in (("-home-coder-projects-OSA-KA", "m1"), ("-tmp", "m2")):
        project_dir = sessions / dirname
        project_dir.mkdir(parents=True)
        (project_dir / "s.jsonl").write_text(
            _usage_record(input_tokens=1_000_000, output_tokens=0, message_id=mid,
                          request_id="r-" + mid, ts=now - timedelta(minutes=5)) + "\n",
            encoding="utf-8",
        )
    monkeypatch.setattr(usage_service, "CLAUDE_SESSIONS_DIR", sessions)
    result = usage_service.scan_subscription_by_person()

    # Неделя = 7 суток до сброса из снимка.
    week_start = datetime.fromisoformat(result["week_start"])
    assert week_start == resets_at - timedelta(weeks=1)
    # Лимит: весь расход ($3 Sonnet + $3 из /tmp) — это 30% подписки.
    assert result["limit_source"] == "subscription"
    assert result["weekly_limit_usd"] == pytest.approx(20.0)
    assert result["unattributed_cost"] == pytest.approx(3.0)
    assert result["all_pct_limit"] == pytest.approx(30.0)
    assert result["totals"]["pct_limit"] == pytest.approx(15.0)


def test_snapshot_from_previous_week_keeps_static_limit(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    old_reset = (now - timedelta(days=2)).replace(second=0, microsecond=0)
    snapshot_file = tmp_path / "claude.json"
    _write_snapshot(snapshot_file, pct=90, resets_at=old_reset,
                    fetched_at=old_reset - timedelta(hours=1))
    monkeypatch.setattr(usage_service, "CLAUDE_USAGE_CACHE_FILE", snapshot_file)
    monkeypatch.setattr(usage_service, "CLAUDE_SESSIONS_DIR", tmp_path / "projects")

    result = usage_service.scan_subscription_by_person()

    assert datetime.fromisoformat(result["week_start"]) == old_reset
    assert result["limit_source"] == "estimate"
    assert result["subscription"]["pct"] is None
