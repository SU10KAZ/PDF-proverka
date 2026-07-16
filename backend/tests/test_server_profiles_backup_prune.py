"""Ротация бэкапов .env при переключении LLM-профиля (server_profiles._prune_profile_backups).

Инвариант: чистим строго своё семейство before-profile-switch-*, храним keep-N
последних по ИМЕНИ (метка времени лексикографически = хронологически), чужие
бэкапы отката не трогаем, свежий бэкап неприкосновенен даже при keep=0.
"""

from __future__ import annotations

import backend.app.services.llm.server_profiles as sp


def _make(dir_path, name: str) -> None:
    (dir_path / name).write_text("X=1\n")


def _names(dir_path) -> set[str]:
    return {p.name for p in dir_path.iterdir()}


def test_prune_keeps_last_n_by_name(tmp_path, monkeypatch):
    monkeypatch.setattr(sp, "ENV_PATH", tmp_path / ".env")
    (tmp_path / ".env").write_text("X=1\n")
    for ts in (
        "20260703-135717",
        "20260704-011238",
        "20260706-093324",
        "20260707-221528",
        "20260715-142816",
    ):
        _make(tmp_path, f"{sp._BACKUP_PREFIX}{ts}")

    removed = sp._prune_profile_backups(keep_last=2)

    assert set(removed) == {
        f"{sp._BACKUP_PREFIX}20260703-135717",
        f"{sp._BACKUP_PREFIX}20260704-011238",
        f"{sp._BACKUP_PREFIX}20260706-093324",
    }
    # остаются два самых свежих по имени
    assert _names(tmp_path) == {
        ".env",
        f"{sp._BACKUP_PREFIX}20260707-221528",
        f"{sp._BACKUP_PREFIX}20260715-142816",
    }


def test_prune_never_touches_foreign_backups(tmp_path, monkeypatch):
    monkeypatch.setattr(sp, "ENV_PATH", tmp_path / ".env")
    (tmp_path / ".env").write_text("X=1\n")
    foreign = {
        ".env.example",
        ".env.backup.rollback-to-ngrok-2026-07-03",
        ".env.backup.before-codex-sandbox-20260715-142816",
        ".env.backup.before-norms-after-merge-20260715-231812",
        ".env.bak.1783413629",
    }
    for name in foreign:
        _make(tmp_path, name)
    for ts in ("20260703-135717", "20260704-011238", "20260706-093324"):
        _make(tmp_path, f"{sp._BACKUP_PREFIX}{ts}")

    sp._prune_profile_backups(keep_last=1)

    survivors = _names(tmp_path)
    # ни один чужой файл не тронут
    assert foreign | {".env"} <= survivors
    # из своего семейства остался ровно один (самый свежий)
    own = {n for n in survivors if n.startswith(sp._BACKUP_PREFIX)}
    assert own == {f"{sp._BACKUP_PREFIX}20260706-093324"}


def test_prune_keep_zero_still_spares_freshest(tmp_path, monkeypatch):
    monkeypatch.setattr(sp, "ENV_PATH", tmp_path / ".env")
    (tmp_path / ".env").write_text("X=1\n")
    for ts in ("20260703-135717", "20260707-221528"):
        _make(tmp_path, f"{sp._BACKUP_PREFIX}{ts}")

    sp._prune_profile_backups(keep_last=0)

    own = {n for n in _names(tmp_path) if n.startswith(sp._BACKUP_PREFIX)}
    assert own == {f"{sp._BACKUP_PREFIX}20260707-221528"}


def test_prune_noop_when_under_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(sp, "ENV_PATH", tmp_path / ".env")
    (tmp_path / ".env").write_text("X=1\n")
    _make(tmp_path, f"{sp._BACKUP_PREFIX}20260707-221528")

    removed = sp._prune_profile_backups(keep_last=10)

    assert removed == []
    assert f"{sp._BACKUP_PREFIX}20260707-221528" in _names(tmp_path)


def test_rewrite_env_creates_backup_and_prunes(tmp_path, monkeypatch):
    """Интеграция: _rewrite_env делает бэкап и тут же соблюдает keep-N."""
    monkeypatch.setattr(sp, "ENV_PATH", tmp_path / ".env")
    monkeypatch.setattr(sp, "_BACKUP_KEEP", 1)
    (tmp_path / ".env").write_text("CHANDRA_AUTH_MODE=basic\n")
    # старый лишний бэкап, который должен уйти после ротации
    _make(tmp_path, f"{sp._BACKUP_PREFIX}20200101-000000")

    diff = sp._rewrite_env({"CHANDRA_AUTH_MODE": "bearer"})

    assert any(d["key"] == "CHANDRA_AUTH_MODE" for d in diff)
    assert (tmp_path / ".env").read_text() == "CHANDRA_AUTH_MODE=bearer\n"
    own = sorted(n for n in _names(tmp_path) if n.startswith(sp._BACKUP_PREFIX))
    # keep=1: старый снесён, остался только свежесозданный
    assert len(own) == 1
    assert own[0] != f"{sp._BACKUP_PREFIX}20200101-000000"
