from __future__ import annotations

from pathlib import Path

from scripts import development_worktree_guard as guard


def test_mutation_is_blocked_at_production_root(tmp_path: Path):
    production = tmp_path / "production"
    production.mkdir()
    assert guard.mutation_allowed(production, production) is False


def test_mutation_is_allowed_in_separate_worktree(tmp_path: Path):
    production = tmp_path / "production"
    worktree = production / ".claude" / "worktrees" / "task"
    worktree.mkdir(parents=True)
    assert guard.mutation_allowed(worktree, production) is True


def test_read_intent_passes_in_production_root(monkeypatch, tmp_path: Path, capsys):
    production = tmp_path / "production"
    production.mkdir()
    monkeypatch.setattr(guard, "resolve_git_toplevel", lambda _cwd: production)
    assert guard.main([
        "--intent", "read", "--cwd", str(production),
        "--production-root", str(production),
    ]) == 0
    assert "intent=read" in capsys.readouterr().out


def test_mutate_intent_fails_closed_in_production_root(monkeypatch, tmp_path: Path, capsys):
    production = tmp_path / "production"
    production.mkdir()
    monkeypatch.setattr(guard, "resolve_git_toplevel", lambda _cwd: production)
    assert guard.main([
        "--intent", "mutate", "--cwd", str(production),
        "--production-root", str(production),
    ]) == 2
    assert "DEVELOPMENT_WORKTREE_GUARD=BLOCKED" in capsys.readouterr().err


def test_missing_git_context_fails_closed(monkeypatch, tmp_path: Path):
    def fail(_cwd):
        raise RuntimeError("git_toplevel_unavailable")

    monkeypatch.setattr(guard, "resolve_git_toplevel", fail)
    assert guard.main(["--intent", "mutate", "--cwd", str(tmp_path)]) == 3
