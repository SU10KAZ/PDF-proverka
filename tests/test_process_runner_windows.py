from webapp.services import process_runner


def test_wraps_cmd_launchers_on_windows(monkeypatch):
    monkeypatch.setattr(process_runner.platform, "system", lambda: "Windows")

    cmd = ["C:\\Users\\user\\AppData\\Roaming\\npm\\claude.CMD", "-p", "--model", "x"]

    assert process_runner._normalize_command_for_windows(cmd) == [
        "cmd.exe",
        "/c",
        "C:\\Users\\user\\AppData\\Roaming\\npm\\claude.CMD",
        "-p",
        "--model",
        "x",
    ]


def test_keeps_regular_executables_unchanged(monkeypatch):
    monkeypatch.setattr(process_runner.platform, "system", lambda: "Windows")

    cmd = ["python.exe", "-m", "webapp.main"]

    assert process_runner._normalize_command_for_windows(cmd) == cmd
