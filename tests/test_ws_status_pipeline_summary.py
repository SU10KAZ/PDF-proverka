"""WS status_change несёт pipeline_summary, broadcast устойчив к вызову из потока.

Контекст (2026-07-16): список «Статус конвейера» на карточке проекта строится
из pipeline_summary, который раньше приходил только с полной карточкой
/api/projects/{id} и замирал на весь прогон. Теперь update_pipeline_log
кладёт pipeline_summary в то же WS-сообщение status, что и плитки.
"""
import asyncio
import threading

from backend.app.models.websocket import WSMessage
from backend.app.services.common import audit_logger
from backend.app.ws.manager import ConnectionManager


# ─── WSMessage.status_change ────────────────────────────────────────────────

def test_status_change_includes_pipeline_summary():
    summary = [{"key": "findings_merge", "label": "Свод замечаний", "status": "running"}]
    msg = WSMessage.status_change("P1", {"findings_merge": "running"}, pipeline_summary=summary)
    assert msg.type == "status"
    assert msg.data["pipeline"] == {"findings_merge": "running"}
    assert msg.data["pipeline_summary"] == summary


def test_status_change_without_summary_keeps_legacy_shape():
    msg = WSMessage.status_change("P1", {"crop_blocks": "done"})
    assert "pipeline_summary" not in msg.data


# ─── ConnectionManager.schedule_broadcast_to_project ────────────────────────

def test_schedule_broadcast_without_any_loop_is_failsoft():
    manager = ConnectionManager()

    def _call():
        # Рабочий поток без event loop и без запомненного server-loop:
        # не должно ни бросить, ни оставить "never awaited" coroutine.
        manager.schedule_broadcast_to_project("P1", WSMessage.status_change("P1", {}))

    t = threading.Thread(target=_call)
    t.start()
    t.join(timeout=5)
    assert not t.is_alive()


def test_schedule_broadcast_from_worker_thread_uses_saved_loop():
    manager = ConnectionManager()
    delivered = threading.Event()

    async def _fake_broadcast(project_id, message):
        assert project_id == "P1"
        delivered.set()

    manager.broadcast_to_project = _fake_broadcast

    loop = asyncio.new_event_loop()
    manager._loop = loop
    loop_thread = threading.Thread(target=loop.run_forever)
    loop_thread.start()
    try:
        worker = threading.Thread(
            target=manager.schedule_broadcast_to_project,
            args=("P1", WSMessage.status_change("P1", {})),
        )
        worker.start()
        worker.join(timeout=5)
        assert delivered.wait(timeout=5), "broadcast не дошёл до server-loop"
    finally:
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=5)
        loop.close()


# ─── update_pipeline_log → broadcast с pipeline_summary ─────────────────────

def test_update_pipeline_log_broadcasts_pipeline_summary(tmp_path, monkeypatch):
    monkeypatch.setattr(audit_logger, "_project_output_dir", lambda pid: tmp_path)

    sent = []
    monkeypatch.setattr(
        audit_logger.ws_manager,
        "schedule_broadcast_to_project",
        lambda pid, msg: sent.append((pid, msg)),
    )

    audit_logger.update_pipeline_log("P1", "findings_merge", "running")

    assert len(sent) == 1
    pid, msg = sent[0]
    assert pid == "P1"
    summary = msg.data.get("pipeline_summary")
    assert isinstance(summary, list) and summary, "pipeline_summary отсутствует в WS status"
    by_key = {row["key"]: row for row in summary}
    assert by_key["findings_merge"]["status"] == "running"
    # Плитки (pipeline) по-прежнему в сообщении — старый контракт не сломан.
    assert "pipeline" in msg.data
