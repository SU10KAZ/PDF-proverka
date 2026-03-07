"""Pydantic-модели для WebSocket-сообщений."""
from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime


class WSMessage(BaseModel):
    """WebSocket-сообщение от сервера клиенту."""
    type: str           # log, progress, status, error, complete
    project: str = ""
    timestamp: str = ""
    data: dict = {}

    @classmethod
    def log(cls, project: str, message: str, level: str = "info", stage: str = ""):
        return cls(
            type="log",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={"level": level, "message": message, "stage": stage},
        )

    @classmethod
    def progress(cls, project: str, current: int, total: int, stage: str = ""):
        pct = round(current / total * 100, 1) if total > 0 else 0
        return cls(
            type="progress",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={
                "stage": stage,
                "current": current,
                "total": total,
                "percent": pct,
            },
        )

    @classmethod
    def status_change(cls, project: str, pipeline: dict):
        return cls(
            type="status",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={"pipeline": pipeline},
        )

    @classmethod
    def error(cls, project: str, message: str, stage: str = ""):
        return cls(
            type="error",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={"message": message, "stage": stage},
        )

    @classmethod
    def heartbeat(cls, project: str, stage: str = "", elapsed_sec: float = 0,
                  process_alive: bool = True, batch_current: int = 0,
                  batch_total: int = 0, eta_sec: Optional[float] = None,
                  tokens: Optional[dict] = None):
        return cls(
            type="heartbeat",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={
                "stage": stage,
                "elapsed_sec": round(elapsed_sec, 1),
                "process_alive": process_alive,
                "batch_current": batch_current,
                "batch_total": batch_total,
                "eta_sec": round(eta_sec, 0) if eta_sec is not None else None,
                "tokens": tokens,
            },
        )

    @classmethod
    def complete(cls, project: str, total_findings: int = 0, by_severity: dict = None, duration_minutes: float = 0):
        return cls(
            type="complete",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={
                "total_findings": total_findings,
                "by_severity": by_severity or {},
                "duration_minutes": duration_minutes,
            },
        )
