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
    def complete(cls, project: str, total_findings: int = 0, by_severity: dict = None,
                 duration_minutes: float = 0, pause_minutes: float = 0):
        return cls(
            type="complete",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={
                "total_findings": total_findings,
                "by_severity": by_severity or {},
                "duration_minutes": duration_minutes,
                "pause_minutes": pause_minutes,
            },
        )

    # --- "Размышление модели": структурированный поток замечаний ---

    @classmethod
    def finding_stage(cls, project: str, stage: str, extra: Optional[dict] = None):
        """Смена фазы в потоке замечаний: merge | critic | corrector | done."""
        data = {"stage": stage}
        if extra:
            data.update(extra)
        return cls(
            type="finding_stage",
            project=project,
            timestamp=datetime.now().isoformat(),
            data=data,
        )

    @classmethod
    def finding_added(cls, project: str, finding: dict):
        """Найдено новое замечание — публикуется после этапа findings_merge."""
        return cls(
            type="finding_added",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={
                "finding_id": finding.get("id") or finding.get("finding_id") or "",
                "severity": finding.get("severity", ""),
                "category": finding.get("category", ""),
                "problem": finding.get("problem") or finding.get("title") or "",
                "sheet": finding.get("sheet"),
                "page": finding.get("page"),
            },
        )

    @classmethod
    def cli_summary(cls, project: str, stage: str, result_md: str,
                    duration_sec: float = 0, cost_usd: float = 0,
                    input_tokens: int = 0, output_tokens: int = 0,
                    cache_read: int = 0, cache_creation: int = 0,
                    model: str = "", is_error: bool = False):
        """Форматированная сводка результата выполнения Claude CLI (вместо сырого JSON)."""
        return cls(
            type="cli_summary",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={
                "stage": stage,
                "result_md": result_md,
                "duration_sec": round(duration_sec, 1),
                "cost_usd": round(cost_usd, 4),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read": cache_read,
                "cache_creation": cache_creation,
                "model": model,
                "is_error": is_error,
            },
        )

    @classmethod
    def finding_verdict(cls, project: str, finding_id: str, verdict: str,
                        details: str = "", suggested_action: Optional[str] = None):
        """Вердикт критика по замечанию."""
        return cls(
            type="finding_verdict",
            project=project,
            timestamp=datetime.now().isoformat(),
            data={
                "finding_id": finding_id,
                "verdict": verdict,
                "details": details,
                "suggested_action": suggested_action,
            },
        )
