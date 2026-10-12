"""Persistable UI progress; does not alter comparison inputs or model calls."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

STAGES = ("SOURCE_PREPARATION", "MAPPING", "MINING", "UNMATCHED_REVIEW",
          "DEDUPE", "SOURCE_VERIFICATION", "RESULT")


class PipelineProgress:
    def __init__(self) -> None:
        self.current: str | None = None
        self.stages = {key: {"status": "WAITING"} for key in STAGES}

    def update(self, status: str, message: str, reason: str, now: str,
               extra: dict[str, Any]) -> dict[str, Any]:
        operation = extra.get("current_stage")
        stage = "RESULT" if operation in {"PERSISTING", "HUMAN_MAPPING"} else operation
        if status == "RUNNING" and stage in self.stages:
            if self.current and self.current != stage:
                previous = self.stages[self.current]
                if previous["status"] == "RUNNING":
                    previous.update(status="COMPLETED", finished_at=now)
                    if "total" in previous:
                        previous["processed"] = previous["total"]
            self.current = stage
            row = self.stages[stage]
            row.setdefault("started_at", now)
            row.update(status="RUNNING", message=message, operation=operation)
            for key in ("processed", "total", "unit", "current_item"):
                if key in extra:
                    row[key] = extra[key]
            if "mapper_portions" in extra:
                row.update(processed=extra["mapper_portion"] - 1,
                           total=extra["mapper_portions"], unit="portion")
        for key, values in extra.pop("pipeline_updates", {}).items():
            if key in self.stages:
                self.stages[key].update(values)
                if values.get("status") in {"COMPLETED", "SKIPPED", "PARTIAL", "REUSED"}:
                    self.stages[key].setdefault("finished_at", now)
        if status in {"FAILED", "COMPLETED", "REVIEW"}:
            key = self.current or "SOURCE_PREPARATION"
            row = self.stages[key]
            if status == "FAILED":
                row.update(status="CANCELLED" if reason == "v3_cancelled" else "FAILED",
                           message=message, finished_at=now)
            elif row["status"] == "RUNNING":
                row.update(status="PARTIAL" if extra.get("human_mapping_published") is False
                           else "COMPLETED", message=message, finished_at=now)
                if "total" in row:
                    row["processed"] = row["total"]
        return {"pipeline_stages": deepcopy(self.stages), "pipeline_current": self.current}
