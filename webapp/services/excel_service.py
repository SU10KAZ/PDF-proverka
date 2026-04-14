"""
Обёртка для generate_excel_report.py.
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from webapp.config import BASE_DIR, REPORTS_DIR, GENERATE_EXCEL_SCRIPT
from webapp.services.process_runner import run_script


async def generate_excel(
    output_path: Optional[str] = None,
    report_type: str = "all",
    project_dirs: Optional[list[str]] = None,
) -> tuple[bool, str]:
    """
    Генерирует Excel-отчёт.

    Args:
        report_type: "findings" | "optimization" | "all"
        project_dirs: список путей к папкам проектов (если None — все)

    Returns:
        (success, file_path_or_error)
    """
    if not output_path:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        prefix = {
            "findings": "findings_report",
            "optimization": "optimization_report",
            "all": "audit_report",
        }.get(report_type, "audit_report")
        output_path = str(REPORTS_DIR / f"{prefix}_{ts}.xlsx")

    args = ["--out", output_path, "--type", report_type]

    if project_dirs:
        args.extend(project_dirs)

    exit_code, stdout, stderr = await run_script(
        str(GENERATE_EXCEL_SCRIPT),
        args,
        env_overrides={"AUDIT_NO_OPEN": "1"},
    )

    if exit_code == 0 and os.path.exists(output_path):
        return True, output_path
    else:
        return False, stderr or f"Exit code: {exit_code}"
