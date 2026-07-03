"""Shadow-режим Вектографа: observe-only решения гейта «взял бы блок вместо Gemma».

Часть механизма «Вектограф» (vectograf), см. docs/vectograf.md. Ничего в пайплайне
НЕ меняет: проходит по image-блокам result.json, прогоняет гейт качества
(evaluate_vectograf_gate) и пишет артефакт `_output/vectograf_shadow.json` —
по нему оцениваем точность гейта перед реальной заменой Gemma-описаний.

Дёшево: не-схемы отсеиваются структурером за миллисекунды БЕЗ открытия PDF
(build_singleline_graph открывает PDF только после feeder_total >= 3);
однолинейка стоит ~1.2 с. Fail-soft: любая ошибка → None, стадия не страдает.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    vectograf_gate_for_block,
)

SHADOW_FILENAME = "vectograf_shadow.json"
_MIN_VECTOR_LEN = 200   # ниже — заведомо не расчётная однолинейка, гейт не зовём


def write_vectograf_shadow(version_dir: Path) -> Optional[dict]:
    """Прогнать гейт по всем image-блокам версии и записать shadow-отчёт. None при ошибке."""
    try:
        version_dir = Path(version_dir)
        rp = version_dir / "02_work" / "result.json"
        if not rp.exists():
            return None
        rj = json.loads(rp.read_text(encoding="utf-8"))
        records = []
        for pg in rj.get("pages", []):
            for b in (pg.get("blocks") or []):
                if str(b.get("block_type")) != "image":
                    continue
                bid = str(b.get("id") or b.get("block_id") or "")
                if not bid:
                    continue
                vt = b.get("pdfplumber_text") or ""
                if len(vt) < _MIN_VECTOR_LEN:
                    records.append({"block_id": bid, "use": False,
                                    "reasons": ["вектор-слой мал/отсутствует"], "metrics": {}})
                    continue
                gate, _graph = vectograf_gate_for_block(version_dir, bid)
                records.append({"block_id": bid, "use": gate.get("use", False),
                                "reasons": gate.get("reasons") or [],
                                "warnings": gate.get("warnings") or [],
                                "metrics": gate.get("metrics") or {}})
        would_use = [r["block_id"] for r in records if r["use"]]
        report = {
            "schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "blocks_total": len(records),
            "would_use_vectograf": len(would_use),
            "would_use_block_ids": would_use,
            "records": records,
        }
        out = version_dir / "_output" / SHADOW_FILENAME
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(out)
        return report
    except Exception:
        return None
