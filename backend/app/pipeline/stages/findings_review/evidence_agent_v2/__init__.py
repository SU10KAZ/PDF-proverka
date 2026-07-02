"""Evidence Agent v2 (EV2) — прод-реализация агента-верификатора замечаний.

Портирована из experiments/evidence_agent_v2/ (исследовательская ветка Андрея
Ивановича) и подключена как боевой Evidence Verifier вместо прежней реализации.

Архитектурное отличие EV2: ВОСПРИЯТИЕ отделено от СУЖДЕНИЯ.
  1) vision-модель только ЧИТАЕТ чертёж и отвечает на узкий вопрос
     "противоречит ли увиденное конкретному утверждению замечания";
  2) самосогласованность: K независимых прогонов восприятия;
  3) детерминированная политика на Python агрегирует голоса и сливает с
     офлайн-сигналами (норма + кросс-блок) в 4-вердикт с консервативным
     смещением — реальное замечание никогда не удаляется автоматически.

Публичная точка входа для сервиса — verify_finding_multi_async(...) -> FusedVerdict.
"""
from __future__ import annotations

from .fusion import FusedVerdict, fuse
from .kb_routing import should_run_evidence_verifier
from .precedent import PrecedentSignal, run_precedent_check
from .verify import (
    DEFAULT_RUNS,
    Verdict,
    verify_finding_multi,
    verify_finding_multi_async,
)

__all__ = [
    "FusedVerdict",
    "fuse",
    "Verdict",
    "DEFAULT_RUNS",
    "verify_finding_multi",
    "verify_finding_multi_async",
    "should_run_evidence_verifier",
    "PrecedentSignal",
    "run_precedent_check",
]
