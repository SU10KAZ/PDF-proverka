"""Пакет findings_review.

LLM-критик замечаний (run_findings_review) удалён — как фильтр он был статистически
бесполезен (recall 17%). Проверку замечаний делает отдельный этап «Верификатор»
(stages/findings_verify). Здесь ОСТАЮТСЯ переиспользуемые детерминированные модули:
  · deterministic_critic.run_deterministic_critic (структурные проверки 1/2/4);
  · deterministic_corrector.run_deterministic_corrector (корректировка, ничего не удаляет);
а также экспериментальная подсистема critic_v2 (OFF).
"""
