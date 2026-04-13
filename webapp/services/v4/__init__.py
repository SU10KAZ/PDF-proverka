"""V4 pipeline — fact-first architecture для audit.

Этот модуль содержит:
- extraction.py       — извлечение typed_facts из блоков (LLM via OpenRouter/CLI)
- canonical_memory.py — канонический граф сущностей из typed_facts
- generators.py       — генераторы кандидатов замечаний (class 2/3/4)
- formatter.py        — конвертация кандидатов → 03_findings.json

Используется через claude_runner.run_block_batch_v4 / run_findings_merge_v4,
которые вызываются условно из pipeline_service если у проекта
pipeline_version == "v4".
"""
