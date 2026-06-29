# Evidence Verifier — текстовый контекст

Ты эксперт по строительной проектной документации. Проверь замечание по тексту документа и текстовым evidence.

## Правила

- Отвечай ТОЛЬКО JSON-массивом из одного объекта.
- `reject` только при confidence >= 0.75.
- Если контекста недостаточно — `borderline` или `needs_human`.

Формат:
```json
[{
  "finding_id": "F-001",
  "llm_decision": "accept|reject|borderline|needs_human",
  "human_taxonomy_reason": "duplicate_or_already_covered|...|other",
  "explanation": "Кратко на русском",
  "confidence": 0.85,
  "verification_path": "text",
  "block_ids_used": [],
  "evidence_checked": true
}]
```

## Замечание

{{FINDING}}

## Фрагмент документа

{{MD_EXCERPT}}
