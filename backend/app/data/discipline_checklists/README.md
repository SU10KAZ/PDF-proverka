# Discipline checklists — production data

8 discipline checklists used by the Stage 01 completeness lens (Phase 1).
Source of truth at copy time:
`experiments/md_analysis_comparison/production_preparation/checklists/`.

| File | Discipline |
|---|---|
| `AR.md`    | Архитектурные решения |
| `EOM.md`   | Электроснабжение и силовое электрооборудование |
| `KJ.md`    | Конструкции железобетонные |
| `KM.md`    | Конструкции металлические |
| `MULTI.md` | Cross-discipline fallback (когда дисциплину однозначно не определить) |
| `OV.md`    | Отопление и вентиляция |
| `SS.md`    | Слаботочные системы |
| `VK.md`    | Водоснабжение и канализация |

## Status

**Scaffolding only.** As of Phase 1 Step 0.2 these files are present on disk
and load-able via `backend.app.services.text_analysis.checklist_loader`, but
**not yet consumed by any pipeline stage**. The completeness lens runner that
consumes them will arrive in a later sub-task.

## Format

Plain Markdown, one item per bullet. Each bullet starts with a tag in
brackets describing how the lens should treat the item:

```
- [problem_class=<key>, severity=<СЕВЕРИТИ>, applies=<doc_type>|<doc_type>...]
  Human-readable description with normative references in (СП ..., п. ...).
```

Section tiers (Mandatory / Recommended / Conditional / Anti-patterns) and
severity mapping are documented in
`experiments/md_analysis_comparison/production_preparation/checklists/checklist_rules.md`
and `checklist_applicability_matrix.md`. Those two reference docs stay in
`production_preparation/` because they describe design semantics, not
runtime data.

## Editing policy

- Section headers (`## Mandatory required ...`, `## Recommended items`,
  `## Conditional items ...`, `## Anti-patterns — DO NOT flag these as findings`)
  are matched **verbatim** by the runner — do not rename.
- New items must include the `[problem_class=..., severity=..., applies=...]`
  tag.
- Normative references must follow the existing format (СП XXX, п. X.Y.Z).
- After editing, ensure `tests/text_analysis/test_checklist_files.py` still
  passes — it asserts header presence, encoding, and per-file minimum size.

## Loader

```python
from backend.app.services.text_analysis import checklist_loader

text  = checklist_loader.load_checklist("EOM")
codes = checklist_loader.available_disciplines()
```

The loader is a thin file-read wrapper. It does not parse items, does not
apply `applies=` gating, does not call the LLM — that is the job of the
future completeness lens runner.
