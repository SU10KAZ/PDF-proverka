# Refactor Structure Plan
**Дата:** 2026-05-06

## Цель
Разделить frontend и backend, внутри backend разложить код по этапам пайплайна.
Сохранить бизнес-логику, JSON-форматы, API-роуты, WebSocket и CLI-команды.

---

## 1. КАРТА ПЕРЕНОСА ФАЙЛОВ

### Frontend: webapp/static/ → frontend/

| Откуда | Куда |
|--------|------|
| webapp/static/index.html | frontend/index.html |
| webapp/static/model-control.html | frontend/model-control.html |
| webapp/static/css/ | frontend/css/ |
| webapp/static/js/ | frontend/js/ |

Добавить новые файлы:
- frontend/package.json
- frontend/vite.config.js
- frontend/README.md

### Backend: webapp/ → backend/app/

| Откуда | Куда |
|--------|------|
| webapp/main.py | backend/app/main.py |
| webapp/config.py | backend/app/core/config.py |
| webapp/routers/*.py | backend/app/api/routers/*.py |
| webapp/models/*.py | backend/app/models/*.py |
| webapp/schemas/ | backend/app/schemas/ |
| webapp/ws/ | backend/app/ws/ |
| webapp/data/ | backend/app/data/ |

### Backend services: webapp/services/ → backend/app/services/

#### common/
| Файл | Куда |
|------|------|
| webapp/services/process_runner.py | backend/app/services/common/process_runner.py |
| webapp/services/cli_utils.py | backend/app/services/common/cli_utils.py |
| webapp/services/audit_logger.py | backend/app/services/common/audit_logger.py |
| webapp/services/usage_service.py | backend/app/services/common/usage_service.py |
| webapp/services/object_service.py | backend/app/services/common/object_service.py |
| webapp/services/project_service.py | backend/app/services/common/project_service.py |
| webapp/services/group_service.py | backend/app/services/common/group_service.py |
| webapp/services/discipline_service.py | backend/app/services/common/discipline_service.py |

#### llm/
| Файл | Куда |
|------|------|
| webapp/services/llm_runner.py | backend/app/services/llm/llm_runner.py |
| webapp/services/claude_runner.py | backend/app/services/llm/claude_runner.py |
| webapp/services/gemini_direct_runner.py | backend/app/services/llm/gemini_direct_runner.py |
| webapp/services/openrouter_block_batch.py | backend/app/services/llm/openrouter_block_batch.py |
| webapp/services/lms_service.py | backend/app/services/llm/lms_service.py |
| webapp/services/lmstudio_lifecycle_service.py | backend/app/services/llm/lmstudio_lifecycle_service.py |
| webapp/services/model_control_service.py | backend/app/services/llm/model_control_service.py |

#### findings/
| Файл | Куда |
|------|------|
| webapp/services/findings_service.py | backend/app/services/findings/findings_service.py |
| webapp/services/finding_quality.py | backend/app/services/findings/finding_quality.py |
| webapp/services/grounding_service.py | backend/app/services/findings/grounding_service.py |

#### knowledge_base/
| Файл | Куда |
|------|------|
| webapp/services/knowledge_base_service.py | backend/app/services/knowledge_base/knowledge_base_service.py |
| webapp/services/missing_norms_service.py | backend/app/services/knowledge_base/missing_norms_service.py |

#### discussions/
| Файл | Куда |
|------|------|
| webapp/services/discussion_service.py | backend/app/services/discussions/discussion_service.py |

#### export/
| Файл | Куда |
|------|------|
| webapp/services/excel_service.py | backend/app/services/export/excel_service.py |

### Pipeline: корень + webapp/services/ → backend/app/pipeline/

| Откуда | Куда |
|--------|------|
| webapp/services/pipeline_service.py | backend/app/pipeline/manager.py |
| webapp/services/resume_detector.py | backend/app/pipeline/resume_detector.py |
| webapp/services/prepare_service.py | backend/app/pipeline/stages/prepare/prepare_service.py |
| webapp/services/task_builder.py | backend/app/pipeline/stages/prepare/task_builder.py |
| webapp/services/prompt_builder.py | backend/app/pipeline/stages/prepare/prompt_builder.py |
| webapp/services/gemma_gate.py | backend/app/pipeline/stages/gemma_enrichment/gemma_gate.py |
| process_project.py | backend/app/pipeline/stages/prepare/process_project.py |
| graph_builder.py | backend/app/pipeline/stages/prepare/graph_builder.py |
| blocks.py | backend/app/pipeline/stages/crop_blocks/blocks.py |
| block_markdown.py | backend/app/pipeline/stages/crop_blocks/block_markdown.py |
| gemma_enrich.py | backend/app/pipeline/stages/gemma_enrichment/gemma_enrich.py |
| gemma_enrichment_contract.py | backend/app/pipeline/stages/gemma_enrichment/gemma_enrichment_contract.py |
| gemma_findings_only.py | backend/app/pipeline/stages/block_analysis/gemma_findings_only.py |
| generate_excel_report.py | backend/app/pipeline/stages/report/generate_excel_report.py |
| norms/_core.py | backend/app/pipeline/stages/norms/core.py |
| norms/_native_verify.py | backend/app/pipeline/stages/norms/native_verify.py |
| norms/external_provider.py | backend/app/pipeline/stages/norms/external_provider.py |

### CLI wrappers (остаются в корне)

Файлы остаются тонкими wrapper-ами, делегируют к backend:
- process_project.py → wrapper → backend.app.pipeline.stages.prepare.process_project
- blocks.py → wrapper → backend.app.pipeline.stages.crop_blocks.blocks
- gemma_enrich.py → wrapper → backend.app.pipeline.stages.gemma_enrichment.gemma_enrich
- generate_excel_report.py → wrapper → backend.app.pipeline.stages.report.generate_excel_report

---

## 2. КАРТА ЗАМЕНЫ ИМПОРТОВ

### webapp.config → backend.app.core.config

Затрагивает все файлы в webapp/routers/, webapp/services/, и корневые скрипты:
```
from webapp.config import X  →  from backend.app.core.config import X
```

### webapp.models → backend.app.models

```
from webapp.models.audit import X  →  from backend.app.models.audit import X
from webapp.models.findings import X  →  from backend.app.models.findings import X
(и т.д.)
```

### webapp.services → backend.app.services (с разбивкой)

```
from webapp.services.process_runner import X  →  from backend.app.services.common.process_runner import X
from webapp.services.audit_logger import X  →  from backend.app.services.common.audit_logger import X
from webapp.services.usage_service import X  →  from backend.app.services.common.usage_service import X
from webapp.services.object_service import X  →  from backend.app.services.common.object_service import X
from webapp.services.project_service import X  →  from backend.app.services.common.project_service import X
from webapp.services.group_service import X  →  from backend.app.services.common.group_service import X
from webapp.services.discipline_service import X  →  from backend.app.services.common.discipline_service import X
from webapp.services.cli_utils import X  →  from backend.app.services.common.cli_utils import X

from webapp.services.llm_runner import X  →  from backend.app.services.llm.llm_runner import X
from webapp.services.claude_runner import X  →  from backend.app.services.llm.claude_runner import X
from webapp.services.gemini_direct_runner import X  →  from backend.app.services.llm.gemini_direct_runner import X
from webapp.services.openrouter_block_batch import X  →  from backend.app.services.llm.openrouter_block_batch import X
from webapp.services.lms_service import X  →  from backend.app.services.llm.lms_service import X
from webapp.services.lmstudio_lifecycle_service import X  →  from backend.app.services.llm.lmstudio_lifecycle_service import X
from webapp.services.model_control_service import X  →  from backend.app.services.llm.model_control_service import X

from webapp.services.findings_service import X  →  from backend.app.services.findings.findings_service import X
from webapp.services.finding_quality import X  →  from backend.app.services.findings.finding_quality import X
from webapp.services.grounding_service import X  →  from backend.app.services.findings.grounding_service import X

from webapp.services.knowledge_base_service import X  →  from backend.app.services.knowledge_base.knowledge_base_service import X
from webapp.services.missing_norms_service import X  →  from backend.app.services.knowledge_base.missing_norms_service import X

from webapp.services.discussion_service import X  →  from backend.app.services.discussions.discussion_service import X
from webapp.services.excel_service import X  →  from backend.app.services.export.excel_service import X

from webapp.services.pipeline_service import X  →  from backend.app.pipeline.manager import X
from webapp.services.resume_detector import X  →  from backend.app.pipeline.resume_detector import X
from webapp.services.prepare_service import X  →  from backend.app.pipeline.stages.prepare.prepare_service import X
from webapp.services.task_builder import X  →  from backend.app.pipeline.stages.prepare.task_builder import X
from webapp.services.prompt_builder import X  →  from backend.app.pipeline.stages.prepare.prompt_builder import X
from webapp.services.gemma_gate import X  →  from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import X
```

### webapp.ws → backend.app.ws

```
from webapp.ws.manager import X  →  from backend.app.ws.manager import X
```

### webapp.routers → backend.app.api.routers

```
from webapp.routers import X  →  from backend.app.api.routers import X
```

### Корневые модули → pipeline/stages/

```
from block_markdown import X  →  from backend.app.pipeline.stages.crop_blocks.block_markdown import X
from blocks import X  →  from backend.app.pipeline.stages.crop_blocks.blocks import X
from gemma_enrichment_contract import X  →  from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import X
from gemma_enrich import X  →  from backend.app.pipeline.stages.gemma_enrichment.gemma_enrich import X
from gemma_findings_only import X  →  from backend.app.pipeline.stages.block_analysis.gemma_findings_only import X
from graph_builder import X  →  from backend.app.pipeline.stages.prepare.graph_builder import X
from process_project import X  →  from backend.app.pipeline.stages.prepare.process_project import X
from norms import X  →  from backend.app.pipeline.stages.norms.core import X (или native_verify / external_provider)
```

---

## 3. ФАЙЛЫ-WRAPPERS (для CLI-совместимости)

Остаются в корне как тонкие делегаторы:

| Файл | Статус |
|------|--------|
| process_project.py | wrapper |
| blocks.py | wrapper |
| gemma_enrich.py | wrapper |
| generate_excel_report.py | wrapper |
| block_markdown.py | wrapper (импортируется как библиотека) |
| gemma_enrichment_contract.py | wrapper |
| gemma_findings_only.py | wrapper |
| graph_builder.py | wrapper |

---

## 4. ФАЙЛЫ БЕЗ ИЗМЕНЕНИЙ

Не переносятся, остаются как есть:
- projects/ (runtime data)
- prompts/ (runtime data)
- knowledge_base/ (runtime data)
- reports/ (runtime data)
- docs/ (документация)
- norms/vault/ (данные нормативов)
- norms/norms_db.json
- norms/norms_paragraphs.json
- .env
- pytest.ini
- scripts/ (benchmark/experiment скрипты)
- Experiments_Kuldyaev/

---

## 5. РИСКИ

1. **Большой pipeline_service.py** — много зависимостей, нужно тщательно обновлять импорты.
2. **Корневые скрипты импортируются как модули** — `from blocks import X` в webapp/services — нужно сохранить wrapper.
3. **webapp/data/** — runtime state (prepare_queue.json и др.) — нужно убедиться, что пути не сломаются.
4. **norms/ имеет собственный __init__.py** — остаётся на месте, импорт через `from norms import X` продолжает работать через sys.path.
5. **tests/ отсутствует** — нет unit-тестов для верификации, только compileall.
6. **.env в корне** — пути могут быть относительными, нужна проверка в config.py.
