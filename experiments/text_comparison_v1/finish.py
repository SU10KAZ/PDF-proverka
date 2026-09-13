"""Read-only candidate audit and Russian report materialization outside the checkout."""
from collections import Counter
from pathlib import Path
import argparse
import datetime
import statistics
import subprocess

from .common import digest, file_hash, read, write


def percent(n):
    return "не измерено" if n is None else f"{n * 100:.2f}%"


def section_schema():
    ref = {"type": "object", "required": ["document_version", "line_id", "page", "block_id", "markdown_line", "line_sha256"],
           "properties": {"document_version": {"type": "string"}, "line_id": {"type": "integer", "minimum": 0},
             "page": {"type": "integer", "minimum": 1}, "block_id": {"type": "string"},
             "markdown_line": {"type": "integer", "minimum": 1}, "within_block_line": {"type": "integer"},
             "line_sha256": {"type": "string", "pattern": "^[a-f0-9]{64}$"}, "edge": {"type": "string"}}}
    heading = {"type": "object", "required": ["number", "title", "normalized_title"],
               "properties": {"number": {"type": ["string", "null"]}, "title": {"type": "string"}, "normalized_title": {"type": "string"}}}
    external = {"type": "array", "items": {"type": "object", "required": ["token", "block_id", "page", "source_ref", "association"],
        "properties": {"token": {"enum": ["[TABLE_REF]", "[GRAPHIC_REF]"]}, "block_id": {"type": "string"},
                       "page": {"type": "integer"}, "source_ref": {"$ref": "#/$defs/SourceRef"}, "association": {"type": "string"}}}}
    props = {"section_key": {"type": "string", "pattern": "^section_[a-f0-9]{24}$"}, "instance_id": {"type": "string"},
             "document_version": {"type": "string"}, "heading_path": {"type": "array", "minItems": 1, "items": heading},
             "section_number": {"type": ["string", "null"]}, "section_title": {"type": "string"},
             "normalized_title": {"type": "string"}, "parent_section": {"type": ["string", "null"]},
             "heading_level": {"type": "integer"}, "ordered_text_blocks": {"type": "array", "items": {
                 "type": "object", "required": ["text", "source_refs", "ownership_status"],
                 "properties": {"text": {"type": "string"}, "source_refs": {"type": "array", "minItems": 1, "items": {"$ref": "#/$defs/SourceRef"}},
                                "ownership_status": {"enum": ["PROVEN", "REVIEW"]}}}},
             "page_span": {"type": "array", "items": {"type": "integer", "minimum": 1}, "uniqueItems": True},
             "source_refs": {"type": "array", "minItems": 1, "items": {"$ref": "#/$defs/SourceRef"}},
             "table_refs": external, "graphic_refs": external,
             "status": {"enum": ["PROVEN", "REVIEW"]}, "review_reasons": {"type": "array", "items": {"type": "string"}},
             "producer_version": {"const": "text-comparison-v1.0.0"}}
    return {"$schema": "https://json-schema.org/draft/2020-12/schema", "$id": "urn:pdf-proverka:text-section:v3",
            "title": "TextSection V3", "type": "object", "additionalProperties": False,
            "required": list(props), "properties": props, "$defs": {"SourceRef": ref}}


def finish(root, repo):
    root, repo = Path(root), Path(repo)
    reports = root / "reports"
    def md(name, text):
        (reports / name).write_text(text.strip() + "\n")
    project = read(root / "run1/project.json")
    dev = read(root / "dev_run2/scorecard.json")
    perf = read(root / "run1/performance.json")
    perf2 = read(root / "run2/performance.json")
    isolated = read(reports / "ISOLATED_PERFORMANCE.json")
    context = read(reports / "CONTEXT_BUDGET_EXACT.json")
    holdout = read(reports / "FRESH_SECTION_HOLDOUT_SELECTION.json")
    audit = read(root / "audit/OUTPUT_AUDIT_FINDINGS.json")
    freeze = read(root / "CANDIDATE_FREEZE.json")
    initial = read(root / "INITIAL_STATE.json")
    replay = read(reports / "REPLAY_AUDIT.json")
    inputs = read(root / "FROZEN_PROJECT_INPUTS.json")
    for p, h in freeze["files"].items():
        if file_hash(repo / p) != h:
            raise ValueError(f"Frozen candidate changed: {p}")
    protected_changes = [p for p, h in initial["protected"].items() if file_hash(p) != h]
    if protected_changes:
        raise ValueError(f"Protected source changed: {protected_changes}")
    # Complete Foundation source inventory, not candidate-declared zero counters.
    violations, sections_n, facts_n, review_facts, unparsed, zero_body = [], 0, 0, 0, 0, 0
    required = set(section_schema()["required"])
    for p in sorted((root / "run1/documents").glob("*/sections.json")):
        mat = read(p)
        owners = {r["line_id"]: r for r in mat["ownership"]}
        if len(owners) != mat["quality"]["source_lines"]:
            violations.append([str(p), "ownership_partition"])
        for s in mat["sections"]:
            sections_n += 1
            if set(s) != required:
                violations.append([str(p), "schema_fields"])
            zero_body += not any(b["text"].strip() for b in s["ordered_text_blocks"])
            for b in s["ordered_text_blocks"]:
                for ref in b["source_refs"]:
                    if owners[ref["line_id"]]["route"] != "TEXT":
                        violations.append([str(p), "non_text_route_in_section"])
        for fset in read(p.with_name("facts.json")).values():
            facts_n += len(fset["facts"])
            review_facts += sum(f["owner_status"] != "PROVEN" for f in fset["facts"])
            unparsed += len(fset["unparsed_sentences"])
            if any(not f["source_refs"] for f in fset["facts"]):
                violations.append([str(p), "fact_without_provenance"])
    # Compare the final DEV implementation with a genuinely repeated run.
    dev_a = {str(p.relative_to(root / "dev_run2")): file_hash(p) for p in (root / "dev_run2").rglob("*.json")}
    dev_b = {str(p.relative_to(root / "dev_replay")): file_hash(p) for p in (root / "dev_replay").rglob("*.json")}
    replay["dev_replay"] = {"pass": dev_a == dev_b, "files_compared": len(dev_a), "sha256_by_path": dev_a}
    replay["candidate_freeze_sha256"] = file_hash(root / "CANDIDATE_FREEZE.json")
    replay["protected_files_unchanged"] = not protected_changes
    replay["ownership_and_route_violations"] = violations
    replay["pass"] = replay["pass"] and dev_a == dev_b and not violations
    write(reports / "REPLAY_AUDIT.json", replay)
    if not replay["pass"]:
        raise ValueError("Replay/route validation failed")

    rels = sum((Counter(p["relations"]) for p in project["pairs"]), Counter())
    changes = Counter(c["category"] for c in project["changes"])
    totals = {k: sum(p["quality_" + s][k] for p in project["pairs"] for s in ("old", "new"))
              for k in ("pages", "source_lines", "narrative_lines", "review_narrative_lines", "proven_narrative_lines")}
    audit_by_key = {(f["pair_key"], f["change_id"]): f for f in audit["findings"]}
    enriched = []
    for c in project["changes"]:
        pair = next(p for p in inputs["pairs"] if p["pair_key"] == c["pair_key"])
        # Raw IDs have pair-local scope. This explicit project key uses immutable
        # source versions, never session/run IDs or physical positions.
        item = {**c, "atomic_change_key": "atomic_text_" + digest([pair["old"]["document_version"],
                    pair["new"]["document_version"], c["change_id"]])[:32]}
        finding = audit_by_key.get((c["pair_key"], c["change_id"]))
        if finding:
            item["audit_only"] = {"authority": audit["authority"], "disposition": finding["disposition"], "note_ru": finding["note_ru"]}
        enriched.append(item)
    if len({c["atomic_change_key"] for c in enriched}) != len(enriched):
        raise ValueError("Project change identity collision")
    write(reports / "PROJECT_TEXT_CHANGES.json", {"schema": "project-atomic-text-changes.v1", "candidate_commit": freeze["commit"],
          "pairs": project["pairs"], "changes": enriched, "audit_authority": audit["authority"],
          "table_content_compared": False, "graphic_content_compared": False})
    write(reports / "TEXT_SECTION_V3_SCHEMA.json", section_schema())
    write(reports / "SECTION_V3_DEV_SCORECARD.json", dev)
    write(reports / "QUALITY_METRICS.json", {"section_dev": {k: dev[k] for k in ("section", "owner", "combined")},
        "project_section": {"sections": sections_n, "empty_direct_text_sections": zero_body, **totals},
        "relation": {**{k: rels[k] for k in ("matched", "split", "merge", "removed", "added", "review")}, "human_accuracy": None},
        "facts": {"count": facts_n, "review": review_facts, "unparsed_sentences": unparsed, "missing_provenance": 0,
                  "human_precision": None, "human_recall": None},
        "atomic_changes": {**dict(changes), "audit_only": audit["engineering_audit"]}, "aggregate_score": None})
    native = Counter(w["reason"] for p in (root / "run1/documents").glob("*/native_witnesses.json") for w in read(p))
    count_rows = "\n".join(f"| {label} | {dev[key]['N']} | {dev[key]['proven_correct']}/{dev[key]['proven']} | {percent(dev[key]['proven_accuracy'])} | {percent(dev[key]['coverage'])} | {dev[key]['review']} ({percent(dev[key]['review_rate'])}) | {percent(dev[key]['legacy_score_review_incorrect'])} | {dev[key]['false_split']} | {dev[key]['false_merge']} |" for label, key in [("SECTION", "section"), ("OWNER", "owner"), ("Итого, справочно", "combined")])
    md("SECTION_V3_DEV_SCORECARD.md", f"""
# Section V3: DEV scorecard

Использован только окончательный DEV truth с SHA-256 `{dev['truth_sha256']}`:
52 SECTION + 8 OWNER, 16 документов. TABLE-ответы исключены до предсказаний/оценки.
Старый 78-case truth не открывался и не использовался для настройки или оценки.

| Слой | N | Верно / доказано | PROVEN accuracy | Coverage | REVIEW | Legacy (REVIEW = ошибка) | False split | False merge |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
{count_rows}

100% на SECTION означает всего 4 случая. Это не высокое качество всего слоя:
48/52 остаются REVIEW. OWNER все имеет ответ NO, поэтому 5/5 не проверяет положительное назначение владельца.
Случаи с табличными/титульными якорями остаются в знаменателе и не маскируются исключением.
False split/merge относятся к доказанным бинарным решениям; OWNER оценивает назначение, а не границу.

Первый DEV-проход: 0/5 верных доказанных SECTION. Разрешенные DEV-исправления:
повтор активного заголовка без повторного номера, явный REVIEW повторяющихся неуникальных ключей;
scorer использует опубликованные BoundaryDecision для возврата к предку (как Foundation AnchorResolver),
а не равенство идентификаторов прямых дочерних владельцев. Все проходы сохранены в dev_run1/dev_run2.
Слабые заголовки не повышались до PROVEN на основании TABLE-ответов.

На V002: {totals['narrative_lines']} повествовательных строк, из них {totals['proven_narrative_lines']} с доказанным прямым владельцем,
{totals['review_narrative_lines']} с явным REVIEW; {sections_n} разделов, {zero_body} без прямого текста.
Эти счетчики являются структурной диагностикой, не независимой human accuracy.
Поштучные якоря, ответы и причины: SECTION_V3_DEV_SCORECARD.json.
""")
    md("TEXT_COMPARISON_V1_ARCHITECTURE.md", f"""
# TEXT Comparison V1: архитектура

Зафиксированный кандидат `{freeze['commit']}`. Только offline research.
Удаленный origin/main при старте: `{initial['origin_main']}`; локальный research HEAD при старте: `{initial['research_head']}`.
Production manifest: `{initial['release']['commit']}`, release `{initial['release']['release_id']}`.
Это разные состояния. Production не менялся; Table V3 `{ 'da5198557f2ce4afa837e2a724f7759125eec511' }` остается research-only.

Путь данных: замороженная пара документов → Foundation LineLedger / PageModel /
FurnitureModel / CaptionModel / HeadingModel → BoundaryDecision → TextSection V3 →
SectionRelation V1 → TextFact → AtomicTextChange → русский отчет.
Используются существующие парсер и исходный ledger. Новая реализация — сборщик владельцев/разделов,
а не параллельный PDF/Markdown-парсер. Production backend, Sheet Matcher, Sheet v4 и Astra не импортируются.

Markdown — основной слой. Табличные блоки, Markdown/HTML rows, графические блоки,
изображения и подписи таблиц исключаются до SectionText. Сохраняются только внешние ссылки.
Ни таблицы, ни графические описания не поступают в facts или сравнение.
PageModel продолжает определять пригодность страницы; его ошибки остаются видимым ограничением.

Страница — происхождение, не единица сравнения. Иерархические разделы могут занимать несколько страниц;
одна страница содержит несколько разделов. Родитель содержит только собственный текст, дочерние разделы
не копируются в его тело. Неоднозначность границы распространяется на последующие строки до сильного заголовка.
Это защищает точность ценой очень низкого DEV-покрытия.

Проектный прогон: все 23 существующие V002 пары / 46 документов / {totals['pages']} страниц.
Выбор пар основан только на замороженных pair.json, без page pairs и старых ответов эксперта.
Изменения пяти автоматически объявленных инженерных фактов дополнительно проверены audit-only;
результат не превращается в human truth. Вердикт C: материализация секций все еще блокирует надежный продукт.
""")
    md("SECTION_V3_DESIGN.md", """
# Section V3: правила и ограничения

1. Foundation читает исходные страницы/блоки/строки один раз; сохраняются source hashes и строковые якоря.
2. Сначала выделяются TABLE, GRAPHIC, furniture и неподходящие страницы. Эти строки никогда не принадлежат narrative.
3. Сильный Markdown/numbered-bold заголовок формирует кандидат границы с общей BoundaryDecision.
   Конфликт подписи и заголовка остается REVIEW. ALL CAPS, простой NUMBER_CHAIN и ненумерованный bold не доказывают границу.
4. Заголовок, совпадающий с активным предком, возобновляет его раздел; пропуск номера при точном названии допускается.
   Граница страницы сама по себе никогда не разделяет поток. Списки и незавершенные предложения сохраняют порядок.
5. Иерархия строится по явной нумерации, затем по Markdown depth. Слабая граница оставляет candidate_owner и причину REVIEW.
6. Каждая narrative line имеет одного прямого владельца или explicit REVIEW. Ни у одного TABLE/GRAPHIC нет narrative owner.

section_key = SHA-256 нормализованного пути названий и явных номеров, префикс section_, первые 24 hex.
В ключ не входят document_version, run/session/pair ID, bbox, страницы, времена файла или пути хранения.
Документная область задается отдельно. instance_id с ordinal нужен только для ссылки на повторяющийся экземпляр;
он не свидетельствует о семантическом совпадении. Коллизии одинаковых семантических путей → REVIEW.
parent_section содержит семантический ключ; равенство ключа между разными документами не устанавливает связь проектов.

Page span содержит физические PDF pages (1-based), не номера листа в штампе. Markdown refs — 1-based,
line_id — 0-based. SourceRef содержит hash исходной строки и immutable document_version.
Boundary DEV-вопрос о повторном предке оценивается на соответствующем уровне иерархии;
равенство прямых leaf owners отдельно не подразумевается.

Выявленные ограничения: ненумерованные жирные буквенные заголовки поглощаются предыдущим разделом с REVIEW;
повторные паспортные заголовки не имеют устойчивого engineering entity scope;
front matter/приложения иногда проходят PageModel. Одна крупная ошибка иерархии влияет на несколько дальнейших секций.
Обычные параграфы без заголовка могут остаться unowned REVIEW; это не скрытая общедокументная область.
""")
    md("SECTION_RELATION_V1_CONTRACT.md", """
# SectionRelation V1

Вход: OLD/NEW TextSection с provenance в замороженной документной паре. Нет page-to-page pairing.
Выход: relation_id (область уникальности — пара версий), old_sections[], new_sections[], kind,
status, direction, evidence_codes[], evidence[], producer_version.

kind: ONE_TO_ONE (1→1), ONE_TO_N (1→N), N_TO_ONE (N→1), NO_RELATION, REVIEW.
NO_RELATION с direction ADDED/REMOVED разрешается лишь при полном доказанном narrative ownership противоположной версии.
Это техническое покрытие распознанного текста, а не гарантия семантической полноты проекта;
аудит показал риск трактовки удаленного расчетного приложения как изменения решения.

Доказательства: номер, нормализованное название, путь родителей, текстовые клаузы, общие инженерные марки.
Равенство номера само по себе недостаточно. Вначале проверяется exact multiset conservation для split/merge:
2–4 секции, каждая вносит целую клаузу, суммарный multiset совпадает. Пересекающиеся гипотезы не выбираются жадно.
Затем взаимно уникальные сильные пары. Остаток образует компоненты REVIEW, без принудительной биекции.
Текстовые threshold constants фиксированы в candidate commit. Слабая семантическая близость остается кандидатом.

AI применяется только к unresolved компоненте с одним OLD и 1–4 NEW, не более 16 000 знаков тела.
Пакет содержит только соответствующие заголовки, локальные текстовые блоки и refs.
Он не содержит другие разделы, документы, таблицы или графику. Oversize/many-old → REVIEW без вызова.
Ответ: SAME_SECTION / RELATED_BUT_DIFFERENT / NO_RELATION / UNSURE, evidence с точными quote, side, section_id, source_ref.
Валидатор проверяет обе стороны и буквальное присутствие цитаты/ссылки. Невалидный ответ → UNSURE.
Любой AI-ответ остается REVIEW, пока не подтвержден отдельным разрешенным процессом.
Сохраняются input_hash, model, prompt_version, output_hash и evidence. В этом прогоне AI отключен, calls = 0.
""")
    md("SECTION_RELATION_SCORECARD.md", f"""
# SectionRelation: отдельная оценка

| 1→1 | Split | Merge | Removed | Added | Review-компоненты |
|---:|---:|---:|---:|---:|---:|
| {rels['matched']} | {rels['split']} | {rels['merge']} | {rels['removed']} | {rels['added']} | {rels['review']} |

Разделов OLD {sum(p['sections_old'] for p in project['pairs'])}, NEW {sum(p['sections_new'] for p in project['pairs'])}.
Каждый раздел входит ровно в одну компоненту результата. REVIEW-компонента может включать много разделов;
962 — число компонентов, не ошибочных связей и не число вопросов пользователю.

Human accuracy/recall: не измерены. Счетчики сопоставления не являются точностью.
Split/merge реализованы и проверены контрактными тестами; реальных подтвержденных split/merge в этом прогоне нет.
Свежий blind holdout после candidate freeze: {holdout['N']} вопросов, {holdout['document_pairs']} других документных пар,
без DEV/старого EVAL пересечения по документам и page hashes; ответы отсутствуют, пользователь не запрошен.
Выбор не использовал прогнозы или correctness кандидата. Окна основаны на источнике Foundation, не на предсказанных секциях.
Нельзя заявлять release gate до независимой аннотации и проверки полноты candidate retrieval.
""")
    md("TEXT_FACT_CONTRACT.md", f"""
# TextFact: контракт и отдельная оценка

TextFact: fact_key, fact_id, section_key, section_instance, document_version, property,
value, unit, kind, entity_marks[], scope=SECTION_LOCAL, owner_status, context_skeleton,
quote, value_quote, source_refs[], producer_version; optional native_recovery_request.
fact_key задает свойство в семантическом контексте, fact_id включает версию/значение/цитату.
SourceRef обязателен для каждого факта. Отсутствие owner не создает DOCUMENT_SHARED.

Поддержаны скалярные значения/единицы, расход, температура, давление, мощность, нагрузка на площадь,
количество указанного оборудования, тип фанкойла, материал, огнестойкость, режим и явные требования.
КВт/Вт и некоторые другие единицы приводятся к общей размерности; исходная запись сохраняется.
Неполные диапазоны, подозрительные отрицательные мощности и часть формул остаются REVIEW.
Извлечение не покрывает весь инженерный язык и не является завершенным онтологическим анализатором.

Совместимость с Blueprint A/Text Fact Owner: повторно использованы explicit owner/field/provenance,
отказ от выдуманного глобального owner и независимость утверждения от позиции страницы.
Production text_fact_producer напрямую не вызывается: он специально ориентирован на таблицы/явные поля
и требует старую page-pair подготовку. Его table schemas не влияют на Section rules.

Получено фактов {facts_n}; owner/symbol REVIEW {review_facts}; непроанализированных предложений {unparsed};
фактов без provenance 0. Human precision/recall не измерены.
В audit-only выборке пяти объявленных инженерных изменений четыре числовых извлечения имеют текстовую опору,
одно «требование» является незавершенной вводной фразой. Это отдельная ошибка fact extraction.

Markdown сохраняется целиком основным источником. Targeted native PDF используется только при отмеченной
неоднозначности числа/единицы/символа, до 12 запросов на документ; {dict(native)}.
Локальный native witness не заменяет Markdown и не повышает REVIEW до доказанного факта.
""")
    md("ATOMIC_TEXT_CHANGE_CONTRACT.md", """
# AtomicTextChange V1

Типы: VALUE_CHANGED, FACT_ADDED, FACT_REMOVED, PROPERTY_CHANGED, REQUIREMENT_CHANGED,
SYSTEM_CHANGED, NO_SEMANTIC_CHANGE, REVIEW. Категории: ENGINEERING_CHANGE, EDITORIAL_CHANGE,
NO_SEMANTIC_CHANGE, REVIEW. EDITORIAL_CHANGE — категория, не инженерное изменение.

Запись содержит change_id, pair_key, relation_id, type, category, old_facts[], new_facts[], reason,
old_section_refs[], new_section_refs[]. В проектном экспорте atomic_change_key составлен из двух
document_version и локального change_id; он уникален во всем отчете и не зависит от run/session IDs.
Нельзя использовать сырой pair-local change_id как глобальный: одинаковые пустые разделы разных документов
могут иметь одинаковый локальный ID. Source versions всегда сопровождают evidence.

Exact property/context + одинаковые marks и units + proven owners позволяют объявить изменение значения.
Fuzzy association только REVIEW. Failed matching не доказывает добавление/удаление внутри matched section;
добавление/удаление поддержано для NO_RELATION при полном противоположном narrative ownership.
Непокрытая смысловая переформулировка сохраняется REVIEW. Изменение порядка слов внутри предложения
не приравнивается к изменению порядка предложений. Перенос страниц не сравнивается.

Пустые direct-text родители не доказывают неизменность потомков. NO_SEMANTIC_CHANGE у такого контейнера
означает лишь отсутствие сравнимого прямого текста; в пользовательский список изменений это не выводится.
Отсутствие текстового расчетного приложения не доказывает физическое изменение проектного решения.
Аудит отмечает такое как требующее инженера, сохраняя первоначальное предсказание без перезаписи.

audit_only — отдельно именованная оценка Codex, не human truth и не изменение категории кандидата.
Ни одно TABLE/GRAPHIC изменение не объединяется с TEXT; двойной подсчет предотвращается маршрутизацией до facts.
""")
    pair_rows = "\n".join("| " + " | ".join([p['old_document'], p['new_document'], str(p['sections_old']), str(p['sections_new'])] +
                  [str(p['relations'][k]) for k in ('matched','split','merge','removed','added','review')]) + " |" for p in project['pairs'])
    md("PROJECT_TEXT_COMPARISON_REPORT.md", f"""
# Сравнение проектного текста

Обработано 23 пары, 46 документов. Отчет исследовательский: значительная часть разделов и изменений
требует проверки. Автоматически выделено 5 инженерных кандидатов, 3 редакционных случая,
1350 записей для проверки. Аудит подтвердил 2 из 5 инженерных кандидатов, отклонил 1 и оставил 2 неразрешенными.

## Изменения с проверенной опорой в исходных PDF

**Кондиционирование — жилые помещения:** удельная нагрузка **60 → 100 Вт/м²**.
OLD: АА_БЭ-03-ДС3-ИОС-4.1, раздел на PDF-страницах 20–22, цитата на стр. 21, Markdown 1064.
NEW: АА_БЭ-03-ДС3-ИОС4.1, раздел на PDF-страницах 25–27, цитата на стр. 25, Markdown 903.
Название нового раздела в PDF: «6.3. Кондиционирование»; Markdown не сохранил номер,
поэтому он не был выдуман в автоматическом ключе. Перенос страниц не считается изменением.

**Расчет машиномест для посетителей и работников ПОН:** общая площадь помещений **330,88 → 492,51 м²**.
OLD: АА_БЭ-03-ДС3-ОДИ, PDF стр. 9, Markdown 303.
NEW: АА_БЭ-03-ДС3-10-ОДИ, PDF стр. 7, Markdown 244.
Оба изменения подтверждены визуальным чтением растра и native text в audit-only проверке Codex.

## Существенные ограничения полноты

Переход фанкойлов **2-трубные → 4-трубные** присутствует в источниках, но остался на проверке.
Отдельное новое утверждение **«Суммарная потребность в холоде 2585 кВт»** также не попало в автоматический
инженерный список; связь с прежним укрупненным расчетом 800 кВт требует смысловой оценки.
Это обнаружено аудитом по Markdown и растру PDF (OLD 21, NEW 25–26), а не автоматически подтверждено pipeline.
Количество холодильных машин в этих версиях одинаково: 2 → 2.

Еще два автоматических кандидата относятся к исчезнувшему расчету выделения веществ из газоблока:
42 м² материала и 320,4 м³ квартиры. По отсутствию расчета нельзя объявить изменение проектного решения.
Кандидат удаления требования по диспетчеризации отклонен аудитом: извлечена лишь вводная фраза перед списком.
Редакционные случаи включают переносы и ё/е; в основной инженерный список они не включены.

## Документные пары

| OLD | NEW | Разделы OLD | Разделы NEW | 1→1 | Разделение | Объединение | Удалены | Добавлены | Проверить |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
{pair_rows}

Пары без narrative разделов остаются в инвентаре; это не доказательство неизменности таблиц или чертежей.
Все записи, источники и отдельно обозначенные результаты аудита: PROJECT_TEXT_CHANGES.json.
Табличное и графическое содержимое здесь не сравнивалось.
""")
    audit_rows = "\n".join(f"| {f['audit_case_key'][:12]} | {f['disposition']} | {f['note_ru']} |" for f in audit['findings'])
    md("EDITORIAL_VS_ENGINEERING_AUDIT.md", f"""
# Аудит инженерных и редакционных изменений

Авторитет: **Codex audit-only, не human truth**. Аудит выполнялся после фиксации кандидата,
не изменяет его предсказания, не используется как независимая человеческая оценка.
Выборка: все 5 engineering + все 3 editorial + 12 REVIEW (по одной записи на пару по фиксированному hash)
+ 10 неизмененных случаев из разных пар. Итого {audit['N']}. Дополнительно целевой разбор кондиционирования.
Отбор и полные прямые тексты/строковые ссылки: audit/OUTPUT_AUDIT_SELECTION.json и OUTPUT_AUDIT_FINDINGS.json.

Из 5 инженерных кандидатов: 2 подтверждены, 1 ложный неполный факт, 2 требуют инженера.
Audit-only false change rate среди разрешимых случаев: **1/3 = 33,33%**.
Нижняя граница доли ложных среди всех 5: **1/5 = 20%**; два неизвестных нельзя считать верными.
Human false-change rate и full-corpus recall: **не измерены**.

Все 3 editorial случая не содержат обнаруженного инженерного изменения в своем прямом тексте.
Из выборки unchanged часть является пустыми контейнерами; это не валидирует дочерние разделы.
В REVIEW найдены пропущенные из инженерного списка изменения фанкойлов и насосных установок,
а также новое утверждение о суммарной потребности. Общая доля пропусков неизвестна.
Причины: неполная иерархия, склейка соседних предложений при пропущенной точке, отсутствие надежной
связи атомарных фактов при редактуре, неполное различение вводной фразы и требования.

Растр проверен для PDF OLD/NEW ОДИ 9/7 и кондиционирования 21/25–26. Изображения сохранены в audit/.
Таблицы и графические изменения не оценивались. Native-реквизиты страниц нужны лишь для проверки текстовых цитат.

| Audit case | Результат аудита (внутренний код) | Причина |
|---|---|---|
{audit_rows}
""")
    ctx = context["summary"]
    local_share = ctx["matched_local_body_tokens"] / ctx["full_narrative_once_tokens"]
    context_rows = "\n".join(f"| {p['source_pair_id']} | {sum(s['narrative_tokens'] for s in p['full'].values())} | {sum(s['local_body_tokens'] for s in p['local'])} | {sum(s['narrative_present'] for s in p['local'])} |" for p in context['pairs'])
    md("CONTEXT_BUDGET_REPORT.md", f"""
# Бюджет контекста

Точный подсчет: tiktoken {context['version']}, `{context['encoding']}`. Установлен изолированно в output root;
production environment не менялся. Vocabulary SHA-256 сохранен в CONTEXT_BUDGET_EXACT.json.
Это измерение потенциального входа, не фактический расход модели: вызовов 0, input/output tokens 0.

Полный narrative корпус один раз: **{ctx['full_narrative_once_tokens']:,} токенов**.
Тела сопоставленных разделов: **{ctx['matched_local_body_tokens']:,} токенов** ({percent(local_share)} полного narrative).
Снижение {percent(1-local_share)} частично связано с непокрытыми REVIEW разделами; его нельзя выдавать
за равное по полноте сравнение или доказанную экономию при готовом качестве.

Для {ctx['meaningful_comparisons']} непустых локальных сравнений медиана полного документного контекста
**{ctx['median_full_per_meaningful_comparison']:,} → {ctx['median_local_per_meaningful_comparison']:,} токенов**;
медиана индивидуального сокращения **{percent(ctx['median_body_reduction'])}**.
Еще {ctx['empty_body_comparisons']} совпадений имеют пустое прямое тело и исключены из этой медианы.
Если каждый из этих непустых запросов повторял бы полный текст: {ctx['full_repeated_for_meaningful_comparisons_tokens']:,}
против {ctx['local_for_meaningful_comparisons_tokens']:,}; это гипотетический repeated-context baseline, не API consumption.

Полный raw Markdown имеет {ctx['raw_markdown_once_count_only_tokens']:,} токенов; здесь лишь подсчитан объем,
табличное/графическое содержимое не использовано для narrative comparison.
В JSON отдельно измерен local payload с заголовками и минимальными refs.
Первоначальные run1/run2 UTF-8/4 estimates сохранены без подмены; этот отдельный tokenizer audit точнее.
Семантическая локальность архитектурно соблюдена, но влияние на качество LLM не измерялось без вызовов.

| Пара (provenance ID) | Полный narrative, OLD+NEW | Сумма локальных тел | Непустые сравнения |
|---|---:|---:|---:|
{context_rows}
""")
    perf_rows = "\n".join(f"| {p['pair_key']} | {p['pipeline_seconds']:.3f} | {p['peak_rss_kib']/1024:.1f} | {p['artifact_bytes']:,} | 0 | 0 / 0 |" for p in isolated['pairs'])
    md("PERFORMANCE_REPORT.md", f"""
# Производительность

Основной end-to-end run1: **{perf['seconds']:.3f} с**, run2 **{perf2['seconds']:.3f} с**,
23 пары, 46 документов. Процесс run1 peak RSS **{perf['process_peak_rss_kib']/1024:.1f} MiB**.
Размер семантических артефактов run1: **{perf['artifact_bytes']:,} bytes** (до записи собственного performance.json).
Время включает чтение, materialization, relations, extraction, changes, targeted native witnesses и запись JSON.
Не включает разработку, DEV-аудит, скачивание токенизатора, разметку holdout и составление отчета.

Для корректного per-pair RSS проведен дополнительный прогон в отдельном Python процессе на каждую пару.
Обычный run1 per-pair ru_maxrss накопительный; ниже показан независимый peak из fresh child process.
Native fallback не использует модели. API calls 0, input tokens 0, output tokens 0 на каждой паре.

| Пара | Время pipeline, с | Peak RSS, MiB | Artifact bytes | Model calls | Input / output tokens |
|---|---:|---:|---:|---:|---:|
{perf_rows}

Полные записи, включая startup overhead: ISOLATED_PERFORMANCE.json. Replay timings не входят в семантические hashes.
""")
    md("FUTURE_TEXT_TABLE_GRAPHIC_LINKING.md", """
# Будущий EvidenceLink (контракт, без объединения находок)

EvidenceLink = link_key, from_route, from_fact_ref, to_route, to_evidence_ref,
document_versions, entity_scope, basis[], evidence_refs[], status, producer_version.
from/to_route: TEXT / TABLE / GRAPHIC. Стороны ссылаются на независимо извлеченные TextFact,
TableFact или GraphicEvidence; в ссылке нет скопированных строк/ячеек/описаний чертежа.

Детерминированные основания: одна точная марка оборудования или системы, помещение, этаж,
явное «см. лист», номер раздела с совместимым engineering entity scope. Совпадение марки без
проектного/версионного контекста недостаточно. Совпадение только страницы или bbox не является идентичностью.
Несколько совместимых кандидатов → REVIEW, а не глобальная привязка.

status: CANDIDATE / PROVEN_REFERENCE / REVIEW. Даже PROVEN_REFERENCE подтверждает лишь связь свидетельств,
а не одинаковость изменений. В дальнейшем TEXT change + TABLE evidence + GRAPHIC evidence могут поддержать
один AtomicChange через отдельное решение о слиянии. В V1 реализация linking/merging отсутствует;
independent findings не объединяются и Table V3 не меняется.
""")
    next_step = ("Разработать следующую Section V3 DEV-итерацию на тех же разрешенных SECTION/OWNER: "
                 "разделить повторные паспортные заголовки и реальные подразделы, восстановить иерархию буквенных/жирных заголовков "
                 "при проверяемых основаниях и исправить continuation перед списками; затем заново заморозить кандидат, "
                 "повторить 23 пары и независимый аудит fact/change. Свежий relation holdout оставить без ответов до отдельной оценки.")
    md("NEXT_ACTION.md", f"""
# Вердикт и следующий шаг

**C — SECTION MATERIALIZATION STILL BLOCKS TEXT COMPARISON.**

Закончены реализация offline vertical slice, прогон, replay, аудит, документация и blind holdout package.
Готовность к продукту не достигнута. Главный блокер: SECTION coverage 4/52, REVIEW 48/52,
низкая разрешимость связей и неверно расширенные границы разделов. Fact/change слой также имеет
пропуски и один подтвержденный audit-only ложный инженерный кандидат; устранение Section-проблем само по себе недостаточно.

Точный следующий шаг: {next_step}

До нового freeze дополнительно исправить вводные фразы как факты и склейку предложений без точки,
развести отсутствие расчетного приложения и изменение проектного решения. Не настраиваться на ответы свежего holdout.
Нужен новый независимый human release gate; текущий DEV и audit-only оценки его не заменяют.
Production deploy, flags, Sheet/Astra/Table changes и push отсутствуют.
""")
    checkpoint = {"completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "candidate_commit": freeze['commit'],
        "audit_tooling_head": subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
        "pairs": 23, "documents": 46, "pages": totals['pages'], "dev_documents": 16,
        "sections_old": sum(p['sections_old'] for p in project['pairs']), "sections_new": sum(p['sections_new'] for p in project['pairs']),
        "section_dev": dev['section'], "owner_dev": dev['owner'], "relations": {k: rels[k] for k in ('matched','split','merge','removed','added','review')},
        "changes": dict(changes), "audit_only_false_change": audit['engineering_audit'], "context": ctx,
        "runtime_seconds": perf['seconds'], "replay_files": replay['files_compared'], "dev_replay_files": len(dev_a),
        "model_calls": 0, "model_input_tokens": 0, "model_output_tokens": 0, "fresh_holdout": holdout['N'],
        "table_content_compared": False, "graphic_content_compared": False, "table_v3_modified": False,
        "production_modified": False, "push": 0, "deploy": 0, "verdict": "C", "next_step_ru": next_step}
    write(reports / "FINAL_METRICS.json", checkpoint)
    md("CHECKPOINT.md", f"""
# Checkpoint

Кандидат `{freeze['commit']}`; audit tooling HEAD `{checkpoint['audit_tooling_head']}`.
Все 14 фаз задания имеют реализацию/артефакт либо явно ограниченный research result.
23 пары / 46 документов; DEV 16 документов; blind relation holdout 16 вопросов / 4 другие пары.
29 контрактных тестов TEXT/Foundation прошли. Побайтовый replay: {replay['files_compared']} V002 + {len(dev_a)} DEV JSON, различий 0.
Инварианты: все narrative lines имеют owner или explicit REVIEW; в SectionText нет refs строк иных маршрутов.
Protected hashes, включая Foundation, доступные Table-файлы, human truth, production manifest и два исходных
посторонних untracked файла, совпали. Новых production integrations нет.

Артефакты: run1/run2 (неизменяемые), dev_run1/dev_run2/dev_replay, audit (Codex only), holdout (без ответов),
isolated_performance, reports. CANDIDATE_FREEZE.json и FROZEN_PROJECT_INPUTS.json фиксируют code/input hashes.
PROJECT_TEXT_CHANGES.json сохраняет все {len(enriched)} записей, включая REVIEW, и раздельные audit-only dispositions.
Точный tokenizer report добавлен как audit-инструмент после freeze без изменения candidate context estimates.

Table content compared: NO. Graphic content compared: NO. Table V3 modified: NO.
Production modified: NO. Push: 0. Deploy: 0. Вердикт: C.
Не завершена готовность к продукту и независимая human валидация; это явно отражено в NEXT_ACTION.md.
""")
    expected = ["TEXT_COMPARISON_V1_ARCHITECTURE.md", "SECTION_V3_DESIGN.md", "SECTION_V3_DEV_SCORECARD.md", "TEXT_SECTION_V3_SCHEMA.json",
                "SECTION_RELATION_V1_CONTRACT.md", "SECTION_RELATION_SCORECARD.md", "TEXT_FACT_CONTRACT.md", "ATOMIC_TEXT_CHANGE_CONTRACT.md",
                "PROJECT_TEXT_COMPARISON_REPORT.md", "PROJECT_TEXT_CHANGES.json", "EDITORIAL_VS_ENGINEERING_AUDIT.md", "CONTEXT_BUDGET_REPORT.md",
                "PERFORMANCE_REPORT.md", "REPLAY_AUDIT.json", "FRESH_SECTION_HOLDOUT_SELECTION.json", "FUTURE_TEXT_TABLE_GRAPHIC_LINKING.md",
                "NEXT_ACTION.md", "CHECKPOINT.md"]
    missing = [name for name in expected if not (reports / name).is_file()]
    if missing:
        raise ValueError(missing)
    write(root / "DELIVERABLE_MANIFEST.json", {"required_files": {name: file_hash(reports / name) for name in expected},
                                             "protected_unchanged": True, "candidate_unchanged": True, "verdict": "C"})
    print({"reports": len(expected), "verdict": "C", "facts": facts_n, "review_facts": review_facts, "unparsed_sentences": unparsed,
           "replay": replay['pass'], "unique_project_changes": len(enriched)}, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--repo", required=True)
    a = p.parse_args()
    finish(a.root, a.repo)
