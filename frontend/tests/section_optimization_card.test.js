import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const js = fs.readFileSync(path.join(root, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(root, 'static/css/styles.css'), 'utf8');

describe('section optimization card', () => {
  it('renders the section-level card first in the regular project grid', () => {
    const card = html.indexOf('class="project-card section-optimization-card"');
    const projectLoop = html.indexOf('v-for="p in group.projects"');
    expect(card).toBeGreaterThan(-1);
    expect(projectLoop).toBeGreaterThan(card);
    expect(html).toContain('v-if="groupIndex === 0"');
    expect(html).toContain('Оптимизация раздела');
    expect(html).toContain('Все корпуса и части как один проект');
    expect(html).toContain('@click="navigateToSectionOptimization(sidebarFilterSection)"');
  });

  it('opens a standalone section optimization page instead of an inline sheet', () => {
    expect(js).toContain("currentView.value = 'section-optimization'");
    expect(js).toContain("/^\\/section\\/([^/]+)\\/optimization$/");
    expect(html).toContain("currentView === 'section-optimization'");
    expect(html).toContain('section-optimization-page__header');
    expect(html).toContain("navigate('/section/' + encodeURIComponent(sidebarFilterSection))");
    expect(html).not.toContain('sectionOptimizationOpen');
  });

  it('groups rows under collapsible project headers without per-row project links', () => {
    expect(html).toContain('group.project.project_name || group.project.project_id');
    expect(html).toContain('toggleSectionOptimizationProject(group.project.project_id)');
    expect(html).toContain("group.project.version_id");
    expect(html).toContain('section-optimization-form7__project-meta');
    expect(html).not.toContain('section-optimization-form7__source');
    expect(html).not.toContain("'Открыть проект ' + row.project_name");
  });

  it('renders specification as form 7 with horizontal section rows', () => {
    expect(html).toContain('Форма 7 · Спецификация');
    expect(html).toContain('Развернуть все проекты');
    expect(html).toContain('Свернуть все проекты');
    expect(html).toContain('<th>Наименование и техническая характеристика</th>');
    expect(html).toContain('<th>Тип, марка, обозначение документа, опросного листа</th>');
    expect(html).toContain('<th>Код оборудования, изделия, материала</th>');
    expect(html).toContain('<th>Завод-изготовитель</th>');
    expect(html).toContain('<th>Единица измерения</th>');
    expect(html).toContain('<th>Количество</th>');
    expect(html).toContain('<th>Масса единицы, кг</th>');
    expect(html).toContain('class="section-optimization-table section-optimization-table--form7"');
    expect(html).toContain('class="section-optimization-form7__project-row"');
    expect(html).toContain('class="section-optimization-form7__category-row"');
    expect(html).toContain('class="section-optimization-form7__missing-row"');
    expect(html).toContain('Спецификация отсутствует');
    expect(html).toContain('class="section-optimization-form7__column-numbers"');
    expect(html).toContain('colspan="9"');
    expect(html).toContain('<td colspan="8"><span>{{ sectionOptimizationSpecificationSectionTitle(row) }}</span></td>');
    expect(js).toContain('function sectionOptimizationSpecificationSectionKey(row)');
    expect(js).toContain('function sectionOptimizationSpecificationProjectKey(row)');
    expect(js).toContain('function sectionOptimizationSpecificationTypeMark(row)');
    expect(js).toContain('const sectionOptimizationSpecificationGroups = computed(() => {');
    expect(js).toContain('function toggleSectionOptimizationProject(projectId)');
    expect(js).toContain('function expandAllSectionOptimizationProjects()');
    expect(js).toContain('function collapseAllSectionOptimizationProjects()');
    expect(html).toContain("section-optimization-form7__project-name--missing");
    expect(html).not.toContain('sectionOptimizationSpecificationsPageCount');
    expect(html).not.toContain('sectionOptimizationSpecificationsPageCount');
  });

  it('separates source rows, accepted decisions and candidates', () => {
    expect(html).toContain("setSectionOptimizationTab('specifications')");
    expect(html).toContain("setSectionOptimizationTab('accepted')");
    expect(html).toContain("setSectionOptimizationTab('signals')");
    expect(html).toContain('sectionOptimizationMeta.accepted_merge_candidates');
  });

  it('keeps the section page focused on the table without summary counters or a global search field', () => {
    expect(html).not.toContain('class="section-optimization-summary"');
    expect(html).not.toContain('placeholder="Поиск по общей таблице..."');
  });

  it('turns the section stages into a runnable pipeline UI', () => {
    expect(html).toContain('Запустить pipeline');
    expect(html).toContain('Заключение умного агента');
    expect(html).toContain('sectionOptimizationGraphicsConclusionLabel(assessment.graphics_review.conclusion)');
    expect(html).toContain('openSectionOptimizationGraphicsBlock(block.project_id, block.block_id, block.page)');
    expect(js).toContain('async function runSectionOptimizationPipeline()');
    expect(js).toContain('async function pollSectionOptimizationPipeline(sectionCode, objectId)');
    expect(js).toContain('function sectionOptimizationPipelineStageMarker(stageKey, index)');
    expect(js).toContain('Анализирует кандидатов: готово ${completed} из ${total}');
    expect(js).toContain('Заключения готовы: ${completed} из ${total}');
    expect(js).toContain('Vision-проверка: готово ${completed} из ${required}');
    expect(js).toContain('function sectionOptimizationGraphicsConclusionLabel(conclusion)');
  });

  it('keeps candidates in a table with explainable accepted source decisions', () => {
    expect(html).toContain('class="section-optimization-table section-optimization-table--signals"');
    expect(html).toContain('<th>Основание / источники</th>');
    expect(html).toContain('<th>Графика</th>');
    expect(html).toContain('Принятые решения:');
    expect(html).toContain('class="section-optimization-source-decisions"');
    expect(html).toContain('class="section-optimization-source-specifications"');
    expect(html).toContain('<th>Текущее решение</th>');
    expect(html).toContain('<th>Принятое предложение</th>');
    expect(html).toContain('<th>Наименование и характеристика</th>');
    expect(html).toContain('Исходные позиции спецификации');
    expect(html).toContain('toggleSectionOptimizationSignal(signal.signal_id)');
    expect(js).toContain('function sectionOptimizationSignalAcceptedItems(signal)');
    expect(js).toContain('function sectionOptimizationSignalSpecificationItems(signal)');
    expect(js).toContain('function sectionOptimizationSignalHasAcceptedSources(signal)');
    expect(html).toContain('Проекты для тиражирования решения');
    expect(html).toContain('Тиражировать');
    expect(js).toContain('function toggleSectionOptimizationSignal(signalId)');
    expect(js).toContain('function sectionOptimizationSignalTypeLabel(signal)');
    expect(js).toContain('function sectionOptimizationSignalGraphicsLabel(signal)');
    expect(js).toContain('evidenceRefs.has(item.source_ref)');
    expect(html).toContain('Запустить умного агента для всех кандидатов');
    expect(html).toContain('Ожидает запуска умного агента');
    expect(html).toContain('sectionOptimizationAgentVerdictLabel(assessment.resolved_verdict || assessment.verdict)');
    expect(html).toContain('@click="startAllSectionOptimizationReplications"');
    expect(html).not.toContain('@click="startSectionOptimizationReplication(signal)"');
    expect(js).toContain('async function startAllSectionOptimizationReplications()');
    expect(js).toContain('function sectionOptimizationReplicationNeedsAgent(signalId)');
    expect(js).toContain('function sectionOptimizationAgentVerdictLabel(verdict)');
    expect(js).toContain('const sectionOptimizationAgentAvailable = computed(() => (');
    expect(js).toContain('const sectionOptimizationGraphicsAgentAvailable = computed(() => (');
    expect(html).toContain('Графический агент подключается…');
    expect(js).toContain("sectionOptimizationReplicationsUrl(sectionCode, '/start-all')");
    expect(js).toContain('async function pollSectionOptimizationReplication(sectionCode, objectId, replicationId)');
    expect(js).toContain("'/replications' + suffix + query");
  });

  it('loads lazily and protects against stale section responses', () => {
    expect(js).toContain("'?object_id=' + encodeURIComponent(objectId)");
    expect(js).toContain("'/optimization/section/' + encodeURIComponent(sectionCode) + query");
    expect(js).toContain('{ withVersion: false }');
    expect(js).toContain('seq !== _sectionOptimizationLoadSeq');
    expect(js).toContain('sidebarFilterSection.value !== sectionCode');
    expect(js).toContain('const cacheKey = `${objectId}|${sectionCode}`');
    expect(js).toContain("(currentObjectId.value || '') !== objectId");
    expect(js).toContain('watch(currentObjectId, (next, previous) => {');
    expect(js).toContain('sectionOptimizationLoadedKey.value = cacheKey');
    expect(js).toContain('const _sectionOptimizationMemoryCache = new Map()');
    expect(html).toContain('Загружаю последний готовый снимок раздела без повторного расчёта.');
    expect(js).toContain("Backend ещё не обновлён: получен устаревший формат сводки раздела.");
    expect(js).toContain('Array.isArray(data.specification_rows)');
  });

  it('has responsive table and card styles', () => {
    expect(css).toContain('.project-card.section-optimization-card {');
    expect(css).toContain('.section-optimization-page__header {');
    expect(css).toContain('читаемых заголовков при 16 px');
    expect(css).toContain('.section-optimization-table--form7 .form7-col--name { width: 28.5%; }');
    expect(css).toContain('.section-optimization-table--form7 .form7-col--unit { width: 7%; }');
    expect(css).toContain('.section-optimization-table--form7 .form7-col--quantity { width: 8%; }');
    expect(css).toContain('.section-optimization-table-wrap { overflow-x: auto;');
    expect(css).toContain('@media (max-width: 680px)');
  });
});
