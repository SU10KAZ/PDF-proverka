import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

function functionSource(name) {
  const start = app.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`function ${name} not found`);
  let depth = 0;
  for (let i = app.indexOf('{', start); i < app.length; i += 1) {
    if (app[i] === '{') depth += 1;
    if (app[i] === '}') depth -= 1;
    if (depth === 0) return app.slice(start, i + 1);
  }
  throw new Error(`function ${name} not closed`);
}

// Константы сторон групп — из исходника, чтобы тест не расходился с кодом.
const SIDE_CONSTANTS = ['SC_SIDES', 'SC_OTHER_SIDE', 'SC_GROUP_BUILDING'].map(name => {
  const match = app.match(new RegExp(`const ${name} = [^;]+;`));
  if (!match) throw new Error(`constant ${name} not found`);
  return match[0];
}).join('\n');

// Функция вместе с функциями, которые она вызывает (последнее имя — результат).
function extractFunction(...names) {
  const name = names[names.length - 1];
  return new Function(`${SIDE_CONSTANTS}\n${names.map(functionSource).join('\n')}; return ${name};`)();
}

describe('группы документов на доске пар (1 → N, N → 1, N ↔ M)', () => {
  it('убирает карту листов вместе с её регионами для сборки нескольких новых документов', () => {
    const grouped = extractFunction('scIsMultiAssembly', 'scIsGroupedComparison');
    expect(grouped(null)).toBe(false);
    expect(grouped({right:{}})).toBe(false);
    expect(grouped({right:{assembly_ref:{},assembly_members:[{},{}]}})).toBe(true);
    expect(grouped({right:{assembly_ref:{}}})).toBe(true); // historical assembly without member metadata
    expect(grouped({right:{assembly_ref:{},assembly_members:[{}]}})).toBe(false);
    // Сборка слева (N → 1) тоже убирает карту листов: номера листов в ней повторяются.
    expect(grouped({left:{assembly_ref:{},assembly_members:[{},{}]}, right:{}})).toBe(true);
    expect(html).toContain('v-if="!scGroupedComparison && scMatchState && scMatchState.suggestions" class="sc-sheet-map"');
    expect(html).toContain('v-else-if="!scGroupedComparison" class="sc-empty-state"');
    const sheetMap = html.slice(html.indexOf('class="sc-sheet-map"'),html.indexOf('Предложения соответствия ещё не построены.'));
    expect(sheetMap).toContain('<sbm-map-footer');
    expect(app).toContain('if (!bridge && scGroupedComparison.value && scBlockStore.value)');
  });

  it('открывает доску пар, когда загружена только одна сторона', () => {
    expect(app).toContain('(!left.pdf_count && !right.pdf_count)');
    expect(app).not.toContain('!left.pdf_count || !right.pdf_count');
  });

  it('ведёт пунктир от середин элементов слева к серединам элементов справа', () => {
    const paths = extractFunction('scFanCounts', 'scFanItems', 'scFanPaths');
    const row = (left, right) => ({left: {}, right: {}, leftMembers: Array(left > 1 ? left : 0).fill({}),
      members: Array(right > 1 ? right : 0).fill({})});
    expect(paths(row(1, 1))).toEqual(['M0 10 C 22 10, 18 10, 40 10']);
    // 1 → N: веер от середины левого документа.
    expect(paths(row(1, 3))).toEqual(['M0 30 C 22 30, 18 10, 40 10', 'M0 30 C 22 30, 18 30, 40 30',
      'M0 30 C 22 30, 18 50, 40 50']);
    // N → 1: зеркальный веер к середине правого документа.
    expect(paths(row(3, 1))).toEqual(['M0 10 C 22 10, 18 30, 40 30', 'M0 30 C 22 30, 18 30, 40 30',
      'M0 50 C 22 50, 18 30, 40 30']);
    // N ↔ M: линии сходятся к середине и расходятся от неё.
    expect(paths(row(2, 4))).toEqual(['M0 20 C 11 20, 9 40, 20 40', 'M0 60 C 11 60, 9 40, 20 40',
      'M20 40 C 31 40, 29 10, 40 10', 'M20 40 C 31 40, 29 30, 40 30', 'M20 40 C 31 40, 29 50, 40 50',
      'M20 40 C 31 40, 29 70, 40 70']);
    expect(paths({left: {}, right: null, leftMembers: [], members: []})).toEqual([]);
    // В шаблоне внутри HTML браузер приводит «:viewBox» к нижнему регистру — только объектный v-bind.
    expect(html).toContain('v-bind="{viewBox: \'0 0 40 \' + scFanItems(row) * 20}" preserveAspectRatio="none"');
    const fan = html.slice(html.indexOf('class="sc-pair-fan-links"'), html.indexOf('class="sc-pair-fan"'));
    expect(fan).not.toContain(':viewBox=');
    expect(css).toContain('stroke-dasharray: 4 3;');
    expect(css).toContain('.sc-pair-fan { display: grid; grid-auto-rows: 1fr;');
  });

  it('даёт у строки кнопки «+ Слева» и «+ Справа» для загрузки нескольких проектов', () => {
    expect(html).toContain('@click="scOpenRowUpload(row)"');
    expect(html).toContain('@drop.prevent.stop="scDropOnRowPlus(row)"');
    expect(html).toContain('@click="scOpenRowUpload(row, \'left\')"');
    expect(html).toContain('@drop.prevent.stop="scDropOnRowPlus(row, \'left\')"');
    expect(app).toContain("scStageFolderDialogStage.value = side === 'left' ? 'stage_1' : 'stage_2';");
    expect(app).toContain("scStageFolderDialogStage.value = 'stage_2';");
    expect(html).toContain(':disabled="scStageUploadIsBusy || !!scStageUploadTarget"');
    expect(app).toContain('batch.groupError = await scAssignDocuments(\n                            batch.target.side, batch.target.rowCode, uploadedCodes, batch.target.groupId);');
    // Цель загрузки работает для своей стадии: «+ Слева» — stage_1.
    const stage = extractFunction('scUploadTargetStage');
    expect(stage({side: 'left'})).toBe('stage_1');
    expect(stage({side: 'right'})).toBe('stage_2');
    expect(stage({afterLeftCode: 'IOS'})).toBe('stage_2');
    expect(stage(null)).toBe('');
  });

  it('склеивает проекты стороны группы в сборку через API групп', () => {
    expect(app).toContain('/document-groups${suffix}');
    expect(app).toContain("body: JSON.stringify({side, [other + '_document_code']: rowCode, member_codes: all, group_id: groupId,\n                        build: false}),");
    expect(app).toContain("{method: 'DELETE'}");
    expect(app).toContain('?side=${side}&build=false`');
    expect(app).toContain('/rebuild`, {method: \'POST\'}');
  });

  it('скрывает участников и сборки групп из свободной раскладки обеих сторон', () => {
    expect(app).toContain('const scBoardDocumentsRight = computed(');
    expect(app).toContain('const scBoardDocumentsLeft = computed(');
    expect(app).toContain('scReconcileDocumentRows(saved.left, saved.right,');
    expect(app).toContain('scBoardDocumentsLeft.value, scBoardDocumentsRight.value)');
    expect(app).toContain('function scReleaseGroupRowSlots()');
  });

  it('показывает состояние сборок по сторонам без подписи состава', () => {
    const status = extractFunction('scGroupSideStatus', 'scGroupStatus');
    const side = (extra = {}) => ({assembled: true, status: 'READY', stale: false,
      attached_version_id: 'v1', assembly_version_id: 'v1', ...extra});
    expect(status({left: {assembled: false}, right: side({status: 'RUNNING'})})).toEqual({tone: 'running', label: 'Собирается сборка…'});
    expect(status({left: side({stale: true}), right: {assembled: false}})).toEqual({tone: 'error', label: 'Нужна пересборка слева'});
    expect(status({left: side({status: 'RUNNING'}), right: side({status: 'FAILED'})})).toEqual({tone: 'error', label: 'Сборка не удалась справа'});
    expect(status({left: side(), right: side()})).toBe(null);
    // Состав набран переносами без сборки: «Нужна сборка» и кнопка «Собрать», а не ошибка.
    expect(status({left: side({status: 'NOT_BUILT'}), right: {assembled: false}}))
      .toEqual({tone: 'incomplete', label: 'Нужна сборка слева'});
    const label = extractFunction('scGroupBuildLabel');
    expect(label({left: side({status: 'NOT_BUILT'}), right: {assembled: false}})).toBe('Собрать');
    expect(label({left: side({stale: true}), right: {assembled: false}})).toBe('Пересобрать');
    expect(html).toContain("@click=\"scRebuildDocumentGroup(row)\">{{ scGroupBuildLabel(row.group) }}</button>");
    expect(app).toContain('?side=${side}&build=false');
    // Подпись «Слева сборка из N проектов · M листов» убрана по просьбе инженера (18.11.2026).
    expect(html).not.toContain('scGroupNote');
    expect(app).not.toContain('function scGroupNote');
  });

  it('обновляет сессию, когда готовая сборка прикреплена справа', () => {
    expect(app).toContain('if (reattached) await scRefreshSession();');
    expect(app).toContain('scDocumentGroupsTimer = setTimeout(scPollDocumentGroups, 4000);');
  });

  it('собирает сборки только группами: ручной вкладки «Сборки» нет, признак флага остался', () => {
    expect(html).not.toContain("scTab==='assemblies'");
    expect(html).not.toContain('<project-assemblies');
    expect(html).not.toContain('project-assemblies.js');
    expect(app).not.toContain('ProjectAssemblies.register');
    // Группы читают флаг сборок из этого признака страницы.
    expect(html).toContain('<script type="application/json" id="project-assemblies-caps">{{assembly_caps}}</script>');
    expect(app).toContain("document.getElementById('project-assemblies-caps')");
  });
});

describe('фоновые загрузки проектов', () => {
  it('сворачивает окно вместо отмены и показывает пакеты внизу', () => {
    expect(html).toContain('@click="scMinimizeStageFolderDialog()">Свернуть</button>');
    expect(html).toContain('class="sc-upload-tray"');
    expect(html).toContain('@click="scShowUploadBatch(batch)"');
    expect(app).toContain('function scCloseStageFolderDialog() {\n            if (scStageUploadIsBusy.value) {\n                scMinimizeStageFolderDialog();');
  });

  it('ставит новую загрузку в очередь, а не блокирует кнопки', () => {
    expect(html).toContain(':disabled="pcDemo || pcBridgeActive || !currentObjectId"\n                        @click="scOpenStageFolderDialog()"');
    expect(app).toContain("while ((batch = scUploadBatches.value.find(item => item.status === 'queued')))");
    // Пачка грузит в объект и версию сравнения, выбранные на её старте.
    expect(app).toContain('batch.objectId, batch.comparisonVersion, batch.stageName, candidate, retainBackup');
  });

  it('сохраняет ошибку сборки группы в пакете и предупреждает при закрытии вкладки', () => {
    expect(app).toContain('batch.groupError = await scAssignDocuments(');
    expect(app).toContain("if (!batch.error) setTimeout(() => { batch.dismissed = true; }, 8000);");
    expect(app).toContain("window.addEventListener('beforeunload'");
  });
});

describe('выбор модели ProjectChange V3 при запуске', () => {
  it('показывает выбор модели в окне запуска, только когда установка предлагает больше одной', () => {
    expect(html).toContain('<fieldset v-if="scV3ModelOptions.length > 1" class="sc-v3-model-choice">');
    expect(html).toContain('@change="scSelectV3Model(option.code)"');
  });

  it('передаёт модель в запрос запуска и берёт список у установки', () => {
    expect(app).toContain('if (scV3ModelOptions.value.length > 1 && scV3Model.value) body.model_profile = scV3Model.value;');
    expect(app).toContain("const models = payload.models && Array.isArray(payload.models.items) ? payload.models.items : [];");
  });
});
