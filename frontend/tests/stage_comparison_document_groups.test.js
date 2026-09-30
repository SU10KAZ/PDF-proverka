import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

function extractFunction(name) {
  const start = app.indexOf(`function ${name}(`);
  let depth = 0;
  for (let i = app.indexOf('{', start); i < app.length; i += 1) {
    if (app[i] === '{') depth += 1;
    if (app[i] === '}') depth -= 1;
    if (depth === 0) return new Function(`${app.slice(start, i + 1)}; return ${name};`)();
  }
  throw new Error(`function ${name} not found`);
}

describe('группы «один слева → несколько справа»', () => {
  it('открывает доску пар, когда загружена только одна сторона', () => {
    expect(app).toContain('(!left.pdf_count && !right.pdf_count)');
    expect(app).not.toContain('!left.pdf_count || !right.pdf_count');
  });

  it('ведёт пунктир от середины левого документа к середине каждого элемента веера', () => {
    const scFanPath = extractFunction('scFanPath');
    expect(scFanPath(1, 0)).toBe('M0 10 C 22 10, 18 10, 40 10');
    expect(scFanPath(3, 0)).toBe('M0 30 C 22 30, 18 10, 40 10');
    expect(scFanPath(3, 2)).toBe('M0 30 C 22 30, 18 50, 40 50');
    // В шаблоне внутри HTML браузер приводит «:viewBox» к нижнему регистру — только объектный v-bind.
    expect(html).toContain('v-bind="{viewBox: \'0 0 40 \' + scFanItems(row) * 20}" preserveAspectRatio="none"');
    const fan = html.slice(html.indexOf('class="sc-pair-fan-links"'), html.indexOf('class="sc-pair-fan"'));
    expect(fan).not.toContain(':viewBox=');
    expect(css).toContain('stroke-dasharray: 4 3;');
    expect(css).toContain('.sc-pair-fan { display: grid; grid-auto-rows: 1fr;');
  });

  it('даёт у строки кнопку «+» для загрузки нескольких проектов справа', () => {
    expect(html).toContain('@click="scOpenRowUpload(row)"');
    expect(html).toContain('@drop.prevent.stop="scDropOnRowPlus(row)"');
    expect(app).toContain("scStageFolderDialogStage.value = 'stage_2';");
    expect(html).toContain(':disabled="scStageUploadIsBusy || !!scStageUploadTarget"');
    expect(app).toContain('batch.groupError = await scAssignRightDocuments(batch.target.leftCode, uploadedCodes);');
  });

  it('склеивает правые проекты группы в сборку через API групп', () => {
    expect(app).toContain('/document-groups${suffix}');
    expect(app).toContain('body: JSON.stringify({left_document_code: leftCode, member_codes: all})');
    expect(app).toContain("{method: 'DELETE'}");
    expect(app).toContain('/rebuild`, {method: \'POST\'}');
  });

  it('скрывает участников и сборку группы из свободной раскладки', () => {
    expect(app).toContain('const scBoardDocumentsRight = computed(');
    expect(app).toContain('scReconcileDocumentOrder(saved.right, scBoardDocumentsRight.value)');
    expect(app).toContain('function scReleaseGroupRowSlots()');
  });

  it('обновляет сессию, когда готовая сборка прикреплена справа', () => {
    expect(app).toContain('if (reattached) await scRefreshSession();');
    expect(app).toContain('scDocumentGroupsTimer = setTimeout(scPollDocumentGroups, 4000);');
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
    expect(html).toContain(':disabled="pcReadOnlySources || !currentObjectId"\n                        @click="scOpenStageFolderDialog()"');
    expect(app).toContain("while ((batch = scUploadBatches.value.find(item => item.status === 'queued')))");
    expect(app).toContain('batch.objectId, batch.stageName, candidate, retainBackup');
  });

  it('сохраняет ошибку сборки группы в пакете и предупреждает при закрытии вкладки', () => {
    expect(app).toContain('batch.groupError = await scAssignRightDocuments(');
    expect(app).toContain("if (!batch.error) setTimeout(() => { batch.dismissed = true; }, 8000);");
    expect(app).toContain("window.addEventListener('beforeunload'");
  });
});
