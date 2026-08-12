import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const APP_JS = fs.readFileSync(path.join(__dirname, '../static/js/app.js'), 'utf8');
const INDEX_HTML = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');

describe('Сравнение стадий — объект только из верхней панели', () => {
  it('не содержит локального selector и удалённых кнопок', () => {
    expect(INDEX_HTML).not.toContain('v-model="scSelectedObjectId"');
    expect(INDEX_HTML).not.toContain('@click="scOpenProject()"');
    expect(INDEX_HTML).not.toContain('@click="scSaveSessionAsCanonical()"');
    expect(INDEX_HTML).not.toContain('@click="scAutoMatchOpenDialog()"');
  });

  it('показывает выбор целой папки и окно выбора отдельных проектов', () => {
    expect(INDEX_HTML).toContain("scUploadStageFolder('stage_1', $event)");
    expect(INDEX_HTML).toContain("scUploadStageFolder('stage_2', $event)");
    expect(INDEX_HTML).toContain('id="sc-stage-folder-stage_1" type="file" webkitdirectory directory multiple');
    expect(INDEX_HTML).toContain('id="sc-stage-folder-stage_2" type="file" webkitdirectory directory multiple');
    expect(INDEX_HTML).toContain('scStageFolderCandidates');
    expect(INDEX_HTML).toContain("'Загрузить выбранные (' + scStageFolderSelectedCount + ')'");
    expect(INDEX_HTML).not.toContain('sc-stage-upload-stage_1');
    expect(INDEX_HTML).not.toContain('sc-stage-upload-stage_2');
    expect(INDEX_HTML).not.toContain('>ZIP<');
    expect(INDEX_HTML).toContain("{{ objectName || 'Выберите объект в верхней панели' }}");
  });

  it('не оставляет под кнопками статусы загрузок и поясняющую плашку', () => {
    expect(INDEX_HTML).not.toContain('Выберите родительскую папку');
    expect(INDEX_HTML).not.toContain('upload-msg-');
    expect(APP_JS).not.toContain('scStageUploadMessage');
  });

  it('не показывает плашку обычного режима связей', () => {
    expect(INDEX_HTML).not.toContain('Режим: со связями блоков');
    expect(INDEX_HTML).not.toContain('Связанные блоки используются как ориентиры');
  });

  it('оставляет MD enrichment и удаляет старые инструменты связи', () => {
    expect(INDEX_HTML).toContain('MD enrichment');
    for (const label of [
      'Блоки без связей',
      'Сопоставить листы',
      'Сопоставить и применить',
      'ИИ-доматчинг',
      'Pipeline V2 связи',
      'Pipeline V2 сущности',
      'Авто-связь по IoU',
      'Связать блоки',
      'Сохранить конфигурацию пары',
    ]) {
      expect(INDEX_HTML).not.toContain(label);
    }
  });
});

describe('app.js — загрузка стадии', () => {
  it('синхронизируется с currentObjectId и вызывает новый endpoint', () => {
    expect(APP_JS).toContain('watch([currentObjectId, objectName, scObjects]');
    expect(APP_JS).toContain('/api/stage-comparison/objects/${encodeURIComponent(currentObjectId.value)}/stages/${stage}/${endpoint}');
    expect(APP_JS).toContain("form.append('relative_paths', JSON.stringify(selectedItems.map(item => item.full)))");
    expect(APP_JS).toContain("_scSubmitStageUpload(stage, input, form, 'upload-folder')");
    expect(APP_JS).toContain('scBuildStageFolderCandidates');
    expect(APP_JS).toContain('scSubmitSelectedStageProjects');
    expect(APP_JS).not.toContain('scUploadStageArchive');
    expect(APP_JS).not.toContain('scChooseStageArchive');
  });

  it('после двух готовых стадий пересоздаёт сессию', () => {
    expect(APP_JS).toContain('if (data.ready_for_comparison)');
    expect(APP_JS).toContain('await scScanFolders()');
  });

  it('не восстанавливает старую сессию после удаления исходных PDF', () => {
    expect(APP_JS).toContain('function scSelectedStagesHaveSources');
    expect(APP_JS).toContain('function scClearStaleSessionWhenSourcesMissing');
    expect(APP_JS).toContain('if (scClearStaleSessionWhenSourcesMissing()) return;');
    expect(APP_JS).toContain('scClearStaleSessionWhenSourcesMissing(obj);');
  });

  it('не открывает глобальную canonical session при входе в раздел', () => {
    const routeStart = APP_JS.indexOf("} else if (hash === '/stage-comparison')");
    const routeEnd = APP_JS.indexOf("} else if (hash === '/')", routeStart);
    const routeBlock = APP_JS.slice(routeStart, routeEnd);
    expect(routeBlock).not.toContain('scTryOpenCanonical');
    expect(routeBlock).not.toContain('scLoadCanonicalConfig');
  });

  it('не содержит обработчиков удалённых инструментов', () => {
    for (const handler of [
      'scToggleAnalysisMode',
      'scSuggestByStamp',
      'scAutoMatchApplySheets',
      'scPv2LpToggle',
      'scPv2EaToggle',
      'scRunAutoLink',
      'scCreateLink',
      'scSavePairTemplate',
    ]) {
      expect(APP_JS).not.toContain(handler);
    }
  });
});
