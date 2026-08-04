import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');

describe('панель «Текст»: полное профильное Markdown-описание блока', () => {
  it('панель txt рендерит profiled_graph_markdown_full как форматированный документ', () => {
    expect(html).toContain('blockLlmText.profiled_graph_markdown_full');
    expect(html).toContain('v-html="blockProfiledMarkdownHtml"');
    expect(html).toContain('class="block-profile-md"');
  });

  it('полный Markdown не выводится сырым текстом и не ограничен summary', () => {
    // главное содержимое — v-html-рендер, а не <pre>{{ … }}</pre>
    expect(html).not.toContain('<pre>{{ blockLlmText.profiled_graph_markdown_full }}</pre>');
    expect(html).not.toMatch(/profiled_graph_markdown_full[^\n]*slice\(/);
    expect(appJs).not.toMatch(/profiled_graph_markdown_full[^\n]*\.slice\(/);
  });

  it('partial показывает спокойное уведомление', () => {
    expect(html).toContain("blockLlmText.profile_shadow.status === 'partial'");
    expect(html).toContain('Описание сформировано частично. Неразрешённые элементы перечислены в конце документа.');
  });

  it('no_graph показывает сообщение о неприменимости профиля', () => {
    expect(html).toContain("blockLlmText.profile_shadow.status === 'no_graph'");
    expect(html).toContain('Для данного блока профиль «План потолков и освещения» неприменим.');
  });

  it('отсутствующий артефакт сохраняет прежние ветки панели', () => {
    // при null-профиле работает прежний рендер: raw user_text и singleline
    expect(html).toContain('blockLlmText && blockLlmText.user_text');
    expect(html).toContain('class="block-llm-text__body"');
    expect(html).toContain('singleline_graph_markdown');
    // существующая pill-кнопка «txt» не заменена новой кнопкой
    expect(html).toContain('tile-status-pill tile-status-pill--txt');
  });

  it('markdown санитизируется: escape до parse + вырезание script/on*/javascript:', () => {
    expect(appJs).toContain('function renderMarkdownSafe(');
    const fn = appJs.slice(appJs.indexOf('function renderMarkdownSafe('));
    expect(fn).toContain("replace(/</g, '&lt;')");
    expect(fn).toMatch(/script\|iframe/);
    expect(fn).toMatch(/on\\w\+/);
    expect(fn).toMatch(/javascript:/);
  });

  it('загрузка ленивая, повторное открытие использует кэш', () => {
    expect(appJs).toContain('const blockLlmTextCache = new Map()');
    expect(appJs).toContain('blockLlmTextCache.has(cacheKey)');
    expect(appJs).toContain('blockLlmTextCache.set(cacheKey, payload)');
    // индикатор загрузки существует
    expect(html).toContain('blockLlmTextLoading');
    expect(html).toContain('Загрузка…');
  });

  it('переключение блоков не показывает текст предыдущего блока', () => {
    expect(appJs).toContain('blockLlmText.value = null; // не показывать текст предыдущего блока');
    expect(appJs).toMatch(/String\(blockLlmText\.value\.block_id\) !== String\(cur && cur\.block_id\)/);
  });

  it('прокрутка живёт внутри правой панели, стили документа заданы', () => {
    expect(css).toMatch(/\.block-llm-text \{[^}]*overflow: auto/);
    expect(css).toContain('.block-profile-md h2');
    expect(css).toContain('.block-profile-md table');
    expect(css).toContain('.block-llm-text__notice');
  });

  it('вкладка «Источник» (просмотр блока) не изменена', () => {
    // рендер изображения блока остаётся прежним
    expect(html).toContain('blockImageSrc');
    expect(appJs).toContain('blockImageSrc');
  });
});
