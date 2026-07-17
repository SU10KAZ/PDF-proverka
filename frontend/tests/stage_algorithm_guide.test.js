import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');

describe('pipeline stage algorithm guide', () => {
  it('centers the CTX card label on three fixed lines', () => {
    expect(html).toContain('class="pipeline-stage pipeline-stage--ctx"');
    expect(html).toContain('class="stage-label stage-label--ctx"');
    expect(html).toContain('<span>Векторные</span>');
    expect(html).toContain('<span>графы</span>');
    expect(html).toContain('<span>блоков</span>');
    // Центрирование даёт базовый .pipeline-stage — отдельное правило для
    // модификатора дублировало бы его и было убрано. Проверяем гарантию
    // (карточка центрирована), а не исчезнувшую деталь реализации.
    expect(css).toMatch(/\.pipeline-stage \{[^}]*justify-content: center;/);
    expect(css).toContain('.stage-label--ctx { display: flex; flex-direction: column; align-items: center;');
  });

  it('hides the legacy block-context alias when the canonical status is present', () => {
    expect(appJs).toContain('const visiblePipelineSummary = computed(() => {');
    expect(appJs).toContain("const canonicalKey = String(row?.canonical_key || '');");
    expect(appJs).toContain('return !canonicalKey || !presentKeys.has(canonicalKey);');
    expect(html).toContain('v-if="visiblePipelineSummary.length > 0"');
    expect(html).toContain('v-for="s in visiblePipelineSummary"');
  });

  it('adds an isolated three-dot action to every visible pipeline stage', () => {
    const buttons = html.match(/class="stage-algorithm-button"/g) || [];
    expect(buttons).toHaveLength(10);
    expect(html).toContain('@click.stop="openStageAlgorithm(\'blocks_analysis\')"');
    expect(html).toContain('aria-label="Алгоритм анализа блоков"');
  });

  it('renders one accessible dialog and supports all close paths', () => {
    expect(html).toContain('role="dialog" aria-modal="true"');
    expect(html).toContain('@click.self="closeStageAlgorithm"');
    expect(html).toContain('@click="closeStageAlgorithm"');
    expect(appJs).toContain("ev.key === 'Escape'");
    expect(appJs).toContain("window.addEventListener('keydown', _stageAlgorithmKeydown)");
    expect(appJs).toContain("window.removeEventListener('keydown', _stageAlgorithmKeydown)");
  });

  it('documents dual Stage 01 comparison and gap search after independent detection', () => {
    expect(appJs).toContain("model.includes('ensemble/gpt-codex')");
    expect(appJs).toContain('GPT и Codex не видят ответы друг друга.');
    expect(appJs).toContain('Судья: ${judge} сравнивает результаты');
    expect(appJs).toContain('Совпадения · расширения · новые · спорные');
    expect(appJs).toContain('gap-search пропущенных проблем');
    expect(appJs).toContain('Замечания + бейджи GPT / Codex');
    expect(appJs).toContain("d.mode === 'gap_search'");
    expect(appJs).toContain('новое: найдено gap-search');
  });

  it('shows the real optimization ensemble and its judge', () => {
    expect(appJs).toContain("configuredModel.includes('ensemble/claude-codex-opt')");
    expect(appJs).toContain('Два независимых анализа запускаются параллельно.');
    expect(appJs).toContain('Судья C OPT Critic: ${judge}');
    expect(appJs).toContain('На этапе объединения модель не голосует');
    expect(html).toContain("'stage-algorithm-step--judge': step.tone === 'judge'");
    expect(css).toContain('.stage-algorithm-step--judge');
    expect(css).toContain('.stage-algorithm-badge--claude');
  });

  it('offers the combined production preset before Full Codex', () => {
    const combined = html.indexOf('>Claude+GPT +Codex</button>');
    const fullCodex = html.indexOf('>Full Codex</button>');
    expect(combined).toBeGreaterThan(-1);
    expect(fullCodex).toBeGreaterThan(combined);
    expect(html).not.toContain('>Claude+GPT</button>');
    expect(html).not.toContain('>+Codex</button>');
    expect(html).not.toContain("applyPreset('dual_detection')");
    expect(html).not.toContain("applyPreset('optimization_ensemble')");

    const combinedPreset = appJs.match(/claude_gpt_codex:\s*\{[\s\S]*?\n\s*codex_exec:/)?.[0] || '';
    expect(combinedPreset).toContain('label: "Claude+GPT +Codex"');
    expect(combinedPreset).toContain('block_batch:            BLOCK_CODEX_ENSEMBLE_MODEL');
    expect(combinedPreset).toContain('optimization:           OPT_CODEX_ENSEMBLE_MODEL');
  });

  it('shows one additive Codex checkbox column and hides internal model columns', () => {
    expect(html).toContain('<th>Codex</th>');
    expect(html).toContain('v-for="m in visibleStageModels"');
    expect(html).toContain('type="checkbox"');
    expect(html).toContain(':checked="isCodexStageChecked(key)"');
    expect(html).toContain('@change="toggleStageCodex(key, $event)"');
    expect(appJs).toContain("model?.provider !== 'codex_cli'");
    expect(appJs).toContain("model?.provider !== 'ensemble'");
    expect(appJs).toContain("model?.provider !== 'optimization_ensemble'");
  });

  it('keeps the model matrix compact on desktop and readable on mobile', () => {
    expect(html).toContain('class="model-config-heading"');
    expect(css).toContain('.model-config-modal { width: min(720px, calc(100vw - 24px));');
    expect(css).toContain('.model-config-table { width: 100%; min-width: 590px; table-layout: fixed;');
    expect(css).toContain('.model-config-stage-name { height: 22px;');
    expect(css).toContain('@media (max-width: 640px)');
  });

  it('keeps the guide compact and responsive', () => {
    expect(css).toContain('.stage-algorithm-modal { max-width: 440px;');
    expect(css).toContain('@media (max-width: 460px)');
    expect(css).toContain('.stage-algorithm-split { grid-template-columns: 1fr;');
  });
});
