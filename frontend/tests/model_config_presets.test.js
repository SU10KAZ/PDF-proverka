import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');

describe('compact model configuration presets', () => {
  it('renders the combined production preset before Full Codex', () => {
    const combined = html.indexOf('>Claude+GPT +Codex</button>');
    const fullCodex = html.indexOf('>Full Codex</button>');
    expect(combined).toBeGreaterThan(-1);
    expect(fullCodex).toBeGreaterThan(combined);
    expect(html).not.toContain('>Claude+GPT</button>');
    expect(html).not.toContain('>+Codex</button>');
  });

  it('maps Claude+GPT +Codex to both supported ensembles', () => {
    const preset = appJs.match(/claude_gpt_codex:\s*\{[\s\S]*?\n\s*codex_exec:/)?.[0] || '';
    expect(preset).toContain('label: "Claude+GPT +Codex"');
    expect(preset).toContain('block_batch:            BLOCK_CODEX_ENSEMBLE_MODEL');
    expect(preset).toContain('optimization:           OPT_CODEX_ENSEMBLE_MODEL');
  });

  it('runs blocks and optimization through their ensembles in Full Codex', () => {
    const preset = appJs.match(/codex_exec:\s*\{[\s\S]*?\n\s*\};/)?.[0] || '';
    expect(preset).toContain('label: "Full Codex"');
    // Блоки — ансамбль GPT+Codex (судья + gap-search), оптимизация — Claude+Codex:
    // обе модели находят РАЗНОЕ, поодиночке теряется часть предложений.
    expect(preset).toContain('block_batch:            BLOCK_CODEX_ENSEMBLE_MODEL');
    expect(preset).toContain('optimization:           OPT_CODEX_ENSEMBLE_MODEL');
    // Остальные этапы остаются чистым Codex.
    expect(preset).toContain('text_analysis:          CODEX_PRESET_MODEL');
    expect(preset).toContain('findings_merge:         CODEX_PRESET_MODEL');
    expect(preset).not.toContain('block_batch:            CODEX_PRESET_MODEL');
    expect(preset).not.toContain('optimization:           CODEX_PRESET_MODEL');
  });

  it('labels a config that matches no preset instead of showing nothing', () => {
    // Осиротевший конфиг (ручная раскладка или пресет поменяли после сохранения)
    // раньше открывал окно пустым: ни подсветки, ни подсказки.
    expect(appJs).toContain('const isCustomStageConfig = computed');
    expect(appJs).toContain("Своя раскладка — не совпадает ни с одним пресетом");
    // Метка только когда конфиг УЖЕ загружен — иначе мигала бы на старте.
    expect(appJs).toContain('Object.keys(stageModelConfig.value || {}).length > 0 && !activePreset.value');
    expect(appJs).toContain('isCustomStageConfig, applyPreset');
    expect(html).toContain("'model-preset-hint--custom': isCustomStageConfig");
    expect(css).toContain('.model-preset-hint--custom');
  });

  it('shows one additive Astra column backed by Codex', () => {
    expect(html).toContain('<th>{{ codexColumnLabel }}</th>');
    expect(appJs).toContain("?.label || 'Astra'");
    expect(appJs).toContain("id: 'codex/gpt-6-astra', label: 'Astra'");
    expect(appJs).not.toContain("id: 'codex/gpt-5.4', label: 'Codex'");
    expect(html).toContain('v-for="m in visibleStageModels"');
    expect(html).toContain(':checked="isCodexStageChecked(key)"');
    expect(html).toContain('@change="toggleStageCodex(key, $event)"');
    expect(appJs).toContain("model?.provider !== 'codex_cli'");
    expect(appJs).toContain("model?.provider !== 'ensemble'");
    expect(appJs).toContain("model?.provider !== 'optimization_ensemble'");
    expect(appJs).toContain("model?.id !== 'openai/gpt-5.4'");
  });

  it('uses the compact desktop matrix with a mobile fallback', () => {
    expect(html).toContain('class="model-config-heading"');
    expect(css).toContain('.model-config-modal { width: min(720px, calc(100vw - 24px));');
    expect(css).toContain('.model-config-table { width: 100%; min-width: 590px; table-layout: fixed;');
    expect(css).toContain('.model-config-stage-name { height: 22px;');
    expect(css).toContain('@media (max-width: 640px)');
  });
});
