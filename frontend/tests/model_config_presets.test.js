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
  it('renders only the Claude-only and Codex-only presets', () => {
    // «Claude+GPT +Codex» и «Full Codex» удалены 24.10.2026 (docs/codex_only_mode.md).
    const claude = html.indexOf('>Только Claude</button>');
    const codex = html.indexOf('>Только Codex</button>');
    expect(claude).toBeGreaterThan(-1);
    expect(codex).toBeGreaterThan(claude);
    expect(html).not.toContain('>Full Codex</button>');
    expect(html).not.toContain('>Claude+GPT +Codex</button>');
    expect(appJs).not.toContain('claude_gpt_codex: {');
    expect(appJs).not.toContain('codex_exec: {');
  });

  it('maps Codex-only to its ensembles and Codex models', () => {
    const preset = appJs.match(/codex_only:\s*\{[\s\S]*?\n\s*\};/)?.[0] || '';
    expect(preset).toContain('label: "Только Codex"');
    expect(preset).toContain('block_batch:            BLOCK_CODEX_ONLY_MODEL');
    expect(preset).toContain('optimization:           OPT_CODEX_ONLY_MODEL');
    expect(preset).toContain('text_analysis:          "codex/gpt-6.1-sol"');
    expect(preset).not.toContain('claude-');
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

  it('shows the concrete Astra and Sol models under the Codex column', () => {
    expect(html).toContain('<th>Codex</th>');
    expect(appJs).toContain("id: 'codex/gpt-6-astra', label: 'Astra'");
    expect(appJs).not.toContain("id: 'codex/gpt-5.4', label: 'Codex'");
    expect(appJs).toContain("if (id === 'codex/gpt-6-astra') return 'Astra';");
    expect(appJs).toContain("if (id === 'codex/gpt-5.6-sol') return 'Sol';");
    expect(appJs).toContain('stageEnsembleDetails.value?.block_batch?.parallel_models');
    expect(appJs).toContain('stageEnsembleDetails.value?.optimization?.parallel_models');
    expect(html).toContain('{{ codexStageModelsLabel(key) }}');
    expect(css).toContain('.model-config-codex-choice');
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
    expect(css).toContain('.model-config-modal { width: min(1120px, calc(100vw - 24px));');
    expect(css).toContain('.model-config-table { width: 100%; min-width: 900px; table-layout: fixed;');
    expect(css).toContain('.model-config-stage-name { height: 22px;');
    expect(css).toContain('@media (max-width: 640px)');
  });

  it('shows the actual routing per stage from the server plan', () => {
    expect(html).toContain('class="model-config-routing-cell"');
    expect(html).toContain('routingStage(key).actions');
    expect(html).toContain('routingLegsBadge(key)');
    expect(appJs).toContain("apiPost('/audit/model/routing-preview'");
    expect(appJs).toContain('stageRouting.value = data.routing');
  });
});
