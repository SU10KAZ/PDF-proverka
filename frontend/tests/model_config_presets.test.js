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
  it('renders the three presets in order', () => {
    const claudeGpt = html.indexOf('>Claude+GPT</button>');
    const plusCodex = html.indexOf('>+Codex</button>');
    const fullCodex = html.indexOf('>Full Codex</button>');
    expect(claudeGpt).toBeGreaterThan(-1);
    expect(plusCodex).toBeGreaterThan(claudeGpt);
    expect(fullCodex).toBeGreaterThan(plusCodex);
  });

  it('maps +Codex to both supported ensembles', () => {
    const preset = appJs.match(/plus_codex:\s*\{[\s\S]*?\n\s*codex_exec:/)?.[0] || '';
    expect(preset).toContain('block_batch:            BLOCK_CODEX_ENSEMBLE_MODEL');
    expect(preset).toContain('optimization:           OPT_CODEX_ENSEMBLE_MODEL');
  });

  it('shows one additive Codex column', () => {
    expect(html).toContain('<th>Codex</th>');
    expect(html).toContain('v-for="m in visibleStageModels"');
    expect(html).toContain(':checked="isCodexStageChecked(key)"');
    expect(html).toContain('@change="toggleStageCodex(key, $event)"');
    expect(appJs).toContain("model?.provider !== 'codex_cli'");
    expect(appJs).toContain("model?.provider !== 'ensemble'");
    expect(appJs).toContain("model?.provider !== 'optimization_ensemble'");
  });

  it('uses the compact desktop matrix with a mobile fallback', () => {
    expect(html).toContain('class="model-config-heading"');
    expect(css).toContain('.model-config-modal { width: min(720px, calc(100vw - 24px));');
    expect(css).toContain('.model-config-table { width: 100%; min-width: 590px; table-layout: fixed;');
    expect(css).toContain('.model-config-stage-name { height: 22px;');
    expect(css).toContain('@media (max-width: 640px)');
  });
});
