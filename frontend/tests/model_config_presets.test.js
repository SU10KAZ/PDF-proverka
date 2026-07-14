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
