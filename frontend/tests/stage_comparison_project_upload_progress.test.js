import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

describe('stage project upload progress', () => {
  it('opens one upload dialog and chooses the stage inside it', () => {
    expect(html).toContain('@click="scOpenStageFolderDialog()"');
    expect(html).toContain('id="sc-stage-upload-stage"');
    expect(html).toContain('v-model="scStageFolderDialogStage"');
    expect(html).toContain('<option value="stage_1">stage_1</option>');
    expect(html).toContain('<option value="stage_2">stage_2</option>');
    expect(html).not.toContain('stage_1 · Стадия П');
    expect(html).not.toContain('stage_2 · Стадия РД');
    expect(html).toContain('@change="scUploadStageFolder($event)"');
    expect(html).not.toContain('class="sc-upload-grid"');
    expect(html).not.toContain("scUploadStageFolder(stageName, $event)");
    expect(app).toContain('function scOpenStageFolderDialog()');
  });

  it('uploads projects separately and exposes per-project progress', () => {
    expect(html).toContain('Каждый проект загружается отдельно');
    expect(html).toContain("v-for=\"candidate in scStageFolderCandidates\"");
    expect(html).toContain('scStageCandidateStatusText(candidate)');
    expect(app).toContain('new XMLHttpRequest()');
    expect(app).toContain('request.upload.onprogress');
    expect(app).toContain("candidate.status = 'processing'");
    expect(app).toContain("form.append('retain_backup'");
    expect(app).toContain('request.status === 413');
    expect(app).toContain('scSubmitSelectedStageProjects');
  });

  it('closes the dialog automatically after a fully successful batch', () => {
    expect(app).toContain('closeAfterSuccess = failed === 0');
    expect(app).toContain('if (closeAfterSuccess) scCloseStageFolderDialog();');
  });
});
