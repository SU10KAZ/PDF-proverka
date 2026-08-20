import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

describe('stage project upload progress', () => {
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
});
