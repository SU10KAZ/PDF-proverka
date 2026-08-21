import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

describe('continuous comparison viewer', () => {
  it('keeps programmatic page selection from becoming a scroll cascade', () => {
    expect(app).toContain('const scContinuousProgrammaticTarget = {left: null, right: null}');
    expect(app).toContain('function scConsumeContinuousProgrammaticScroll(side, pane)');
    expect(app).toContain('if (scConsumeContinuousProgrammaticScroll(side, pane)) return;');
    expect(app).toContain('if (previous === element) return;');
    expect(app).toContain('x: 0.5, y: 0.5, viewportX: 0.5, viewportY: 0.5');
  });

  it('synchronises the normalised sheet anchor between both panes', () => {
    expect(app).toContain('function scContinuousAnchorAt(side, clientX = null, clientY = null)');
    expect(app).toContain('function scContinuousCounterpart(side, page)');
    expect(app).toContain('function scSyncContinuousAnchor(side, anchor)');
    expect(app).toContain('scSyncContinuousAnchor(side, anchor);');
  });

  it('zooms beyond 400 percent around the cursor in both panes', () => {
    expect(html).toContain('@wheel="scOnContinuousWheel(side, $event)"');
    expect(app).toContain('function scContinuousZoomAt(side, multiplier, clientX, clientY)');
    expect(app).toMatch(/scContinuousZoom\.value = Math\.min\(\s*SC_ZOOM_MAX,/);
    expect(app).not.toContain('Math.min(4, Math.max(0.5, scContinuousZoom.value * multiplier))');
  });

  it('supports grab-to-pan and keeps enlarged raster pages sharp with tiles', () => {
    expect(html).toContain('@pointerdown="scOnContinuousPanStart(side, $event)"');
    expect(html).toContain('@pointermove="scOnContinuousPanMove($event)"');
    expect(html).toContain('class="sc-continuous-page__tile"');
    expect(app).toContain('pane.scrollLeft += scContinuousPanState.x - event.clientX;');
    expect(app).toContain('function scRefreshContinuousTiles(side)');
    expect(css).toContain('.sc-continuous-pane.is-panning { cursor: grabbing; }');
    expect(css).toContain('.sc-continuous-page__tile {');
  });

  it('keeps unmatched map rows aligned with same-size placeholder sheets', () => {
    expect(app).toContain('const scContinuousSlots = computed(() =>');
    expect(app).toContain('leftPage: leftPages[index] || null');
    expect(app).toContain('rightPage: rightPages[index] || null');
    expect(app).toContain('counterpartPage ? scContinuousDims[targetSide][counterpartPage] : null');
    expect(html).toContain("'is-placeholder': entry.placeholder");
    expect(html).toContain('Лист отсутствует в этой стадии');
    expect(css).toContain('.sc-continuous-page.is-placeholder {');
  });
});
