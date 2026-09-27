import {createRequire} from 'node:module';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {describe, expect, it, vi} from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const button = html.match(/<button\b[^>]*id="sc-sheet-map-launch"[^>]*>[\s\S]*?<\/button>/)[0];
const attr = name => button.match(new RegExp(`${name}="([^"]+)"`))[1];
const ref = value => ({value});

function savedMap() {
    return {
        suggestions: {
            left_sheet_index: [{pdf_page: 1}, {pdf_page: 2}],
            right_sheet_index: [{pdf_page: 11}, {pdf_page: 12}, {pdf_page: 13}],
            suggestions: [], review_count: 3,
        },
        // OLD 2 is explicitly unpaired; NEW 12/13 need no additional confirmation.
        links: {links: [{left_pages: [1], right_pages: [11], source: 'manual'}],
            unlinked_left_pages: [2], updated_at: '2026-09-27T10:00:00Z'},
    };
}

function harness(matching = savedMap()) {
    const ctx = {
        SC_PRODUCTION_REVIEW: review,
        computed: getter => ({get value() { return getter(); }}),
        scActivePair: ref({id: 'pair-1', left: {filename: 'OLD.pdf'}, right: {filename: 'NEW.pdf'}}),
        scMatchState: ref(matching),
        // Stale snapshot from before the user saved manual links.
        scPairData: ref({sheet_matching: {suggestions: matching.suggestions, links: {links: []}}}),
        scProductionInputMode: ref('PAGE'), scProductionAiMode: ref('FAST'),
        scProductionAiModeChangedByUser: ref(false), scTab: ref('links'),
        scComparisonLaunchModes: [{code: 'FAST'}, {code: 'STANDARD'}],
        scProductionAiModeOptions: ref([{code: 'FAST'}, {code: 'STANDARD'}]),
        scBlockStore: ref(null), pcReviewSheetCount: ref(3),
        scProcessingError: ref(''), scSessionError: ref(''),
        scComparisonLaunchPending: ref(null),
        scLoadProductionAiModes: vi.fn(), scLoadProductionReview: vi.fn(async () => {}),
        // No network or models: stop at the existing run boundary.
        scRunProductionComparison: vi.fn(async () => {}),
        scProcessPair: vi.fn(async () => { throw new Error('Saved map must not be rebuilt'); }),
    };
    for (const name of ['pcReadOnlySources', 'scLinkSaving', 'scProductionMutating',
        'scProductionRunActive', 'scComparisonLaunchBusy', 'scComparisonLaunchDialogOpen',
        'scComparisonLaunchAwaitingSheetMap']) ctx[name] = ref(false);
    vm.createContext(ctx);
    const gateStart = app.indexOf('const scSheetMapDecisionsComplete =');
    vm.runInContext(app.slice(gateStart, app.indexOf(';', gateStart) + 1)
        + '\nglobalThis.scSheetMapDecisionsComplete = scSheetMapDecisionsComplete;', ctx);
    vm.runInContext('let scComparisonLaunchTarget = null;\n' + app.slice(
        app.indexOf('function scComparisonLaunchModeAllowed(code)'),
        app.indexOf('async function scProcessCurrentSelection()'),
    ), ctx);
    const evaluate = name => {
        const bindings = Object.fromEntries(Object.entries(ctx)
            .map(([key, value]) => [key, value && 'value' in Object(value) ? value.value : value]));
        return vm.runInNewContext(attr(name), bindings);
    };
    return {ctx, visible: () => evaluate('v-if'), disabled: () => evaluate(':disabled'),
        click: () => vm.runInContext(attr('@click'), ctx)};
}

describe('Stage 2 launch from a saved sheet map', () => {
    it('offers launch after Process/manual save without a pending launch, HM or prelinks', async () => {
        const h = harness();
        expect(review.comparisonLaunchDecisionsComplete(h.ctx.scMatchState.value)).toBe(true);
        expect(h.visible()).toBe(true);
        expect(h.disabled()).toBe(false);
        h.click();
        expect(h.ctx.scComparisonLaunchDialogOpen.value).toBe(true);
        expect(h.ctx.scProductionInputMode.value).toBe('DOCUMENT');
        await h.ctx.scStartComparisonLaunch('STANDARD');
        expect(h.ctx.scProcessPair).not.toHaveBeenCalled();
        expect(h.ctx.scRunProductionComparison).toHaveBeenCalledOnce();
        expect(h.ctx.scProductionAiMode.value).toBe('STANDARD');
        expect(h.ctx.scComparisonLaunchPending.value).toBeNull();
        expect(h.ctx.scTab.value).toBe('diffs');
    });

    it('does not offer launch for an unsaved or incomplete map', () => {
        for (const links of [{links: []}, {...savedMap().links, unlinked_left_pages: []}]) {
            const h = harness({...savedMap(), links});
            expect(h.visible()).toBe(false);
            h.click();
            expect(h.ctx.scComparisonLaunchDialogOpen.value).toBe(false);
        }
    });

    it.each(['pcReadOnlySources', 'scLinkSaving', 'scProductionMutating',
        'scProductionRunActive', 'scComparisonLaunchBusy'])('blocks launch while %s', flag => {
        const h = harness();
        h.ctx[flag].value = true;
        expect(h.disabled()).toBe(true);
        h.click();
        expect(h.ctx.scComparisonLaunchDialogOpen.value).toBe(false);
    });

    it('retains the existing resume control for an already pending launch', () => {
        const h = harness();
        h.ctx.scComparisonLaunchPending.value = {pairId: 'pair-1'};
        h.ctx.scComparisonLaunchAwaitingSheetMap.value = true;
        expect(h.visible()).toBe(false);
        h.click();
        expect(h.ctx.scComparisonLaunchDialogOpen.value).toBe(false);
        expect(html).toContain('@click="scConfirmComparisonSheetMap()"');
    });

    it('routes the semantic workspace CTA to the current document pair', () => {
        expect(html).toContain('@launch="scOpenSheetMapAnalysis()"');
        expect(html).not.toContain('@launch="scOpenComparisonLaunchDialog(scCurrentSheetMapRow())"');
    });
});
