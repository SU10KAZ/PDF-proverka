import {describe, expect, it} from 'vitest';
import B from '../static/js/stage-comparison-batch.js';

const row = (l, r, extra = {}) => ({left: {pdf_path: '/o/' + l, filename: l}, right: {pdf_path: '/n/' + r, filename: r}, ...extra});

describe('очередь анализа выбранных пар', () => {
    it('берёт только полные пары, без повторов, и подписывает сборки', () => {
        const group = row('ИОС2.1.pdf', 'assembly.pdf', {group: {}, members: [1, 2, 3]});
        const rows = [row('A.pdf', 'B.pdf'), {left: {pdf_path: '/o/x'}}, row('A.pdf', 'B.pdf'), group];
        expect(B.payload(rows)).toEqual([
            {left_pdf: '/o/A.pdf', right_pdf: '/n/B.pdf', label: 'A.pdf → B.pdf'},
            {left_pdf: '/o/ИОС2.1.pdf', right_pdf: '/n/assembly.pdf', label: 'ИОС2.1.pdf → сборка из 3'},
        ]);
    });

    it('показывает место в очереди и статус строки', () => {
        const batch = {status: 'RUNNING', items: [
            {left_pdf: '/o/A.pdf', right_pdf: '/n/B.pdf', status: 'RUNNING', label: 'A → B'},
            {left_pdf: '/o/C.pdf', right_pdf: '/n/D.pdf', status: 'QUEUED'},
            {left_pdf: '/o/E.pdf', right_pdf: '/n/F.pdf', status: 'FAILED', message: 'boom'}]};
        expect(B.badge(B.itemFor(batch, row('C.pdf', 'D.pdf')))).toMatchObject({label: 'В очереди · №2', tone: 'muted'});
        expect(B.badge(B.itemFor(batch, row('E.pdf', 'F.pdf')))).toMatchObject({tone: 'error', title: 'boom'});
        expect(B.itemFor(batch, row('X.pdf', 'Y.pdf'))).toBeNull();
        const s = B.summary(batch);
        expect(s.running).toBe(true);
        expect(s.text).toContain('1 из 3 готово');
        expect(s.text).toContain('сейчас: A → B');
        expect(s.text).toContain('с ошибкой: 1');
    });

    it('итог завершённой очереди', () => {
        const s = B.summary({status: 'DONE', items: [{status: 'COMPLETED'}, {status: 'REVIEW'}]});
        expect(s.running).toBe(false);
        expect(s.text).toBe('Очередь анализа выполнена: 2 из 2');
        expect(B.summary(null)).toBeNull();
    });
});
