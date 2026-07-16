import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const appJs = fs.readFileSync(path.resolve(here, '../static/js/app.js'), 'utf8');

describe('изоляция выбранного объекта', () => {
    it('хранит выбор в sessionStorage, а localStorage использует только для миграции/fallback', () => {
        expect(appJs).toContain('sessionStorage.getItem(OBJECT_STORAGE_KEY)');
        expect(appJs).toContain('sessionStorage.setItem(OBJECT_STORAGE_KEY, id)');
        expect(appJs).toContain('localStorage.removeItem(OBJECT_STORAGE_KEY)');
    });

    it('добавляет X-Object-Id только к same-origin API-запросам', () => {
        expect(appJs).toContain("url.startsWith('/api/')");
        expect(appJs).toContain("h.set('X-Object-Id', oid)");
    });
});
