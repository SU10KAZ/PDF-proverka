import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');

describe('paid API monthly budget', () => {
  it('uses the calendar-month amount in the header and cost panel', () => {
    expect(html).toContain('formatCostShort(paidCost.monthly_spent_usd)');
    expect(html).toContain('За {{ formatPaidMonth(paidCost.month_key) }}:');
    expect(html).toContain('Новый месячный счётчик начинается автоматически 1-го числа.');
    expect(appJs).toContain('monthly_limit_usd: 250');
    expect(appJs).toContain('function formatPaidMonth(monthKey)');
  });

  it('renders the $250 progress bar, remaining budget and over-limit state', () => {
    expect(html).toContain('class="paid-monthly-budget__track"');
    expect(html).toContain("'paid-monthly-budget__fill--warning'");
    expect(html).toContain("'paid-monthly-budget__fill--danger'");
    expect(html).toContain('paidCost.monthly_remaining_usd');
    expect(html).toContain('paidCost.monthly_over_limit_usd');
    expect(css).toContain('.paid-monthly-budget__fill--warning');
    expect(css).toContain('.paid-monthly-budget__fill--danger');
  });

  it('shows when the monthly value was reconciled with OpenRouter', () => {
    expect(html).toContain('paidCost.monthly_calibrated_to_usd != null');
    expect(html).toContain('Сверено с OpenRouter:');
    expect(html).toContain('formatSignedCost(paidCost.monthly_adjustment_usd)');
  });
});
