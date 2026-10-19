(function (root) {
    'use strict';
    // Панель «Изменения в листе» в просмотрщике пары листов (docs/version_comparison_sheet_changes_plan.md §6).
    // Открытие панели и перелистывание читают только сохранённый результат (GET …/sheets/{spid}/changes):
    // модель не вызывается. Запуск описания листа — только кнопкой с подтверждением передачи.

    const TEMPLATE = `
<aside class="vcsc-panel" aria-label="Изменения в листе">
  <div class="vcsc-head">
    <div class="vcsc-title">Изменения в листе</div>
  </div>
  <div v-if="loading && !data" class="vc-muted vcsc-pad">Загрузка…</div>
  <div v-if="error" class="vc-error vcsc-pad">{{ error }}</div>
  <template v-if="data">
    <div class="vcsc-passport">
      <div class="vcsc-sheet">{{ data.title }}</div>
      <div v-if="data.source === 'whole_specification'" class="vc-muted">По полной спецификации · только изменения этого листа</div>
      <div class="vc-muted"><span class="vc-chip vc-chip-a">A</span> {{ data.a.label }} · стр. {{ data.a.page }}
        <span class="vcv-arrow">→</span> <span class="vc-chip vc-chip-b">B</span> {{ data.b.label }} · стр. {{ data.b.page }}</div>
      <div class="vcsc-state" :class="'s-' + data.state">● {{ data.state_label }}<template v-if="data.changes"> · изменений: {{ data.changes.length }}</template><template v-if="data.unresolved && data.unresolved.length"> · требует уточнения: {{ data.unresolved.length }}</template></div>
      <div v-if="data.diagnostics && data.diagnostics.executor && data.diagnostics.executor.executor !== 'codex_exec'" class="vcsc-warn">
        ТЕСТОВЫЙ ОТВЕТ подменного исполнителя ({{ data.diagnostics.executor.executor }}) — не описание модели.</div>
      <div v-else-if="data.diagnostics && data.diagnostics.executor" class="vc-muted">Описал: {{ data.diagnostics.executor.model }}, усилие {{ data.diagnostics.executor.effort }} · {{ (data.created_at || '').replace('T', ' ').slice(0, 16) }} UTC</div>
      <div v-if="data.stale" class="vcsc-warn">Описание устарело: {{ data.stale }}. Переходы к чертежу могут не совпадать.</div>
      <div v-if="data.review" class="vcsc-warn"><b>Проверить:</b> {{ data.review }}</div>
    </div>

    <div v-if="['queued', 'running'].includes(data.state)" class="vcsc-pad vc-muted">
      {{ data.state === 'running' ? 'Идёт описание листа' : 'Лист в очереди' }}<template v-if="data.job"> · задание {{ data.job.job_id }}</template>
      <div><button class="btn btn-outline btn-xs" :disabled="busy" title="Остановить описание этого листа" @click="cancel">Отменить задание</button></div>
    </div>

    <div v-if="data.state === 'not_processed'" class="vcsc-pad">
      <p><b>Не обработано.</b> Для этой пары листов описания ещё нет — это не означает, что изменений нет.</p>
      <p v-if="data.old_job" class="vc-muted">Есть описание прежним способом (по записям сравнения) — задание {{ data.old_job }}.</p>
      <template v-if="data.enabled">
        <button v-if="!estimate" class="btn btn-outline btn-sm" :disabled="busy" title="Подготовить данные листа и показать, что будет передано ИИ. На этом шаге ничего не отправляется" @click="loadEstimate">{{ busy ? 'Сборка пакета…' : 'Описать этот лист…' }}</button>
        <div v-if="estimate" class="vcsc-estimate">
          <div>Будет передано исполнителю: кандидатов {{ estimate.sheets[0].candidates }}, изображений {{ estimate.sheets[0].images }}
            ({{ mb(estimate.sheets[0].image_bytes) }}), текст {{ estimate.sheets[0].prompt_chars.toLocaleString('ru-RU') }} знаков.</div>
          <div class="vc-muted">Модель {{ estimate.executor.model }}, усилие {{ estimate.executor.effort }}<template v-if="estimate.executor.cli_version">, {{ estimate.executor.cli_version }}</template>. Обращений: 1, не больше 3 с дозапросом и повтором.</div>
          <div class="vc-muted">{{ estimate.transfer }}</div>
          <label><input type="checkbox" v-model="consent"> Подтверждаю передачу этого листа исполнителю</label>
          <div><button class="btn btn-primary btn-sm" :disabled="!consent || busy" title="Отправить этот лист исполнителю ИИ и начать описание изменений. Доступно после отметки согласия на передачу" @click="start">Передать и описать</button>
            <button class="btn btn-outline btn-sm" title="Ничего не отправлять и скрыть подготовленные данные" @click="estimate = null; consent = false">Отмена</button></div>
        </div>
      </template>
      <p v-else class="vc-muted">Запуск описаний на этой установке выключен.</p>
    </div>
    <div v-if="data.state === 'engine_same'" class="vcsc-pad vc-muted">По данным сравнения содержание листа не изменилось (только служебные зоны) — описание не требуется.</div>
    <div v-if="data.source === 'whole_specification' && data.message" class="vcsc-pad">{{ data.message }}</div>

    <ol v-if="data.changes && data.changes.length" class="vcsc-list">
      <li v-for="c in data.changes" :key="c.id" class="vcsc-item">
        <div v-for="(l, k) in leads(c)" :key="k" class="vcsc-lead"><span v-if="l.label" class="vcsc-lead-obj">{{ l.label }}: </span><template v-if="l.before">Было: <span class="was">{{ quote(l.before, l.unit) }}</span></template><template v-else>Не было</template> → <template v-if="l.after">Стало: <span class="now">{{ quote(l.after, l.unit) }}</span></template><template v-else>Не стало</template></div>
        <div><b>{{ c.object }}</b><template v-if="rest(c)"> — {{ rest(c) }}</template></div>
        <div v-if="data.source === 'whole_specification' && c.place" class="vcsc-note">{{ c.place }}</div>
        <div v-if="c.limitation" class="vcsc-note">Неясно: {{ c.limitation }}</div>
        <div v-if="data.source !== 'whole_specification'" class="vcsc-actions">
          <button class="btn btn-outline btn-xs" :disabled="!c.evidence_count" title="Приблизить место этого изменения на листах A и B. Недоступно, если у пункта нет привязки к чертежу" @click="show(c)">Показать на чертеже</button>
          <span v-if="c.confirmation === 'image'" class="vcsc-mini" title="Значение прочитано с изображения, в текстовом слое PDF его нет">с изображения</span>
          <span v-if="c.confirmation === 'unverified'" class="vcsc-mini warn" title="Значение не найдено ни в тексте своей стороны, ни на указанном изображении">не подтверждено</span>
        </div>
      </li>
    </ol>
    <details v-if="data.unresolved && data.unresolved.length" class="vcsc-unresolved">
      <summary class="vcsc-sub">Требует уточнения ({{ data.unresolved.length }})</summary>
      <div v-for="u in data.unresolved" :key="u.id" class="vcsc-item">
        <b>{{ u.object }}</b><span v-if="u.place" class="vc-muted"> ({{ u.place }})</span> — {{ u.question }}
        <div v-if="data.source !== 'whole_specification'" class="vcsc-actions"><button class="btn btn-outline btn-xs" :disabled="!u.evidence_count" title="Приблизить это место на листах A и B. Недоступно, если у пункта нет привязки к чертежу" @click="show(u)">Показать на чертеже</button></div>
      </div>
    </details>
    <div v-if="data.state === 'no_construction_changes' && data.source !== 'whole_specification'" class="vcsc-pad">Строительных изменений не выявлено. Что проверялось — в «Диагностике».</div>

    <div v-if="data.changes" class="vcsc-pad vcsc-buttons">
      <button class="btn btn-outline btn-xs" title="Скопировать список изменений листа в буфер обмена, чтобы вставить в письмо или документ" @click="copyText">{{ copied ? 'Скопировано' : 'Копировать текст' }}</button>
      <a class="btn btn-outline btn-xs" :href="mdUrl()" download title="Скачать описание изменений этого листа файлом MD">MD листа</a>
    </div>
    <details v-if="data.declared && data.declared.length" class="vcsc-more"><summary>Заявлено проектировщиком в разрешении ({{ data.declared.length }})</summary>
      <div v-for="(d, i) in data.declared" :key="i" class="vc-muted">{{ d.izm }}: {{ d.text }}</div></details>
    <details v-if="data.excluded && data.excluded.length" class="vcsc-more"><summary>Не включено в сводку</summary>
      <div v-for="(e, i) in data.excluded" :key="i" class="vc-muted">{{ e.label }} — {{ e.count }} ({{ e.by }})</div></details>
    <details v-if="data.diagnostics" class="vcsc-more"><summary>Диагностика</summary>
      <div class="vc-muted">Исполнитель: {{ data.diagnostics.executor.model }}, усилие {{ data.diagnostics.executor.effort }}<template v-if="data.diagnostics.executor.cli_version">, {{ data.diagnostics.executor.cli_version }}</template>.</div>
      <div class="vc-muted">Запусков: {{ data.diagnostics.launches }}<template v-if="data.diagnostics.cached"> (готовый ответ из кэша)</template>. Время: подготовка {{ data.diagnostics.prepare_s }} с, всего {{ data.diagnostics.total_s }} с.</div>
      <div v-for="c in data.diagnostics.calls" :key="c.launch" class="vc-muted">#{{ c.launch }} {{ c.purpose }}: {{ c.seconds }} с, {{ c.ok ? 'ответ принят' : (c.error_kind || 'ошибка') }}; вход {{ (c.usage || {}).input_tokens || 0 }}, кэш {{ (c.usage || {}).cached_input_tokens || 0 }}, выход {{ (c.usage || {}).output_tokens || 0 }}; изображений {{ c.images }}</div>
      <div v-if="data.diagnostics.coverage" class="vc-muted">Кандидатов {{ data.diagnostics.coverage.candidates }}: в изменениях {{ data.diagnostics.coverage.in_changes }}, требуют уточнения {{ data.diagnostics.coverage.in_unresolved }}, исключено моделью {{ data.diagnostics.coverage.excluded }}, не рассмотрено {{ data.diagnostics.coverage.not_reviewed.length }}<template v-if="data.diagnostics.coverage.not_reviewed.length"> ({{ data.diagnostics.coverage.not_reviewed.join(', ') }})</template>.</div>
      <div v-if="data.diagnostics.pointer_ledger_counts" class="vc-muted">Указатели сравнения: {{ ledgerText(data.diagnostics.pointer_ledger_counts) }}.</div>
      <div v-for="(i, k) in data.diagnostics.issues" :key="k" class="vc-muted">{{ i.code }}: {{ i.text }}</div>
      <div class="vc-muted"><a :href="packetUrl('prompt.md')" target="_blank" title="Открыть в новой вкладке текст задания, переданного ИИ по этому листу">текст пакета</a> · <a :href="packetUrl('manifest.json')" target="_blank" title="Открыть в новой вкладке служебный перечень того, что было передано ИИ по этому листу">манифест</a></div>
    </details>
    <details v-if="data.history && data.history.length > 1" class="vcsc-more"><summary>История ({{ data.history.length }})</summary>
      <div v-for="h in data.history" :key="h.job_id" class="vc-muted">{{ (h.created_at || '').replace('T', ' ').slice(0, 16) }} · {{ h.state }} · {{ (h.executor || {}).model }} {{ (h.executor || {}).effort }} · {{ h.job_id }}</div></details>
    <div v-if="data.disclaimer" class="vcsc-pad vc-muted">{{ data.disclaimer }}</div>
  </template>
</aside>`;

    const LEDGER = {candidate: 'в кандидатах', attached: 'внутри мест', linked: 'через связь', excluded: 'исключено с причиной', unassigned: 'не разобрано'};

    root.VersionComparisonSheetChanges = {
        TEMPLATE,
        register(app) {
            app.component('vc-sheet-changes', {
                template: TEMPLATE,
                props: {objectId: String, run: Object, spid: String},
                emits: ['state', 'focus'],
                setup(props, {emit}) {
                    const {ref, reactive, watch, onMounted, onBeforeUnmount} = root.Vue;
                    const VC = root.VersionComparison;
                    const data = ref(null), loading = ref(false), error = ref(''), busy = ref(false);
                    const estimate = ref(null), consent = ref(false), copied = ref(false);
                    const open = reactive({});
                    let timer = null, seq = 0;
                    const base = () => `/objects/${encodeURIComponent(props.objectId)}/runs/${props.run.run_id}`;

                    async function load() {
                        clearTimeout(timer);
                        const my = ++seq;
                        loading.value = true; error.value = '';
                        try {
                            const d = await VC.api(`${base()}/sheets/${props.spid}/changes`);
                            if (my !== seq) return;           // ответ для прежнего листа — не показывать
                            data.value = d;
                            emit('state', d.state, d.source || '');
                            if (['queued', 'running'].includes(d.state)) timer = setTimeout(load, 3000);
                        } catch (e) { if (my === seq) { error.value = e.message; emit('state', 'error'); } }
                        finally { if (my === seq) loading.value = false; }
                    }
                    async function loadEstimate() {
                        busy.value = true; error.value = '';
                        try {
                            estimate.value = await VC.api(`${base()}/ai/estimate`, {method: 'POST', body: {spids: [props.spid], method: 'sheet_changes'}});
                        } catch (e) { error.value = e.message; }
                        finally { busy.value = false; }
                    }
                    async function start() {
                        busy.value = true; error.value = '';
                        try {
                            await VC.api(`${base()}/ai/jobs`, {method: 'POST', body: {spids: [props.spid], consent: consent.value, method: 'sheet_changes'}});
                            estimate.value = null; consent.value = false;
                            await load();
                        } catch (e) { error.value = e.message; }
                        finally { busy.value = false; }
                    }
                    async function cancel() {
                        if (!data.value || !data.value.job) return;
                        busy.value = true;
                        try { await VC.api(`${base()}/ai/jobs/${data.value.job.job_id}/cancel`, {method: 'POST'}); await load(); }
                        catch (e) { error.value = e.message; }
                        finally { busy.value = false; }
                    }
                    async function show(item) {
                        error.value = '';
                        try {
                            const f = await VC.api(`${base()}/sheets/${props.spid}/changes/focus?item=${encodeURIComponent(item.id)}`);
                            if (!f.available) { error.value = f.reason || 'Место на чертеже не найдено.'; return; }
                            emit('focus', {spid: props.spid, ids: [item.id], label: item.object, boxes: f.boxes, union_px: f.union_px,
                                index: 0, all: false});
                        } catch (e) { error.value = e.message; }
                    }
                    function copyText() {
                        const d = data.value;
                        const lines = [`${d.title} (${d.a.label} → ${d.b.label})`];
                        (d.changes || []).forEach((c, i) => {
                            const r = rest(c);
                            lines.push(`${i + 1}. ${leads(c).map(l => leadText(l) + '. ').join('')}${c.object}${r ? ' — ' + r : ''}`);
                            if (d.source === 'whole_specification' && c.place) lines.push(c.place);
                            if (c.table) c.table.rows.forEach(r => lines.push('   - ' + r.object + ': ' + c.table.columns.filter(k => r.cells[k])
                                .map(k => `${k} ${r.cells[k].before || '—'} → ${r.cells[k].after || '—'} ${r.cells[k].unit}`.trim()).join('; ')));
                        });
                        (d.unresolved || []).forEach(u => lines.push(`Требует уточнения: ${u.object} — ${u.question}${u.place ? ' ' + u.place : ''}`));
                        if (d.message) lines.push(d.message);
                        const text = lines.join('\n');
                        const done = () => { copied.value = true; setTimeout(() => { copied.value = false; }, 1500); };
                        if (root.navigator && root.navigator.clipboard) root.navigator.clipboard.writeText(text).then(done, () => {});
                    }
                    // «Было → Стало» — в начало каждого пункта. Берётся марка/обозначение, иначе первое поле;
                    // без полей — по строкам таблицы пункта (своя строка на каждый объект). Удалённые строки
                    // таблицы (было — стало пусто) дают «было», если в поле пункта прежнего значения нет.
                    const MARK = /марк|обознач/i;
                    function leads(c) {
                        const fs = (c.fields || []).filter(x => x.before || x.after);
                        if (c.source === 'whole_specification') {
                            return fs.map(f => ({label: f.name, before: f.before, after: f.after, unit: f.unit}));
                        }
                        const t = c.table;
                        const removed = t ? t.rows.filter(r => { const v = Object.values(r.cells || {}); return v.length && v.every(x => x.before && !x.after); }) : [];
                        const f = fs.find(x => MARK.test(x.name)) || fs[0];
                        if (f) {
                            // У удалённой строки — её марка из названия объекта («Прежний люк Лк-3П» → «Лк-3П»).
                            const mark = o => ((o || '').match(/[A-Za-zА-Яа-яЁё]{1,4}-[0-9][0-9A-Za-zА-Яа-яЁё*\/.\-]*/g) || [o]).pop();
                            const before = f.before || removed.map(r => mark(r.object)).join('», «');
                            return [{label: '', before, after: f.after, unit: f.unit}];
                        }
                        if (!t || !t.columns.length) return [];
                        const col = t.columns.find(k => MARK.test(k)) || t.columns[0];
                        return t.rows.map(r => ({r, x: r.cells[col]})).filter(({x}) => x && (x.before || x.after))
                            .map(({r, x}) => ({label: x.before && (r.object || '').includes(x.before) ? '' : r.object,
                                before: x.before, after: x.after, unit: x.unit})).slice(0, 4);
                    }
                    const leadText = l => `${l.label ? l.label + ': ' : ''}${l.before ? 'Было: ' + quote(l.before, l.unit) : 'Не было'} → ${l.after ? 'Стало: ' + quote(l.after, l.unit) : 'Не стало'}`;
                    const quote = (v, unit) => v ? `«${v}${unit && !v.endsWith(unit) ? ' ' + unit : ''}»` : '—';
                    // Та же пара «было → стало» в конце текста модели после строки-начала — повтор, убираем.
                    function rest(c) {
                        let s = c.summary || '';
                        const ls = leads(c), l = ls.length === 1 ? ls[0] : null;
                        if (l && l.before && l.after) {
                            const q = `«${l.before}» → «${l.after}»`;
                            if (s.includes(q)) s = s.replace(q, '').replace(/\s*—\s*\.?\s*$/, '').trim();
                        }
                        return s;
                    }
                    const mb = b => `${((b || 0) / 1048576).toFixed(1)} МБ`;
                    const mdUrl = () => `${VC.API}${base()}/sheets/${props.spid}/changes.md`;
                    const packetUrl = n => `${VC.API}${base()}/sheets/${props.spid}/changes/packet/${n}`;
                    const ledgerText = c => Object.entries(c || {}).map(([k, v]) => `${LEDGER[k] || k} ${v}`).join(', ');
                    const toggle = k => { open[k] = !open[k]; };
                    const isOpen = k => !!open[k];
                    watch(() => [props.objectId, props.run.run_id, props.spid], () => {
                        data.value = null; emit('state', '', ''); estimate.value = null; consent.value = false; copied.value = false; load();
                    });
                    onMounted(load);
                    onBeforeUnmount(() => { ++seq; clearTimeout(timer); });
                    return {data, loading, error, busy, estimate, consent, copied, loadEstimate, start, cancel, show, copyText,
                        mb, mdUrl, packetUrl, ledgerText, toggle, isOpen, leads, quote, rest};
                },
            });
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
