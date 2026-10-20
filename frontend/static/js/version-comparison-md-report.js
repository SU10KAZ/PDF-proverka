(function (root) {
    'use strict';
    // «Отчёт PDF» по MD-сводке изменений (backend: version_comparison_ai/md_report).
    // Вход — текущая «Сводка изменений (MD)» серии или загруженный MD-файл. Запуск — только с явным
    // подтверждением передачи текста исполнителю; ход, причины ошибок и скачивание проверенного PDF.

    const ACTIVE = ['queued', 'running', 'cancelling', 'pausing'];
    const RESUMABLE = ['interrupted', 'cancelled', 'paused_quota', 'paused_auth', 'paused', 'incomplete', 'failed'];

    const TEMPLATE = `
<div class="vcai-overlay" @keydown.esc="$emit('close')" tabindex="0">
  <div class="vcai-window vcmr-window">
    <div class="vcai-head">
      <div>
        <div class="vcai-title">Отчёт для заказчика по «Сводке изменений (MD)»</div>
        <div class="vc-muted">{{ project.title || project.project_id }} — «Изменения проектных решений»: по каждой выдаче масштаб
          и главные изменения для закупки, монтажа и эксплуатации. Одно обращение к ИИ на переход, по фактам MD-сводки;
          исходные PDF не открываются.</div>
      </div>
      <div class="vcai-head-actions"><button class="btn btn-outline btn-sm" title="Закрыть окно" @click="$emit('close')">✕ Закрыть</button></div>
    </div>
    <div class="vcai-body">
      <div v-if="error" class="vc-error" @click="error = ''">{{ error }}</div>
      <div v-if="canOperate && rt && !rt.enabled" class="vc-warning">ИИ-обработка в разделе выключена (ai_settings.json).</div>
      <section v-if="canOperate" class="vcmr-start">
        <div class="vcmr-row">
          <label><input type="radio" value="series" v-model="mode" :disabled="!seriesReady"> текущая «Сводка изменений (MD)» серии</label>
          <label><input type="radio" value="file" v-model="mode"> загрузить MD-файл</label>
          <input v-if="mode === 'file'" type="file" accept=".md,text/markdown,text/plain" @change="pickFile">
        </div>
        <div v-if="mode === 'series' && !seriesReady" class="vc-muted">Сводки серии ещё нет — сначала «ИИ-описание изменений».</div>
        <div v-if="rt" class="vc-muted">Исполнитель: Codex, модель <b>{{ modelLabel }}</b>,
          усилие {{ rt.executor.effort }}<template v-if="rt.executor.cli_version">, CLI {{ rt.executor.cli_version }}</template>;
          одновременно до {{ rt.settings.client_parallel || 10 }} переходов; упавший переход отправляется повторно
          до {{ rt.settings.client_retries != null ? rt.settings.client_retries : 3 }} раз. {{ rt.transfer }}</div>
        <label class="vcmr-consent"><input type="checkbox" v-model="consent"> Подтверждаю передачу текста сводки исполнителю</label>
        <button class="btn btn-primary btn-sm" :disabled="!canStart || starting" title="Передать текст сводки исполнителю ИИ и начать составление отчёта PDF. Доступно после выбора источника и отметки согласия на передачу" @click="start">{{ starting ? 'Запуск…' : 'Сформировать отчёт для заказчика' }}</button>
      </section>
      <h4 class="vcmr-h">Отчёты проекта</h4>
      <div v-if="!reports.length" class="vc-muted">Отчётов ещё нет.</div>
      <table v-else class="vcmr-table">
        <thead><tr><th>Создан</th><th>Источник</th><th>Состояние</th><th>Ход</th><th></th></tr></thead>
        <tbody>
          <tr v-for="r in reports" :key="r.report_id">
            <td>{{ fmtTime(r.created_at) }}</td>
            <td><div>{{ r.source.filename }}</div>
              <div class="vc-muted">{{ r.origin && r.origin.kind === 'series' ? 'сводка серии' : 'загружен' }} · {{ kb(r.source.bytes) }} · строк: {{ r.source.lines }} · переходов: {{ r.source.transitions }} · SHA-256 {{ r.source.sha256.slice(0, 12) }}…</div></td>
            <td><span class="vc-run-state" :class="'st-' + r.state">{{ r.state_label }}</span>
              <div v-if="isActive(r)" class="vc-muted">{{ r.stage_label }}</div>
              <div v-if="r.reason" class="vc-error-inline">{{ r.reason }}</div>
              <div v-for="w in warnings(r)" :key="w" class="vc-warning">⚠ {{ w }}</div></td>
            <td><template v-if="r.kind === 'client' && r.progress && r.progress.client_total != null">
                <div>переходов: {{ r.progress.client_done || 0 }} из {{ r.progress.client_total }}</div>
                <div v-if="r.progress.failed" class="vc-error-inline">не подготовлено: {{ r.progress.failed }}</div>
                <div v-if="r.discipline && r.discipline.name" class="vc-muted" :title="r.discipline.by === 'title' ? 'Раздел определён по титульному листу' : 'Раздел определён по шифру'">раздел: {{ r.discipline.name }}</div>
                <div v-else-if="r.discipline" class="vc-muted">раздел не определён — отбор без приоритетов раздела</div>
                <div v-if="r.usage && r.usage.calls" class="vc-muted">обращений {{ r.usage.calls }}, вход {{ fmtNum(r.usage.input_tokens) }} ток.</div>
              </template>
              <template v-else-if="r.progress && r.progress.groups_total">
                <div>анализ: {{ r.progress.groups_done || 0 }} из {{ r.progress.groups_total }}</div>
                <div>проверка: {{ r.progress.units_done || 0 }} из {{ r.progress.units_total }}</div>
                <div v-if="r.usage && r.usage.calls" class="vc-muted">обращений {{ r.usage.calls }}, вход {{ fmtNum(r.usage.input_tokens) }} ток.</div>
                <div v-if="r.reuse && r.reuse.reused" class="vc-muted" title="Сколько частей отчёта взято готовыми из прошлых отчётов того же файла сводки, без нового обращения к ИИ">
                  взято готовым: {{ r.reuse.reused }} из {{ r.reuse.units }}<template v-if="r.reuse.from.length"> (отчёт {{ r.reuse.from.join(', ') }})</template></div>
              </template></td>
            <td class="vcmr-actions">
              <a v-if="canDownload && r.pdf_ready" class="btn btn-primary btn-xs" :href="pdfUrl(r)" download :title="r.kind === 'client' ? 'Скачать отчёт для заказчика в PDF' : 'Скачать аналитический отчёт в PDF'">Скачать PDF</a>
              <span v-if="r.kind !== 'client'" class="vc-muted">аналитический отчёт</span>
              <a v-if="canOperate && r.kind !== 'client' && r.client && r.client.ready" class="btn btn-outline btn-xs" :href="clientUrl(r)" download title="Отчёт для заказчика: одна хронологическая таблица главных изменений по выдачам, без внутренних ID, реестров и служебных подписей. Скачивается сохранённый файл — без обращения к ИИ.">PDF для заказчика</a>
              <button v-else-if="canOperate && r.kind !== 'client' && r.client && r.client.available && !clientBusy(r)" class="btn btn-outline btn-xs" title="Подготовить содержание для заказчика: отбор главных изменений по каждому переходу из полного набора фактов этого отчёта. Перед запуском покажем объём и спросим подтверждение." @click="prepareClient(r)">Подготовить отчёт для заказчика</button>
              <span v-if="clientBusy(r)" class="vc-muted">для заказчика: готовится<template v-if="r.client.usage && r.client.usage.calls">, обращений {{ r.client.usage.calls }}</template>…</span>
              <div v-if="canOperate && r.client && ['failed', 'incomplete', 'interrupted'].includes(r.client.state)" class="vc-error-inline">для заказчика: {{ r.client.reason || r.client.state }}</div>
              <a v-if="canOperate" class="btn btn-outline btn-xs" :href="sourceUrl(r)" download :title="'Скачать исходный MD, по которому составлен этот отчёт: сохранённый при приёме файл, SHA-256 ' + r.source.sha256.slice(0, 12) + '…. Новую актуальную сводку формирует кнопка «Сводка изменений (MD)» проекта.'">Исходный MD</a>
              <button v-if="canOperate && isActive(r)" class="btn btn-danger-outline btn-xs" title="Остановить формирование отчёта. Готовые части сохранятся — отчёт можно будет продолжить" @click="act(r, 'cancel')">Отменить</button>
              <button v-if="canOperate && canResume(r)" class="btn btn-outline btn-xs" title="Продолжить: готовые переходы берутся из сохранённых, заново отправляются только неподготовленные" @click="act(r, 'resume')">Продолжить</button>
              <button v-if="canOperate && r.rebuildable && (r.state === 'done' || (r.state === 'failed' && r.stage === 'pdf'))" class="btn btn-outline btn-xs" :title="r.kind === 'client' ? 'Сверстать PDF заново из сохранённого отбора по переходам, без обращения к ИИ: применяет новую вёрстку' : 'Собрать отчёт и PDF заново из сохранённых ответов ИИ, без нового обращения к ИИ: применяет новую вёрстку и подписи источников'" @click="act(r, 'rebuild-pdf')">Пересобрать PDF</button>
              <button v-if="canOperate && !isActive(r)" class="btn btn-outline btn-xs vcmr-del" title="Удалить отчёт вместе с его PDF. Готовые части этого отчёта новые отчёты больше не смогут взять — при следующем отчёте по той же сводке ИИ сделает их заново." @click="remove(r)">Удалить</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</div>`;

    root.VersionComparisonMdReport = {
        register(app) {
            app.component('vc-md-report', {
                template: TEMPLATE,
                props: {objectId: {type: String, required: true}, project: {type: Object, required: true},
                    seriesId: {type: String, default: ''}, seriesReady: {type: Boolean, default: false}},
                emits: ['close'],
                setup(props) {
                    const {ref, computed, onMounted, onBeforeUnmount} = root.Vue;
                    const VC = root.VersionComparison;
                    const enc = s => encodeURIComponent(s);
                    const base = () => `/objects/${enc(props.objectId)}/md-reports`;
                    const rt = ref(null), reports = ref([]), error = ref(''), consent = ref(false), starting = ref(false), reuse = ref(true);
                    const mode = ref(props.seriesReady ? 'series' : 'file'), file = ref(null);
                    let timer = null, alive = true;
                    // Права вошедшего (portal_auth.js): viewer видит и скачивает готовые отчёты, но не запускает.
                    const canOperate = !(root.portalCan && !root.portalCan.operate());
                    const canDownload = !(root.portalCan && !root.portalCan.download('version_comparison'));
                    const isActive = r => ACTIVE.includes(r.state);
                    const canResume = r => RESUMABLE.includes(r.state) && !(r.state === 'failed' && r.stage === 'pdf');
                    const canStart = computed(() => rt.value && rt.value.enabled && consent.value &&
                        (mode.value === 'series' ? props.seriesReady && props.seriesId : !!file.value));
                    async function load() {
                        clearTimeout(timer);
                        try {
                            reports.value = await VC.api(`${base()}?project_id=${enc(props.project.project_id)}`);
                        } catch (e) { error.value = e.message; }
                        if (alive && reports.value.some(r => isActive(r) || clientBusy(r))) timer = setTimeout(load, 3000);
                    }
                    function pickFile(ev) { file.value = (ev.target.files || [])[0] || null; }
                    async function start() {
                        if (!canOperate) return;  // viewer: только просмотр
                        starting.value = true;
                        error.value = '';
                        try {
                            if (mode.value === 'series') {
                                await VC.api(`/objects/${enc(props.objectId)}/series/${enc(props.seriesId)}/md-report`,
                                    {method: 'POST', body: {consent: consent.value, reuse: reuse.value}});
                            } else {
                                const fd = new FormData();
                                fd.append('file', file.value);
                                fd.append('project_id', props.project.project_id);
                                fd.append('consent', consent.value ? 'true' : 'false');
                                fd.append('reuse', reuse.value ? 'true' : 'false');
                                await VC.api(base(), {method: 'POST', body: fd});
                            }
                            consent.value = false;
                            await load();
                        } catch (e) { error.value = e.message; }
                        finally { starting.value = false; }
                    }
                    async function act(r, what) {
                        if (!canOperate) return;  // viewer: только просмотр
                        try { await VC.api(`${base()}/${enc(r.report_id)}/${what}`, {method: 'POST'}); }
                        catch (e) { error.value = e.message; }
                        await load();
                    }
                    const clientBusy = r => !!(r.client && ['queued', 'running'].includes(r.client.state));
                    // Отдельное действие: оценка объёма → подтверждение → задание; скачивание готового PDF — без ИИ.
                    async function prepareClient(r) {
                        if (!canOperate) return;  // viewer: только просмотр
                        error.value = '';
                        try {
                            const url = `${base()}/${enc(r.report_id)}/client-prepare`;
                            const st = await VC.api(url);
                            if (st.ready) { await load(); return; }
                            const e = st.estimate || {};
                            const tr = (e.transitions || []).map(t => t.transition).join(', ');
                            const msg = `Подготовить отчёт для заказчика?\n\nПереходов без готового содержания: ${e.calls} (${tr}).\n` +
                                `Обращений к ИИ: ${e.calls}, повторов не больше ${e.extra_calls_max}.\n` +
                                `Оценка входа: ≈${fmtNum(Math.round((e.input_tokens || 0) / 1000))} тыс. токенов ` +
                                `(с повторами — до ${fmtNum(Math.round((e.input_tokens_max || 0) / 1000))} тыс.).\n` +
                                `Исполнитель: ${e.executor || ''} ${e.model || ''} ${e.effort || ''}.\n\n` +
                                'Факты этих переходов будут переданы исполнителю. Готовые переходы повторно не запрашиваются.';
                            if (!root.confirm(msg)) return;
                            await VC.api(url, {method: 'POST', body: {consent: true}});
                        } catch (e) { error.value = e.message; }
                        await load();
                    }
                    async function remove(r) {
                        if (!canOperate) return;  // viewer: только просмотр
                        if (!root.confirm(`Удалить отчёт от ${fmtTime(r.created_at)} (${(r.source || {}).filename || r.report_id})? Это нельзя отменить.`)) return;
                        try { await VC.api(`${base()}/${enc(r.report_id)}`, {method: 'DELETE'}); }
                        catch (e) { error.value = e.message; }
                        await load();
                    }
                    const pdfUrl = r => `${VC.API}${base()}/${enc(r.report_id)}/report.pdf`;
                    const sourceUrl = r => `${VC.API}${base()}/${enc(r.report_id)}/source.md`;
                    const clientUrl = r => `${VC.API}${base()}/${enc(r.report_id)}/client.pdf`;
                    const warnings = r => ((r.checks_summary && r.checks_summary.warnings) || []).slice(0, 3);
                    const fmtTime = t => (t ? new Date(t).toLocaleString('ru-RU', {dateStyle: 'short', timeStyle: 'short'}) : '');
                    const fmtNum = n => (n || 0).toLocaleString('ru-RU');
                    const kb = n => `${Math.round((n || 0) / 1024)} КБ`;
                    // Технический идентификатор и отображаемое имя модели из кэша моделей Codex (если сервер его отдал).
                    const modelLabel = computed(() => {
                        if (!rt.value) return '';
                        const name = rt.value.model_limits && rt.value.model_limits.display_name;
                        return rt.value.executor.model + (name ? ` (${name})` : '');
                    });
                    onMounted(async () => {
                        // viewer: сведения об исполнителе нужны только для запуска (служебный эндпоинт).
                        if (canOperate) {
                            try { rt.value = await VC.api('/md-reports/runtime'); } catch (e) { error.value = e.message; }
                        }
                        await load();
                    });
                    onBeforeUnmount(() => { alive = false; clearTimeout(timer); });
                    return {canOperate, canDownload, rt, reports, error, consent, starting, reuse, mode, file, canStart, isActive, canResume, pickFile, remove,
                        start, act, pdfUrl, sourceUrl, clientUrl, clientBusy, prepareClient, warnings, fmtTime, fmtNum, kb, modelLabel};
                },
            });
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
