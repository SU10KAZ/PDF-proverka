(function (root) {
    'use strict';
    // «Отчёт PDF» по MD-сводке изменений (backend: version_comparison_ai/md_report).
    // Вход — текущая «Сводка изменений (MD)» серии или загруженный MD-файл. Запуск — только с явным
    // подтверждением передачи текста исполнителю; ход, причины ошибок и скачивание проверенного PDF.

    const ACTIVE = ['queued', 'running', 'cancelling'];
    const RESUMABLE = ['interrupted', 'cancelled', 'paused_quota', 'paused_auth', 'incomplete', 'failed'];

    const TEMPLATE = `
<div class="vcai-overlay" @keydown.esc="$emit('close')" tabindex="0">
  <div class="vcai-window vcmr-window">
    <div class="vcai-head">
      <div>
        <div class="vcai-title">Аналитический отчёт по сводке изменений (PDF)</div>
        <div class="vc-muted">{{ project.title || project.project_id }} — короткий отчёт: масштаб изменений каждой выдачи,
          последствия работ по предыдущей версии, серьёзные изменения. Анализ — по MD-сводке, исходные PDF не открываются.</div>
      </div>
      <div class="vcai-head-actions"><button class="btn btn-outline btn-sm" @click="$emit('close')">✕ Закрыть</button></div>
    </div>
    <div class="vcai-body">
      <div v-if="error" class="vc-error" @click="error = ''">{{ error }}</div>
      <div v-if="rt && !rt.enabled" class="vc-warning">ИИ-обработка в разделе выключена (ai_settings.json).</div>
      <section class="vcmr-start">
        <div class="vcmr-row">
          <label><input type="radio" value="series" v-model="mode" :disabled="!seriesReady"> текущая «Сводка изменений (MD)» серии</label>
          <label><input type="radio" value="file" v-model="mode"> загрузить MD-файл</label>
          <input v-if="mode === 'file'" type="file" accept=".md,text/markdown,text/plain" @change="pickFile">
        </div>
        <div v-if="mode === 'series' && !seriesReady" class="vc-muted">Сводки серии ещё нет — сначала «Описать изменения ИИ».</div>
        <div v-if="rt" class="vc-muted">Исполнитель: Codex, модель <b>{{ modelLabel }}</b>,
          усилие {{ rt.executor.effort }}<template v-if="rt.executor.cli_version">, CLI {{ rt.executor.cli_version }}</template>;
          одновременно до {{ rt.settings.parallel }} изолированных обращений. {{ rt.transfer }}</div>
        <label class="vcmr-consent"><input type="checkbox" v-model="consent"> Подтверждаю передачу текста сводки исполнителю</label>
        <button class="btn btn-primary btn-sm" :disabled="!canStart || starting" @click="start">{{ starting ? 'Запуск…' : 'Сформировать аналитический отчёт' }}</button>
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
            <td><template v-if="r.progress && r.progress.groups_total">
                <div>анализ: {{ r.progress.groups_done || 0 }} из {{ r.progress.groups_total }}</div>
                <div>проверка: {{ r.progress.units_done || 0 }} из {{ r.progress.units_total }}</div>
                <div v-if="r.usage && r.usage.calls" class="vc-muted">обращений {{ r.usage.calls }}, вход {{ fmtNum(r.usage.input_tokens) }} ток.</div>
                <div v-if="r.reuse && r.reuse.reused" class="vc-muted" title="Совместимые проверенные результаты: тот же файл (SHA-256), те же промпты этапа, модель и входы">
                  взято готовым: {{ r.reuse.reused }} из {{ r.reuse.units }}<template v-if="r.reuse.from.length"> (отчёт {{ r.reuse.from.join(', ') }})</template></div>
              </template></td>
            <td class="vcmr-actions">
              <a v-if="r.pdf_ready" class="btn btn-primary btn-xs" :href="pdfUrl(r)" download>Скачать PDF</a>
              <button v-if="isActive(r)" class="btn btn-danger-outline btn-xs" @click="act(r, 'cancel')">Отменить</button>
              <button v-if="canResume(r)" class="btn btn-outline btn-xs" title="Готовые результаты этапов берутся с диска, повторяются только недостающие" @click="act(r, 'resume')">Продолжить</button>
              <button v-if="r.state === 'failed' && r.stage === 'pdf'" class="btn btn-outline btn-xs" title="Без обращения к модели" @click="act(r, 'rebuild-pdf')">Пересобрать PDF</button>
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
                    const rt = ref(null), reports = ref([]), error = ref(''), consent = ref(false), starting = ref(false);
                    const mode = ref(props.seriesReady ? 'series' : 'file'), file = ref(null);
                    let timer = null, alive = true;
                    const isActive = r => ACTIVE.includes(r.state);
                    const canResume = r => RESUMABLE.includes(r.state) && !(r.state === 'failed' && r.stage === 'pdf');
                    const canStart = computed(() => rt.value && rt.value.enabled && consent.value &&
                        (mode.value === 'series' ? props.seriesReady && props.seriesId : !!file.value));
                    async function load() {
                        clearTimeout(timer);
                        try {
                            reports.value = await VC.api(`${base()}?project_id=${enc(props.project.project_id)}`);
                        } catch (e) { error.value = e.message; }
                        if (alive && reports.value.some(isActive)) timer = setTimeout(load, 3000);
                    }
                    function pickFile(ev) { file.value = (ev.target.files || [])[0] || null; }
                    async function start() {
                        starting.value = true;
                        error.value = '';
                        try {
                            if (mode.value === 'series') {
                                await VC.api(`/objects/${enc(props.objectId)}/series/${enc(props.seriesId)}/md-report`,
                                    {method: 'POST', body: {consent: consent.value}});
                            } else {
                                const fd = new FormData();
                                fd.append('file', file.value);
                                fd.append('project_id', props.project.project_id);
                                fd.append('consent', consent.value ? 'true' : 'false');
                                await VC.api(base(), {method: 'POST', body: fd});
                            }
                            consent.value = false;
                            await load();
                        } catch (e) { error.value = e.message; }
                        finally { starting.value = false; }
                    }
                    async function act(r, what) {
                        try { await VC.api(`${base()}/${enc(r.report_id)}/${what}`, {method: 'POST'}); }
                        catch (e) { error.value = e.message; }
                        await load();
                    }
                    const pdfUrl = r => `${VC.API}${base()}/${enc(r.report_id)}/report.pdf`;
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
                        try { rt.value = await VC.api('/md-reports/runtime'); } catch (e) { error.value = e.message; }
                        await load();
                    });
                    onBeforeUnmount(() => { alive = false; clearTimeout(timer); });
                    return {rt, reports, error, consent, starting, mode, file, canStart, isActive, canResume, pickFile,
                        start, act, pdfUrl, warnings, fmtTime, fmtNum, kb, modelLabel};
                },
            });
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
