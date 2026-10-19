(function (root) {
    'use strict';
    // «Весь процесс по всем шифрам»: ИИ-проверка сомнительных пар (Луна, version_comparison_ai/sheet_match.py) →
    // сравнение соседних → ИИ-описание изменений → сводка MD — по всем шифрам объекта через очередь этапов.
    // Пары, принятые с проверками, встают в матрицы с пометкой «ИИ»; остальное — подсказки в ячейках. Ручные правки главнее.
    const STATE = {none: 'не запускалась', running: 'идёт', cancelling: 'останавливается…', done: 'готово',
        cancelled: 'остановлена', interrupted: 'прервана (можно досчитать)', failed: 'ошибка',
        paused_quota: 'остановлена: исчерпана квота Codex', paused_auth: 'остановлена: Codex не авторизован',
        pausing: 'ставится на паузу…', paused: 'на паузе (можно продолжить)'};
    const PHASE = {collect: 'сбор сомнительных мест по шифрам', forward: 'вопросы Луне', reverse: 'встречная проверка',
        apply: 'запись пар в матрицы', done: 'готово', luna: 'ИИ-проверка сомнительных пар', series: 'сравнение соседних версий',
        ai: 'ИИ-описание изменений: обычные листы', specifications: 'ИИ-описание изменений: спецификации', summary: 'Сводка изменений (MD)'};
    const ACTIVE = ['running', 'cancelling', 'pausing'];

    const template = `
<div class="vc-luna" v-if="objectId">
  <div class="vc-luna-bar">
    <button class="btn btn-sm" :class="active ? 'btn-outline' : 'btn-accent'" :disabled="busy || active || (job && !job.enabled)"
            :title="job && !job.enabled ? 'ИИ в «Сравнении версий» выключен на сервере' : 'Запустить для всех шифров объекта весь процесс: ИИ-проверка сомнительных пар → сравнение соседних → ИИ-описание изменений → «Сводка изменений (MD)».'"
            @click="start">🌙 Весь процесс по всем шифрам</button>
    <template v-if="job && job.state !== 'none'">
      <span class="vc-run-state" :class="'st-' + job.state">{{ stateLabel }}</span>
      <span v-if="active" class="vc-muted">{{ phaseLabel }}<template v-if="job.phase === 'collect'">: шифров {{ pr.projects_done }} из {{ pr.projects }}</template><template v-else-if="job.phase === 'forward'">: {{ pr.asked }} из {{ pr.questions }}</template><template v-else-if="job.phase === 'reverse'">: {{ pr.reverse_asked }} из {{ pr.reverse }}</template></span>
      <span v-if="job.scope && job.scope.length" class="vc-muted">· по {{ job.scope.length === 1 ? 'одному проекту' : 'проектам: ' + job.scope.length }}</span>
      <span v-if="job.pipeline" class="vc-muted">· этапов {{ job.pipeline.progress.done }} из {{ job.pipeline.progress.total }}<template v-if="job.pipeline.current"> · {{ job.pipeline.current.code }}: {{ job.pipeline.current.label }}</template></span>
      <span v-if="job.pipeline && job.reason" class="vc-error">{{ job.reason }}</span>
      <button v-if="job.state === 'running'" class="btn btn-outline btn-xs" title="Начатые вопросы доводятся (ответы сохраняются), новые не задаются" @click="pause">⏸ Пауза</button>
      <button v-if="active" class="btn btn-outline btn-xs" :disabled="job.state === 'cancelling'" title="Остановить: готовые ответы ИИ сохранятся, следующие этапы не начнутся" @click="cancel">Остановить</button>
      <button v-if="!active && job.state !== 'done'" class="btn btn-outline btn-xs" :disabled="busy" title="Продолжить с того же места: готовые ответы ИИ повторно не запрашиваются" @click="resume">{{ job.state === 'paused' ? 'Продолжить' : 'Досчитать' }}</button>
      <button v-if="!active && hasResult" class="btn btn-outline btn-xs" :disabled="busy" title="Убрать пары и подсказки ИИ из матриц всех шифров (ответы сохранятся — повторный запуск будет быстрым)" @click="clear">Убрать пары ИИ</button>
      <button class="btn btn-outline btn-xs" title="Показать или скрыть подробности: вопросы, ответы, токены по шифрам" @click="open = !open">{{ open ? 'Скрыть' : 'Подробнее' }}</button>
    </template>
  </div>
  <div v-if="error" class="vc-error" @click="error=''">{{ error }}</div>
  <div v-if="open && job && job.state !== 'none'" class="vc-luna-panel">
    <div class="vc-luna-stats">
      <span>вопросов: <b>{{ pr.questions }}</b> + встречных <b>{{ pr.reverse }}</b></span>
      <span>обращений: {{ pr.calls }}<template v-if="pr.cached">, из сохранённых ответов: {{ pr.cached }}</template><template v-if="pr.failed">, сбоев: {{ pr.failed }}</template></span>
      <span>токены: вход {{ mln(pr.input_tokens) }} млн (кэш {{ mln(pr.cached_input_tokens) }}), выход {{ mln(pr.output_tokens) }} млн</span>
      <span class="vc-muted">модель {{ (job.settings || {}).model }} · {{ (job.settings || {}).effort }} · одновременно {{ (job.settings || {}).parallel }}</span>
    </div>
    <div v-if="job.reason" class="vc-cell-reason">{{ job.reason }}</div>
    <template v-if="job.totals && Object.keys(job.totals).length">
      <div class="vc-luna-stats">
        <span class="ok">пар поставлено: <b>{{ t('accepted') }}</b></span>
        <span>подсказок: <b>{{ hints }}</b></span>
        <span>«ни один»: {{ t('none') }}</span>
        <span>пары средней уверенности: подтверждено {{ t('medium_confirm') + t('medium_weak') }}, <span class="bad">ИИ против: {{ t('medium_other') + t('medium_none') }}</span></span>
      </div>
      <div class="vc-luna-table"><table>
        <tr><th>Шифр</th><th>вопросов</th><th>пар ИИ</th><th>подсказок</th><th>против пары алгоритма</th></tr>
        <tr v-for="p in (job.projects || []).filter(p => p.questions)" :key="p.project_id">
          <td>{{ p.code }}</td><td class="n">{{ p.questions }}</td><td class="n ok">{{ p.pairs || '' }}</td>
          <td class="n">{{ hintCount(p) || '' }}</td><td class="n bad">{{ ((p.counts || {}).medium_other || 0) + ((p.counts || {}).medium_none || 0) || '' }}</td>
        </tr>
      </table></div>
      <div class="vc-muted">В матрице: пара от ИИ — пометка «ИИ» (наведите — доводы ИИ и встречной проверки); у «⁇» — подсказка ИИ;
        у пары средней уверенности, с которой ИИ не согласен, — «ИИ ?». Неверную пару ИИ отвяжите (⤫) или перетащите нужный лист — ручная правка главнее.</div>
    </template>
  </div>
</div>`;

    root.VersionComparisonLuna = {
        register(app) {
            app.component('vc-luna-match', {
                template, props: {objectId: {type: String, required: true}}, emits: ['updated'],
                setup(props, {emit}) {
                    const {ref, computed, onMounted, onBeforeUnmount, watch} = root.Vue;
                    const VC = root.VersionComparison;
                    const job = ref(null), busy = ref(false), error = ref(''), open = ref(false);
                    let timer = null, wasActive = false;
                    const base = () => `/objects/${encodeURIComponent(props.objectId)}/ai-sheet-match`;
                    const active = computed(() => !!job.value && ACTIVE.includes(job.value.state));
                    const pr = computed(() => (job.value && job.value.progress) || {});
                    const stateLabel = computed(() => (job.value && job.value.pipeline ? 'Процесс: ' : 'ИИ-проверка пар: ') + (STATE[(job.value || {}).state] || (job.value || {}).state));
                    const phaseLabel = computed(() => PHASE[(job.value || {}).phase] || '');
                    const t = k => ((job.value && job.value.totals) || {})[k] || 0;
                    const hints = computed(() => ['hint_reverse', 'hint_confidence', 'hint_table', 'hint_taken', 'hint_double'].reduce((s, k) => s + t(k), 0));
                    const hasResult = computed(() => !!job.value && job.value.state === 'done' && !job.value.cleared_at);
                    const hintCount = p => Object.entries(p.counts || {}).filter(([k]) => k.startsWith('hint_')).reduce((s, [, v]) => s + v, 0);
                    const mln = n => ((n || 0) / 1e6).toFixed(1);

                    async function load() {
                        clearTimeout(timer);
                        try {
                            job.value = await VC.api(base());
                            const now = ACTIVE.includes(job.value.state);
                            if (wasActive && !now) emit('updated');
                            wasActive = now;
                            if (now) timer = setTimeout(load, 3000);
                        } catch (e) { error.value = e.message; }
                    }
                    async function start() {
                        const ok = root.confirm('Запустить единый процесс для всех шифров объекта?\n\n'
                            + 'По очереди: ИИ-проверка сомнительных пар → сравнение соседних версий → ИИ-описание изменений (обычные листы, полные спецификации) → «Сводка изменений (MD)».\n'
                            + 'Исполнителю (codex exec, подписка Codex, модель ' + ((job.value && job.value.settings) || {}).model + ') будут переданы '
                            + 'изображения и текст обычных листов, а также подготовленный MD полных спецификаций (gpt-6-luna, до 50 версий одновременно). Секреты не передаются.\n'
                            + 'Пары ставятся автоматически только при уверенном ответе и подтверждении встречной проверкой; '
                            + 'остальное — подсказки. Ручные правки главнее.');
                        if (!ok) return;
                        busy.value = true;
                        try { job.value = await VC.api(base(), {method: 'POST', body: {consent: true}}); open.value = true; wasActive = true; timer = setTimeout(load, 2000); }
                        catch (e) { error.value = e.message; }
                        finally { busy.value = false; }
                    }
                    async function pause() {
                        try { job.value = await VC.api(base() + '/pause', {method: 'POST'}); load(); } catch (e) { error.value = e.message; }
                    }
                    // Продолжить с тем же охватом (проект со страницы «Проекты» или весь объект); готовые ответы — из кэша.
                    async function resume() {
                        busy.value = true;
                        try {
                            // Проверка без процесса (запущена кнопкой в строке проекта) продолжается сама по себе, без остальных этапов.
                            job.value = await VC.api(base(), {method: 'POST', body: {consent: true, project_ids: (job.value && job.value.scope) || [],
                                luna_only: !(job.value && job.value.pipeline)}});
                            wasActive = true; timer = setTimeout(load, 2000);
                        } catch (e) { error.value = e.message; }
                        finally { busy.value = false; }
                    }
                    async function cancel() {
                        try { job.value = await VC.api(base() + '/cancel', {method: 'POST'}); load(); } catch (e) { error.value = e.message; }
                    }
                    async function clear() {
                        if (!root.confirm('Убрать пары и подсказки ИИ из матриц всех шифров объекта?')) return;
                        busy.value = true;
                        try { job.value = await VC.api(base() + '/clear', {method: 'POST'}); emit('updated'); }
                        catch (e) { error.value = e.message; }
                        finally { busy.value = false; }
                    }
                    watch(() => props.objectId, () => { job.value = null; load(); });
                    onMounted(load);
                    onBeforeUnmount(() => clearTimeout(timer));
                    return {job, busy, error, open, active, pr, stateLabel, phaseLabel, t, hints, hasResult, hintCount, mln,
                        start, cancel, clear, pause, resume};
                },
            });
        },
    };
})(window);
