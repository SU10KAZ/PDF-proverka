(function (root) {
    'use strict';
    // Страница «Проекты» раздела «Сравнение версий»: все проекты объекта и этапы их подготовки и проверки
    // одной таблицей. Данные — GET /objects/{id}/overview (только чтение, расчёты не запускает).
    // Щелчок по проекту открывает вторую страницу — матрицу листов проекта (version-comparison.js).
    const STAGES = [
        {key: 'upload', num: '1', label: 'Загрузка', hint: 'Все версии PDF проекта загружены в раздел.'},
        {key: 'profile', num: '2', label: 'Разбор версий', hint: 'Система прочитала каждую версию: какие в ней листы, их номера и названия по штампу.'},
        {key: 'matrix', num: '3', label: 'Сопоставление листов', hint: 'Таблица «какой лист одной версии соответствует какому листу другой». «Не сопост.» — листы, которые алгоритм не связал уверенно.'},
        {key: 'luna', num: '4', label: 'ИИ-проверка сомнительных пар', hint: 'ИИ смотрит только листы, которые алгоритм не связал уверенно, и ставит пару или подсказку. Другие этапы не запускает.'},
        {key: 'series', num: '5', label: 'Сравнение соседних', hint: 'Каждая версия сравнивается со следующей (V1→V2, V2→V3 …) по изображению листов. ИИ не используется.'},
        {key: 'ai', num: '6', label: 'ИИ-описание изменений', hint: 'ИИ описывает словами изменения на листах, сравнивает полные спецификации и собирает «Сводку изменений (MD)».'},
        {key: 'report', num: '7', label: 'Сводка и отчёт', hint: '«Сводка изменений (MD)» — все изменения проекта одним файлом; PDF-отчёт — короткий анализ по этой сводке.'},
    ];
    const TONE = {done: 'ok', skip: 'ok', attention: 'warn', partial: 'warn', running: 'run', queued: 'run', pausing: 'run',
        todo: 'todo', stale: 'warn', interrupted: 'warn', paused: 'warn', failed: 'bad', blocked: 'off', na: 'off'};
    const ICON = {ok: '✓', warn: '!', run: '⟳', todo: '○', bad: '✕', off: '·'};
    const FILTERS = [
        {key: 'all', label: 'Все', hint: 'Показать все проекты объекта.'},
        {key: 'act', label: 'Ждут действия', hint: 'Проекты, где следующий шаг за вами: что-то нужно запустить или проверить.'},
        {key: 'run', label: 'Идут', hint: 'Проекты, где сейчас идёт расчёт или загрузка.'},
        {key: 'bad', label: 'Ошибки', hint: 'Проекты с ошибкой загрузки, разбора или одного из этапов.'},
        {key: 'ok', label: 'Готовы', hint: 'Проекты, где всё посчитано и действий не требуется.'},
    ];
    const enc = s => encodeURIComponent(s);
    const natural = (a, b) => String(a || '').localeCompare(String(b || ''), 'ru', {numeric: true, sensitivity: 'base'});
    const n = v => (v || 0).toLocaleString('ru-RU');
    // Марка из шифра: «133/23-ГК-ТХ.ВТ» → «ТХ», «133/23-ГК-ЭМ2» → «ЭМ».
    function mark(code) {
        const tail = String(code || '').split('-').pop() || '';
        const m = tail.match(/^[А-ЯЁA-Z]+/i);
        return m ? m[0].toUpperCase() : '—';
    }
    function fmtDate(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        if (isNaN(d)) return '';
        return d.toLocaleString('ru-RU', {day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit'});
    }

    function stageView(key, s, p) {
        s = s || {state: 'na'};
        const st = s.state;
        let text = '', note = '', title = '';
        if (key === 'upload') {
            if (st === 'done') { text = 'загружен'; note = p.updated || ''; title = p.uploaded_at ? 'Последняя версия загружена ' + fmtDate(p.uploaded_at) : ''; }
            else { text = s.label || st; note = s.files ? `файлов ${s.files}` : ''; title = s.error || s.waiting_reason || ''; }
        } else if (key === 'profile') {
            text = {done: 'готов', running: 'идёт', todo: 'не начат', failed: 'ошибка'}[st] || st;
            note = `${s.ready} из ${s.total}` + (s.failed ? ` · ошибок ${s.failed}` : '');
            if (st === 'todo') title = 'Разбор ещё не запускался — «Подготовить» в колонке сопоставления';
            if (s.failed) title = 'Разбор части версий закончился ошибкой — «Повторить»';
        } else if (key === 'matrix') {
            text = {done: 'готово', attention: 'не сопост. ' + s.unmatched, stale: 'устарело', todo: 'не собрано', na: '—'}[st] || st;
            if (s.rows != null && st !== 'na') note = `строк ${s.rows}` + (s.manual ? ` · ✎ ${s.manual}` : '');
            title = st === 'attention' ? `Не сопоставлено ячеек: ${s.unmatched}. Разберите в матрице проекта или запустите ИИ-проверку сомнительных пар.`
                : st === 'stale' ? 'Версии, их порядок или правила изменились — «Подготовить» пересоберёт сопоставление'
                : st === 'todo' ? 'Сопоставление ещё не собиралось — «Подготовить»' : '';
            if (s.groups) title += (title ? '\n' : '') + `Ячеек в группах таблиц: ${s.groups}`;
        } else if (key === 'luna') {
            text = {done: 'проверено', attention: 'проверено', skip: 'не нужна', todo: 'не проверялось', running: 'идёт',
                pausing: 'на паузу…', paused: 'пауза', interrupted: 'прервано', na: '—'}[st] || st;
            if (s.questions) note = `вопросов ${s.questions}` + (s.accepted ? ` · пар ${s.accepted}` : '') + (s.hints ? ` · подсказок ${s.hints}` : '') + (s.against ? ` · против ${s.against}` : '');
            title = st === 'skip' ? 'Сомнительных пар у проекта не было' : s.questions ? `Вопросов ИИ: ${s.questions}; пар поставлено: ${s.accepted}; подсказок у «⁇»: ${s.hints}` : '';
            if (s.against) title += `\nИИ против пары алгоритма: ${s.against} — проверить в матрице («ИИ ?»)`;
            if (s.reason && ['paused', 'interrupted'].includes(st)) title += (title ? '\n' : '') + s.reason;
        } else if (key === 'series') {
            text = {done: 'готово', running: 'идёт', interrupted: 'прервано', failed: 'ошибка', todo: 'не запускалось',
                pausing: 'на паузу…', paused: 'пауза', stale: 'по старому порядку', na: '—'}[st] || st;
            if (st !== 'na' && s.total) note = `${s.done} из ${s.total}` + (st === 'done' && s.changed != null ? ` · с изм. ${s.changed}` : '');
            title = st !== 'na' && s.total ? `Переходов посчитано: ${s.done} из ${s.total}` + (s.changed != null ? `; с изменениями: ${s.changed}` : '') : '';
            if (s.sheets_total) title += `\nЛистов посчитано ${n(s.sheets_done)} из ${n(s.sheets_total)}\nЕсть отличия: ${n(s.different)} · нужна проверка: ${n(s.needs_review)} · состав +${n(s.added)} / −${n(s.removed)}`
                + (s.failed ? `\nОшибок расчёта: ${s.failed}` : '') + (s.finished_at ? `\nЗакончено ${fmtDate(s.finished_at)}` : '');
            if (st === 'stale') title = 'Сводка посчитана для прежнего порядка или состава версий — нужно «Заново» в столбце «Сравнение соседних»';
        } else if (key === 'ai') {
            text = {done: 'готово', partial: 'готово', running: 'идёт', queued: 'в очереди №' + s.position, paused: 'пауза', pausing: 'на паузу…',
                interrupted: 'прервано', todo: 'не запускалось', blocked: 'ждёт сравнения', na: '—'}[st] || st;
            if (s.sheets) note = `листов ${n(s.sheets_done)} из ${n(s.sheets)}` + (s.sheets_failed || s.failed ? ` · ошибок ${s.sheets_failed || s.failed}` : '')
                + (s.review ? ` · проверить ${s.review}` : '');
            if (st === 'partial') title = 'Описания готовы. «Проверить» — листы, где ИИ изменений не нашёл, но сравнение оставило пометки (например, пара листов сопоставлена неуверенно): откройте проект, блок «ИИ-описание изменений», столбец «Проверить».'
                + (s.sheets_failed ? ' «Ошибок» — листы, которые ИИ не смог описать.' : '');
            if (s.calls) title += (title ? '\n' : '') + `Обращений: ${n(s.calls)}` + (s.finished_at ? ` · закончено ${fmtDate(s.finished_at)}` : '');
            if (st === 'todo' && p.stages.series.changed === 0) { text = 'не нужно'; title = 'Переходов с изменениями содержания нет'; }
        } else if (key === 'report') {
            const r = s.md_report;
            text = {done: 'PDF готов', running: 'PDF строится', pausing: 'на паузу…', paused: 'пауза',
                interrupted: 'прервано', failed: 'ошибка PDF'}[st] || (s.summary_md ? 'есть MD' : '—');
            if (s.summary_md) note = st === 'done' ? 'MD + PDF' : 'PDF не формировался';
            if (r) title = `Отчёт PDF: ${r.state}` + (r.finished_at ? ` · ${fmtDate(r.finished_at)}` : '');
        }
        const tone = (key === 'ai' && st === 'todo' && p.stages.series.changed === 0) ? 'ok'
            : (key === 'report' && st === 'todo') ? 'todo' : (TONE[st] || 'todo');
        return {tone, icon: ICON[tone], text, note, title};
    }

    const template = `
<div class="vcp">
  <div class="vcp-tiles">
    <button v-for="f in filters" :key="f.key" class="vcp-tile" :class="['t-' + f.key, {on: filter === f.key}]" :title="f.hint" @click="filter = f.key">
      <span class="vcp-tile-n">{{ counts[f.key] }}</span><span class="vcp-tile-l">{{ f.label }}</span>
    </button>
    <div class="vcp-tile vcp-tile-info" title="Сколько всего версий у показанных проектов и сколько переходов между соседними версиями.">
      <span class="vcp-tile-n">{{ totals.versions }}</span><span class="vcp-tile-l">версий · {{ totals.transitions }} переходов</span>
    </div>
  </div>

  <div class="vcp-toolbar">
    <input class="vc-input vcp-search" type="search" v-model="query" placeholder="Поиск по шифру…" aria-label="Поиск по шифру">
    <label class="vcp-ctl">Марка
      <select v-model="markFilter" class="vc-input"><option value="">все</option>
        <option v-for="m in marks" :key="m.key" :value="m.key">{{ m.key }} ({{ m.n }})</option></select>
    </label>
    <label class="vcp-ctl">Порядок
      <select v-model="sortSel" class="vc-input">
        <option value="code|asc">по шифру</option>
        <option value="step|asc">сначала требующие действия</option>
        <option value="updated|asc">по последней загрузке</option>
        <option value="versions|desc">по числу версий</option>
        <option v-if="!SORT_PRESETS.includes(sortSel)" :value="sortSel">{{ sortLabel }}</option>
      </select>
    </label>
    <span class="vc-spacer"></span>
    <span class="vc-muted" v-if="loadedAt">обновлено {{ loadedAt }}</span>
    <button class="btn btn-outline btn-sm" :disabled="loading" title="Перечитать состояние проектов сейчас. Страница и так обновляется сама." @click="load">Обновить</button>
  </div>

  <div v-if="error" class="vc-error" @click="error=''">{{ error }}</div>

  <div v-if="data && data.pending_uploads.length" class="vcp-pending">
    <b>В загрузке, ещё не в списке проектов:</b>
    <span v-for="u in data.pending_uploads" :key="u.batch_id + u.item_id" class="vcp-pending-item" :class="{bad: u.state === 'failed'}" :title="u.error || u.waiting_reason || ''">
      {{ u.code || u.title }} — {{ u.label }}<template v-if="u.files"> · файлов {{ u.files }}</template>
    </span>
  </div>

  <div v-if="queue && queue.items.length" class="vcp-queue" :class="'q-' + queue.state">
    <div class="vcp-queue-head">
      <b>Очередь запуска</b>
      <span class="vcp-chip" :class="queue.state === 'running' ? 'tone-run' : queue.state === 'paused' ? 'tone-warn' : 'tone-ok'">
        <span class="vcp-ico">{{ queue.state === 'running' ? '⟳' : queue.state === 'paused' ? '!' : '✓' }}</span>{{ QSTATE[queue.state] || queue.state }}</span>
      <span v-if="queue.current" class="vcp-queue-cur">{{ queue.current.code }} · {{ queue.current.label }}<template v-if="queue.current.note"> — {{ queue.current.note }}</template></span>
      <span class="vc-muted">готово {{ queue.counts.done }} из {{ queue.items.length }}<template v-if="queue.counts.pending"> · ждут {{ queue.counts.pending }}</template><template v-if="queue.counts.skipped"> · пропущено {{ queue.counts.skipped }}</template><template v-if="queue.counts.failed"> · <span class="vcp-bad">ошибок {{ queue.counts.failed }}</span></template></span>
      <span class="vc-spacer"></span>
      <button v-if="queue.state === 'running'" class="vcp-act k-pause" @click="queueAct('pause')" title="Текущий этап — на паузу (начатое доводится), следующие не начинаются">⏸ Пауза очереди</button>
      <button v-if="['paused', 'failed'].includes(queue.state)" class="vcp-act primary" @click="queueAct('resume')" title="Текущий этап — с того же места, затем следующие">▶ Продолжить очередь</button>
      <button v-if="queue.counts.pending || queue.counts.done || queue.counts.failed || queue.counts.skipped" class="vcp-act" @click="queueAct('clear')" title="Убрать ждущие этапы и историю; идущий этап продолжит работу">Очистить</button>
      <button class="vcp-act" title="Показать или скрыть список этапов в очереди" @click="queueOpen = !queueOpen">{{ queueOpen ? 'Скрыть список' : 'Список' }}</button>
    </div>
    <ol v-if="queueOpen" class="vcp-queue-list">
      <li v-for="(it, i) in queue.items" :key="i" :class="'qi-' + it.state">
        <span class="vcp-qi-state">{{ QITEM[it.state] || it.state }}</span> {{ it.code }} · {{ it.label }}
        <span v-if="it.reason || it.note" class="vc-muted"> — {{ it.reason || it.note }}</span>
        <button v-if="it.state === 'pending'" class="vcp-act" title="Убрать из очереди" @click="queueRemove(it)">✕</button>
      </li>
    </ol>
  </div>

  <div v-if="selCount" class="vcp-selbar">
    <b>Выбрано: {{ selCount }}</b>
    <span v-for="s in stages.filter(x => selBy[x.key])" :key="s.key" class="vc-muted">· {{ s.label }}: {{ selBy[s.key] }}</span>
    <span class="vc-spacer"></span>
    <button class="vcp-act primary" title="Отмеченные этапы выполнятся по одному: проект за проектом, внутри проекта — слева направо." @click="runSelected">▶ Запустить выбранные по очереди</button>
    <button class="vcp-act" title="Снять все отмеченные галочки" @click="clearSel">Снять выбор</button>
  </div>

  <div class="vcp-table-wrap">
    <table class="vcp-table">
      <thead>
        <tr>
          <th class="vcp-col-code">Проект · следующий шаг<button class="vcp-sort" :class="{on: sortKey === 'code'}" :title="sortTitle('code')" @click="toggleSort('code')">{{ sortArrow('code') }}</button></th>
          <th class="vcp-col-ver">Версии<button class="vcp-sort" :class="{on: sortKey === 'versions'}" :title="sortTitle('versions')" @click="toggleSort('versions')">{{ sortArrow('versions') }}</button></th>
          <th v-for="s in stages" :key="s.key" class="vcp-col-stage" :title="s.hint">
            <label v-if="canOperate && SELECTABLE.includes(s.key)" class="vcp-colsel" :title="'Выбрать все строки, где этап «' + s.label + '» можно запустить или продолжить'">
              <input type="checkbox" :checked="colState(s.key) === 'all'" :indeterminate.prop="colState(s.key) === 'some'"
                     :disabled="colState(s.key) === 'none'" @change="toggleCol(s.key, $event.target.checked)">
            </label><span class="vcp-stage-num">{{ s.num }}</span>{{ s.label }}<button class="vcp-sort" :class="{on: sortKey === 'st:' + s.key}" :title="sortTitle('st:' + s.key)" @click="toggleSort('st:' + s.key)">{{ sortArrow('st:' + s.key) }}</button></th>
        </tr>
      </thead>
      <tbody>
        <tr v-if="!data && loading"><td :colspan="stages.length + 2" class="vc-muted">Загрузка…</td></tr>
        <tr v-else-if="data && !rows.length"><td :colspan="stages.length + 2" class="vc-muted">Нет проектов по выбранным условиям.</td></tr>
        <tr v-for="p in rows" :key="p.project_id" class="vcp-row" tabindex="0" @click="open(p)" @keydown.enter="open(p)"
            :title="'Открыть сравнение листов проекта ' + p.title">
          <td class="vcp-col-code">
            <div class="vcp-code">{{ p.title }}</div>
            <div class="vcp-next-line">
              <span class="vcp-next" :class="'tone-' + p.next.tone">{{ p.next.label }}</span>
              <button v-if="canDownload && p.stages.report.summary_md" class="btn btn-outline btn-xs vcp-md" title="Скачать «Сводку изменений (MD)»: все изменения проекта от первой версии до последней одним файлом."
                      @click.stop="downloadMd(p)">Сводка изменений (MD)</button>
            </div>
          </td>
          <td class="vcp-col-ver">
            <div><b>{{ p.versions }}</b><span class="vc-muted"> · {{ p.pages }} стр.</span></div>
            <div class="vc-muted vcp-last" :title="'Первая: ' + p.first_label + '\\nПоследняя: ' + p.last_label">{{ p.last_label }}</div>
          </td>
          <td v-for="s in stages" :key="s.key" class="vcp-col-stage" :class="{'vcp-sel': sel[selKey(p, s.key)]}">
            <label v-if="canSelect(p, s.key)" class="vcp-cellsel" @click.stop :title="'Выбрать для запуска по очереди: ' + s.label">
              <input type="checkbox" :checked="!!sel[selKey(p, s.key)]" @change="toggleSel(p, s.key, $event.target.checked)">
            </label>
            <span v-else-if="queuedAt(p, s.key)" class="vcp-qbadge" :title="'Очередь запуска: ' + queuedAt(p, s.key).text">{{ queuedAt(p, s.key).short }}</span>
            <template v-for="v in [p.view[s.key]]" :key="s.key">
              <div class="vcp-chip" :class="'tone-' + v.tone" :title="v.title"><span class="vcp-ico">{{ v.icon }}</span>{{ v.text }}</div>
              <div v-if="v.note" class="vcp-note">{{ v.note }}</div>
              <div v-if="s.key === 'series' && p.stages.series.state === 'running'" class="vc-progress vcp-bar"><span :style="{width: pct(p.stages.series.sheets_done, p.stages.series.sheets_total) + '%'}"></span></div>
              <div v-if="s.key === 'ai' && ['running', 'paused', 'pausing', 'interrupted'].includes(p.stages.ai.state)" class="vc-progress vcp-bar"><span :style="{width: pct(p.stages.ai.sheets_done, p.stages.ai.sheets) + '%'}"></span></div>
            </template>
            <div v-if="acts(p, s.key).length" class="vcp-acts" @click.stop>
              <template v-for="a in acts(p, s.key)" :key="a.kind">
                <a v-if="a.href" class="vcp-act" :href="a.href" :title="a.title" @click.stop>{{ a.icon }} {{ a.label }}</a>
                <button v-else type="button" class="vcp-act" :class="['k-' + a.kind, {primary: a.primary}]" :disabled="a.disabled || busy[p.project_id + s.key]"
                        :title="a.title" @click.stop="act(p, s.key, a)">{{ busy[p.project_id + s.key] ? '…' : a.icon }} {{ a.label }}</button>
              </template>
            </div>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="vc-muted vcp-legend">
    <span><span class="vcp-chip tone-ok"><span class="vcp-ico">✓</span>готово</span></span>
    <span><span class="vcp-chip tone-warn"><span class="vcp-ico">!</span>есть что проверить</span></span>
    <span><span class="vcp-chip tone-run"><span class="vcp-ico">⟳</span>идёт</span></span>
    <span><span class="vcp-chip tone-todo"><span class="vcp-ico">○</span>не запускалось</span></span>
    <span><span class="vcp-chip tone-bad"><span class="vcp-ico">✕</span>ошибка</span></span>
    <span>Наведите на этап — подробности. Щелчок по строке — сравнение листов проекта.</span>
    <span v-if="canOperate">⏸ Пауза — начатое доводится и сохраняется, новое не начинается; ▶ Продолжить — с того же места; ↻ — заново по текущему состоянию проекта.</span>
  </div>

  <div v-if="dlg.open" class="modal-overlay" @click.self="dlg.open = false">
    <div class="modal vc-modal vcp-dlg" role="dialog" aria-modal="true" :aria-label="dlg.title">
      <div class="vc-modal-head"><h3>{{ dlg.title }}</h3><button class="btn btn-outline btn-sm" @click="dlg.open = false" aria-label="Закрыть">✕</button></div>
      <div v-if="dlg.loading" class="vc-muted">Оценка объёма…</div>
      <p v-for="(l, i) in dlg.lines" :key="i" :class="{'vc-muted': l.muted, 'vc-warning': l.warn}">{{ l.text }}</p>
      <label v-if="dlg.consent" class="vcp-consent"><input type="checkbox" v-model="dlg.agreed"> {{ dlg.consent }}</label>
      <div v-if="dlg.error" class="vc-error" @click="dlg.error = ''">{{ dlg.error }}</div>
      <div class="vc-modal-actions">
        <button class="btn btn-outline" @click="dlg.open = false">Отмена</button>
        <button class="btn btn-primary" :disabled="dlg.loading || dlg.busy || dlg.blocked || (dlg.consent && !dlg.agreed)" @click="dlgOk">{{ dlg.busy ? 'Запуск…' : dlg.ok }}</button>
      </div>
    </div>
  </div>
</div>`;

    root.VersionComparisonProjects = {
        mark, stageView,
        register(app) {
            app.component('vc-projects-overview', {
                template,
                props: {objectId: {type: String, required: true}, reloadKey: {type: Number, default: 0}},
                emits: ['open'],
                setup(props, {emit}) {
                    const {ref, computed, watch, onMounted, onBeforeUnmount} = root.Vue;
                    const api = root.VersionComparison.api;
                    const data = ref(null), loading = ref(false), error = ref(''), loadedAt = ref('');
                    const store = key => { try { return localStorage.getItem('vc-projects:' + key) || ''; } catch (e) { return ''; } };
                    const keep = (key, v) => { try { localStorage.setItem('vc-projects:' + key, v); } catch (e) { /* без хранилища */ } };
                    const filter = ref(store('filter') || 'all'), query = ref(''), markFilter = ref(store('mark')), sortKey = ref(store('sort') || 'code');
                    // Направление: asc — как в названии стрелки ▲ (А→Я, меньше версий, сначала ошибки); desc — обратно.
                    const sortDir = ref(store('dir') || (sortKey.value === 'versions' ? 'desc' : 'asc'));
                    watch(filter, v => keep('filter', v)); watch(markFilter, v => keep('mark', v)); watch(sortKey, v => keep('sort', v));
                    watch(sortDir, v => keep('dir', v));
                    const SORT_PRESETS = ['code|asc', 'step|asc', 'updated|asc', 'versions|desc'];
                    const sortSel = computed({get: () => sortKey.value + '|' + sortDir.value,
                        set: v => { const [k, d] = v.split('|'); sortKey.value = k; sortDir.value = d || 'asc'; }});
                    const setSort = (k, d) => { sortKey.value = k; sortDir.value = d; };
                    // Один треугольник у столбца: первое нажатие — ▲, повторное — ▼ и обратно.
                    const toggleSort = k => setSort(k, sortKey.value === k && sortDir.value === 'asc' ? 'desc' : 'asc');
                    const sortArrow = k => sortKey.value === k && sortDir.value === 'desc' ? '▼' : '▲';
                    const SORT_TEXT = {code: ['по шифру от А до Я', 'по шифру от Я до А'], versions: ['сначала меньше версий', 'сначала больше версий'],
                        st: ['по состоянию: сначала ошибки и «проверить», в конце готовые', 'по состоянию: сначала готовые, в конце ошибки']};
                    function sortTitle(k) {
                        const t = SORT_TEXT[k.startsWith('st:') ? 'st' : k];
                        const now = sortKey.value === k ? (sortDir.value === 'asc' ? 0 : 1) : -1;
                        return (now >= 0 ? 'Сейчас: ' + t[now] + '. ' : '') + 'Нажмите: ' + t[now === 0 ? 1 : 0];
                    }
                    const sortLabel = computed(() => {
                        const arrow = sortDir.value === 'asc' ? ' ▲' : ' ▼';
                        if (sortKey.value === 'code') return 'по шифру' + arrow;
                        if (sortKey.value === 'versions') return 'по числу версий' + arrow;
                        const st = STAGES.find(x => 'st:' + x.key === sortKey.value);
                        return (st ? 'по этапу «' + st.label + '»' : 'по шифру') + arrow;
                    });
                    let timer = null, alive = true;
                    // Права вошедшего (portal_auth.js): viewer без запусков и очереди.
                    const canOperate = !(root.portalCan && !root.portalCan.operate());
                    const canDownload = !(root.portalCan && !root.portalCan.download('version_comparison'));

                    async function load() {
                        clearTimeout(timer);
                        const oid = props.objectId;
                        loading.value = true;
                        try {
                            const d = await api(`/objects/${enc(oid)}/overview`);
                            if (!alive || oid !== props.objectId) return;
                            data.value = d;
                            await loadQueue();
                            loadedAt.value = new Date().toLocaleTimeString('ru-RU', {hour: '2-digit', minute: '2-digit', second: '2-digit'});
                            error.value = '';
                        } catch (e) { error.value = e.message || String(e); }
                        finally { loading.value = false; }
                        // Идёт расчёт или загрузка — обновлять чаще.
                        const busy = data.value && (data.value.pending_uploads.length
                            || data.value.projects.some(p => p.next.tone === 'run') || (queue.value && queue.value.state === 'running'));
                        if (alive) timer = setTimeout(load, busy ? 5000 : 30000);
                    }

                    const all = computed(() => (data.value ? data.value.projects : []).map(p => {
                        const row = Object.assign({}, p, {mark: mark(p.code || p.title), updated: (p.uploaded_at || '').slice(0, 10).split('-').reverse().join('.')});
                        row.view = {};
                        for (const s of STAGES) row.view[s.key] = stageView(s.key, p.stages[s.key], row);
                        return row;
                    }));
                    // Марка и поиск сужают и счётчики плиток; плитки — фильтр по состоянию.
                    const scoped = computed(() => {
                        const q = query.value.trim().toLowerCase();
                        return all.value.filter(p => (!markFilter.value || p.mark === markFilter.value)
                            && (!q || String(p.title || '').toLowerCase().includes(q)));
                    });
                    const group = p => p.next.tone === 'run' ? 'run' : p.next.tone === 'bad' ? 'bad' : p.next.tone === 'ok' || p.next.tone === 'muted' ? 'ok' : 'act';
                    const counts = computed(() => {
                        const c = {all: scoped.value.length, act: 0, run: 0, bad: 0, ok: 0};
                        for (const p of scoped.value) c[group(p)]++;
                        return c;
                    });
                    const totals = computed(() => ({
                        versions: scoped.value.reduce((s, p) => s + p.versions, 0),
                        transitions: scoped.value.reduce((s, p) => s + Math.max(0, p.versions - 1), 0),
                    }));
                    const marks = computed(() => {
                        const m = {};
                        for (const p of all.value) m[p.mark] = (m[p.mark] || 0) + 1;
                        return Object.keys(m).sort(natural).map(key => ({key, n: m[key]}));
                    });
                    const ORDER = {bad: 0, act: 1, run: 2, ok: 3};
                    // Сортировка столбца этапа по состоянию: ошибки → «проверить» → идёт → не запускалось → готово → нет этапа.
                    const TONE_RANK = {bad: 0, warn: 1, run: 2, todo: 3, ok: 4, off: 5};
                    const rows = computed(() => {
                        const list = scoped.value.filter(p => filter.value === 'all' || group(p) === filter.value);
                        const byCode = (a, b) => natural(a.title, b.title);
                        const key = sortKey.value;
                        const byStage = k => (a, b) => TONE_RANK[a.view[k].tone] - TONE_RANK[b.view[k].tone]
                            || natural(a.view[k].text + ' ' + (a.view[k].note || ''), b.view[k].text + ' ' + (b.view[k].note || '')) || byCode(a, b);
                        const cmp = key.startsWith('st:') && STAGES.some(x => 'st:' + x.key === key) ? byStage(key.slice(3)) : {
                            code: byCode,
                            step: (a, b) => ORDER[group(a)] - ORDER[group(b)] || byCode(a, b),
                            updated: (a, b) => String(b.uploaded_at || '').localeCompare(String(a.uploaded_at || '')) || byCode(a, b),
                            versions: (a, b) => a.versions - b.versions || byCode(a, b),
                        }[key] || byCode;
                        const dir = sortDir.value === 'desc' ? -1 : 1;
                        return list.sort((a, b) => dir * cmp(a, b));
                    });
                    const pct = (a, b) => b ? Math.round(100 * a / b) : 0;

                    // ---------------------------------------------------------- выбор и очередь запуска
                    const SELECTABLE = ['luna', 'series', 'ai', 'report'];
                    const QSTATE = {running: 'идёт', paused: 'на паузе', failed: 'ошибка — процесс остановлен', done: 'выполнена', idle: 'пусто'};
                    const QITEM = {pending: 'ждёт', running: 'идёт', paused: 'пауза', done: 'готово', failed: 'ошибка', skipped: 'пропуск'};
                    const queue = ref(null), queueOpen = ref(false);
                    const sel = root.Vue.reactive({});
                    const selKey = (p, key) => p.project_id + '|' + key;
                    // Спецификации и сводка MD — шаги столбца «ИИ-описание изменений».
                    const COLUMN = {specifications: 'ai', summary: 'ai'};
                    const inQueue = (p, key) => queue.value && queue.value.items.find(it => it.project_id === p.project_id
                        && (it.stage === key || COLUMN[it.stage] === key) && ['pending', 'running', 'paused'].includes(it.state));
                    function queuedAt(p, key) {
                        const it = inQueue(p, key);
                        if (!it) return null;
                        if (it.state !== 'pending') return {short: it.state === 'running' ? 'очередь: идёт' : 'очередь: пауза', text: QITEM[it.state]};
                        const n = queue.value.items.filter(x => x.state === 'pending').indexOf(it) + 1;
                        return {short: 'в очереди №' + n, text: 'ждёт, №' + n};
                    }
                    // Что можно запустить или продолжить; готовое — кнопкой «Заново» в ячейке. ИИ-проверку пар
                    // повторяют галочкой (кнопки «Перепроверить» нет): готовые ответы ИИ берутся из сохранённых.
                    const OPEN = {luna: ['todo', 'paused', 'interrupted', 'done', 'attention', 'skip'], series: ['todo', 'stale', 'paused', 'interrupted', 'failed'],
                        ai: ['todo', 'blocked', 'paused', 'interrupted'], report: ['todo', 'blocked', 'paused', 'interrupted', 'failed']};
                    function canSelect(p, key) {
                        if (!canOperate) return false;  // viewer: запуск по очереди недоступен
                        if (!SELECTABLE.includes(key) || p.versions < 2 || inQueue(p, key)) return false;
                        const st = (p.stages[key] || {}).state;
                        if (!OPEN[key].includes(st)) return false;
                        if (key === 'ai' && st === 'todo' && p.stages.series.changed === 0) return false;
                        return true;
                    }
                    function toggleSel(p, key, on) { if (on) sel[selKey(p, key)] = true; else delete sel[selKey(p, key)]; }
                    function colState(key) {
                        const can = rows.value.filter(p => canSelect(p, key));
                        if (!can.length) return 'none';
                        const n = can.filter(p => sel[selKey(p, key)]).length;
                        return n === 0 ? 'off' : n === can.length ? 'all' : 'some';
                    }
                    function toggleCol(key, on) { for (const p of rows.value) if (canSelect(p, key)) toggleSel(p, key, on); }
                    // Выбор действует только для строк, которые сейчас можно запустить.
                    const selected = computed(() => all.value.flatMap(p => SELECTABLE.filter(k => sel[selKey(p, k)] && canSelect(p, k))
                        .map(k => ({p, key: k}))));
                    const selCount = computed(() => selected.value.length);
                    const selBy = computed(() => Object.fromEntries(SELECTABLE.map(k => [k, selected.value.filter(x => x.key === k).length])));
                    function clearSel() { for (const k of Object.keys(sel)) delete sel[k]; }
                    function runSelected() {
                        if (!canOperate) return;
                        // Порядок — как строки в таблице сейчас; этапы проекта — слева направо.
                        const order = rows.value.flatMap(p => SELECTABLE.filter(k => sel[selKey(p, k)] && canSelect(p, k)).map(k => ({p, key: k})));
                        const ai = order.filter(x => x.key !== 'series').length;
                        const projects = [...new Set(order.map(x => x.p.project_id))].length;
                        const label = k => STAGES.find(s => s.key === k).label;
                        const preview = order.slice(0, 8).map((x, i) => ({text: `${i + 1}. ${x.p.title} · ${label(x.key)}`, muted: true}));
                        if (order.length > 8) preview.push({text: `… и ещё ${order.length - 8}`, muted: true});
                        confirmDialog({title: `Запустить по очереди: ${order.length} этап(ов), проектов ${projects}`,
                            lines: ['Этапы идут по одному: проект за проектом, внутри проекта — слева направо. «ИИ-описание изменений» — это обычные листы, затем полные спецификации и «Сводка изменений (MD)»; ошибка в них останавливает очередь.']
                                .concat(preview)
                                .concat(queue.value && ['paused', 'failed'].includes(queue.value.state)
                                    ? [{text: 'Очередь запуска сейчас остановлена — выбранное встанет в неё и начнётся после «Продолжить очередь».', warn: true}] : [])
                                .concat(ai ? [{text: `С ИИ (проверка пар, описание изменений, PDF-отчёт): ${ai}. Объём каждого видно в окне запуска этого этапа в строке.`, warn: true}] : []),
                            consent: ai ? CONSENT : '', ok: 'Поставить в очередь',
                            run: async () => {
                                queue.value = await api(objPath() + '/stage-queue', {method: 'POST', body: {consent: !!ai,
                                    items: order.map(x => ({project_id: x.p.project_id, stage: x.key}))}});
                                clearSel();
                                queueOpen.value = true;
                            }});
                    }
                    async function loadQueue() {
                        if (!canOperate) return;  // viewer: очередь запуска — служебная (только admin)
                        try { queue.value = await api(objPath() + '/stage-queue'); } catch (e) { /* очередь недоступна — страница работает без неё */ }
                    }
                    async function queueAct(what) {
                        if (!canOperate) return;
                        try { queue.value = await api(objPath() + '/stage-queue/' + what, {method: 'POST', body: {}}); }
                        catch (e) { error.value = e.message || String(e); }
                        load();
                    }
                    async function queueRemove(it) {
                        if (!canOperate) return;
                        try { queue.value = await api(objPath() + '/stage-queue/remove', {method: 'POST', body: {project_id: it.project_id, stage: it.stage}}); }
                        catch (e) { error.value = e.message || String(e); }
                    }

                    // ---------------------------------------------------------- кнопки этапов
                    const {reactive} = root.Vue;
                    const busy = reactive({});
                    const dlg = reactive({open: false, title: '', lines: [], consent: '', agreed: false, ok: 'Запустить',
                        loading: false, busy: false, blocked: false, error: '', run: null});
                    const objPath = () => `/objects/${enc(props.objectId)}`;
                    const post = (path, body) => api(objPath() + path, {method: 'POST', body: body || {}});
                    const B = (kind, icon, label, run, extra) => Object.assign({kind, icon, label, run}, extra || {});
                    const START = (label, run, extra) => B('start', '▶', label, run, Object.assign({primary: true}, extra));
                    const PAUSE = run => B('pause', '⏸', 'Пауза', run, {title: 'Начатое доводится и сохраняется, новое не начинается'});
                    const PAUSING = () => B('pausing', '⏸', 'ставится…', null, {disabled: true, title: 'Ждём, пока доработают уже начатые задачи'});
                    const RESUME = run => B('resume', '▶', 'Продолжить', run, {primary: true, title: 'С того же места: готовое не пересчитывается'});
                    const RESTART = (run, extra) => B('restart', '↻', 'Заново', run, extra);

                    // viewer: из кнопок этапа остаются только скачивания (PDF), и то при праве скачивания.
                    function acts(p, key) {
                        const list = stageActs(p, key);
                        return list.filter(a => (a.href ? canDownload : canOperate));
                    }
                    function stageActs(p, key) {
                        const st = p.stages[key] || {}, state = st.state, pid = enc(p.project_id);
                        const mReady = ['done', 'attention'].includes(p.stages.matrix.state);
                        const sid = p.stages.series.series_id;
                        // Пока ИИ описывает переходы, серию не пересчитывать и отчёт не строить: сводка ещё неполная.
                        const aiBusy = ['running', 'queued', 'pausing'].includes(p.stages.ai.state)
                            ? {disabled: true, title: 'Идёт ИИ-описание изменений — дождитесь окончания или поставьте его на паузу'} : {};
                        if (key === 'profile') {
                            return st.failed ? [B('retry', '↻', 'Повторить', () => Promise.all(st.failed_versions.map(v => post(`/versions/${enc(v)}/profile/retry`))),
                                {primary: true, title: 'Повторить разбор версий с ошибкой'})] : [];
                        }
                        if (key === 'matrix') {
                            if (p.versions < 2 || p.stages.profile.state === 'failed' || p.stages.profile.state === 'running') return [];
                            return ['todo', 'stale'].includes(state) || p.stages.profile.state === 'todo'
                                ? [START('Подготовить', () => post(`/projects/${pid}/prepare`), {title: 'Разобрать версии и сопоставить листы между ними. Нужно после загрузки новых версий или смены их порядка.'})] : [];
                        }
                        if (key === 'luna') {
                            if (state === 'na' || !mReady) return [];
                            const other = st.busy_other ? {disabled: true, title: 'ИИ-проверка пар сейчас идёт по другому проекту объекта — дождитесь её окончания'} : {};
                            if (state === 'running') return [PAUSE(() => post('/ai-sheet-match/pause'))];
                            if (state === 'pausing') return [PAUSING()];
                            if (['paused', 'interrupted'].includes(state)) {
                                return [RESUME(() => post('/ai-sheet-match', {consent: true, project_ids: st.scope || [p.project_id], luna_only: true}))];
                            }
                            if (state === 'todo') return [START('Проверить', () => lunaDialog(p), Object.assign({title: 'ИИ проверит только листы, которые алгоритм не связал уверенно. Другие этапы не запускаются.'}, other))];
                            return [];      // проверить ещё раз — галочка в ячейке и «Запустить выбранные по очереди»
                        }
                        if (key === 'series') {
                            if (state === 'na') return [];
                            const notReady = mReady ? {} : {disabled: true, title: 'Сначала «Подготовить»: сопоставление листов'};
                            if (state === 'todo') return [START('Запустить', () => post(`/projects/${pid}/series`), Object.assign({title: 'Сравнить каждую версию со следующей по изображению листов. ИИ не используется.'}, notReady))];
                            if (state === 'running') return [PAUSE(() => post(`/series/${enc(sid)}/pause`))];
                            if (state === 'pausing') return [PAUSING()];
                            if (['paused', 'interrupted', 'failed'].includes(state)) return [RESUME(() => post(`/series/${enc(sid)}/resume`))];
                            return [RESTART(() => confirmDialog({
                                title: 'Сравнение соседних заново · ' + p.title,
                                lines: ['Все переходы будут пересчитаны по текущему порядку и составу версий.',
                                    {text: 'Пары листов, которые не менялись, берутся из кэша — пересчёт быстрый. ИИ-описание и отчёт после этого запускаются заново.', muted: true}],
                                ok: 'Пересчитать', run: () => post(`/projects/${pid}/series`)}),
                            Object.assign({primary: state === 'stale', title: state === 'stale' ? 'Порядок или состав версий изменился — пересчитать' : 'Пересчитать все переходы заново'}, notReady, aiBusy))];
                        }
                        if (key === 'ai') {
                            if (['na', 'blocked'].includes(state) || !sid) return [];
                            const base = `/series/${enc(sid)}/ai-sheet-changes`;
                            if (['todo', 'done', 'partial'].includes(state) && inQueue(p, 'ai')) return [];      // шаги идут в очереди
                            if (state === 'todo') return p.stages.series.changed === 0 ? [] : [START('Запустить', () => aiDialog(p, false),
                                {title: 'ИИ опишет изменения на листах, затем сравнит полные спецификации и соберёт «Сводку изменений (MD)».'})];
                            if (state === 'running') return [PAUSE(() => post(base + '/pause'))];
                            if (state === 'pausing') return [PAUSING()];
                            if (state === 'queued') return [B('dequeue', '✕', 'Из очереди', () => post(base + '/cancel'), {title: 'Убрать проект из очереди ИИ-описаний'})];
                            if (['paused', 'interrupted'].includes(state)) return [RESUME(() => post(base + '/resume'))];
                            return [RESTART(() => aiDialog(p, true), {title: 'Описать заново: обычные листы, спецификации и сводку. Листы без изменений берутся из сохранённых ответов.'})];
                        }
                        if (key === 'report') {
                            const r = st.md_report;
                            if (state === 'blocked' || !sid) return [];
                            if (state === 'todo') return [START('Сформировать PDF-отчёт', () => reportDialog(p, false),
                                Object.assign({title: 'ИИ подготовит короткий PDF-отчёт по «Сводке изменений (MD)».'}, aiBusy))];
                            if (state === 'running') return [PAUSE(() => post(`/md-reports/${enc(r.report_id)}/pause`))];
                            if (state === 'pausing') return [PAUSING()];
                            if (['paused', 'interrupted', 'failed'].includes(state)) {
                                return [RESUME(() => post(`/md-reports/${enc(r.report_id)}/resume`)),
                                    RESTART(() => reportDialog(p, true), Object.assign({title: 'Сформировать PDF-отчёт с начала по текущей сводке'}, aiBusy))];
                            }
                            if (state === 'done') {
                                return [B('pdf', '⤓', 'PDF', null, {href: `${root.VersionComparison.API}${objPath()}/md-reports/${enc(r.report_id)}/report.pdf`, title: 'Скачать готовый PDF-отчёт'}),
                                    RESTART(() => reportDialog(p, true), Object.assign({title: 'Сформировать PDF-отчёт заново по текущей сводке'}, aiBusy))];
                            }
                        }
                        return [];
                    }

                    async function act(p, key, a) {
                        if (!canOperate) return;
                        const k = p.project_id + key;
                        busy[k] = true;
                        try { await a.run(); } catch (e) { error.value = e.message || String(e); }
                        finally { busy[k] = false; }
                        load();
                    }
                    function confirmDialog(o) {
                        Object.assign(dlg, {open: true, title: o.title, lines: (o.lines || []).map(l => typeof l === 'string' ? {text: l} : l),
                            consent: o.consent || '', agreed: false, ok: o.ok || 'Запустить', loading: !!o.load, busy: false,
                            blocked: false, error: '', run: o.run});
                        if (o.load) {
                            o.load().then(extra => {
                                dlg.lines = dlg.lines.concat((extra.lines || []).map(l => typeof l === 'string' ? {text: l} : l));
                                dlg.blocked = !!extra.blocked;
                            }).catch(e => { dlg.error = e.message || String(e); dlg.blocked = true; })
                              .finally(() => { dlg.loading = false; });
                        }
                    }
                    async function dlgOk() {
                        dlg.busy = true;
                        try { await dlg.run(); dlg.open = false; load(); }
                        catch (e) { dlg.error = e.message || String(e); }
                        finally { dlg.busy = false; }
                    }
                    const CONSENT = 'Подтверждаю передачу материалов исполнителю (Codex на подписке сервера)';
                    function lunaDialog(p) {
                        confirmDialog({title: 'ИИ-проверка сомнительных пар · ' + p.title,
                            lines: ['ИИ смотрит листы «⁇» с кандидатами и пары, в которых алгоритм не уверен. Пара ставится сама только при уверенном ответе, подтверждённом встречной проверкой; остальное — подсказки в матрице.',
                                {text: 'Другие этапы не запускаются. Готовые ответы ИИ берутся из сохранённых. Ручные правки в матрице главнее.', muted: true}],
                            consent: CONSENT, ok: 'Проверить',
                            load: async () => {
                                const j = await api(objPath() + '/ai-sheet-match');
                                const c = j.settings || {};
                                return {lines: [{text: `Модель ${c.model || '?'} · усилие ${c.effort || '?'} · одновременно ${c.parallel || '?'}`, muted: true}]
                                    .concat(j.enabled === false ? [{text: 'ИИ в «Сравнении версий» выключен на сервере.', warn: true}] : []), blocked: j.enabled === false};
                            },
                            run: () => post('/ai-sheet-match', {consent: true, project_ids: [p.project_id], luna_only: true})});
                    }
                    function aiDialog(p, again) {
                        const base = `/series/${enc(p.stages.series.series_id)}/ai-sheet-changes`;
                        const q = queue.value, busyQ = q && q.items.some(it => ['pending', 'running', 'paused'].includes(it.state));
                        confirmDialog({title: (again ? 'ИИ-описание изменений заново · ' : 'ИИ-описание изменений · ') + p.title,
                            lines: ['По порядку: обычные листы → полные спецификации → «Сводка изменений (MD)».']
                                .concat(again ? [{text: 'Новые описания заменят текущие. Листы, которые не менялись, берутся из сохранённых ответов без обращения к ИИ.', muted: true}] : [])
                                .concat(q && ['paused', 'failed'].includes(q.state) ? [{text: 'Очередь запуска сейчас остановлена — проект встанет в неё и начнётся после «Продолжить очередь».', warn: true}]
                                    : busyQ ? [{text: 'В очереди запуска уже идут другие этапы — этот начнётся после них.', warn: true}] : []),
                            consent: CONSENT, ok: again ? 'Описать заново' : 'Запустить',
                            load: async () => {
                                const e = await api(objPath() + base + '/estimate');
                                const withSheets = e.pairs.filter(x => x.sheets).length;
                                const lines = [`Переходов с изменениями: ${withSheets} из ${e.pairs.length} · листов в модель: ${e.sheets} · обращений ${e.calls_primary}, не больше ${e.calls_max}.`,
                                    {text: `${(e.executor || {}).model || '?'}, усилие ${(e.executor || {}).effort || '?'} · одновременно до ${e.parallel} листов`, muted: true},
                                    {text: e.transfer, muted: true}];
                                if (e.queue && e.queue.busy) lines.push({text: `Очередь ИИ-описаний занята${e.queue.current ? ' («' + e.queue.current + '»)' : ''} — проект встанет в очередь и начнётся сам.`, warn: true});
                                if (!e.enabled) lines.push({text: 'ИИ-описания на этой установке выключены.', warn: true});
                                return {lines, blocked: !e.enabled || !e.sheets};
                            },
                            run: async () => {
                                queue.value = await post('/stage-queue', {consent: true, items: [{project_id: p.project_id, stage: 'ai', restart: !!again}]});
                            }});
                    }
                    function reportDialog(p, again) {
                        confirmDialog({title: (again ? 'PDF-отчёт заново · ' : 'PDF-отчёт · ') + p.title,
                            lines: ['Короткий аналитический PDF по «Сводке изменений (MD)» проекта.',
                                {text: 'Готовые части прошлых отчётов с тем же содержанием используются повторно.', muted: true}],
                            consent: CONSENT, ok: again ? 'Сформировать заново' : 'Сформировать',
                            load: async () => {
                                const r = await api('/md-reports/runtime');
                                return {lines: [{text: `${(r.executor || {}).model || '?'} · ${r.transfer || ''}`, muted: true}]
                                    .concat(r.enabled === false ? [{text: 'ИИ-обработка выключена на сервере.', warn: true}] : []), blocked: r.enabled === false};
                            },
                            run: () => post(`/series/${enc(p.stages.series.series_id)}/md-report`, {consent: true, reuse: true})});
                    }
                    function open(p) { emit('open', p.project_id); }
                    function downloadMd(p) {
                        const sid = p.stages.series.series_id;
                        if (!sid) return;
                        const a = document.createElement('a');
                        a.href = `${root.VersionComparison.API}/objects/${enc(props.objectId)}/series/${enc(sid)}/ai-sheet-changes/summary.md`;
                        a.download = '';
                        document.body.appendChild(a); a.click(); a.remove();
                    }

                    watch(() => props.objectId, () => { clearSel(); queue.value = null; });
                    watch(() => [props.objectId, props.reloadKey], () => { data.value = data.value && data.value.object_id === props.objectId ? data.value : null; load(); });
                    onMounted(load);
                    onBeforeUnmount(() => { alive = false; clearTimeout(timer); });
                    return {canOperate, canDownload, stages: STAGES, filters: FILTERS, data, loading, error, loadedAt, filter, query, markFilter, sortKey, sortDir, sortSel, sortLabel, SORT_PRESETS, toggleSort, sortArrow, sortTitle,
                        counts, totals, marks, rows, pct, open, downloadMd, load, acts, act, busy, dlg, dlgOk,
                        SELECTABLE, QSTATE, QITEM, queue, queueOpen, sel, selKey, queuedAt, canSelect, toggleSel, colState, toggleCol,
                        selCount, selBy, clearSel, runSelected, queueAct, queueRemove};
                },
            });
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
