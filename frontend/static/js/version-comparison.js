(function (root) {
    'use strict';
    // Раздел «Сравнение версий»: проекты сверху вниз, листы по строкам,
    // версии по столбцам, выбор пары A → B и наложение. План:
    // docs/version_comparison_plan.md. Логика живёт здесь, в app.js — только
    // маршрут и регистрация компонента.
    const API = '/api/version-comparison';

    const STATUS = {
        same: {label: 'Совпадает', icon: '✓', cls: 'same'},
        same_zones_changed: {label: 'Совпадает, изменены служебные зоны', icon: '◐', cls: 'zones'},
        different: {label: 'Есть отличия', icon: '●', cls: 'diff'},
        needs_review: {label: 'Нужна проверка', icon: '⚠', cls: 'review'},
        added: {label: 'Лист добавлен', icon: '+', cls: 'added'},
        removed: {label: 'Лист удалён', icon: '−', cls: 'removed'},
        unmatched: {label: 'Не сопоставлен', icon: '⁇', cls: 'review'},
    };
    const CHANNEL_LABELS = {
        geometry: 'графика', color: 'цвет и оформление линий', text: 'текст', text_coverage: 'читаемость надписей',
        zones: 'служебные зоны', annotations: 'PDF-пометки', warnings: 'предупреждения',
        spec_positions: 'позиции СО',
    };

    function statusInfo(code) {
        return STATUS[code] || {label: code || '—', icon: '·', cls: 'none'};
    }

    async function api(path, options = {}) {
        const opts = Object.assign({headers: {}}, options);
        if (opts.body && typeof opts.body !== 'string' && !(opts.body instanceof FormData)) {
            opts.body = JSON.stringify(opts.body);
            opts.headers['Content-Type'] = 'application/json';
        }
        const res = await fetch(API + path, opts);
        let data = null;
        try { data = await res.json(); } catch (e) { data = null; }
        if (!res.ok) {
            const msg = (data && (data.detail || data.message)) || ('Ошибка ' + res.status);
            const error = new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
            error.status = res.status;
            throw error;
        }
        return data;
    }

    function uploadFile(objectId, uploadId, file, relPath, onProgress) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', `${API}/objects/${encodeURIComponent(objectId)}/uploads/${uploadId}/files`);
            xhr.upload.onprogress = e => { if (e.lengthComputable) onProgress(e.loaded); };
            xhr.onload = () => {
                let data = null;
                try { data = JSON.parse(xhr.responseText); } catch (e) { data = null; }
                if (xhr.status >= 200 && xhr.status < 300) resolve(data);
                else reject(new Error((data && data.detail) || ('Ошибка загрузки ' + xhr.status)));
            };
            xhr.onerror = () => reject(new Error('Сеть недоступна'));
            const fd = new FormData();
            fd.append('file', file, file.name);
            fd.append('relative_path', relPath || file.name);
            xhr.send(fd);
        });
    }

    const PAGE_TEMPLATE = `
<div class="vc-page">
  <div class="vc-head">
    <div>
      <h2 class="vc-title">Сравнение версий</h2>
    </div>
    <div class="vc-head-actions">
      <label class="vc-field">Объект
        <select v-model="objectId" @change="loadCatalog">
          <option value="">— выберите объект —</option>
          <option v-for="o in objects" :key="o.id" :value="o.id">{{ o.name }}{{ o.versions ? ' · версий: ' + o.versions : '' }}</option>
        </select>
      </label>
      <button class="btn btn-primary" :disabled="!objectId" @click="openUpload">⇪ Загрузить проект со всеми версиями</button>
    </div>
  </div>
  <vc-batch-upload ref="batchUpload" v-if="objectId" :key="objectId" :object-id="objectId" @updated="loadCatalog"></vc-batch-upload>
  <div v-if="error" class="vc-error" @click="error=''">{{ error }}</div>
  <div v-if="!objectId" class="vc-empty">Выберите объект. Версии привязываются к объекту, но хранятся отдельно от проектов аудита.</div>
  <div v-else-if="catalogLoading" class="vc-empty">Загрузка каталога…</div>
  <div v-else-if="!projects.length" class="vc-empty">В объекте ещё нет версий. Нажмите «Загрузить проект со всеми версиями» и выберите папку с PDF.</div>

  <section v-for="p in projects.filter(p => p.project_id === selectedProject)" :key="p.project_id" class="vc-project">
    <header class="vc-project-head" @click="toggleProject(p)">
      <span class="vc-caret" :title="expanded[p.project_id] && !topHidden[p.project_id] ? 'Свернуть выбор пары и сводку переходов' : 'Развернуть'">{{ expanded[p.project_id] && !topHidden[p.project_id] ? '▼' : '▶' }}</span>
      <select class="vc-project-picker" aria-label="Загруженные проекты" v-model="selectedProject" @click.stop @change="selectProject">
        <option v-for="project in sortedProjects()" :key="project.project_id" :value="project.project_id">{{ project.title }}</option>
      </select>
      <span class="vc-muted">· версий: {{ p.versions.length }}</span>
      <span class="vc-spacer"></span>
      <template v-if="linkDraft(p)">
        <button class="btn btn-primary btn-sm" :disabled="linksBusy(p)" @click.stop="applyLinks(p)">{{ linksBusy(p) ? 'Применение…' : 'Применить сопоставления (' + linkDraft(p).edits.length + ')' }}</button>
        <button class="btn btn-outline btn-sm" :disabled="linksLocked(p)" @click.stop="undoEdit(p)">Отменить правку</button>
      </template>
      <template v-if="expanded[p.project_id] && !topHidden[p.project_id] && matrices[p.project_id]?.state === 'ready'">
              <template v-if="pairReady(p) && pairRun(p) && pairRun(p).state !== 'done'">
                <button class="btn btn-outline btn-sm" title="Расчёт этой пары идёт или прерван — показать его ход" @click.stop="pickRun(p, pairRun(p).run_id)">Расчёт этой пары: {{ runStateLabel(pairRun(p)) }}</button>
              </template>
              <button class="btn btn-sm" :class="pairReady(p) && pairRun(p) ? 'btn-outline' : 'btn-primary'" :disabled="!pairReady(p) || runBusy(p)" @click.stop="startRun(p, 'all')">{{ pairReady(p) && pairRun(p) ? 'Пересчитать' : 'Сравнить все сопоставленные листы' }}</button>
              <button class="btn btn-accent btn-sm" :disabled="!pairReady(p) || !manual(p).length || runBusy(p)" @click.stop="startRun(p, 'manual')">Сравнить выбранные листы ({{ manual(p).length }})</button>
      </template>
      <button class="btn btn-outline btn-sm" :disabled="linksBusy(p) || seriesBusy(p) || p.versions.length < 2" title="Сравнить каждую версию со следующей: (0)→(1), (1)→(2)…; выбранные сопоставления применятся перед расчётом" @click.stop="startSeries(p)">Сравнить все соседние</button>
      <button class="btn btn-outline btn-sm" :disabled="!!linkDraft(p) || linksBusy(p)" @click.stop="openOrder(p)">Порядок версий</button>
    </header>
    <div v-if="expanded[p.project_id]" class="vc-project-body">
      <template v-for="m in [matrices[p.project_id]]" :key="'m' + p.project_id">
        <div v-if="!m || m.state === 'loading'" class="vc-empty">Загрузка…</div>
        <div v-else-if="m.state === 'profiling'" class="vc-empty">Разбор версий: готово {{ m.progress ? m.progress.ready : 0 }} из {{ m.progress ? m.progress.total : '?' }}…</div>
        <div v-else-if="m.state === 'building'" class="vc-empty">Сопоставление листов между версиями…</div>
        <div v-else-if="m.state === 'failed'" class="vc-error">Не удалось подготовить версии: {{ m.error || failedVersions(m) }}</div>
        <template v-else-if="m.state === 'ready'">
          <template v-if="!topHidden[p.project_id]">
          <div v-if="manual(p).length || pending[p.project_id] || !pairReady(p)" class="vc-pairbar">
            <div class="vc-pair-actions">
              <span v-for="(mp, i) in manual(p)" :key="i" class="vc-manual-chip">стр.{{ mp.a_page }} ↔ стр.{{ mp.b_page }} <button @click="manual(p).splice(i, 1)">×</button></span>
              <span v-if="pending[p.project_id]" class="vc-hint">выбрана стр.{{ pending[p.project_id].page }} в A — Shift+щелчок по листу в столбце B</span>
            </div>
            <div v-if="!pairReady(p)" class="vc-hint">Выберите две разные версии.</div>
          </div>

          <div v-if="seriesOf(p)" class="vc-series">
            <template v-for="s in [seriesOf(p)]" :key="s.series_id">
              <div class="vc-series-head">
                <b>Сводка переходов</b>
                <span class="vc-run-state" :class="'st-' + s.state">{{ runStateLabel(s) }}</span>
                <span class="vc-muted">переходов готово: {{ s.progress.done }} из {{ s.progress.total }}</span>
                <button v-if="['queued','running'].includes(s.state)" class="btn btn-danger-outline btn-sm" @click="cancelSeries(p, s)">Отменить</button>
                <button v-if="seriesMatchesOrder(p, s) && ['interrupted','cancelled','failed'].includes(s.state)" class="btn btn-outline btn-sm" @click="resumeSeries(p, s)">Досчитать</button>
                <a v-if="s.state === 'done'" class="btn btn-outline btn-sm" :href="seriesReportUrl(s)">Excel сводки</a>
              </div>
              <div v-if="!seriesMatchesOrder(p, s)" class="vc-hint">Сохранённая сводка для прежнего порядка версий.</div>
              <table class="vc-series-table">
                <thead><tr><th>Переход A → B</th><th>Ход расчёта</th><th>Листов</th><th title="Совпадает">✓</th><th title="Изменены только служебные зоны">◐</th><th title="Есть отличия">●</th><th title="Нужна проверка">⚠</th><th>Состав</th><th>Вывод</th><th></th></tr></thead>
                <tbody>
                  <tr v-for="(pr, i) in s.pairs" :key="i" :class="{cur: isOpenTransition(p, pr)}">
                    <td class="vc-tr-name">{{ pr.a_label }} → {{ pr.b_label }}</td>
                    <td class="vc-tr-progress" :title="pairStateLabel(pr)">
                      <span class="vc-progress" :class="'st-' + (pr.run_state || pr.state)"><span :style="{width: progressPct(pr) + '%'}"></span></span>
                      <span class="vc-muted"><template v-if="pr.progress">{{ pr.progress.done }} из {{ pr.progress.total }}</template><template v-else>{{ pairStateLabel(pr) }}</template></span>
                      <span v-if="pr.progress && pr.progress.failed" class="vc-error">ошибок: {{ pr.progress.failed }}</span>
                    </td>
                    <template v-if="pr.summary && pr.summary.compared != null">
                      <td>{{ pr.summary.compared }}</td><td>{{ pr.summary.same }}</td><td>{{ pr.summary.zones_only }}</td>
                      <td>{{ pr.summary.different }}</td><td>{{ pr.summary.needs_review }}</td>
                      <td>+{{ pr.summary.added }} / −{{ pr.summary.removed }}</td>
                      <td class="vc-tr-verdict">{{ pr.summary.verdict.join('; ') }}</td>
                    </template>
                    <td v-else colspan="7" class="vc-muted">{{ pairStateLabel(pr) }}<template v-if="pr.progress"> · {{ pr.progress.done }} из {{ pr.progress.total }}</template></td>
                    <td><button v-if="pr.run_id" class="btn btn-outline btn-xs" @click="openTransition(p, pr)">Открыть</button></td>
                  </tr>
                </tbody>
              </table>
            </template>
          </div>

          <div v-if="aiSeriesEst[p.project_id] || (aiSeriesOf(p) && aiSeriesOf(p).state !== 'none')" class="vc-series vc-ai-series">
            <template v-if="aiSeriesEst[p.project_id]">
              <template v-for="e in [aiSeriesEst[p.project_id]]" :key="'est'">
                <div class="vc-series-head"><b>ИИ-описание переходов: подтверждение</b>
                  <span class="vc-muted">{{ e.executor.model }}, усилие {{ e.executor.effort }} · одновременно до {{ e.parallel }} листов</span></div>
                <div>Переходов с изменениями: <b>{{ e.pairs.filter(x => x.sheets).length }}</b> из {{ e.pairs.length }} · листов в модель: <b>{{ e.sheets }}</b>
                  · обращений {{ e.calls_primary }}, не больше {{ e.calls_max }} (дозапрос и повторы). Листы без изменений содержания в модель не отправляются.</div>
                <div v-if="e.pairs.some(x => x.added || x.removed)" class="vc-muted">В том числе листов без пары (по одному обращению «что на листе»): добавлено {{ e.pairs.reduce((n, x) => n + x.added, 0) }}, удалено {{ e.pairs.reduce((n, x) => n + x.removed, 0) }}.</div>
                <div v-for="x in e.pairs.filter(x => !x.sheets)" :key="'s' + x.index" class="vc-muted">{{ x.a_label }} → {{ x.b_label }}: {{ x.reason }}</div>
                <div class="vc-muted">{{ e.transfer }}</div>
                <div v-if="!e.enabled" class="vc-error">Запуск ИИ-описаний на этой установке выключен.</div>
                <label><input type="checkbox" v-model="aiSeriesConsent[p.project_id]"> Подтверждаю передачу листов исполнителю</label>
                <div class="vc-ai-series-actions">
                  <button class="btn btn-primary btn-sm" :disabled="!aiSeriesConsent[p.project_id] || !e.enabled || !e.sheets" @click="startAiSeries(p)">Запустить</button>
                  <button class="btn btn-outline btn-sm" @click="aiSeriesEst[p.project_id] = null">Отмена</button>
                </div>
              </template>
            </template>
            <template v-else-if="aiSeriesOf(p)">
              <template v-for="a in [aiSeriesOf(p)]" :key="'ai'">
                <div class="vc-series-head">
                  <b>ИИ-описание переходов</b>
                  <span class="vc-run-state" :class="'st-' + a.state">{{ aiSeriesStateLabel(a.state) }}</span>
                  <span class="vc-muted">листов готово: {{ a.total.sheets_done }} из {{ a.total.sheets }}<template v-if="a.total.sheets_failed"> · ошибок {{ a.total.sheets_failed }}</template>
                    · время {{ fmtDuration(a.wall_s) }} · обращений {{ a.total.calls }} · токены: вход {{ fmtNum(a.total.input_tokens) }}, выход {{ fmtNum(a.total.output_tokens) }}</span>
                  <button v-if="a.state === 'running'" class="btn btn-danger-outline btn-sm" @click="cancelAiSeries(p)">Отменить</button>
                  <button v-if="['interrupted', 'paused'].includes(a.state)" class="btn btn-outline btn-sm" @click="resumeAiSeries(p)">Досчитать</button>
                </div>
                <table class="vc-series-table">
                  <thead><tr><th>Переход A → B</th><th>Ход описания</th><th>Листов</th><th>Изм.</th><th>Вопр.</th><th>Ошибок</th><th>Обращ.</th><th>Вход</th><th>Выход</th><th>Время</th><th></th></tr></thead>
                  <tbody>
                    <template v-for="x in a.pairs" :key="x.index">
                    <tr>
                      <td class="vc-tr-name">{{ x.a_label }} → {{ x.b_label }}</td>
                      <template v-if="x.job_id">
                        <td class="vc-tr-progress">
                          <span class="vc-progress" :class="'st-' + x.state"><span :style="{width: (x.sheets_total ? Math.round(100 * x.sheets_done / x.sheets_total) : 0) + '%'}"></span></span>
                          <span class="vc-muted">{{ x.sheets_done }} из {{ x.sheets_total }}<template v-if="x.running"> · идёт {{ x.running }}</template> · {{ aiSeriesStateLabel(x.state) }}</span>
                        </td>
                        <td>{{ x.sheets_total }}</td><td>{{ x.changes }}</td><td>{{ x.unresolved }}</td>
                        <td :class="{'vc-error': x.sheets_failed}">{{ x.sheets_failed || '' }}</td>
                        <td>{{ x.calls }}</td><td>{{ fmtNum(x.input_tokens) }}</td><td>{{ fmtNum(x.output_tokens) }}</td>
                        <td>{{ fmtDuration(pairWall(x)) }}</td>
                      </template>
                      <td v-else colspan="9" class="vc-muted">{{ x.reason }}</td>
                      <td><button v-if="x.run_id && x.job_id" class="btn btn-outline btn-xs" @click="openTransitionAi(p, x)">Открыть</button></td>
                    </tr>
                    <tr v-if="x.singles && x.singles.length" class="vc-ai-singles">
                      <td colspan="11">
                        <div v-for="sg in x.singles" :key="sg.spid" class="vc-ai-single">
                          <span class="vc-ai-single-kind" :class="'k-' + sg.kind">{{ sg.label }}</span>
                          <b>{{ sg.title || 'Лист' }}</b> <span class="vc-muted">· {{ sg.side === 'B' ? x.b_label : x.a_label }}, стр. {{ sg.page }}</span>
                          <template v-if="sg.description"> — <i>{{ sg.description.sheet_kind }}</i>. {{ sg.description.summary }}
                            <span v-if="sg.description.marks.length" class="vc-muted"> Марки: {{ sg.description.marks.slice(0, 12).join(', ') }}<template v-if="sg.description.marks.length > 12"> …</template></span>
                            <span v-if="sg.description.limitation" class="vc-warning"> Неясно: {{ sg.description.limitation }}</span>
                          </template>
                          <span v-else class="vc-muted"> — {{ aiSeriesStateLabel(sg.state) }}</span>
                          <button class="btn btn-outline btn-xs" @click="showSingle(x, sg)">Открыть</button>
                        </div>
                      </td>
                    </tr>
                    </template>
                  </tbody>
                </table>
                <div class="vc-muted">Описание каждого листа — в просмотрщике пары, окно «Изменения в листе».</div>
              </template>
            </template>
          </div>
          </template>

          <div v-if="activeRun[p.project_id]" class="vc-runbar">
            <template v-for="run in [activeRun[p.project_id]]" :key="run.run_id">
              <span><b>{{ run.a.label || run.a.file_name }}</b> → <b>{{ run.b.label || run.b.file_name }}</b> · {{ run.scope === 'manual' ? 'выбранные листы' : 'все листы' }}</span>
              <span class="vc-run-state" :class="'st-' + run.state">{{ runStateLabel(run) }}</span>
              <span v-if="run.progress" class="vc-progress"><span :style="{width: progressPct(run) + '%'}"></span></span>
              <span v-if="run.progress" class="vc-muted">{{ run.progress.done }} из {{ run.progress.total }}</span>
              <span v-if="run.compute_worker && ['queued','running','interrupted'].includes(run.state)" class="vc-muted">
                <template v-if="!run.compute_worker.online">Вычислитель недоступен — очередь сохранена</template>
                <template v-else-if="run.compute_worker.waiting_reason">Новые листы ждут свободной памяти; готовые результаты сохранены</template>
              </span>
              <template v-if="run.state === 'done'">
                <button v-if="!reportOf(run) || reportOf(run).state === 'none' || reportOf(run).state === 'failed'" class="btn btn-outline btn-sm" title="Excel-реестр и HTML-отчёт (архив, открывается без сети)" @click="buildReports(p, run)">Отчёты</button>
                <span v-else-if="reportOf(run).state === 'building'" class="vc-muted">отчёты строятся…</span>
                <template v-else-if="reportOf(run).state === 'ready'">
                  <a class="btn btn-outline btn-sm" :href="reportUrl(run, 'report.xlsx')">Excel</a>
                  <a class="btn btn-outline btn-sm" :href="reportUrl(run, 'report_html.zip')">HTML (ZIP)</a>
                </template>
                <span v-if="reportOf(run) && reportOf(run).state === 'failed'" class="vc-error" :title="reportOf(run).error">ошибка отчёта</span>
              </template>
              <button v-if="['queued','running'].includes(run.state)" class="btn btn-danger-outline btn-sm" @click="cancelRun(p, run)">Отменить</button>
              <button v-if="['interrupted','cancelled'].includes(run.state)" class="btn btn-outline btn-sm" @click="resumeRun(p, run)">Досчитать</button>
            </template>
            <button class="btn btn-primary btn-sm" :disabled="!firstDoneItem(activeRun[p.project_id])" title="Открыть наложение листов выбранного прогона" @click="openActive(p)">Открыть сравнение</button>
            <button class="btn btn-outline btn-sm" :disabled="!aiSeriesReady(p) || aiSeriesBusy(p)"
                    :title="aiSeriesReady(p) ? 'Описать изменения всех соседних переходов (1)→(2) … до последнего: в ИИ уходят только листы с изменениями содержания; ход — в блоке «ИИ-описание переходов»' : 'Сначала «Сравнить все соседние» — ИИ описывает посчитанные переходы серии'"
                    @click="describeAll(p)">Описать изменения ИИ</button>
          </div>

          <div class="vc-matrix-wrap" v-fit-bottom>
            <table class="vc-matrix" :ref="el => matrixElements[p.project_id] = el" :inert="linksLocked(p) || undefined" :aria-busy="linksBusy(p)">
              <thead>
                <tr>
                  <th class="vc-rowhead">Лист \\ версия
                    <label class="vc-only-changed" :class="{off: !pairHasRun(p)}" :title="pairHasRun(p) ? 'Скрыть листы без изменений в выбранной паре A → B (совпадает / изменены только служебные зоны)' : 'Для выбранной пары A → B нет прогона «все листы» — выберите его в меню прогонов'">
                      <input type="checkbox" v-model="onlyChanged[p.project_id]" :disabled="!pairHasRun(p)"> только изменения
                      <span v-if="onlyChanged[p.project_id] && pairHasRun(p)" class="vc-muted">({{ visibleRows(p, m).length }} из {{ m.rows.length }})</span>
                    </label>
                  </th>
                  <th v-for="v in m.versions" :key="v.version_id" :class="{'col-a': v.version_id === pairs[p.project_id].a, 'col-b': v.version_id === pairs[p.project_id].b}">
                    <div class="vc-colhead">
                      <div class="vc-col-label" :title="colTitle(v)">{{ v.user_label || v.file_name }}
                      </div>
                      <div class="vc-col-pick">
                        <button class="btn btn-outline btn-xs" :class="{'vc-pick-a': v.version_id === pairs[p.project_id].a}" :aria-pressed="v.version_id === pairs[p.project_id].a" @click="setSide(p, 'a', v.version_id)">A</button>
                        <button class="btn btn-outline btn-xs" :class="{'vc-pick-b': v.version_id === pairs[p.project_id].b}" :aria-pressed="v.version_id === pairs[p.project_id].b" @click="setSide(p, 'b', v.version_id)">B</button>
                      </div>
                    </div>
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in visibleRows(p, m)" :key="row.row_id" :data-row-id="row.row_id" :style="row.draftHeight ? {height: row.draftHeight + 'px'} : undefined">
                  <th class="vc-rowhead" :title="row.display_title">
                    <div class="vc-row-title">{{ row.display_title }}</div>
                    <div class="vc-row-status" v-if="rowItem(p, row)">
                      <button class="vc-badge" :class="'b-' + statusInfo(rowItem(p, row).result_status).cls" @click="openViewer(p, rowItem(p, row))" :disabled="rowItem(p, row).kind !== 'compare' || rowItem(p, row).status !== 'done'">
                        {{ statusInfo(rowItem(p, row).result_status || rowItem(p, row).status).icon }} {{ itemLabel(rowItem(p, row)) }}
                      </button>
                    </div>
                  </th>
                  <td v-for="v in m.versions" :key="v.version_id" class="vc-cell"
                      :class="cellClass(p, row, v)"
                      @dragover.prevent @drop="dropOnCell(p, row, v, $event)">
                    <template v-for="c in [row.cells[v.version_id]]" :key="'c' + row.row_id + v.version_id">
                      <div v-if="c.state === 'matched'" class="vc-thumb" draggable="true"
                           @dragstart="dragStart(v.version_id, c.page, $event)"
                           @click="clickCell(p, row, v, c, $event)"
                           :title="cellTitle(c)">
                        <img loading="lazy" decoding="async" :src="thumbUrl(v, c.page)" alt="">
                        <div class="vc-thumb-meta">стр.{{ c.page }}<span v-if="c.sheet_no"> · л.{{ c.sheet_no }}</span>
                          <span v-if="c.confidence === 'medium'" class="vc-flag" title="Сопоставлено с невысокой уверенностью">?</span>
                          <span v-if="c.manual" class="vc-flag" title="Связь задана вручную">✎</span>
                          <span v-if="c.dups && c.dups.length" class="vc-flag" :title="'Дубль листа: стр. ' + c.dups.join(', ')">⧉</span>
                          <span v-if="c.scan" class="vc-flag vc-scan" title="Скан: страница — картинка, текст не извлекается и не сравнивается; сравнивается только изображение">скан</span>
                          <span v-if="partOf(m, v.version_id, c.page)" class="vc-flag vc-part" :title="partTitle(partOf(m, v.version_id, c.page))">◫</span>
                          <span v-if="partsFrom(m, v.version_id, c.page).length" class="vc-flag vc-part" :title="'Лист разделён в следующей версии: стр. ' + partsFrom(m, v.version_id, c.page).map(x => x.b_page).join(', ')">⇉</span>
                        </div>
                        <button class="vc-cell-menu" title="Отвязать в отдельную строку" @click.stop="detach(p, v, c)">⤫</button>
                      </div>
                      <div v-else-if="c.state === 'unmatched'" class="vc-empty-cell unmatched" :title="c.reason">
                        <div>⁇ не сопоставлен</div>
                        <div class="vc-cands">кандидаты:
                          <button v-for="cand in c.candidates" :key="cand.page" class="btn btn-outline btn-xs" :disabled="!row.anchor" @click="attach(p, row, v, cand.page)" :title="'оценка ' + cand.score">стр.{{ cand.page }}</button>
                        </div>
                        <button class="btn btn-outline btn-xs" :disabled="!row.anchor" @click="markAbsent(p, row, v)">листа нет</button>
                      </div>
                      <div v-else class="vc-empty-cell absent" :title="c.reason">∅ нет в версии<span v-if="c.manual"> ✎</span></div>
                    </template>
                  </td>
                </tr>
                <tr v-if="m.unassigned && Object.keys(m.unassigned).length" class="vc-unassigned">
                  <th class="vc-rowhead">Несвязанные страницы</th>
                  <td v-for="v in m.versions" :key="v.version_id" class="vc-cell">
                    <div v-for="pg in (m.unassigned[v.version_id] || [])" :key="pg" class="vc-thumb small" draggable="true" @dragstart="dragStart(v.version_id, pg, $event)">
                      <img loading="lazy" :src="thumbUrl(v, pg)" alt=""><div class="vc-thumb-meta">стр.{{ pg }}</div>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-if="linkDraft(p) || (m.edits && m.edits.length)" class="vc-legend">
            <span v-if="linkDraft(p)">Выбрано правок: {{ linkDraft(p).edits.length }}. Примените сопоставления или нажмите кнопку сравнения после всех правок.</span>
            <button class="btn btn-outline btn-xs" :disabled="linksLocked(p)" @click="undoEdit(p)">Отменить последнюю правку</button>
          </div>
        </template>
      </template>
    </div>
  </section>

  <div v-if="upload.open" class="modal-overlay" @click.self="closeUpload">
    <div class="modal vc-modal">
      <div class="vc-modal-head"><h3>Загрузка проекта и версий</h3><button class="btn btn-outline btn-sm" @click="closeUpload">✕</button></div>
      <div v-if="upload.step === 1">
        <button class="btn btn-outline" :disabled="upload.busy" @click="upload.open=false; batchUpload.openNew()">Загрузить несколько проектов из папок</button>
        <label class="vc-field">Куда загрузить
          <select v-model="upload.target" :disabled="upload.busy">
            <option value="">Новый проект со всеми версиями</option>
            <option v-for="p in sortedProjects()" :key="p.project_id" :value="p.project_id">Добавить версии в «{{ p.title }}»</option>
          </select>
        </label>
        <p class="vc-muted">Выберите папку с PDF (вложенные папки тоже читаются) или отдельные файлы и ZIP. Исходные файлы не изменяются.</p>
        <div class="vc-upload-pick">
          <label class="btn btn-outline">Выбрать папку<input type="file" webkitdirectory directory multiple hidden @change="pickFiles($event)"></label>
          <label class="btn btn-outline">Выбрать файлы / ZIP<input type="file" multiple accept=".pdf,.zip" hidden @change="pickFiles($event)"></label>
        </div>
        <div v-if="upload.files.length" class="vc-muted">Выбрано файлов: {{ upload.files.length }} · {{ mb(upload.totalBytes) }}</div>
        <div v-if="upload.busy" class="vc-progress wide"><span :style="{width: uploadPct() + '%'}"></span></div>
        <div v-if="upload.busy" class="vc-muted">{{ upload.message }}</div>
        <div class="vc-modal-actions"><button class="btn btn-primary" :disabled="!upload.files.length || upload.busy" @click="sendFiles">Загрузить и разобрать</button></div>
      </div>
      <div v-else-if="upload.step === 2">
        <p class="vc-muted">{{ upload.proposal.note }} Перемещайте файлы кнопками ↑ ↓ и между проектами списком справа.</p>
        <div v-for="(g, gi) in upload.groups" :key="gi" class="vc-group">
          <div class="vc-group-head">
            <input class="vc-input" v-model="g.title" placeholder="Название проекта">
            <select v-model="g.project_id" class="vc-input">
              <option :value="null">новый проект</option>
              <option v-for="p in sortedProjects()" :key="p.project_id" :value="p.project_id">добавить в «{{ p.title }}»</option>
            </select>
          </div>
          <div v-for="w in g.warnings" :key="w" class="vc-warning">⚠ {{ w }}</div>
          <table class="vc-files">
            <tr><th>№</th><th>Файл</th><th>Метка версии</th><th>Подсказки</th><th></th></tr>
            <tr v-for="(f, fi) in g.files" :key="f.file_key" :class="{'vc-bad': f.error}">
              <td>{{ fi + 1 }}</td>
              <td :title="f.file_name">{{ f.file_name }}<div v-if="f.error" class="vc-error-inline">{{ f.error }}</div></td>
              <td><input class="vc-input" v-model="f.label"></td>
              <td class="vc-muted">{{ hint(f) }}</td>
              <td class="vc-nowrap">
                <button class="btn btn-outline btn-xs" :disabled="fi === 0" @click="moveFile(g, fi, -1)">↑</button>
                <button class="btn btn-outline btn-xs" :disabled="fi === g.files.length - 1" @click="moveFile(g, fi, 1)">↓</button>
                <select class="vc-input xs" @change="moveToGroup(gi, fi, $event)">
                  <option value="">в проект…</option>
                  <option v-for="(g2, gj) in upload.groups" :key="gj" v-show="gj !== gi" :value="gj">{{ g2.title || ('проект ' + (gj + 1)) }}</option>
                  <option value="new">новый проект</option>
                </select>
              </td>
            </tr>
          </table>
        </div>
        <div class="vc-modal-actions">
          <button class="btn btn-outline" @click="closeUpload">Отмена</button>
          <button class="btn btn-primary" :disabled="upload.busy || hasBadFiles()" @click="confirmUpload">Подтвердить группировку и порядок</button>
        </div>
      </div>
    </div>
  </div>

  <div v-if="order.open" class="modal-overlay" @click.self="order.open=false">
    <div class="modal vc-modal">
      <div class="vc-modal-head"><h3>Порядок версий · {{ order.title }}</h3><button class="btn btn-outline btn-sm" @click="order.open=false">✕</button></div>
      <p class="vc-muted">Порядок задаёте вы. Номер в имени файла, «Изм.» по штампам и дата выпуска — только подсказки.</p>
      <table class="vc-files">
        <tr><th>№</th><th>Файл</th><th>Метка</th><th>Подсказки</th><th></th></tr>
        <tr v-for="(v, i) in order.items" :key="v.version_id">
          <td>{{ i + 1 }}</td><td>{{ v.file_name }}</td>
          <td><input class="vc-input" v-model="v.user_label"></td>
          <td class="vc-muted">{{ hint({detected: v.detected}) }}</td>
          <td class="vc-nowrap"><button class="btn btn-outline btn-xs" :disabled="i === 0" @click="moveOrder(i, -1)">↑</button>
            <button class="btn btn-outline btn-xs" :disabled="i === order.items.length - 1" @click="moveOrder(i, 1)">↓</button></td>
        </tr>
      </table>
      <div class="vc-modal-actions">
        <button class="btn btn-outline" @click="order.open=false">Отмена</button>
        <button class="btn btn-primary" :disabled="order.busy" @click="saveOrder">Сохранить</button>
      </div>
    </div>
  </div>

  <vc-ai-report v-if="aiPanel.open" :object-id="objectId" :run="aiPanel.run" @close="aiPanel.open=false" @open-evidence="openEvidence"></vc-ai-report>
  <div v-if="sheetView.open" class="vc-sheetview" tabindex="0" @click.self="sheetView.open = false" @keydown.esc="sheetView.open = false">
    <div class="vc-sheetview-box">
      <div class="vc-sheetview-head">
        <div><b>{{ sheetView.title }}</b><div class="vc-muted">{{ sheetView.note }}</div></div>
        <div class="vc-sheetview-actions">
          <button class="btn btn-outline btn-xs" @click="sheetView.zoom = !sheetView.zoom">{{ sheetView.zoom ? 'Вписать' : 'Крупнее' }}</button>
          <button class="btn btn-outline btn-xs" @click="sheetView.open = false">✕ Закрыть</button>
        </div>
      </div>
      <div class="vc-sheetview-body" :class="{zoom: sheetView.zoom}"><img :src="sheetView.url" alt="" @click="sheetView.zoom = !sheetView.zoom"></div>
    </div>
  </div>
  <vc-viewer v-if="viewer.open" :object-id="objectId" :run="viewer.run" :items="viewer.items" :start-spid="viewer.spid" :focus="viewer.focus" @close="viewer.open=false"></vc-viewer>
</div>`;

    // Таблица имеет высоту окна под закреплённой шапкой. Сводка остаётся
    // в обычном потоке выше: прокрутка страницы поднимает таблицу к шапке.
    function scrollParentOf(el) {
        for (let n = el.parentElement; n && n !== document.body; n = n.parentElement) {
            const oy = getComputedStyle(n).overflowY;
            if (oy === 'auto' || oy === 'scroll') return n;
        }
        return null;
    }
    const px = v => parseFloat(v) || 0;
    function spaceBelow(el, sp) {
        let sum = px(getComputedStyle(el).marginBottom);
        for (let n = el; n && n.parentElement && n !== sp; n = n.parentElement) {
            const pcs = getComputedStyle(n.parentElement);
            // gap flex/grid-контейнера стоит перед каждым следующим видимым соседом.
            const column = pcs.display.includes('grid') || (pcs.display.includes('flex') && pcs.flexDirection.startsWith('column'));
            // В строчном flex соседи стоят сбоку и высоту снизу не добавляют.
            if (pcs.display.includes('flex') && !column) { sum += px(pcs.paddingBottom) + px(pcs.borderBottomWidth); continue; }
            const gap = column ? px(pcs.rowGap) : 0;
            for (let sib = n.nextElementSibling; sib; sib = sib.nextElementSibling) {
                const cs = getComputedStyle(sib);
                if (cs.display === 'none' || cs.position === 'absolute' || cs.position === 'fixed') continue;
                sum += gap + sib.offsetHeight + px(cs.marginTop) + px(cs.marginBottom);
            }
            sum += px(pcs.paddingBottom) + px(pcs.borderBottomWidth);
            if (n.parentElement !== sp) sum += px(pcs.marginBottom);
        }
        return sum;
    }
    function fitToBottom(el) {
        const sp = scrollParentOf(el);
        const viewH = sp ? sp.clientHeight : window.innerHeight;
        const header = sp && sp.querySelector(':scope > .header');
        const headerH = header ? header.getBoundingClientRect().height : 0;
        const body = el.closest('.vc-project-body');
        const runbar = body && body.querySelector('.vc-runbar');
        const runbarH = runbar ? runbar.getBoundingClientRect().height : 0;
        const gap = body ? px(getComputedStyle(body).rowGap) : 0;
        // Leave room for the run controls and all bottom padding when the page
        // reaches its scroll limit, so column titles never slide under the header.
        const h = Math.max(120, Math.floor(viewH - headerH - runbarH - gap - spaceBelow(el, sp) - 8));
        el.style.maxHeight = h + 'px';
    }
    const fitBottom = {
        mounted(el) {
            el._vcFit = () => fitToBottom(el);
            window.addEventListener('resize', el._vcFit);
            requestAnimationFrame(el._vcFit);
        },
        updated(el) { requestAnimationFrame(el._vcFit); },
        unmounted(el) { window.removeEventListener('resize', el._vcFit); },
    };

    root.VersionComparison = {
        API, STATUS, CHANNEL_LABELS, statusInfo, api,
        register(app) {
            app.component('version-comparison-page', {
                template: PAGE_TEMPLATE,
                directives: {fitBottom},
                setup() {
                    const {ref, reactive, watch, onMounted, onBeforeUnmount} = root.Vue;
                    const objects = ref([]);
                    const objectId = ref('');
                    const projects = ref([]);
                    // Выпадающие списки проектов — по алфавиту, номера по значению (АР2 раньше АР10).
                    const sortedProjects = () => [...projects.value].sort((x, y) =>
                        String(x.title || '').localeCompare(String(y.title || ''), 'ru', {numeric: true, sensitivity: 'base'}));
                    const selectedProject = ref('');
                    const catalogLoading = ref(false);
                    const error = ref('');
                    const expanded = reactive({});
                    // Свёрнута верхняя часть карточки (пара A/B и сводка переходов); прогон и матрица остаются.
                    const topHidden = reactive({});
                    const matrices = reactive({});
                    const matrixElements = {};
                    const linkDrafts = reactive({});
                    const linkSaving = reactive({});
                    const linkKey = p => objectId.value + ':' + p.project_id;
                    const linkDraft = p => linkDrafts[linkKey(p)];
                    const linksBusy = p => !!linkSaving[linkKey(p)];
                    const linksLocked = p => linksBusy(p) || !!linkDraft(p)?.saved;
                    const pairs = reactive({});
                    const manualPairs = reactive({});
                    const pending = reactive({});
                    const activeRun = reactive({});
                    const matrixRunCache = reactive({});
                    const matrixRequests = {};
                    const runs = reactive({});
                    const seriesMap = reactive({});
                    const viewer = reactive({open: false, run: null, items: [], spid: '', focus: null});
                    // Лист без пары (или пара ещё не посчитана) — открывается сам лист крупно.
                    const sheetView = reactive({open: false, url: '', title: '', note: '', zoom: false});
                    const aiPanel = reactive({open: false, run: null, project: null});
                    const batchUpload = ref(null);
                    const upload = reactive({open: false, step: 1, files: [], totalBytes: 0, sent: 0, busy: false,
                        message: '', uploadId: '', proposal: null, groups: [], target: ''});
                    const order = reactive({open: false, busy: false, project: null, title: '', items: []});
                    const timers = {};
                    const seriesRequests = {};
                    let alive = true;
                    let dragData = null;

                    function fail(e) { error.value = (e && e.message) || String(e); }
                    const enc = s => encodeURIComponent(s);

                    async function loadObjects() {
                        try {
                            const data = await api('/objects');
                            objects.value = data.items || [];
                            const saved = (() => { try { return localStorage.getItem('version-comparison:object') || ''; } catch (e) { return ''; } })();
                            if (!objectId.value && saved && objects.value.some(o => o.id === saved)) {
                                objectId.value = saved;
                                await loadCatalog();
                            }
                        } catch (e) { fail(e); }
                    }

                    function selectProject() {
                        const p = projects.value.find(p => p.project_id === selectedProject.value);
                        if (p) { expanded[p.project_id] = true; topHidden[p.project_id] = false; loadMatrix(p); }
                    }
                    async function loadCatalog() {
                        try { localStorage.setItem('version-comparison:object', objectId.value || ''); } catch (e) { /* без хранилища */ }
                        projects.value = [];
                        if (!objectId.value) return;
                        catalogLoading.value = true;
                        try {
                            const data = await api(`/objects/${enc(objectId.value)}/catalog`);
                            projects.value = data.projects || [];
                            if (!projects.value.some(p => p.project_id === selectedProject.value)) selectedProject.value = projects.value[0]?.project_id || '';
                            for (const p of projects.value) {
                                if (!pairs[p.project_id]) {
                                    const v = p.versions;
                                    pairs[p.project_id] = {a: v.length > 1 ? v[v.length - 2] : (v[0] || ''), b: v[v.length - 1] || ''};
                                }
                                if (!manualPairs[p.project_id]) manualPairs[p.project_id] = [];
                                if (p.project_id === selectedProject.value && expanded[p.project_id] === undefined) {
                                    expanded[p.project_id] = true;
                                    loadMatrix(p);
                                } else if (expanded[p.project_id]) {
                                    loadMatrix(p);
                                }
                            }
                        } catch (e) { fail(e); }
                        finally { catalogLoading.value = false; }
                    }

                    function schedule(key, fn, ms) {
                        clearTimeout(timers[key]);
                        if (alive) timers[key] = setTimeout(fn, ms);
                    }

                    async function loadMatrix(p) {
                        if (linkDraft(p) || linksBusy(p)) return;
                        if (!matrices[p.project_id]) matrices[p.project_id] = {state: 'loading'};
                        try {
                            const m = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/matrix`);
                            if (linkDraft(p) || linksBusy(p)) return;
                            matrices[p.project_id] = m;
                            if (['profiling', 'building'].includes(m.state)) schedule('m' + p.project_id, () => loadMatrix(p), 1500);
                            else if (m.state === 'ready') { loadRuns(p); loadSeries(p); }
                        } catch (e) { fail(e); }
                    }

                    async function loadRuns(p) {
                        try {
                            const data = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/runs`);
                            runs[p.project_id] = data.runs || [];
                            if (!activeRun[p.project_id] && runs[p.project_id].length) await pickRun(p, runs[p.project_id][0].run_id);
                        } catch (e) { fail(e); }
                    }

                    const refreshPending = data => ['queued', 'running', 'cancelling'].includes(data.state)
                        || (data.state === 'interrupted' && data.auto_resume);
                    const retryRefresh = e => !e.status || e.status >= 500;

                    async function pickRun(p, runId) {
                        const oid = objectId.value;
                        try {
                            const run = await api(`/objects/${enc(objectId.value)}/runs/${runId}`);
                            if (oid !== objectId.value) return;
                            activeRun[p.project_id] = run;
                            matrixRunCache[run.run_id] = run;
                            if (run.state === 'done' && !reports[run.run_id]) pollReports(run);
                            if (refreshPending(run)) schedule('r' + p.project_id, () => pickRun(p, runId), 2000);
                            else if (runs[p.project_id] && !runs[p.project_id].some(r => r.run_id === runId)) loadRuns(p);
                        } catch (e) {
                            if (oid !== objectId.value) return;
                            fail(e);
                            if (retryRefresh(e)) schedule('r' + p.project_id, () => pickRun(p, runId), 5000);
                        }
                    }

                    const seriesOf = p => seriesMap[p.project_id] || null;
                    const seriesMatchesOrder = (p, s) => !p.versions || (s.pairs.length === p.versions.length - 1
                        && s.pairs.every((pr, i) => pr.a === p.versions[i] && pr.b === p.versions[i + 1]));
                    const seriesBusy = p => !!(seriesOf(p) && ['queued', 'running', 'cancelling'].includes(seriesOf(p).state));
                    async function loadSeries(p, seriesId) {
                        const oid = objectId.value;
                        const request = seriesRequests[p.project_id] = (seriesRequests[p.project_id] || 0) + 1;
                        const current = () => alive && oid === objectId.value && seriesRequests[p.project_id] === request;
                        clearTimeout(timers['s' + p.project_id]);
                        try {
                            let s;
                            if (seriesId) s = await api(`/objects/${enc(objectId.value)}/series/${seriesId}`);
                            else s = ((await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/series`)).series || [])[0];
                            if (!s || !current()) return;
                            const previous = seriesMap[p.project_id];
                            seriesMap[p.project_id] = s;
                            // Ход ИИ-описания переходов — когда в серии есть посчитанный переход.
                            if ((!previous || previous.series_id !== s.series_id || !aiSeriesMap[p.project_id])
                                && s.pairs.some(x => x.run_state === 'done')) loadAiSeries(p);
                            if (refreshPending(s)) {
                                schedule('s' + p.project_id, () => loadSeries(p, s.series_id), 2000);
                                // Прогон текущего перехода — в строке прогона и в матрице.
                                const cur = s.pairs.find(x => x.run_state && ['queued', 'running', 'cancelling'].includes(x.run_state));
                                if (cur && (!activeRun[p.project_id] || activeRun[p.project_id].run_id !== cur.run_id)) openTransition(p, cur);
                            } else if (previous && previous.series_id !== s.series_id
                                && previous.pairs.some(pr => pr.run_id && pr.run_id === activeRun[p.project_id]?.run_id)) {
                                const selected = pairs[p.project_id] || {};
                                const next = s.pairs.find(pr => pr.run_id && pr.a === selected.a && pr.b === selected.b)
                                    || s.pairs.find(pr => pr.run_id);
                                if (next) openTransition(p, next);
                            }
                            if (p.project_id === selectedProject.value && !seriesMatchesOrder(p, s))
                                schedule('s' + p.project_id, () => loadSeries(p), 5000);
                        } catch (e) {
                            if (!current()) return;
                            fail(e);
                            if (retryRefresh(e)) schedule('s' + p.project_id, () => loadSeries(p, seriesId), 5000);
                        }
                    }
                    // «Описать изменения ИИ»: способ «Изменения в листе» по всем соседним переходам серии.
                    const aiSeriesMap = reactive({}), aiSeriesEst = reactive({}), aiSeriesConsent = reactive({});
                    const aiSeriesOf = p => aiSeriesMap[p.project_id] || null;
                    const aiSeriesReady = p => !!(seriesOf(p) && seriesMatchesOrder(p, seriesOf(p)) && seriesOf(p).pairs.some(x => x.run_state === 'done'));
                    const aiSeriesBusy = p => !!(aiSeriesOf(p) && aiSeriesOf(p).state === 'running');
                    const aiBase = p => `/objects/${enc(objectId.value)}/series/${seriesOf(p).series_id}/ai-sheet-changes`;
                    async function loadAiSeries(p) {
                        clearTimeout(timers['ai' + p.project_id]);
                        if (!seriesOf(p)) return;
                        const sid = seriesOf(p).series_id;
                        try {
                            const d = await api(aiBase(p));
                            if (!alive || !seriesOf(p) || seriesOf(p).series_id !== sid) return;
                            aiSeriesMap[p.project_id] = d;
                            if (d.state === 'running') schedule('ai' + p.project_id, () => loadAiSeries(p), 3000);
                        } catch (e) { if (retryRefresh(e)) schedule('ai' + p.project_id, () => loadAiSeries(p), 10000); }
                    }
                    async function describeAll(p) {
                        try { aiSeriesConsent[p.project_id] = false; aiSeriesEst[p.project_id] = await api(aiBase(p) + '/estimate'); }
                        catch (e) { fail(e); }
                    }
                    async function startAiSeries(p) {
                        try {
                            aiSeriesMap[p.project_id] = await api(aiBase(p), {method: 'POST', body: {consent: !!aiSeriesConsent[p.project_id]}});
                            aiSeriesEst[p.project_id] = null;
                            schedule('ai' + p.project_id, () => loadAiSeries(p), 2000);
                        } catch (e) { fail(e); }
                    }
                    async function cancelAiSeries(p) {
                        try { aiSeriesMap[p.project_id] = await api(aiBase(p) + '/cancel', {method: 'POST'}); } catch (e) { fail(e); }
                        schedule('ai' + p.project_id, () => loadAiSeries(p), 2000);
                    }
                    async function resumeAiSeries(p) {
                        try { aiSeriesMap[p.project_id] = await api(aiBase(p) + '/resume', {method: 'POST'}); } catch (e) { fail(e); }
                        schedule('ai' + p.project_id, () => loadAiSeries(p), 2000);
                    }
                    const AI_SERIES_STATES = {running: 'идёт', queued: 'в очереди', cancelling: 'останавливается', done: 'готово',
                        partial: 'готово, есть ошибки', interrupted: 'прервано', cancelled: 'отменено', failed: 'ошибка',
                        paused: 'пауза — лимит подписки или вход', paused_quota: 'пауза — лимит подписки', paused_auth: 'пауза — вход в Codex', skipped: 'пропуск', none: '—'};
                    const aiSeriesStateLabel = st => AI_SERIES_STATES[st] || st;
                    const fmtNum = n => (n || 0).toLocaleString('ru-RU');
                    const fmtDuration = s => s == null ? '—' : (s >= 60 ? `${Math.floor(s / 60)} мин ${Math.round(s % 60)} с` : `${Math.round(s)} с`);
                    const pairWall = x => {
                        const t0 = x.started_at && Date.parse(x.started_at), t1 = x.finished_at ? Date.parse(x.finished_at) : Date.now();
                        return t0 ? (t1 - t0) / 1000 : null;
                    };
                    function showSingle(x, sg) {
                        const sha = sg.side === 'B' ? x.sha_b : x.sha_a;
                        if (!sha) return;
                        Object.assign(sheetView, {open: true, zoom: false,
                            note: sg.label + ' — пары для сравнения нет' + (sg.description ? ' · ' + sg.description.sheet_kind : ''),
                            url: `${API}/objects/${enc(objectId.value)}/sources/${sha}/pages/${sg.page}/thumb?w=2000`,
                            title: `${sg.title || 'Лист'} · ${sg.side === 'B' ? x.b_label : x.a_label} · стр. ${sg.page}`});
                    }
                    function openTransitionAi(p, x) {
                        const pr = seriesOf(p) && seriesOf(p).pairs[x.index];
                        if (pr && pr.run_id === x.run_id) openTransition(p, pr);
                    }
                    async function startSeries(p) {
                        if (linksBusy(p) || !(await applyLinks(p))) return;
                        if (!expanded[p.project_id]) { expanded[p.project_id] = true; await loadMatrix(p); }
                        const m = matrices[p.project_id];
                        if (!m || m.state !== 'ready') { error.value = 'Сопоставление листов ещё не готово — дождитесь окончания разбора версий.'; return; }
                        try {
                            const s = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/series`, {method: 'POST'});
                            seriesMap[p.project_id] = s;
                            schedule('s' + p.project_id, () => loadSeries(p, s.series_id), 1000);
                        } catch (e) { fail(e); }
                    }
                    async function cancelSeries(p, s) {
                        try { seriesMap[p.project_id] = await api(`/objects/${enc(objectId.value)}/series/${s.series_id}/cancel`, {method: 'POST'});
                            schedule('s' + p.project_id, () => loadSeries(p, s.series_id), 1000); } catch (e) { fail(e); }
                    }
                    async function resumeSeries(p, s) {
                        try { seriesMap[p.project_id] = await api(`/objects/${enc(objectId.value)}/series/${s.series_id}/resume`, {method: 'POST'}); }
                        catch (e) { fail(e); }
                        finally { schedule('s' + p.project_id, () => loadSeries(p, s.series_id), 1000); }
                    }
                    function openTransition(p, pr) {
                        if (!pr.run_id) return;
                        if (!pairs[p.project_id]) pairs[p.project_id] = {a: '', b: ''};
                        pairs[p.project_id].a = pr.a;
                        pairs[p.project_id].b = pr.b;
                        manualPairs[p.project_id] = [];
                        pickRun(p, pr.run_id);
                    }
                    const isOpenTransition = (p, pr) => !!(pr.run_id && activeRun[p.project_id] && activeRun[p.project_id].run_id === pr.run_id);
                    // Файл, число страниц, «Изм.» и дата выпуска — в подсказке заголовка столбца, не в самом заголовке.
                    const colTitle = v => [v.file_name, `стр.: ${v.page_count}`,
                        (v.detected && v.detected.izm_max != null) ? `Изм.${v.detected.izm_max} (по штампам)` : '',
                        (v.detected && v.detected.release_dates && v.detected.release_dates.length) ? `выпуск: ${v.detected.release_dates[v.detected.release_dates.length - 1]}` : '']
                        .filter(Boolean).join('\n');
                    const pairStateLabel = pr => ({queued: 'в очереди', running: 'считается…', done: 'готово', cancelled: 'отменено',
                        interrupted: 'прерван', failed: 'ошибка' + (pr.error ? ': ' + pr.error : '')}[pr.state] || pr.state);

                    function toggleProject(p) {
                        if (!expanded[p.project_id]) {
                            expanded[p.project_id] = true;
                            topHidden[p.project_id] = false;
                            loadMatrix(p);
                        } else {
                            topHidden[p.project_id] = !topHidden[p.project_id];
                        }
                    }

                    const manual = p => manualPairs[p.project_id] || (manualPairs[p.project_id] = []);
                    const pairReady = p => pairs[p.project_id] && pairs[p.project_id].a && pairs[p.project_id].b && pairs[p.project_id].a !== pairs[p.project_id].b;
                    const runBusy = p => linksBusy(p) || (activeRun[p.project_id] && ['queued', 'running', 'cancelling'].includes(activeRun[p.project_id].state));

                    function setSide(p, side, vid) {
                        const s = pairs[p.project_id];
                        if (side === 'a' && s.b === vid) s.b = s.a;
                        if (side === 'b' && s.a === vid) s.a = s.b;
                        s[side] = vid;
                        manualPairs[p.project_id] = [];
                        pending[p.project_id] = null;
                    }

                    async function startRun(p, scope) {
                        if (linksBusy(p) || !(await applyLinks(p))) return;
                        const s = pairs[p.project_id];
                        try {
                            const run = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/runs`, {
                                method: 'POST', body: {a_version: s.a, b_version: s.b, scope, sheet_pairs: scope === 'manual' ? manual(p) : []}});
                            activeRun[p.project_id] = run;
                            runs[p.project_id] = [run].concat(runs[p.project_id] || []);
                            schedule('r' + p.project_id, () => pickRun(p, run.run_id), 1000);
                        } catch (e) { fail(e); }
                    }
                    async function cancelRun(p, run) {
                        try { activeRun[p.project_id] = await api(`/objects/${enc(objectId.value)}/runs/${run.run_id}/cancel`, {method: 'POST'});
                            schedule('r' + p.project_id, () => pickRun(p, run.run_id), 1000); } catch (e) { fail(e); }
                    }
                    async function resumeRun(p, run) {
                        try { activeRun[p.project_id] = await api(`/objects/${enc(objectId.value)}/runs/${run.run_id}/resume`, {method: 'POST'}); }
                        catch (e) { fail(e); }
                        finally { schedule('r' + p.project_id, () => pickRun(p, run.run_id), 1000); }
                    }

                    function selectedMatrixRun(p) {
                        const pair = pairs[p.project_id];
                        const matches = r => r && pair && r.scope === 'all' && r.a && r.b
                            && r.a.version_id === pair.a && r.b.version_id === pair.b;
                        const candidate = pairRun(p);
                        if (candidate && matches(matrixRunCache[candidate.run_id])) return matrixRunCache[candidate.run_id];
                        return matches(activeRun[p.project_id]) ? activeRun[p.project_id] : null;
                    }
                    async function syncPairRun(p) {
                        const candidate = pairRun(p);
                        if (!candidate) return;
                        const oid = objectId.value;
                        const key = oid + ':' + candidate.run_id;
                        if (matrixRequests[key]) return;
                        const cached = matrixRunCache[candidate.run_id];
                        if (cached && cached.state === 'done') return;
                        matrixRequests[key] = true;
                        try {
                            const run = await api(`/objects/${enc(oid)}/runs/${candidate.run_id}`);
                            if (oid !== objectId.value) return;
                            matrixRunCache[run.run_id] = run;
                        } catch (e) { fail(e); }
                        finally { delete matrixRequests[key]; }
                    }
                    // Both selects and column A/B buttons use the same pair-dependent result.
                    watch(() => JSON.stringify([objectId.value, pairs, runs, seriesMap]), () => {
                        Object.keys(pairs).forEach(project_id => syncPairRun({project_id}));
                    });

                    function rowItem(p, row) {
                        const run = selectedMatrixRun(p);
                        if (!run || run.scope !== 'all') return null;
                        const s = pairs[p.project_id];
                        if (run.a.version_id !== s.a || run.b.version_id !== s.b) return null;
                        return run.items.find(it => it.row_id === row.row_id) || null;
                    }
                    // Галочка «только изменения»: прячет строки, где у выбранной пары A → B лист совпадает
                    // (в т. ч. изменены только служебные зоны) или листа нет ни в A, ни в B.
                    const onlyChanged = reactive({});
                    const UNCHANGED = ['same', 'same_zones_changed'];
                    function pairHasRun(p) {
                        const run = selectedMatrixRun(p);
                        const s = pairs[p.project_id];
                        return !!(run && s && run.scope === 'all' && run.a.version_id === s.a && run.b.version_id === s.b);
                    }
                    function visibleRows(p, m) {
                        if (!onlyChanged[p.project_id] || !pairHasRun(p)) return m.rows;
                        return m.rows.filter(row => {
                            const it = rowItem(p, row);
                            return !!it && !(it.status === 'done' && UNCHANGED.includes(it.result_status));
                        });
                    }
                    function itemLabel(it) {
                        if (it.kind === 'compare' && it.status !== 'done') {
                            return {queued: 'в очереди', running: 'считается…', failed: 'ошибка', cancelled: 'отменено'}[it.status] || it.status;
                        }
                        const info = statusInfo(it.result_status);
                        if (it.part) {
                            const cov = it.part_coverage != null ? `, покрытие ${Math.round(it.part_coverage * 100)}%` : '';
                            const tag = it.part === 'floors' ? ' (этаж типового листа)' : ` (часть листа${cov})`;
                            return info.label + (it.result_status === 'different' && it.regions ? ' · ' + it.regions : '') + tag;
                        }
                        if (it.result_status !== 'different' || !it.regions) return info.label;
                        // Оформление линий (толщина, тон, наложение) — отдельно от основных отличий.
                        const style = it.style_regions || 0;
                        const main = it.regions - style;
                        return info.label + (main ? ' · ' + main : '') + (style ? ` (+${style} оформл.)` : '');
                    }
                    function runCounts(run) {
                        const out = {};
                        for (const it of run.items || []) {
                            const k = it.result_status || null;
                            if (k) out[k] = (out[k] || 0) + 1;
                        }
                        return out;
                    }
                    const progressPct = run => run.progress && run.progress.total ? Math.round(100 * run.progress.done / run.progress.total) : 0;
                    function runStateLabel(run) {
                        if (run.state === 'interrupted' && run.auto_resume) return 'ожидает автоматического продолжения';
                        return {queued: 'в очереди', running: 'идёт сравнение', cancelling: 'отменяется…', done: 'готово',
                            cancelled: 'отменено', interrupted: 'прерван (можно досчитать)', failed: 'ошибка'}[run.state] || run.state;
                    }
                    function cellClass(p, row, v) {
                        const s = pairs[p.project_id] || {};
                        const it = v.version_id === s.b ? rowItem(p, row) : null;
                        return {'col-a': v.version_id === s.a, 'col-b': v.version_id === s.b,
                            'is-pending': pending[p.project_id] && pending[p.project_id].row === row.row_id && v.version_id === s.a,
                            // Лист версии B, в котором сравнение с A нашло отличия, — в красной рамке.
                            'is-diff': !!(it && it.result_status === 'different')};
                    }
                    // Разделение листа 1 → N (этап 2): связи между соседними версиями.
                    const partOf = (m, vid, page) => (m.parts || []).find(x => x.b_version === vid && x.b_page === page) || null;
                    const partsFrom = (m, vid, page) => (m.parts || []).filter(x => x.a_version === vid && x.a_page === page);
                    const partTitle = x => x.kind === 'floors'
                        ? `Этаж типового листа «${x.a_title}» (стр. ${x.a_page} предыдущей версии) — сравнивается весь лист`
                        : `Часть листа «${x.a_title}» (стр. ${x.a_page} предыдущей версии) — сравнивается своя область`;
                    function cellTitle(c) {
                        const parts = [`стр. PDF ${c.page}`];
                        if (c.sheet_no) parts.push(`лист ${c.sheet_no}`);
                        if (c.confidence) parts.push({origin: 'первое появление', high: 'уверенное сопоставление', medium: 'невысокая уверенность', manual: 'ручная связь'}[c.confidence] || c.confidence);
                        if (c.score !== null && c.score !== undefined) parts.push('оценка ' + c.score);
                        return parts.join(' · ') + '\nЩелчок — сравнить с предыдущей версией (первый столбец — со следующей); Shift+щелчок — ручная пара листов';
                    }
                    function thumbUrl(v, page) {
                        return `${API}/objects/${enc(objectId.value)}/sources/${v.sha256}/pages/${page}/thumb?w=160`;
                    }
                    // Щелчок по листу: пара «предыдущая версия → версия листа» (для первого столбца — «он → следующая»)
                    // становится A → B, и открывается сравнение этого листа. Нет пары или пара не посчитана —
                    // открывается сам лист. Shift+щелчок — прежний ручной выбор пары листов (A, затем B).
                    async function clickCell(p, row, v, c, ev) {
                        if (ev && ev.shiftKey) return manualClick(p, row, v, c);
                        const m = matrices[p.project_id];
                        if (!m || !m.versions) return;
                        const vs = m.versions.map(x => x.version_id);
                        const i = vs.indexOf(v.version_id);
                        if (i < 0) return;
                        const label = vid => { const x = m.versions.find(y => y.version_id === vid); return x ? (x.user_label || x.file_name) : vid; };
                        if (vs.length < 2) return showSheet(v, c, row, 'в проекте одна версия — сравнивать не с чем');
                        const a = vs[i === 0 ? 0 : i - 1], b = vs[i === 0 ? 1 : i];
                        const s = pairs[p.project_id] || (pairs[p.project_id] = {a: '', b: ''});
                        if (s.a !== a || s.b !== b) { s.a = a; s.b = b; manualPairs[p.project_id] = []; }
                        pending[p.project_id] = null;
                        const cand = pairRun(p);
                        let run = cand ? matrixRunCache[cand.run_id] : null;
                        if (cand && (!run || run.state !== 'done')) {
                            try { run = await api(`/objects/${enc(objectId.value)}/runs/${cand.run_id}`); matrixRunCache[run.run_id] = run; }
                            catch (e) { run = null; }
                        }
                        const it = run && run.scope === 'all' ? run.items.find(x => x.row_id === row.row_id) : null;
                        if (it && it.kind === 'compare' && it.status === 'done') return openViewer(p, it, run);
                        const other = v.version_id === a ? b : a;
                        const why = !cand ? `пара ${label(a)} → ${label(b)} ещё не посчитана — «Сравнить все сопоставленные листы» или «Сравнить все соседние»`
                            : (it && it.kind === 'compare' ? 'сравнение этого листа ещё не готово' : `у листа нет пары в версии ${label(other)}`);
                        showSheet(v, c, row, why, label(v.version_id));
                    }
                    function showSheet(v, c, row, note, vlabel) {
                        Object.assign(sheetView, {open: true, zoom: false, note,
                            url: `${API}/objects/${enc(objectId.value)}/sources/${v.sha256}/pages/${c.page}/thumb?w=2000`,
                            title: `${row.display_title || 'Лист'} · ${vlabel || v.user_label || v.file_name} · стр. ${c.page}`});
                    }
                    function manualClick(p, row, v, c) {
                        const s = pairs[p.project_id];
                        if (v.version_id === s.a) {
                            pending[p.project_id] = {row: row.row_id, page: c.page};
                        } else if (v.version_id === s.b && pending[p.project_id]) {
                            const a_page = pending[p.project_id].page;
                            if (!manual(p).some(x => x.a_page === a_page && x.b_page === c.page)) manual(p).push({a_page, b_page: c.page});
                            pending[p.project_id] = null;
                        } else if (v.version_id === s.b) {
                            const it = rowItem(p, row);
                            if (it && it.kind === 'compare' && it.status === 'done') openViewer(p, it);
                        }
                    }

                    const copyMatrix = m => JSON.parse(JSON.stringify(m));
                    function previewLink(m, body, index) {
                        const find = anchor => anchor && m.rows.find(r => r.cells[anchor[0]]?.state === 'matched' && r.cells[anchor[0]].page === anchor[1]);
                        const empty = () => ({state: 'absent', manual: true, reason: 'Выбрано вручную; примените сопоставления после всех правок'});
                        const separate = (vid, cell, title) => {
                            m.rows.push({row_id: 'draft-' + index, anchor: [vid, cell.page], display_title: title,
                                manual_new: true, cells: Object.fromEntries(m.versions.map(v => [v.version_id,
                                    v.version_id === vid ? {...cell, manual: true} : empty()]))});
                        };
                        if (body.op === 'absent') {
                            const dst = find(body.row_anchor);
                            if (!dst) return false;
                            const cell = dst.cells[body.version];
                            if (cell.state === 'matched') separate(body.version, cell, dst.display_title);
                            dst.cells[body.version] = empty();
                        } else {
                            const [vid, page] = body.page, src = find(body.page);
                            const dst = body.op === 'move' ? find(body.to_row_anchor) : null;
                            if (!src || (body.op === 'move' && (!dst || dst === src))) return false;
                            const cell = src.cells[vid];
                            src.cells[vid] = empty();
                            if (dst) {
                                if (dst.cells[vid].state === 'matched') separate(vid, dst.cells[vid], dst.display_title);
                                dst.cells[vid] = {...cell, dups: [], manual: true, confidence: 'manual'};
                                for (const r of m.rows) {
                                    const c = r.cells[vid];
                                    if (c.candidates) c.candidates = c.candidates.filter(x => x.page !== page);
                                }
                            } else separate(vid, cell, src.display_title);
                        }
                        // Опорный лист может сам быть перенесён. Следующая правка использует
                        // оставшийся лист строки, как сервер при последовательном применении.
                        for (const r of m.rows) {
                            const v = m.versions.find(v => r.cells[v.version_id].state === 'matched');
                            r.anchor = v ? [v.version_id, r.cells[v.version_id].page] : null;
                        }
                        return true;
                    }
                    function edit(p, body) {
                        if (linksLocked(p)) return;
                        const m = matrices[p.project_id];
                        if (!m || m.state !== 'ready') return;
                        const key = linkKey(p), draft = linkDrafts[key] || {base: copyMatrix(m), edits: []};
                        const preview = copyMatrix(m);
                        // Освободившаяся ячейка не должна схлопывать строку выше курсора.
                        const heights = new Map(Array.from(matrixElements[p.project_id]?.querySelectorAll('tbody tr[data-row-id]') || [],
                            el => [el.dataset.rowId, el.getBoundingClientRect().height]));
                        for (const row of preview.rows) row.draftHeight = heights.get(row.row_id) || row.draftHeight;
                        if (!previewLink(preview, body, draft.edits.length)) return;
                        draft.edits.push(body);
                        linkDrafts[key] = draft;
                        matrices[p.project_id] = preview;
                        onlyChanged[p.project_id] = false;
                    }
                    async function applyLinks(p) {
                        const key = linkKey(p), draft = linkDrafts[key], oid = objectId.value;
                        if (linkSaving[key]) return false;
                        if (!draft) return true;
                        linkSaving[key] = true;
                        error.value = '';
                        const base = `/objects/${enc(oid)}/projects/${p.project_id}`;
                        try {
                            let m;
                            if (draft.saved) m = await api(base + '/matrix');
                            else {
                                m = await api(base + '/links/batch', {method: 'POST', body: {edits: draft.edits}});
                                draft.saved = true;
                            }
                            // Таблица остаётся в DOM и сохраняет прокрутку даже во время пересчёта.
                            while (['building', 'profiling'].includes(m.state)) {
                                await new Promise(resolve => setTimeout(resolve, 1000));
                                if (!alive || oid !== objectId.value) return false;
                                m = await api(base + '/matrix');
                            }
                            if (m.state !== 'ready') throw new Error(m.error || 'Не удалось применить сопоставления. Повторите попытку.');
                            if (!alive || oid !== objectId.value) return false;
                            matrices[p.project_id] = m;
                            delete linkDrafts[key];
                            return true;
                        } catch (e) { fail(e); return false; }
                        finally { linkSaving[key] = false; }
                    }
                    function dragStart(vid, page, ev) {
                        dragData = {vid, page};
                        try { ev.dataTransfer.setData('text/plain', `${vid}:${page}`); } catch (e) { /* ignore */ }
                    }
                    function dropOnCell(p, row, v, ev) {
                        if (!row.anchor || !dragData || dragData.vid !== v.version_id) { dragData = null; return; }
                        const cell = row.cells[v.version_id];
                        if (cell.state === 'matched' && cell.page === dragData.page) { dragData = null; return; }
                        edit(p, {op: 'move', page: [dragData.vid, dragData.page], to_row_anchor: row.anchor});
                        dragData = null;
                    }
                    const detach = (p, v, c) => edit(p, {op: 'detach', page: [v.version_id, c.page]});
                    const attach = (p, row, v, page) => edit(p, {op: 'move', page: [v.version_id, page], to_row_anchor: row.anchor});
                    const markAbsent = (p, row, v) => edit(p, {op: 'absent', row_anchor: row.anchor, version: v.version_id});
                    async function undoEdit(p) {
                        if (linksLocked(p)) return;
                        const draft = linkDraft(p);
                        if (draft) {
                            draft.edits.pop();
                            const m = copyMatrix(draft.base);
                            draft.edits.forEach((body, i) => previewLink(m, body, i));
                            matrices[p.project_id] = m;
                            if (!draft.edits.length) delete linkDrafts[linkKey(p)];
                            return;
                        }
                        try { matrices[p.project_id] = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/links/undo`, {method: 'POST'});
                            if (matrices[p.project_id].state !== 'ready') schedule('m' + p.project_id, () => loadMatrix(p), 800); } catch (e) { fail(e); }
                    }
                    function failedVersions(m) {
                        return (m.versions || []).filter(v => v.profile.state === 'failed').map(v => `${v.file_name}: ${v.profile.error}`).join('; ');
                    }

                    const reports = reactive({});
                    const reportOf = run => reports[run.run_id] || null;
                    const reportUrl = (run, name) => `${API}/objects/${enc(objectId.value)}/runs/${run.run_id}/reports/${name}`;
                    const seriesReportUrl = s => `${API}/objects/${enc(objectId.value)}/series/${s.series_id}/report.xlsx`;
                    async function pollReports(run) {
                        try {
                            reports[run.run_id] = await api(`/objects/${enc(objectId.value)}/runs/${run.run_id}/reports`);
                            if (reports[run.run_id].state === 'building') schedule('rep' + run.run_id, () => pollReports(run), 2000);
                        } catch (e) { fail(e); }
                    }
                    async function buildReports(p, run) {
                        try {
                            reports[run.run_id] = await api(`/objects/${enc(objectId.value)}/runs/${run.run_id}/reports`, {method: 'POST'});
                            schedule('rep' + run.run_id, () => pollReports(run), 1500);
                        } catch (e) { fail(e); }
                    }
                    // Последний прогон выбранной пары A → B (все листы): по нему «Пересчитать» и «Расчёт этой пары».
                    function pairRun(p) {
                        const pr = pairs[p.project_id];
                        if (!pr) return null;
                        const list = (runs[p.project_id] || []).filter(r => r.scope === 'all'
                            && r.a && r.b && r.a.version_id === pr.a && r.b.version_id === pr.b);
                        const own = list.find(r => r.state === 'done');
                        if (own) return own;
                        // Пара могла быть посчитана серией «Сравнить все соседние».
                        const sp = seriesOf(p) && seriesOf(p).pairs.find(x => x.a === pr.a && x.b === pr.b && x.run_id);
                        if (sp && (sp.run_state === 'done' || !list.length)) return {run_id: sp.run_id, state: sp.run_state || sp.state};
                        return list[0] || null;
                    }
                    const firstDoneItem = run => (run && run.items || []).find(it => it.kind === 'compare' && it.status === 'done') || null;
                    // Кнопка в строке прогона открывает текущий результат сравнения.
                    function openActive(p) {
                        const first = firstDoneItem(activeRun[p.project_id]);
                        if (first) openViewer(p, first, activeRun[p.project_id]);
                    }
                    function openViewer(p, item, selectedRun = null, focus = null) {
                        const run = selectedRun || selectedMatrixRun(p);
                        if (!run) return;
                        viewer.run = run;
                        viewer.items = run.items.filter(it => it.kind === 'compare' && it.status === 'done');
                        viewer.spid = item.spid;
                        viewer.focus = focus;
                        viewer.open = true;
                    }
                    // ИИ-описание — по паре A → B, выбранной в матрице (16.10.2026), а не по прогону в блоке ниже:
                    // только законченный прогон всех листов этой пары.
                    function aiRun(p) {
                        const run = selectedMatrixRun(p);
                        return run && run.state === 'done' ? run : null;
                    }
                    // ИИ-описание: панель открывает сохранённый результат; переход — в просмотрщик с рамками доказательств.
                    function showAiReport(p, run) {
                        if (!run) return;
                        aiPanel.run = run;
                        aiPanel.project = p;
                        aiPanel.open = true;
                    }
                    function openEvidence(payload) {
                        if (viewer.open && viewer.run && viewer.run.run_id === aiPanel.run.run_id) {
                            viewer.spid = payload.spid;
                            viewer.focus = Object.assign({}, payload.focus);
                            return;
                        }
                        openViewer(aiPanel.project, {spid: payload.spid}, aiPanel.run, payload.focus);
                    }

                    // ------------------------------------------------ загрузка
                    function openUpload() {
                        Object.assign(upload, {open: true, step: 1, files: [], totalBytes: 0, sent: 0, busy: false, message: '', uploadId: '', proposal: null, groups: [], target: ''});
                    }
                    async function closeUpload() {
                        if (upload.uploadId && upload.step === 2) {
                            try { await api(`/objects/${enc(objectId.value)}/uploads/${upload.uploadId}`, {method: 'DELETE'}); } catch (e) { /* ignore */ }
                        }
                        upload.open = false;
                    }
                    function pickFiles(ev) {
                        const files = Array.from(ev.target.files || []).filter(f => /\.(pdf|zip)$/i.test(f.name));
                        upload.files = files;
                        upload.totalBytes = files.reduce((s, f) => s + f.size, 0);
                    }
                    const mb = n => (n / 1048576).toFixed(1) + ' МБ';
                    const uploadPct = () => upload.totalBytes ? Math.min(100, Math.round(100 * upload.sent / upload.totalBytes)) : 0;
                    async function sendFiles() {
                        upload.busy = true;
                        upload.sent = 0;
                        try {
                            const created = await api(`/objects/${enc(objectId.value)}/uploads`, {method: 'POST'});
                            upload.uploadId = created.upload_id;
                            let base = 0;
                            for (const [i, f] of upload.files.entries()) {
                                upload.message = `Файл ${i + 1} из ${upload.files.length}: ${f.name} (разбор штампов…)`;
                                await uploadFile(objectId.value, upload.uploadId, f, f.webkitRelativePath || f.name, loaded => { upload.sent = base + loaded; });
                                base += f.size;
                                upload.sent = base;
                            }
                            upload.message = 'Подбор группировки и порядка…';
                            const prop = await api(`/objects/${enc(objectId.value)}/uploads/${upload.uploadId}/proposal`);
                            upload.proposal = prop;
                            upload.groups = prop.groups.map(g => ({title: g.title, code: g.code, project_id: null,
                                warnings: g.warnings, files: g.files.map(f => Object.assign({}, f, {label: f.suggested_label}))}));
                            if (upload.target) {
                                const target = projects.value.find(p => p.project_id === upload.target);
                                upload.groups = [{title: target.title, code: target.code, project_id: target.project_id,
                                    warnings: upload.groups.flatMap(g => g.warnings || []), files: upload.groups.flatMap(g => g.files)}];
                            }
                            upload.step = 2;
                        } catch (e) { fail(e); }
                        finally { upload.busy = false; }
                    }
                    function hint(f) {
                        const d = f.detected || {};
                        const out = [];
                        if (d.file_number !== null && d.file_number !== undefined) out.push(`№ в имени: ${d.file_number}`);
                        if (d.izm_max !== null && d.izm_max !== undefined) out.push(`Изм. по штампам: ${d.izm_max}`);
                        if (d.release_dates && d.release_dates.length) out.push(`выпуск: ${d.release_dates[d.release_dates.length - 1]}`);
                        if (d.page_count) out.push(`стр.: ${d.page_count}`);
                        return out.join(' · ') || '—';
                    }
                    function moveFile(g, i, d) {
                        const [f] = g.files.splice(i, 1);
                        g.files.splice(i + d, 0, f);
                    }
                    function moveToGroup(gi, fi, ev) {
                        const target = ev.target.value;
                        ev.target.value = '';
                        if (target === '') return;
                        const [f] = upload.groups[gi].files.splice(fi, 1);
                        if (target === 'new') upload.groups.push({title: f.file_name, code: '', project_id: null, warnings: [], files: [f]});
                        else upload.groups[Number(target)].files.push(f);
                        upload.groups = upload.groups.filter(g => g.files.length);
                    }
                    const hasBadFiles = () => upload.groups.some(g => g.files.some(f => f.error));
                    async function confirmUpload() {
                        upload.busy = true;
                        try {
                            await api(`/objects/${enc(objectId.value)}/uploads/${upload.uploadId}/confirm`, {method: 'POST', body: {
                                projects: upload.groups.map(g => ({project_id: g.project_id, title: g.title, code: g.code,
                                    files: g.files.map(f => ({file_key: f.file_key, label: f.label}))}))}});
                            upload.open = false;
                            for (const k of Object.keys(matrices)) delete matrices[k];
                            await loadCatalog();
                            loadObjects();
                        } catch (e) { fail(e); }
                        finally { upload.busy = false; }
                    }

                    // ------------------------------------------------- порядок
                    function openOrder(p) {
                        const m = matrices[p.project_id];
                        const recs = p.version_records || [];
                        order.items = recs.map(v => ({version_id: v.version_id, file_name: v.file_name, user_label: v.user_label,
                            detected: Object.assign({page_count: v.page_count}, v.detected || {})}));
                        order.project = p;
                        order.title = p.title;
                        order.open = true;
                        void m;
                    }
                    function moveOrder(i, d) {
                        const [v] = order.items.splice(i, 1);
                        order.items.splice(i + d, 0, v);
                    }
                    async function saveOrder() {
                        order.busy = true;
                        const p = order.project;
                        try {
                            await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/order`, {method: 'PUT', body: {version_ids: order.items.map(v => v.version_id)}});
                            const before = Object.fromEntries((p.version_records || []).map(v => [v.version_id, v.user_label]));
                            for (const v of order.items) {
                                if (before[v.version_id] !== v.user_label) {
                                    await api(`/objects/${enc(objectId.value)}/versions/${v.version_id}`, {method: 'PATCH', body: {label: v.user_label}});
                                }
                            }
                            order.open = false;
                            delete matrices[p.project_id];
                            await loadCatalog();
                        } catch (e) { fail(e); }
                        finally { order.busy = false; }
                    }

                    onMounted(loadObjects);
                    onBeforeUnmount(() => { alive = false; Object.values(timers).forEach(clearTimeout); });

                    return {aiPanel, aiRun, showAiReport, openEvidence, objects, objectId, projects, sortedProjects, selectedProject, selectProject, catalogLoading, error, expanded, matrices, pairs, pending, activeRun, runs,
                        viewer, upload, batchUpload, order, statusInfo, loadCatalog, toggleProject, topHidden, onlyChanged, pairHasRun, visibleRows, manual, pairReady, runBusy, setSide,
                        startRun, cancelRun, resumeRun, pickRun, pairRun, openActive, firstDoneItem, rowItem, itemLabel, runCounts, progressPct, runStateLabel,
                        syncPairRun, cellClass, cellTitle, thumbUrl, clickCell, sheetView, dragStart, dropOnCell, detach, attach, markAbsent,
                        matrixElements, linkDraft, linksBusy, linksLocked, applyLinks, undoEdit, failedVersions, openViewer, openUpload, closeUpload, pickFiles, mb, uploadPct, sendFiles, hint,
                        moveFile, moveToGroup, hasBadFiles, confirmUpload, openOrder, moveOrder, saveOrder,
                        aiSeriesEst, aiSeriesConsent, aiSeriesOf, aiSeriesReady, aiSeriesBusy, describeAll, startAiSeries, cancelAiSeries,
                        resumeAiSeries, aiSeriesStateLabel, showSingle, fmtNum, fmtDuration, pairWall, openTransitionAi,
                        seriesOf, seriesMatchesOrder, seriesBusy, startSeries, cancelSeries, resumeSeries, openTransition, isOpenTransition,
                        pairStateLabel, colTitle,
                        partOf, partsFrom, partTitle,
                        reportOf, reportUrl, seriesReportUrl, buildReports};
                },
            });
            if (root.VersionComparisonBatch) root.VersionComparisonBatch.register(app);
            if (root.VersionComparisonViewer) root.VersionComparisonViewer.register(app);
            if (root.VersionComparisonAI) root.VersionComparisonAI.register(app);
            if (root.VersionComparisonSheetChanges) root.VersionComparisonSheetChanges.register(app);
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
