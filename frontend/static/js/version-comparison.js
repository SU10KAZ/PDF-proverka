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
      <button class="btn btn-primary" :disabled="!objectId" title="Загрузить PDF: один проект со всеми версиями, новые версии в существующий проект или сразу несколько проектов из папок; там же — прежние пакеты загрузки." @click="openUpload">⇪ Загрузить проект со всеми версиями</button>
    </div>
  </div>
  <vc-batch-upload ref="batchUpload" v-if="objectId" :key="objectId" :object-id="objectId" @updated="loadCatalog"></vc-batch-upload>
  <div v-if="aiQueue && (aiQueue.current || aiQueue.waiting.length)" class="vc-ai-queue"
       title="«ИИ-описание изменений» идёт по одному проекту за раз; следующий в очереди начинается сам, когда закончится текущий">
    <b>Очередь «ИИ-описание изменений»:</b> {{ aiQueueText() }}
  </div>
  <div v-if="error" class="vc-error" @click="error=''">{{ error }}</div>
  <div v-if="!objectId" class="vc-empty">Выберите объект в шапке платформы (слева вверху) — раздел покажет его проекты и версии.</div>
  <div v-else-if="catalogLoading" class="vc-empty">Загрузка каталога…</div>
  <div v-else-if="!projects.length" class="vc-empty">В объекте ещё нет версий. Нажмите «Загрузить проект со всеми версиями» и выберите папку с PDF.</div>

  <div v-if="objectId && projects.length" class="vc-tabs" role="tablist">
    <button class="vc-tab" role="tab" :class="{on: view === 'projects'}" :aria-selected="view === 'projects'" title="Все проекты объекта и этапы их проверки одной таблицей" @click="showProjects">Проекты и этапы<span class="vc-tab-n">{{ projects.length }}</span></button>
    <button class="vc-tab" role="tab" :class="{on: view === 'project'}" :aria-selected="view === 'project'" :disabled="!selectedProject" title="Матрица листов открытого проекта: какие листы есть в каждой версии и что в них изменилось" @click="showProject">Сравнение листов<span v-if="selectedTitle()" class="vc-tab-n">{{ selectedTitle() }}</span></button>
  </div>
  <vc-projects-overview v-if="view === 'projects' && objectId && projects.length" :object-id="objectId" :reload-key="catalogVersion" @open="openProject"></vc-projects-overview>

  <section v-for="p in (view === 'project' ? projects.filter(p => p.project_id === selectedProject) : [])" :key="p.project_id" class="vc-project">
    <header class="vc-project-head" @click="toggleProject(p)">
      <span class="vc-caret" :title="expanded[p.project_id] && !topHidden[p.project_id] ? 'Свернуть выбор пары и сводку переходов' : 'Развернуть'">{{ expanded[p.project_id] && !topHidden[p.project_id] ? '▼' : '▶' }}</span>
      <span class="vc-project-name" title="Проект, с которым работаете. Другой проект — на странице «Проекты и этапы»">{{ p.title }}</span>
      <span class="vc-muted">· версий: {{ p.versions.length }}</span>
      <span class="vc-spacer"></span>
      <template v-if="linkDraft(p)">
        <button class="btn btn-primary btn-sm" :disabled="linksBusy(p)" title="Сохранить ваши ручные связи листов; затянутые ими сравнения пересчитаются." @click.stop="applyLinks(p)">{{ linksBusy(p) ? 'Применение…' : 'Применить сопоставления (' + linkDraft(p).edits.length + ')' }}</button>
        <button class="btn btn-outline btn-sm" :disabled="linksLocked(p)" title="Отменить последнюю ручную связь листов, пока она не применена" @click.stop="undoEdit(p)">Отменить правку</button>
      </template>
      <a v-if="aiSummaryReady(p)" class="btn btn-outline btn-sm" :href="aiSummaryUrl(p)" download @click.stop
         title="Скачать все изменения от первой версии до последней одним файлом: переход → лист → изменение.">Сводка изменений (MD)</a>
      <button v-else class="btn btn-outline btn-sm" disabled title="Сводка станет доступна после описания листов или сравнения полных спецификаций">Сводка изменений (MD)</button>
      <button class="btn btn-outline btn-sm" :disabled="!!linkDraft(p) || linksBusy(p)" title="Задать порядок версий и их подписи. От порядка зависят переходы V1→V2, V2→V3 …" @click.stop="openOrder(p)">Порядок версий</button>
      <button class="btn btn-outline btn-sm" title="Открыть окно PDF-отчёта: выбрать источник (сводка проекта или свой MD-файл), запустить и скачать прошлые отчёты."
              @click.stop="openMdReport(p)">Сформировать PDF-отчёт…</button>
    </header>
    <div v-if="expanded[p.project_id]" class="vc-project-body">
      <template v-for="m in [matrices[p.project_id]]" :key="'m' + p.project_id">
        <div v-if="!m || m.state === 'loading'" class="vc-empty">Загрузка…</div>
        <div v-else-if="m.state === 'profiling'" class="vc-empty">Разбор версий: готово {{ m.progress ? m.progress.ready : 0 }} из {{ m.progress ? m.progress.total : '?' }}…</div>
        <div v-else-if="m.state === 'building'" class="vc-empty">Сопоставление листов между версиями…</div>
        <div v-else-if="m.state === 'failed'" class="vc-error">Не удалось подготовить версии: {{ m.error || failedVersions(m) }}</div>
        <template v-else-if="m.state === 'ready'">
          <template v-if="!topHidden[p.project_id]">
          <div v-if="!pairReady(p)" class="vc-pairbar">
            <div class="vc-hint">Выберите две разные версии.</div>
          </div>

          <div v-if="seriesOf(p)" class="vc-series">
            <template v-for="s in [seriesOf(p)]" :key="s.series_id">
              <div class="vc-series-head">
                <b>Сводка переходов</b>
                <span class="vc-run-state" :class="'st-' + s.state">{{ runStateLabel(s) }}</span>
                <span class="vc-muted">переходов готово: {{ s.progress.done }} из {{ s.progress.total }}</span>
                <button v-if="['queued','running'].includes(s.state)" class="btn btn-danger-outline btn-sm" title="Остановить сравнение соседних версий. Уже посчитанные листы сохранятся." @click="cancelSeries(p, s)">Отменить</button>
                <button v-if="seriesMatchesOrder(p, s) && ['interrupted','cancelled','failed','paused'].includes(s.state)" class="btn btn-outline btn-sm" title="Продолжить с того же места: посчитанное заново не считается." @click="resumeSeries(p, s)">{{ s.state === 'paused' ? 'Продолжить' : 'Досчитать' }}</button>
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
                    <td><button v-if="pr.run_id" class="btn btn-outline btn-xs" title="Показать в матрице этот переход: версии станут парой A и B." @click="openTransition(p, pr)">Открыть</button></td>
                  </tr>
                </tbody>
              </table>
            </template>
          </div>

          <div v-if="aiSeriesOf(p) && aiSeriesOf(p).state !== 'none'" class="vc-series vc-ai-series">
            <template v-if="aiSeriesOf(p)">
              <template v-for="a in [aiSeriesOf(p)]" :key="'ai'">
                <div class="vc-series-head">
                  <b>ИИ-описание изменений</b>
                  <template v-if="a.state === 'queued'">
                    <span class="vc-run-state st-queued">{{ a.queue.action === 'resume' ? 'досчёт в очереди' : 'в очереди' }}: №{{ a.queue.position }}</span>
                    <span class="vc-muted">{{ aiQueueAhead(a) }}</span>
                    <button class="btn btn-outline btn-sm" title="Убрать проект из очереди ИИ-описаний; ничего не будет описано." @click="cancelAiSeries(p)">Убрать из очереди</button>
                  </template>
                  <template v-else>
                    <span class="vc-run-state" :class="'st-' + a.state" :title="a.state === 'partial' ? aiPartialTitle : ''">{{ aiStateText(a.state, a.total.review, a.total.sheets_failed) }}</span>
                    <span class="vc-muted">листов готово: {{ a.total.sheets_done }} из {{ a.total.sheets }}<template v-if="a.total.sheets_failed"> · ошибок {{ a.total.sheets_failed }}</template>
                      · время {{ fmtDuration(a.wall_s) }} · обращений {{ a.total.calls }} · токены: вход {{ fmtNum(a.total.input_tokens) }}, выход {{ fmtNum(a.total.output_tokens) }}</span>
                    <button v-if="['running', 'paused'].includes(a.state)" class="btn btn-danger-outline btn-sm" title="Остановить описание. Готовые описания листов сохранятся." @click="cancelAiSeries(p)">Отменить</button>
                    <button v-if="['interrupted', 'paused'].includes(a.state)" class="btn btn-outline btn-sm" title="Описать оставшиеся листы; готовые не описываются заново." @click="resumeAiSeries(p)">Досчитать</button>
                  </template>
                </div>
                <div v-if="a.queue_error" class="vc-error">Из очереди не запустилось: {{ a.queue_error }}</div>
                <div v-if="a.state === 'paused' && a.queue && a.queue.current && a.queue.waiting" class="vc-warning">
                  Очередь ждёт этот проект (за ним ещё {{ a.queue.waiting }}): «Досчитать» — продолжить, «Отменить» — пропустить и начать следующий.</div>
                <table v-if="a.state !== 'queued'" class="vc-series-table">
                  <thead><tr><th>Переход A → B</th><th>Ход описания</th><th>Листов</th><th>Изм.</th><th>Вопр.</th><th>Ошибок</th>
                    <th title="Листы, где ИИ изменений не нашёл, но сравнение оставило пометки (например, пара листов сопоставлена неуверенно). Их нужно посмотреть самому.">Проверить</th><th>Обращ.</th><th>Вход</th><th>Выход</th><th>Время</th><th></th></tr></thead>
                  <tbody>
                    <template v-for="x in a.pairs" :key="x.index">
                    <tr>
                      <td class="vc-tr-name">
                        <button v-if="(x.singles || []).length || (x.review || []).length" class="vc-ai-fold" :aria-expanded="!!aiFold[x.run_id]"
                                :title="(aiFold[x.run_id] ? 'Свернуть' : 'Показать') + ' листы перехода: проверить — ' + (x.review || []).length + ', добавлен или удалён — ' + (x.singles || []).length"
                                @click="aiFold[x.run_id] = !aiFold[x.run_id]">{{ aiFold[x.run_id] ? '▾' : '▸' }}</button>
                        {{ x.a_label }} → {{ x.b_label }}</td>
                      <template v-if="x.job_id">
                        <td class="vc-tr-progress">
                          <span class="vc-progress" :class="'st-' + x.state"><span :style="{width: (x.sheets_total ? Math.round(100 * x.sheets_done / x.sheets_total) : 0) + '%'}"></span></span>
                          <span class="vc-muted">{{ x.sheets_done }} из {{ x.sheets_total }}<template v-if="x.running"> · идёт {{ x.running }}</template> · {{ aiStateText(x.state, (x.review || []).length, x.sheets_failed) }}</span>
                        </td>
                        <td>{{ x.sheets_total }}</td><td>{{ x.changes }}</td><td>{{ x.unresolved }}</td>
                        <td :class="{'vc-error': x.sheets_failed}">{{ x.sheets_failed || '' }}</td>
                        <td><button v-if="x.review && x.review.length" class="btn btn-outline btn-xs vc-review-btn" title="Открыть эти листы по очереди в окне сверки; справа написано, что именно проверить." @click="openReview(p, x)">{{ x.review.length }} ›</button></td>
                        <td>{{ x.calls }}</td><td>{{ fmtNum(x.input_tokens) }}</td><td>{{ fmtNum(x.output_tokens) }}</td>
                        <td>{{ fmtDuration(pairWall(x)) }}</td>
                      </template>
                      <td v-else colspan="10" class="vc-muted">{{ x.reason }}</td>
                      <td><button v-if="x.run_id && x.job_id" class="btn btn-outline btn-xs" title="Открыть окно сверки листов этого перехода с первого изменённого листа: наложение A и B, справа — что ИИ написал по листу." @click="openTransitionAi(p, x)">Открыть</button></td>
                    </tr>
                    <tr v-if="aiFold[x.run_id] && x.review && x.review.length" class="vc-ai-singles">
                      <td colspan="12">
                        <div v-for="r in x.review" :key="r.spid" class="vc-ai-single">
                          <span class="vc-ai-single-kind k-review">Проверить</span>
                          <b>{{ r.title || 'Лист' }}</b> <span class="vc-muted">· A стр. {{ r.a_page }} → B стр. {{ r.b_page }}</span> — {{ r.why }}
                          <button class="btn btn-outline btn-xs" title="Открыть этот лист в окне сверки: наложение A и B, справа — вывод ИИ и что проверить." @click="openReview(p, x, r.spid)">Открыть</button>
                        </div>
                      </td>
                    </tr>
                    <tr v-if="aiFold[x.run_id] && x.singles && x.singles.length" class="vc-ai-singles">
                      <td colspan="12">
                        <div v-for="sg in x.singles" :key="sg.spid" class="vc-ai-single">
                          <span class="vc-ai-single-kind" :class="'k-' + sg.kind">{{ sg.label }}</span>
                          <b>{{ sg.title || 'Лист' }}</b> <span class="vc-muted">· {{ sg.side === 'B' ? x.b_label : x.a_label }}, стр. {{ sg.page }}</span>
                          <template v-if="sg.description"> — <i>{{ sg.description.sheet_kind }}</i>. {{ sg.description.summary }}
                            <span v-if="sg.description.marks.length" class="vc-muted"> Марки: {{ sg.description.marks.slice(0, 12).join(', ') }}<template v-if="sg.description.marks.length > 12"> …</template></span>
                            <span v-if="sg.description.limitation" class="vc-warning"> Неясно: {{ sg.description.limitation }}</span>
                          </template>
                          <span v-else class="vc-muted"> — {{ aiSeriesStateLabel(sg.state) }}</span>
                          <button class="btn btn-outline btn-xs" title="Пары в соседней версии нет — открыть лист крупно, справа вывод ИИ: что на листе, марки и что ИИ не смог прочитать." @click="showSingle(x, sg)">Открыть</button>
                        </div>
                      </td>
                    </tr>
                    </template>
                  </tbody>
                </table>
                <div v-if="a.state !== 'queued'" class="vc-muted">Описание каждого листа — в просмотрщике пары, окно «Изменения в листе».</div>
              </template>
            </template>
          </div>
          </template>

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
                        <button class="btn btn-outline btn-xs" :class="{'vc-pick-a': v.version_id === pairs[p.project_id].a}" :aria-pressed="v.version_id === pairs[p.project_id].a" title="Сделать эту версию стороной A (было) выбранной пары" @click="setSide(p, 'a', v.version_id)">A</button>
                        <button class="btn btn-outline btn-xs" :class="{'vc-pick-b': v.version_id === pairs[p.project_id].b}" :aria-pressed="v.version_id === pairs[p.project_id].b" title="Сделать эту версию стороной B (стало) выбранной пары" @click="setSide(p, 'b', v.version_id)">B</button>
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
                      <button class="vc-badge" :class="'b-' + statusInfo(rowItem(p, row).result_status).cls" title="Открыть сравнение этого листа в паре A → B: наложение, A, B и рядом" @click="rowItem(p, row).kind === 'chain' ? chainPair(p, row, rowItem(p, row)) : openViewer(p, rowItem(p, row))" :disabled="rowItem(p, row).kind !== 'chain' && (rowItem(p, row).kind !== 'compare' || rowItem(p, row).status !== 'done')">
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
                          <span v-if="c.confidence === 'group'" class="vc-flag vc-group" :title="groupTitle(c)">▤ группа</span>
                          <span v-if="c.manual" class="vc-flag" title="Связь задана вручную">✎</span>
                          <span v-if="c.ai" class="vc-flag vc-ai" :title="aiTitle(m, row, v)">ИИ</span>
                          <span v-else-if="aiDoubt(m, row, v)" class="vc-flag vc-ai-doubt" :title="aiTitle(m, row, v)">ИИ ?</span>
                          <span v-if="c.dups && c.dups.length" class="vc-flag" :title="'Дубль листа: стр. ' + c.dups.join(', ')">⧉</span>
                          <span v-if="c.scan" class="vc-flag vc-scan" title="Скан: страница — картинка, текст не извлекается и не сравнивается; сравнивается только изображение">скан</span>
                          <span v-if="partOf(m, v.version_id, c.page)" class="vc-flag vc-part" :title="partTitle(partOf(m, v.version_id, c.page))">◫</span>
                          <span v-if="partsFrom(m, v.version_id, c.page).length" class="vc-flag vc-part" :title="'Лист разделён в следующей версии: стр. ' + partsFrom(m, v.version_id, c.page).map(x => x.b_page).join(', ')">⇉</span>
                        </div>
                        <button class="vc-cell-menu" title="Отвязать в отдельную строку" @click.stop="detach(p, v, c)">⤫</button>
                      </div>
                      <div v-else-if="c.state === 'unmatched'" class="vc-empty-cell unmatched" :title="c.reason">
                        <div>⁇ не сопоставлен <button v-if="c.candidates && c.candidates.length" class="btn btn-outline btn-xs" title="Показать лист и кандидатов рядом" @click="sideBySide(p, row, v, c)">рядом</button></div>
                        <div v-if="c.reason_code" class="vc-cell-reason">{{ c.reason }}</div>
                        <div v-for="n in [aiNote(m, row, v)].filter(Boolean)" :key="'ai' + row.row_id" class="vc-ai-hint" :title="aiTitle(m, row, v)">
                          🌙 {{ n.page ? 'ИИ: вероятно стр.' + n.page + (n.confidence ? ' (' + n.confidence + ')' : '') : (n.decision === 'none' ? 'ИИ: ни один кандидат' : 'ИИ: ' + n.label) }}
                          <button v-if="n.page" class="btn btn-outline btn-xs" :disabled="!row.anchor" title="Связать с листом, который предлагает ИИ. Это ручная правка — затем «Применить сопоставления»." @click="attach(p, row, v, n.page)">связать</button>
                        </div>
                        <div class="vc-cands">кандидаты:
                          <button v-for="cand in c.candidates" :key="cand.page" class="btn btn-outline btn-xs" :disabled="!row.anchor" @click="attach(p, row, v, cand.page)" :title="'Связать с этой страницей (сходство ' + cand.score + '). Затем «Применить сопоставления».'">стр.{{ cand.page }}</button>
                        </div>
                        <button class="btn btn-outline btn-xs" :disabled="!row.anchor" title="Отметить, что этого листа в версии нет. Это ручная правка — затем «Применить сопоставления»." @click="markAbsent(p, row, v)">листа нет</button>
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
            <button class="btn btn-outline btn-xs" :disabled="linksLocked(p)" title="Отменить последнюю ручную связь листов" @click="undoEdit(p)">Отменить последнюю правку</button>
          </div>
        </template>
      </template>
    </div>
  </section>

  <div v-if="upload.open" class="modal-overlay" @click.self="closeUpload">
    <div class="modal vc-modal">
      <div class="vc-modal-head"><h3>Загрузка проекта и версий</h3><button class="btn btn-outline btn-sm" title="Закрыть окно" @click="closeUpload">✕</button></div>
      <div v-if="upload.step === 1">
        <button class="btn btn-outline" :disabled="upload.busy || (batchUpload && batchUpload.busy)" title="Пакетная загрузка: каждая папка — отдельный проект со своими версиями." @click="upload.open=false; batchUpload.openNew()">Загрузить несколько проектов из папок</button>
        <div v-if="batchUpload && batchUpload.saved.length" class="vc-upload-batches">
          <div class="vc-muted">Пакеты загрузки (несколько проектов сразу):</div>
          <div v-for="b in batchUpload.saved" :key="b.batch_id" class="vc-upload-batch">
            <span>{{ batchUpload.batchLabel(b) }}</span><span v-if="!b.started" class="vc-warning"> · файлы догружены не полностью</span>
            <button class="btn btn-outline btn-xs" :disabled="upload.busy || batchUpload.busy"
                    :title="b.started ? 'Открыть пакет: ход обработки проектов, проверка состава и порядка версий, пауза очереди' : 'Открыть пакет, чтобы догрузить файлы (выберите те же исходные папки) или удалить его'"
                    @click="upload.open=false; batchUpload.openBatch(b.batch_id)">Открыть</button>
          </div>
        </div>
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
        <div class="vc-modal-actions"><button class="btn btn-primary" :disabled="!upload.files.length || upload.busy" title="Отправить выбранные файлы; система предложит, как разложить их по проектам и в каком порядке." @click="sendFiles">Загрузить и разобрать</button></div>
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
                <button class="btn btn-outline btn-xs" :disabled="fi === 0" title="Поднять выше в порядке версий" @click="moveFile(g, fi, -1)">↑</button>
                <button class="btn btn-outline btn-xs" :disabled="fi === g.files.length - 1" title="Опустить ниже в порядке версий" @click="moveFile(g, fi, 1)">↓</button>
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
          <button class="btn btn-outline" title="Закрыть окно без загрузки" @click="closeUpload">Отмена</button>
          <button class="btn btn-primary" :disabled="upload.busy || hasBadFiles()" title="Сохранить проекты и порядок версий так, как показано выше, и начать разбор." @click="confirmUpload">Подтвердить группировку и порядок</button>
        </div>
      </div>
    </div>
  </div>

  <div v-if="order.open" class="modal-overlay" @click.self="order.open=false">
    <div class="modal vc-modal">
      <div class="vc-modal-head"><h3>Порядок версий · {{ order.title }}</h3><button class="btn btn-outline btn-sm" title="Закрыть окно" @click="order.open=false">✕</button></div>
      <p class="vc-muted">Порядок задаёте вы. Номер в имени файла, «Изм.» по штампам и дата выпуска — только подсказки.</p>
      <table class="vc-files">
        <tr><th>№</th><th>Файл</th><th>Метка</th><th>Подсказки</th><th></th></tr>
        <tr v-for="(v, i) in order.items" :key="v.version_id">
          <td>{{ i + 1 }}</td><td>{{ v.file_name }}</td>
          <td><input class="vc-input" v-model="v.user_label"></td>
          <td class="vc-muted">{{ hint({detected: v.detected}) }}</td>
          <td class="vc-nowrap"><button class="btn btn-outline btn-xs" :disabled="i === 0" title="Поднять выше в порядке версий" @click="moveOrder(i, -1)">↑</button>
            <button class="btn btn-outline btn-xs" :disabled="i === order.items.length - 1" title="Опустить ниже в порядке версий" @click="moveOrder(i, 1)">↓</button></td>
        </tr>
      </table>
      <div class="vc-modal-actions">
        <button class="btn btn-outline" title="Закрыть окно без изменений" @click="order.open=false">Отмена</button>
        <button class="btn btn-primary" :disabled="order.busy" title="Сохранить порядок и подписи. Сопоставление листов пересоберётся, сравнения по старому порядку станут устаревшими." @click="saveOrder">Сохранить</button>
      </div>
    </div>
  </div>

  <vc-md-report v-if="mdReport.open" :object-id="objectId" :project="mdReport.project" :series-id="mdReport.seriesId"
                :series-ready="mdReport.seriesReady" @close="mdReport.open = false"></vc-md-report>
  <vc-ai-report v-if="aiPanel.open" :object-id="objectId" :run="aiPanel.run" @close="aiPanel.open=false" @open-evidence="openEvidence"></vc-ai-report>
  <div v-if="sheetView.open" class="vc-sheetview" tabindex="0" @click.self="sheetView.open = false" @keydown.esc="sheetView.open = false">
    <div class="vc-sheetview-box">
      <div class="vc-sheetview-head">
        <div><b>{{ sheetView.title }}</b><div class="vc-muted">{{ sheetView.note }}</div></div>
        <div class="vc-sheetview-actions">
          <span class="vc-muted">Ctrl+колесо — масштаб, перетаскивание — сдвиг</span>
          <label v-if="sheetView.items.length > 1" class="vc-muted" title="Масштаб и сдвиг одного листа повторяются на остальных (по долям листа)"><input type="checkbox" v-model="sheetView.sync.on"> вместе</label>
          <button class="btn btn-outline btn-xs" title="Крупнее" @click="sheetZoom('in')">＋</button>
          <button class="btn btn-outline btn-xs" title="Мельче" @click="sheetZoom('out')">−</button>
          <button class="btn btn-outline btn-xs" title="Показать лист целиком" @click="sheetZoom('fit')">Вписать</button>
          <template v-if="sheetView.nav">
            <button class="btn btn-outline btn-xs" :disabled="sheetView.nav.i <= 0" title="Предыдущий лист без пары в этом переходе" @click="stepSingle(-1)">‹ лист</button>
            <span class="vc-muted">{{ sheetView.nav.i + 1 }} / {{ sheetView.nav.list.length }}</span>
            <button class="btn btn-outline btn-xs" :disabled="sheetView.nav.i >= sheetView.nav.list.length - 1" title="Следующий лист без пары в этом переходе" @click="stepSingle(1)">лист ›</button>
          </template>
          <button class="btn btn-outline btn-xs" title="Закрыть окно" @click="sheetView.open = false">✕ Закрыть</button>
        </div>
      </div>
      <div class="vc-sheetview-side">
        <div v-for="it in sheetView.items" :key="it.key" class="vc-side-col" :class="{ref: it.ref, solo: sheetView.items.length === 1}">
          <div v-if="it.label || it.attach" class="vc-side-head"><b>{{ it.label }}</b><span class="vc-muted"> {{ it.note }}</span>
            <button v-if="it.attach" class="btn btn-primary btn-xs" title="Связать эти листы. Это ручная правка — затем «Применить сопоставления»." @click="it.attach(); sheetView.open = false">{{ it.actionLabel || 'Связать' }}</button></div>
          <vc-zoom-pane :page="it.page" :thumb-url="it.url" :sync="sheetView.sync"></vc-zoom-pane>
        </div>
        <aside v-if="sheetView.ai" class="vc-sheet-ai">
          <template v-for="sg in [sheetView.ai]" :key="sg.spid">
            <div class="vc-sheet-ai-head"><span class="vc-ai-single-kind" :class="'k-' + sg.kind">{{ sg.label }}</span> <b>Вывод ИИ по листу</b></div>
            <template v-if="sg.description">
              <div><span class="vc-muted">Тип листа:</span> {{ sg.description.sheet_kind }}</div>
              <p>{{ sg.description.summary }}</p>
              <template v-if="sg.description.objects && sg.description.objects.length">
                <div class="vc-muted">Что на листе:</div>
                <ul><li v-for="(o, k) in sg.description.objects" :key="k"><b>{{ o.name }}</b><template v-if="o.detail"> — {{ o.detail }}</template></li></ul>
              </template>
              <div v-if="sg.description.marks && sg.description.marks.length"><span class="vc-muted">Марки:</span> {{ sg.description.marks.join(', ') }}</div>
              <div v-if="sg.description.limitation" class="vc-warning">Неясно: {{ sg.description.limitation }}</div>
              <div class="vc-muted vc-sheet-ai-note">Описание составлено ИИ по изображению и тексту листа и инженером не подтверждено.</div>
            </template>
            <div v-else class="vc-muted">Описания нет: {{ aiSeriesStateLabel(sg.state) }}.</div>
          </template>
        </aside>
      </div>
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
        const gap = body ? px(getComputedStyle(body).rowGap) : 0;
        // Leave room for all bottom padding when the page reaches its scroll limit,
        // so column titles never slide under the header.
        const h = Math.max(120, Math.floor(viewH - headerH - gap - spaceBelow(el, sp) - 8));
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
                // Объект — тот, что выбран в шапке платформы (currentObjectId в app.js); своего выбора у раздела нет.
                props: {portalObjectId: {type: String, default: null}},
                setup(props) {
                    const {ref, reactive, watch, onMounted, onBeforeUnmount} = root.Vue;
                    const objects = ref([]);
                    const objectId = ref('');
                    const projects = ref([]);
                    // Выпадающие списки проектов — по алфавиту, номера по значению (АР2 раньше АР10).
                    const sortedProjects = () => [...projects.value].sort((x, y) =>
                        String(x.title || '').localeCompare(String(y.title || ''), 'ru', {numeric: true, sensitivity: 'base'}));
                    const selectedProject = ref('');
                    // Две страницы раздела: «Проекты и этапы» (version-comparison-projects.js) и «Сравнение листов» проекта.
                    const view = ref((() => { try { return localStorage.getItem('version-comparison:view') || 'projects'; } catch (e) { return 'projects'; } })());
                    const catalogVersion = ref(0);
                    watch(view, v => { try { localStorage.setItem('version-comparison:view', v); } catch (e) { /* без хранилища */ } });
                    const selectedTitle = () => (projects.value.find(p => p.project_id === selectedProject.value) || {}).title || '';
                    function showProjects() { view.value = 'projects'; }
                    function showProject() { if (selectedProject.value) { view.value = 'project'; selectProject(); } }
                    // Со страницы «Проекты и этапы»: матрица перечитывается — там могли закончиться этапы
                    // (например, ИИ-проверка сомнительных пар поставила пары).
                    function openProject(pid) {
                        selectedProject.value = pid;
                        try { localStorage.setItem('version-comparison:project', pid); } catch (e) { /* без хранилища */ }
                        showProject();
                        const p = projects.value.find(x => x.project_id === pid);
                        if (p && expanded[pid] && matrices[pid]) loadMatrix(p);
                    }
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
                    const activeRun = reactive({});
                    const matrixRunCache = reactive({});
                    const matrixRequests = {};
                    const runs = reactive({});
                    const seriesMap = reactive({});
                    const viewer = reactive({open: false, run: null, items: [], spid: '', focus: null});
                    // Лист без пары (или пара ещё не посчитана) — открывается сам лист крупно.
                    const sheetView = reactive({open: false, title: '', note: '', items: [], sync: {}, ai: null, nav: null});
                    // Окно листа: каждый лист — vc-zoom-pane (version-comparison-zoom.js); sync — общий вид листов «рядом».
                    const pagePath = (sha, page) => `/objects/${enc(objectId.value)}/sources/${sha}/pages/${page}`;
                    function openSheets(fields) {
                        Object.assign(sheetView, {open: true, note: '', ai: null, nav: null, sync: {on: true, rev: 0, cmd: {kind: '', n: 0}}}, fields);
                    }
                    function sheetZoom(kind) { sheetView.sync.cmd = {kind, n: sheetView.sync.cmd.n + 1}; }
                    const aiPanel = reactive({open: false, run: null, project: null});
                    // «Отчёт PDF» по MD-сводке: окно version-comparison-md-report.js.
                    const mdReport = reactive({open: false, project: null, seriesId: '', seriesReady: false});
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
                            if (props.portalObjectId !== null) return;          // объект задаёт платформа
                            const saved = (() => { try { return localStorage.getItem('version-comparison:object') || ''; } catch (e) { return ''; } })();
                            if (!objectId.value && saved && objects.value.some(o => o.id === saved)) {
                                objectId.value = saved;
                                await loadCatalog();
                            }
                        } catch (e) { fail(e); }
                    }
                    // Смена объекта в шапке платформы — раздел переключается на него.
                    function usePortalObject(v) {
                        if (v === null || v === objectId.value) return;
                        objectId.value = v || '';
                        selectedProject.value = '';
                        loadCatalog();
                    }
                    watch(() => props.portalObjectId, usePortalObject);

                    function selectProject() {
                        try { localStorage.setItem('version-comparison:project', selectedProject.value || ''); } catch (e) { /* без хранилища */ }
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
                            if (!projects.value.some(p => p.project_id === selectedProject.value)) {
                                const saved = (() => { try { return localStorage.getItem('version-comparison:project') || ''; } catch (e) { return ''; } })();
                                selectedProject.value = projects.value.some(p => p.project_id === saved) ? saved : (projects.value[0]?.project_id || '');
                            }
                            catalogVersion.value++;
                            for (const p of projects.value) {
                                if (!pairs[p.project_id]) {
                                    const v = p.versions;
                                    pairs[p.project_id] = {a: v.length > 1 ? v[v.length - 2] : (v[0] || ''), b: v[v.length - 1] || ''};
                                }
                                if (view.value !== 'project') continue;
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

                    const refreshPending = data => ['queued', 'running', 'cancelling', 'pausing'].includes(data.state)
                        || (data.state === 'interrupted' && data.auto_resume);
                    const retryRefresh = e => !e.status || e.status >= 500;

                    async function pickRun(p, runId) {
                        const oid = objectId.value;
                        try {
                            const run = await api(`/objects/${enc(objectId.value)}/runs/${runId}`);
                            if (oid !== objectId.value) return;
                            activeRun[p.project_id] = run;
                            matrixRunCache[run.run_id] = run;
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
                    const aiSeriesMap = reactive({});
                    // Листы перехода (проверить / добавлен / удалён) под строкой — свёрнуты, ▸ раскрывает.
                    const aiFold = reactive({});
                    const specSummaryMap = reactive({});
                    const aiSeriesOf = p => aiSeriesMap[p.project_id] || null;
                    const aiBase = p => `/objects/${enc(objectId.value)}/series/${seriesOf(p).series_id}/ai-sheet-changes`;
                    const aiSummaryUrl = p => `${API}${aiBase(p)}/summary.md`;
                    const aiSummaryReady = p => !!(seriesOf(p) && specSummaryMap[p.project_id]?.pipeline?.ready !== false && ((aiSeriesOf(p)?.pairs || []).some(x => x.job_id)
                        || (specSummaryMap[p.project_id]?.series_id === seriesOf(p).series_id && specSummaryMap[p.project_id]?.ready)));
                    async function loadAiSeries(p) {
                        clearTimeout(timers['ai' + p.project_id]);
                        if (!seriesOf(p)) return;
                        const sid = seriesOf(p).series_id;
                        try {
                            const [d, specs] = await Promise.all([api(aiBase(p)),
                                api(`/objects/${enc(objectId.value)}/series/${sid}/whole-specifications/status`)]);
                            if (!alive || !seriesOf(p) || seriesOf(p).series_id !== sid) return;
                            aiSeriesMap[p.project_id] = d;
                            specSummaryMap[p.project_id] = {...specs, series_id: sid};
                            if (['running', 'queued'].includes(d.state) || specs.pipeline?.ready === false) schedule('ai' + p.project_id, () => loadAiSeries(p), d.state === 'queued' ? 5000 : 3000);
                        } catch (e) { if (retryRefresh(e)) schedule('ai' + p.project_id, () => loadAiSeries(p), 10000); }
                    }
                    async function cancelAiSeries(p) {
                        try { aiSeriesMap[p.project_id] = await api(aiBase(p) + '/cancel', {method: 'POST'}); } catch (e) { fail(e); }
                        schedule('ai' + p.project_id, () => loadAiSeries(p), 2000);
                        loadAiQueue();
                    }
                    async function resumeAiSeries(p) {
                        try { aiSeriesMap[p.project_id] = await api(aiBase(p) + '/resume', {method: 'POST'}); } catch (e) { fail(e); }
                        schedule('ai' + p.project_id, () => loadAiSeries(p), 2000);
                        loadAiQueue();
                    }
                    // Очередь проектов «Описать изменения ИИ» (одна на установку): идёт один, остальные ждут по порядку.
                    const aiQueue = ref(null);
                    async function loadAiQueue() {
                        clearTimeout(timers.aiQueue);
                        try {
                            const q = await api('/ai-sheet-changes/queue');
                            if (!alive) return;
                            aiQueue.value = q;
                            if (q.current || q.waiting.length) schedule('aiQueue', loadAiQueue, 5000);
                        } catch (e) { if (retryRefresh(e)) schedule('aiQueue', loadAiQueue, 15000); }
                    }
                    const AI_QUEUE_PHASES = {running: 'идёт', paused: 'на паузе', interrupted: 'досчитывается после перезапуска', queued: 'ждёт'};
                    const aiQueueName = x => `«${x.code}»` + (x.object_id !== objectId.value ? ' (другой объект)' : '');
                    function aiQueueText() {
                        const q = aiQueue.value;
                        if (!q) return '';
                        const parts = [];
                        if (q.current) parts.push(`${AI_QUEUE_PHASES[q.current.phase] || q.current.phase} ${aiQueueName(q.current)}`);
                        if (q.waiting.length) parts.push('далее: ' + q.waiting.map(aiQueueName).join(' → '));
                        return parts.join(' · ');
                    }
                    function aiQueueAhead(a) {
                        const ahead = (a.queue && a.queue.ahead) || [];
                        if (!ahead.length) return 'начнётся в ближайшие секунды';
                        return 'перед ним: ' + ahead.map(x => aiQueueName(x) + (x.phase !== 'queued' ? ` — ${AI_QUEUE_PHASES[x.phase] || x.phase}` : '')).join(', ')
                            + '; начнётся сам, когда они закончатся';
                    }
                    function openMdReport(p) {
                        Object.assign(mdReport, {open: true, project: p, seriesId: seriesOf(p) ? seriesOf(p).series_id : '',
                            seriesReady: aiSummaryReady(p)});
                    }
                    const AI_SERIES_STATES = {running: 'идёт', queued: 'в очереди', cancelling: 'останавливается', done: 'готово',
                        partial: 'готово, есть ошибки', interrupted: 'прервано', cancelled: 'отменено', failed: 'ошибка',
                        paused: 'пауза', paused_quota: 'пауза — лимит подписки', paused_auth: 'пауза — вход в Codex', paused_user: 'пауза',
                        pausing: 'ставится на паузу…', skipped: 'пропуск', none: '—'};
                    const aiSeriesStateLabel = st => AI_SERIES_STATES[st] || st;
                    const fmtNum = n => (n || 0).toLocaleString('ru-RU');
                    const fmtDuration = s => s == null ? '—' : (s >= 60 ? `${Math.floor(s / 60)} мин ${Math.round(s % 60)} с` : `${Math.round(s)} с`);
                    const pairWall = x => {
                        const t0 = x.started_at && Date.parse(x.started_at), t1 = x.finished_at ? Date.parse(x.finished_at) : Date.now();
                        return t0 ? (t1 - t0) / 1000 : null;
                    };
                    // Лист без пары (добавлен / удалён): сравнивать не с чем — лист крупно и справа вывод ИИ;
                    // ‹ лист › — по остальным листам без пары этого перехода.
                    function singleView(x, list, i) {
                        const sg = list[i], sha = sg.side === 'B' ? x.sha_b : x.sha_a;
                        return {title: `${sg.label}: ${sg.title || 'Лист'} · ${sg.side === 'B' ? x.b_label : x.a_label} · стр. ${sg.page}`,
                            note: `${x.a_label} → ${x.b_label} · пары в соседней версии нет, сравнивать не с чем`,
                            items: [{key: sg.spid, page: pagePath(sha, sg.page), url: `${API}${pagePath(sha, sg.page)}/thumb?w=2000`}],
                            ai: sg, nav: {x, list, i}};
                    }
                    function showSingle(x, sg) {
                        const list = (x.singles || []).filter(s => s.side === 'B' ? x.sha_b : x.sha_a);
                        const i = list.findIndex(s => s.spid === sg.spid);
                        if (i >= 0) openSheets(singleView(x, list, i));
                    }
                    function stepSingle(d) {
                        const n = sheetView.nav, j = n ? n.i + d : -1;
                        if (n && j >= 0 && j < n.list.length) Object.assign(sheetView, singleView(n.x, n.list, j));
                    }
                    // «Открыть» в строке перехода: та же пара A → B в матрице и окно сверки с первого изменённого листа;
                    // справа в окне — «Изменения в листе» (что ИИ написал по листу). only — только эти листы (кнопка «Проверить»).
                    async function openTransitionAi(p, x, only = null, start = null) {
                        const pr = seriesOf(p) && seriesOf(p).pairs[x.index];
                        if (!pr || pr.run_id !== x.run_id) return;
                        openTransition(p, pr);
                        try {
                            const run = await api(`/objects/${enc(objectId.value)}/runs/${pr.run_id}`);
                            const done = (run.items || []).filter(it => it.kind === 'compare' && it.status === 'done' && (!only || only.includes(it.spid)));
                            const first = (start && done.find(it => it.spid === start))
                                || done.find(it => !['same', 'same_zones_changed'].includes(it.result_status)) || done[0];
                            if (!first) return fail(new Error('В этом переходе нет посчитанных пар листов — открывать нечего.'));
                            openViewer(p, first, run);
                            if (only) viewer.items = done;
                        } catch (e) { fail(e); }
                    }
                    const openReview = (p, x, start = null) => openTransitionAi(p, x, (x.review || []).map(r => r.spid), start);
                    // «Частично» — не ошибка: ошибки — сбои ИИ (колонка «Ошибок»), остальное — листы «проверить».
                    const aiPartialTitle = 'Готово, но часть листов требует внимания: «ошибок» — листы, которые ИИ не смог описать; «проверить» — ИИ изменений не нашёл, но сравнение оставило пометки.';
                    function aiStateText(state, review, failed) {
                        if (state !== 'partial') return aiSeriesStateLabel(state);
                        const parts = [failed ? `ошибок ${failed}` : '', review ? `проверить листов: ${review}` : ''].filter(Boolean);
                        return 'готово' + (parts.length ? ', ' + parts.join(', ') : ', есть замечания');
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

                    const pairReady = p => pairs[p.project_id] && pairs[p.project_id].a && pairs[p.project_id].b && pairs[p.project_id].a !== pairs[p.project_id].b;

                    function setSide(p, side, vid) {
                        const s = pairs[p.project_id];
                        if (side === 'a' && s.b === vid) s.b = s.a;
                        if (side === 'b' && s.a === vid) s.a = s.b;
                        s[side] = vid;
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
                        if (it.kind === 'chain') return 'пара через версию — подтвердите';
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
                            cancelled: 'отменено', interrupted: 'прерван (можно досчитать)', failed: 'ошибка',
                            pausing: 'ставится на паузу…', paused: 'на паузе (можно продолжить)'}[run.state] || run.state;
                    }
                    function cellClass(p, row, v) {
                        const s = pairs[p.project_id] || {};
                        const it = v.version_id === s.b ? rowItem(p, row) : null;
                        return {'col-a': v.version_id === s.a, 'col-b': v.version_id === s.b,
                            // Лист версии B, в котором сравнение с A нашло отличия, — в красной рамке.
                            'is-diff': !!(it && it.result_status === 'different'),
                            // Лист из группы листов таблицы (строки перенесены между листами) — общий фон группы.
                            'is-group': !!(row.cells[v.version_id] && row.cells[v.version_id].confidence === 'group')};
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
                        if (c.confidence) parts.push({origin: 'первое появление', high: 'уверенное сопоставление', medium: 'невысокая уверенность', manual: 'ручная связь', group: 'группа листов таблицы', ai: 'пара от ИИ-проверки'}[c.confidence] || c.confidence);
                        if (c.score !== null && c.score !== undefined) parts.push('оценка ' + c.score);
                        return parts.join(' · ') + '\nЩелчок — сравнить с предыдущей версией (первый столбец — со следующей); Shift+щелчок — ручная пара листов';
                    }
                    // Группа листов таблицы: строки спецификации разошлись по нескольким листам другой версии —
                    // лист стоит в строке по порядку внутри группы, это не утверждение «лист в лист».
                    function groupTitle(c) {
                        const g = (c.parts && c.parts.group) || '';
                        return 'Группа листов таблицы' + (g ? ` (${g})` : '') + ': строки перенесены между листами, '
                            + 'лист поставлен по порядку внутри группы. Сравнение — по позициям спецификации: '
                            + 'перенос строки на другой лист изменением не считается.';
                    }
                    // Ответ Луны по ячейке: ключ «версия A|страница A|версия B», где A — предыдущая версия строки.
                    function aiNote(m, row, v) {
                        const notes = m && m.ai && m.ai.notes;
                        if (!notes) return null;
                        const i = m.versions.findIndex(x => x.version_id === v.version_id);
                        if (i < 1) return null;
                        const va = m.versions[i - 1].version_id, ca = row.cells[va];
                        if (!ca || ca.state !== 'matched') return null;
                        return notes[`${va}|${ca.page}|${v.version_id}`] || null;
                    }
                    function aiDoubt(m, row, v) {
                        const n = aiNote(m, row, v);
                        return !!n && ['medium_other', 'medium_none'].includes(n.decision);
                    }
                    function aiTitle(m, row, v) {
                        const n = aiNote(m, row, v);
                        if (!n) return 'Пару связал ИИ (проверка сомнительных пар)';
                        const linked = (row.cells[v.version_id] || {}).state === 'matched';
                        const head = n.decision === 'accepted' && !linked
                            ? `ИИ и встречная проверка указали стр. ${n.page}, но пара не поставлена автоматически: строки листов уже связаны с другими версиями по-разному (мост через версию) — решите вручную`
                            : n.decision === 'accepted' ? 'Пару связал ИИ, встречная проверка подтвердила'
                            : n.decision === 'medium_other' ? `ИИ не согласен с парой алгоритма: тот же лист — стр. ${n.page}`
                            : n.decision === 'medium_none' ? 'ИИ не согласен с парой алгоритма: ни одна из страниц [пара, соседи] не тот же лист'
                            : (n.label || 'Ответ ИИ');
                        const lines = [head + (n.confidence ? ` (уверенность ${n.confidence})` : ''), 'Довод ИИ: ' + (n.reason || '—')];
                        if (n.reverse_reason) lines.push('Встречная проверка: ' + n.reverse_reason);
                        if (n.decision === 'accepted' && linked) lines.push('Неверно — отвяжите лист (⤫) или перетащите нужный: ручная правка главнее.');
                        return lines.join('\n');
                    }
                    function reloadMatrices() {
                        catalogVersion.value++;
                        for (const p of projects.value) if (expanded[p.project_id]) loadMatrix(p);
                    }
                    function thumbUrl(v, page) {
                        return `${API}/objects/${enc(objectId.value)}/sources/${v.sha256}/pages/${page}/thumb?w=160`;
                    }
                    // Щелчок по листу: пара «предыдущая версия → версия листа» (для первого столбца — «он → следующая»)
                    // становится A → B, и открывается сравнение этого листа. Нет пары или пара не посчитана —
                    // открывается сам лист. Считаются только соседние версии (страница «Проекты и этапы», столбец «Сравнение соседних»).
                    async function clickCell(p, row, v, c) {
                        const m = matrices[p.project_id];
                        if (!m || !m.versions) return;
                        const vs = m.versions.map(x => x.version_id);
                        const i = vs.indexOf(v.version_id);
                        if (i < 0) return;
                        const label = vid => { const x = m.versions.find(y => y.version_id === vid); return x ? (x.user_label || x.file_name) : vid; };
                        if (vs.length < 2) return showSheet(v, c, row, 'в проекте одна версия — сравнивать не с чем');
                        const a = vs[i === 0 ? 0 : i - 1], b = vs[i === 0 ? 1 : i];
                        const s = pairs[p.project_id] || (pairs[p.project_id] = {a: '', b: ''});
                        if (s.a !== a || s.b !== b) { s.a = a; s.b = b; }
                        const cand = pairRun(p);
                        let run = cand ? matrixRunCache[cand.run_id] : null;
                        if (cand && (!run || run.state !== 'done')) {
                            try { run = await api(`/objects/${enc(objectId.value)}/runs/${cand.run_id}`); matrixRunCache[run.run_id] = run; }
                            catch (e) { run = null; }
                        }
                        const it = run && run.scope === 'all' ? run.items.find(x => x.row_id === row.row_id) : null;
                        // Пара прогона не совпадает с текущей связью строки — прежний результат не открываем.
                        const ca = row.cells[a], cb = row.cells[b];
                        const moved = it && it.kind === 'compare' && (it.pair_stale || !ca || !cb || ca.page !== it.a_page || cb.page !== it.b_page);
                        if (moved) {
                            return showPair(p, row, [[a, it.a_page, 'было в расчёте'], [b, it.b_page, 'было в расчёте']],
                                (it.pair_stale || 'связь листов изменилась после расчёта') + ' — сравнение по прежней паре устарело: «Заново» в столбце «Сравнение соседних» на странице «Проекты и этапы»');
                        }
                        if (it && it.kind === 'compare' && it.status === 'done') return openViewer(p, it, run);
                        const other = v.version_id === a ? b : a;
                        const why = !cand ? `пара ${label(a)} → ${label(b)} ещё не посчитана — сравниваются только соседние версии (столбец «Сравнение соседних» на странице «Проекты и этапы»)`
                            : (it && it.kind === 'compare' ? 'сравнение этого листа ещё не готово' : `у листа нет пары в версии ${label(other)}`);
                        showSheet(v, c, row, why, label(v.version_id));
                    }
                    // Спорная ячейка: опорный лист соседней версии и кандидаты рядом; «Связать» — та же ручная правка, что и кнопка кандидата.
                    function sideBySide(p, row, v, c) {
                        const m = matrices[p.project_id];
                        if (!m) return;
                        const vs = m.versions.map(x => x.version_id), i = vs.indexOf(v.version_id);
                        const order = [i - 1, i + 1, ...vs.map((_, k) => k)].filter(k => k >= 0 && k < vs.length && k !== i);
                        const k = order.find(k => row.cells[vs[k]] && row.cells[vs[k]].state === 'matched');
                        const label = vid => { const x = m.versions.find(y => y.version_id === vid); return x ? (x.user_label || x.file_name) : vid; };
                        const items = [];
                        if (k !== undefined) {
                            const rv = m.versions[k], rc = row.cells[rv.version_id];
                            items.push({key: 'ref', ref: true, label: `${label(rv.version_id)} · стр. ${rc.page}${rc.sheet_no ? ' · л.' + rc.sheet_no : ''}`,
                                note: '(лист строки)', page: pagePath(rv.sha256, rc.page), url: `${API}${pagePath(rv.sha256, rc.page)}/thumb?w=1400`});
                        }
                        for (const cand of c.candidates || []) {
                            items.push({key: 'c' + cand.page, label: `${label(v.version_id)} · стр. ${cand.page}${cand.sheet_no ? ' · л.' + cand.sheet_no : ''}`,
                                note: `кандидат, оценка ${cand.score}`, page: pagePath(v.sha256, cand.page), url: `${API}${pagePath(v.sha256, cand.page)}/thumb?w=1400`,
                                attach: row.anchor ? () => attach(p, row, v, cand.page) : null});
                        }
                        openSheets({items,
                            title: `${row.display_title || 'Лист'} · нужна проверка: ${label(v.version_id)}`,
                            note: (c.reason_code ? 'Почему не принято автоматически: ' + c.reason + '. ' : '') + 'Кандидаты отсортированы по оценке сходства (не процент достоверности); при равной оценке — по положению в файле. «Связать» добавляет ручную правку в черновик.'});
                    }
                    // Пара через версию (прогон «все листы» несоседней пары): не сравнивалась — оба листа рядом.
                    function chainPair(p, row, it) {
                        const s = pairs[p.project_id];
                        showPair(p, row, [[s.a, it.a_page, 'A'], [s.b, it.b_page, 'B']],
                            'Пара собрана по цепочке через промежуточные версии и не сравнивалась: сравниваются только соседние версии.');
                    }
                    function showPair(p, row, cells, note) {
                        const m = matrices[p.project_id];
                        const items = cells.map(([vid, page, what]) => {
                            const v = m.versions.find(x => x.version_id === vid);
                            return {key: vid + page, label: `${v.user_label || v.file_name} · стр. ${page}`, note: what,
                                page: pagePath(v.sha256, page), url: `${API}${pagePath(v.sha256, page)}/thumb?w=1400`};
                        });
                        openSheets({items, note, title: row.display_title || 'Лист'});
                    }
                    function showSheet(v, c, row, note, vlabel) {
                        openSheets({note, items: [{key: 'one', page: pagePath(v.sha256, c.page), url: `${API}${pagePath(v.sha256, c.page)}/thumb?w=2000`}],
                            title: `${row.display_title || 'Лист'} · ${vlabel || v.user_label || v.file_name} · стр. ${c.page}`});
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

                    // Последний прогон выбранной пары A → B (все листы): по нему открываются листы матрицы.
                    function pairRun(p) {
                        const pr = pairs[p.project_id];
                        if (!pr) return null;
                        const list = (runs[p.project_id] || []).filter(r => r.scope === 'all'
                            && r.a && r.b && r.a.version_id === pr.a && r.b.version_id === pr.b);
                        const own = list.find(r => r.state === 'done');
                        if (own) return own;
                        // Пара могла быть посчитана серией «Сравнение соседних».
                        const sp = seriesOf(p) && seriesOf(p).pairs.find(x => x.a === pr.a && x.b === pr.b && x.run_id);
                        if (sp && (sp.run_state === 'done' || !list.length)) return {run_id: sp.run_id, state: sp.run_state || sp.state};
                        return list[0] || null;
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

                    onMounted(() => { usePortalObject(props.portalObjectId); loadObjects(); loadAiQueue(); });
                    onBeforeUnmount(() => { alive = false; Object.values(timers).forEach(clearTimeout); });

                    return {view, catalogVersion, selectedTitle, showProjects, showProject, openProject,
                        aiPanel, aiRun, showAiReport, openEvidence, objects, objectId, projects, sortedProjects, selectedProject, selectProject, catalogLoading, error, expanded, matrices, pairs, activeRun, runs,
                        viewer, upload, batchUpload, order, statusInfo, loadCatalog, toggleProject, topHidden, onlyChanged, pairHasRun, visibleRows, pairReady, setSide,
                        pickRun, pairRun, rowItem, itemLabel, runCounts, progressPct, runStateLabel,
                        syncPairRun, cellClass, cellTitle, groupTitle, aiNote, aiDoubt, aiTitle, reloadMatrices, thumbUrl, clickCell, sheetView, sheetZoom, sideBySide, chainPair, dragStart, dropOnCell, detach, attach, markAbsent,
                        matrixElements, linkDraft, linksBusy, linksLocked, applyLinks, undoEdit, failedVersions, openViewer, openUpload, closeUpload, pickFiles, mb, uploadPct, sendFiles, hint,
                        moveFile, moveToGroup, hasBadFiles, confirmUpload, openOrder, moveOrder, saveOrder,
                        aiSeriesOf, cancelAiSeries,
                        resumeAiSeries, aiQueue, aiQueueText, aiQueueAhead, aiSummaryUrl, aiSummaryReady, mdReport, openMdReport, aiSeriesStateLabel, aiFold, aiStateText, aiPartialTitle, openReview, showSingle, stepSingle, fmtNum, fmtDuration, pairWall, openTransitionAi,
                        seriesOf, seriesMatchesOrder, cancelSeries, resumeSeries, openTransition, isOpenTransition,
                        pairStateLabel, colTitle,
                        partOf, partsFrom, partTitle};
                },
            });
            if (root.VersionComparisonBatch) root.VersionComparisonBatch.register(app);
            if (root.VersionComparisonProjects) root.VersionComparisonProjects.register(app);
            if (root.VersionComparisonViewer) root.VersionComparisonViewer.register(app);
            if (root.VersionComparisonZoom) root.VersionComparisonZoom.register(app);
            if (root.VersionComparisonAI) root.VersionComparisonAI.register(app);
            if (root.VersionComparisonSheetChanges) root.VersionComparisonSheetChanges.register(app);
            if (root.VersionComparisonMdReport) root.VersionComparisonMdReport.register(app);
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
