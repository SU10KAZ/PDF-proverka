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
            throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
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
      <div class="vc-sub">Наложение листов разных версий проекта без ИИ: общее — серым, только в исходной версии — красным, только в сравниваемой — синим.</div>
    </div>
    <div class="vc-head-actions">
      <label class="vc-field">Объект
        <select v-model="objectId" @change="loadCatalog">
          <option value="">— выберите объект —</option>
          <option v-for="o in objects" :key="o.id" :value="o.id">{{ o.name }}{{ o.versions ? ' · версий: ' + o.versions : '' }}</option>
        </select>
      </label>
      <button class="btn btn-primary" :disabled="!objectId" @click="openUpload">⇪ Загрузить версии</button>
    </div>
  </div>
  <div v-if="error" class="vc-error" @click="error=''">{{ error }}</div>
  <div v-if="!objectId" class="vc-empty">Выберите объект. Версии привязываются к объекту, но хранятся отдельно от проектов аудита.</div>
  <div v-else-if="catalogLoading" class="vc-empty">Загрузка каталога…</div>
  <div v-else-if="!projects.length" class="vc-empty">В объекте ещё нет версий. Нажмите «Загрузить версии» и выберите папку с PDF.</div>

  <section v-for="p in projects" :key="p.project_id" class="vc-project">
    <header class="vc-project-head" @click="toggleProject(p)">
      <span class="vc-caret">{{ expanded[p.project_id] ? '▼' : '▶' }}</span>
      <span class="vc-project-title">{{ p.title }}</span>
      <span v-if="p.code" class="vc-code">{{ p.code }}</span>
      <span class="vc-muted">· версий: {{ p.versions.length }}</span>
      <span class="vc-spacer"></span>
      <button class="btn btn-outline btn-sm" :disabled="seriesBusy(p) || p.versions.length < 2" title="Сравнить каждую версию со следующей: (0)→(1), (1)→(2)…" @click.stop="startSeries(p)">Сравнить все соседние</button>
      <button class="btn btn-outline btn-sm" title="Повторы и пропуски номеров, дубли, пустые формы СО, ведомость ↔ штампы" @click.stop="openComposition(p)">Контроль состава</button>
      <button class="btn btn-outline btn-sm" @click.stop="openOrder(p)">Порядок версий</button>
    </header>
    <div v-if="expanded[p.project_id]" class="vc-project-body">
      <template v-for="m in [matrices[p.project_id]]" :key="'m' + p.project_id">
        <div v-if="!m || m.state === 'loading'" class="vc-empty">Загрузка…</div>
        <div v-else-if="m.state === 'profiling'" class="vc-empty">Разбор версий: готово {{ m.progress ? m.progress.ready : 0 }} из {{ m.progress ? m.progress.total : '?' }}…</div>
        <div v-else-if="m.state === 'building'" class="vc-empty">Сопоставление листов между версиями…</div>
        <div v-else-if="m.state === 'failed'" class="vc-error">Не удалось подготовить версии: {{ m.error || failedVersions(m) }}</div>
        <template v-else-if="m.state === 'ready'">
          <div class="vc-pairbar">
            <div class="vc-pair">
              <span class="vc-chip vc-chip-a">A · исходная</span>
              <select v-model="pairs[p.project_id].a">
                <option v-for="v in m.versions" :key="v.version_id" :value="v.version_id">{{ versionTitle(v) }}</option>
              </select>
              <button class="btn btn-outline btn-sm" title="Поменять местами" @click="swap(p)">⇄</button>
              <span class="vc-chip vc-chip-b">B · сравниваемая</span>
              <select v-model="pairs[p.project_id].b">
                <option v-for="v in m.versions" :key="v.version_id" :value="v.version_id">{{ versionTitle(v) }}</option>
              </select>
            </div>
            <div class="vc-pair-actions">
              <button class="btn btn-primary btn-sm" :disabled="!pairReady(p) || runBusy(p)" @click="startRun(p, 'all')">Сравнить все сопоставленные листы</button>
              <button class="btn btn-accent btn-sm" :disabled="!pairReady(p) || !manual(p).length || runBusy(p)" @click="startRun(p, 'manual')">Сравнить выбранные листы ({{ manual(p).length }})</button>
              <span v-for="(mp, i) in manual(p)" :key="i" class="vc-manual-chip">стр.{{ mp.a_page }} ↔ стр.{{ mp.b_page }} <button @click="manual(p).splice(i, 1)">×</button></span>
              <span v-if="pending[p.project_id]" class="vc-hint">выбрана стр.{{ pending[p.project_id].page }} в A — щёлкните лист в столбце B</span>
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
                <button v-if="['interrupted','cancelled','failed'].includes(s.state)" class="btn btn-outline btn-sm" @click="resumeSeries(p, s)">Досчитать</button>
              </div>
              <table class="vc-series-table">
                <thead><tr><th>Переход A → B</th><th>Листов</th><th title="Совпадает">✓</th><th title="Изменены только служебные зоны">◐</th><th title="Есть отличия">●</th><th title="Нужна проверка">⚠</th><th>Состав</th><th>Вывод</th><th></th></tr></thead>
                <tbody>
                  <tr v-for="(pr, i) in s.pairs" :key="i" :class="{cur: isOpenTransition(p, pr)}">
                    <td class="vc-tr-name">{{ pr.a_label }} → {{ pr.b_label }}</td>
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

          <div v-if="activeRun[p.project_id]" class="vc-runbar">
            <template v-for="run in [activeRun[p.project_id]]" :key="run.run_id">
              <span><b>{{ run.a.label || run.a.file_name }}</b> → <b>{{ run.b.label || run.b.file_name }}</b> · {{ run.scope === 'manual' ? 'выбранные листы' : 'все листы' }}</span>
              <span class="vc-run-state" :class="'st-' + run.state">{{ runStateLabel(run) }}</span>
              <span v-if="run.progress" class="vc-progress"><span :style="{width: progressPct(run) + '%'}"></span></span>
              <span v-if="run.progress" class="vc-muted">{{ run.progress.done }} из {{ run.progress.total }}</span>
              <span class="vc-counts">
                <span v-for="(n, code) in runCounts(run)" :key="code" class="vc-badge" :class="'b-' + statusInfo(code).cls">{{ statusInfo(code).icon }} {{ statusInfo(code).label }}: {{ n }}</span>
              </span>
              <button v-if="run.permit && run.permit.available" class="btn btn-outline btn-sm" title="Разрешение на внесение изменений версии B: заявлено проектировщиком ↔ найдено сравнением" @click="openPermit(p, run)">Разрешение: заявлено {{ run.permit.declared_and_found + run.permit.declared_no_diff }} · без отличий {{ run.permit.declared_no_diff }} · не заявлено {{ run.permit.diff_not_declared }}</button>
              <button v-if="run.spec && run.spec.available" class="btn btn-outline btn-sm" title="Спецификации сравнены по позициям через страницы" @click="openSpec(p, run)">СО по позициям: {{ run.spec.changes }} изм.</button>
              <button v-if="['queued','running'].includes(run.state)" class="btn btn-danger-outline btn-sm" @click="cancelRun(p, run)">Отменить</button>
              <button v-if="['interrupted','cancelled'].includes(run.state)" class="btn btn-outline btn-sm" @click="resumeRun(p, run)">Досчитать</button>
            </template>
            <select class="vc-run-select" :value="activeRun[p.project_id].run_id" @change="pickRun(p, $event.target.value)">
              <option v-for="r in runs[p.project_id] || []" :key="r.run_id" :value="r.run_id">{{ runTitle(r) }}</option>
            </select>
          </div>

          <div class="vc-matrix-wrap">
            <table class="vc-matrix">
              <thead>
                <tr>
                  <th class="vc-rowhead">Лист \\ версия</th>
                  <th v-for="v in m.versions" :key="v.version_id" :class="{'col-a': v.version_id === pairs[p.project_id].a, 'col-b': v.version_id === pairs[p.project_id].b}">
                    <div class="vc-colhead">
                      <div class="vc-col-label">{{ v.user_label || v.file_name }}
                        <span v-if="v.version_id === pairs[p.project_id].a" class="vc-chip vc-chip-a">A</span>
                        <span v-if="v.version_id === pairs[p.project_id].b" class="vc-chip vc-chip-b">B</span>
                      </div>
                      <div class="vc-col-meta" :title="v.file_name">{{ v.file_name }}</div>
                      <div class="vc-col-meta">стр.: {{ v.page_count }}<span v-if="v.detected.izm_max !== null && v.detected.izm_max !== undefined"> · Изм.{{ v.detected.izm_max }} (по штампам)</span></div>
                      <div v-if="v.detected.release_dates && v.detected.release_dates.length" class="vc-col-meta">выпуск: {{ v.detected.release_dates[v.detected.release_dates.length - 1] }}</div>
                      <div class="vc-col-pick">
                        <button class="btn btn-outline btn-xs" @click="setSide(p, 'a', v.version_id)">A</button>
                        <button class="btn btn-outline btn-xs" @click="setSide(p, 'b', v.version_id)">B</button>
                      </div>
                    </div>
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in m.rows" :key="row.row_id">
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
                           @click="clickCell(p, row, v, c)"
                           :title="cellTitle(c)">
                        <img loading="lazy" decoding="async" :src="thumbUrl(v, c.page)" alt="">
                        <div class="vc-thumb-meta">стр.{{ c.page }}<span v-if="c.sheet_no"> · л.{{ c.sheet_no }}</span>
                          <span v-if="c.confidence === 'medium'" class="vc-flag" title="Сопоставлено с невысокой уверенностью">?</span>
                          <span v-if="c.manual" class="vc-flag" title="Связь задана вручную">✎</span>
                          <span v-if="c.dups && c.dups.length" class="vc-flag" :title="'Дубль листа: стр. ' + c.dups.join(', ')">⧉</span>
                          <span v-if="partOf(m, v.version_id, c.page)" class="vc-flag vc-part" :title="partTitle(partOf(m, v.version_id, c.page))">◫</span>
                          <span v-if="partsFrom(m, v.version_id, c.page).length" class="vc-flag vc-part" :title="'Лист разделён в следующей версии: стр. ' + partsFrom(m, v.version_id, c.page).map(x => x.b_page).join(', ')">⇉</span>
                        </div>
                        <button class="vc-cell-menu" title="Отвязать в отдельную строку" @click.stop="detach(p, v, c)">⤫</button>
                      </div>
                      <div v-else-if="c.state === 'unmatched'" class="vc-empty-cell unmatched" :title="c.reason">
                        <div>⁇ не сопоставлен</div>
                        <div class="vc-cands">кандидаты:
                          <button v-for="cand in c.candidates" :key="cand.page" class="btn btn-outline btn-xs" @click="attach(p, row, v, cand.page)" :title="'оценка ' + cand.score">стр.{{ cand.page }}</button>
                        </div>
                        <button class="btn btn-outline btn-xs" @click="markAbsent(p, row, v)">листа нет</button>
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
          <div class="vc-legend">
            Ячейки: миниатюра — лист есть; <b>∅</b> — листа нет в этой версии (кандидатов не осталось); <b>⁇</b> — не сопоставлен (есть кандидаты: проверьте); <b>?</b> — невысокая уверенность; <b>⧉</b> — дубль; <b>✎</b> — ручная связь.
            Перетащите миниатюру в другую строку того же столбца, чтобы перепривязать лист. Щелчок по листу в столбце A, затем в столбце B — ручная пара для сравнения.
            <button v-if="m.edits && m.edits.length" class="btn btn-outline btn-xs" @click="undoEdit(p)">Отменить последнюю правку ({{ m.edits.length }})</button>
          </div>
        </template>
      </template>
    </div>
  </section>

  <div v-if="upload.open" class="modal-overlay" @click.self="closeUpload">
    <div class="modal vc-modal">
      <div class="vc-modal-head"><h3>Загрузка версий</h3><button class="btn btn-outline btn-sm" @click="closeUpload">✕</button></div>
      <div v-if="upload.step === 1">
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
              <option v-for="p in projects" :key="p.project_id" :value="p.project_id">добавить в «{{ p.title }}»</option>
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

  <div v-if="permitView.open" class="modal-overlay" @click.self="permitView.open=false">
    <div class="modal vc-modal vc-spec-modal">
      <div class="vc-modal-head"><h3>Разрешение · {{ permitView.title }}</h3><button class="btn btn-outline btn-sm" @click="permitView.open=false">✕</button></div>
      <div v-if="permitView.loading" class="vc-empty">Загрузка…</div>
      <template v-else-if="permitView.data">
        <p class="vc-muted">Сверка по номерам листов (по штампу), без смыслового анализа. Заявленными считаются строки разрешений B с «Изм.» новее последнего «Изм.» версии A ({{ permitView.data.izm_a }}): {{ permitView.data.izm_new.join(', ') || 'нет' }}.</p>
        <div class="vc-spec-scroll">
          <h4>Заявлено проектировщиком</h4>
          <table class="vc-files vc-spec-table">
            <tr><th>Разрешение</th><th>Изм.</th><th>Лист</th><th>Содержание изменения</th><th>Код</th></tr>
            <tr v-for="(r, i) in permitView.data.rows" :key="i"><td>{{ r.permit }}</td><td>{{ r.izm }}</td><td>{{ r.sheet }}</td><td>{{ r.text }}</td><td>{{ r.code }}</td></tr>
          </table>
          <div v-if="permitView.data.all_sheets_declared" class="vc-warning">Заявлены изменения «всех листов» — сверка по листам не выполняется.</div>
          <h4>Заявлено и найдено: {{ permitView.data.declared_and_found.length }}</h4>
          <div class="vc-permit-list"><span v-for="e in permitView.data.declared_and_found" :key="e.ref" class="vc-badge b-diff" @click="openRef(e.ref)">{{ e.ref }}</span></div>
          <h4>Заявлено, но отличий не найдено: {{ permitView.data.declared_no_diff.length }}</h4>
          <div v-for="e in permitView.data.declared_no_diff" :key="e.ref" class="vcv-tchange" @click="openRef(e.ref)"><b>{{ e.ref }}</b> — {{ statusInfo(e.statuses[0]).label }}; заявлено: {{ e.declared.join('; ') }}</div>
          <h4>Отличия найдены, но в разрешении не заявлены: {{ permitView.data.diff_not_declared.length }}</h4>
          <div v-for="e in permitView.data.diff_not_declared" :key="e.ref + e.title" class="vcv-tchange" @click="openRef(e.ref)"><b>{{ e.ref }}</b> — {{ e.title }} ({{ statusInfo(e.status).label }})</div>
          <template v-if="permitView.data.needs_review.length">
            <h4>Нужна проверка: {{ permitView.data.needs_review.length }}</h4>
            <div v-for="e in permitView.data.needs_review" :key="e.ref" class="vcv-tchange"><b>{{ e.ref }}</b> — {{ e.reason }}</div>
          </template>
          <div v-if="permitView.data.unparsed_refs.length" class="vc-muted">Не разобраны ссылки на листы: {{ permitView.data.unparsed_refs.map(u => u.sheet).join('; ') }}</div>
        </div>
      </template>
    </div>
  </div>

  <div v-if="compView.open" class="modal-overlay" @click.self="compView.open=false">
    <div class="modal vc-modal vc-spec-modal">
      <div class="vc-modal-head"><h3>Контроль состава · {{ compView.title }}</h3><button class="btn btn-outline btn-sm" @click="compView.open=false">✕</button></div>
      <p class="vc-muted">Наблюдения, а не выводы: повторы и пропуски номеров листов, нарушенный порядок, дубли, пустые формы СО, ведомость рабочих чертежей ↔ штампы.</p>
      <div v-if="!compView.data || compView.data.state !== 'ready'" class="vc-empty">{{ compView.data && compView.data.state === 'failed' ? ('Ошибка: ' + compView.data.error) : 'Проверка версий… (разбор СО может занять до минуты)' }}</div>
      <div v-else class="vc-spec-scroll">
        <div v-for="v in compView.data.versions" :key="v.version_id" class="vc-comp-version">
          <h4>{{ v.label }} <span class="vc-muted">· разрешений: {{ v.permits.length }}<template v-if="v.permits.length"> (Изм. {{ v.permits.map(x => x.izm.join(',')).join('; ') }})</template></span></h4>
          <div v-if="!v.observations.length" class="vc-muted">Замечаний к составу нет.</div>
          <div v-for="(o, i) in v.observations" :key="i" class="vcv-text">• {{ o.text }}</div>
        </div>
      </div>
    </div>
  </div>

  <div v-if="specView.open" class="modal-overlay" @click.self="specView.open=false">
    <div class="modal vc-modal vc-spec-modal">
      <div class="vc-modal-head"><h3>Спецификация по позициям · {{ specView.title }}</h3><button class="btn btn-outline btn-sm" @click="specView.open=false">✕</button></div>
      <div v-if="specView.loading" class="vc-empty">Загрузка…</div>
      <template v-else-if="specView.data">
        <p class="vc-muted">Позиции сопоставлены через страницы (ключ: родитель, наименование, марка). Переехали между листами без изменений: {{ specView.data.moved_between_pages }} — это не изменения.
          <label class="vc-nowrap"><input type="checkbox" v-model="specView.showRenumbered"> показать перенумерацию ({{ specCount('renumbered') }})</label></p>
        <div class="vc-spec-filters">
          <span class="vc-badge">изменено: {{ specCount('changed') }}</span>
          <span class="vc-badge b-added">добавлено: {{ specCount('added') }}</span>
          <span class="vc-badge b-removed">удалено: {{ specCount('removed') }}</span>
          <span v-if="specCount('ambiguous')" class="vc-badge b-review">неоднозначно: {{ specCount('ambiguous') }}</span>
          <span v-if="specView.data.totals.length" class="vc-muted">итоги по марке расходятся: {{ specView.data.totals.length }}</span>
        </div>
        <div class="vc-spec-scroll">
          <table class="vc-files vc-spec-table">
            <tr><th>Вид</th><th>Позиция</th><th>Раздел</th><th>Было → стало</th><th>Стр. A</th><th>Стр. B</th></tr>
            <tr v-for="c in specRows()" :key="c.id" class="vc-spec-row" @click="openSpecChange(c)">
              <td class="vc-nowrap">{{ specKind(c.kind) }}</td>
              <td>{{ c.label }}</td>
              <td class="vc-muted">{{ c.section }}</td>
              <td><div v-for="(v, f) in c.fields" :key="f" class="vcv-text"><span class="vc-muted">{{ specField(f) }}:</span> <span class="was">{{ v[0] || '—' }}</span> → <span class="now">{{ v[1] || '—' }}</span></div></td>
              <td>{{ c.a_pages.join(', ') }}</td><td>{{ c.b_pages.join(', ') }}</td>
            </tr>
          </table>
          <details v-if="specView.data.totals.length" class="vcv-details">
            <summary>Итоги по марке (сумма количества по всем листам)</summary>
            <table class="vc-files"><tr><th>Марка, размер, ед.</th><th>A</th><th>B</th><th>Δ</th></tr>
              <tr v-for="(t, i) in specView.data.totals" :key="i"><td>{{ t.label }}</td><td>{{ t.a }}</td><td>{{ t.b }}</td><td>{{ t.delta > 0 ? '+' : '' }}{{ t.delta }}</td></tr></table>
          </details>
        </div>
      </template>
    </div>
  </div>

  <vc-viewer v-if="viewer.open" :object-id="objectId" :run="viewer.run" :items="viewer.items" :start-spid="viewer.spid" @close="viewer.open=false"></vc-viewer>
</div>`;

    root.VersionComparison = {
        API, STATUS, CHANNEL_LABELS, statusInfo, api,
        register(app) {
            app.component('version-comparison-page', {
                template: PAGE_TEMPLATE,
                setup() {
                    const {ref, reactive, onMounted, onBeforeUnmount} = root.Vue;
                    const objects = ref([]);
                    const objectId = ref('');
                    const projects = ref([]);
                    const catalogLoading = ref(false);
                    const error = ref('');
                    const expanded = reactive({});
                    const matrices = reactive({});
                    const pairs = reactive({});
                    const manualPairs = reactive({});
                    const pending = reactive({});
                    const activeRun = reactive({});
                    const runs = reactive({});
                    const seriesMap = reactive({});
                    const viewer = reactive({open: false, run: null, items: [], spid: ''});
                    const upload = reactive({open: false, step: 1, files: [], totalBytes: 0, sent: 0, busy: false,
                        message: '', uploadId: '', proposal: null, groups: []});
                    const order = reactive({open: false, busy: false, project: null, title: '', items: []});
                    const timers = {};
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

                    async function loadCatalog() {
                        try { localStorage.setItem('version-comparison:object', objectId.value || ''); } catch (e) { /* без хранилища */ }
                        projects.value = [];
                        if (!objectId.value) return;
                        catalogLoading.value = true;
                        try {
                            const data = await api(`/objects/${enc(objectId.value)}/catalog`);
                            projects.value = data.projects || [];
                            for (const p of projects.value) {
                                if (!pairs[p.project_id]) {
                                    const v = p.versions;
                                    pairs[p.project_id] = {a: v.length > 1 ? v[v.length - 2] : (v[0] || ''), b: v[v.length - 1] || ''};
                                }
                                if (!manualPairs[p.project_id]) manualPairs[p.project_id] = [];
                                if (projects.value.length === 1 && expanded[p.project_id] === undefined) {
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
                        if (!matrices[p.project_id]) matrices[p.project_id] = {state: 'loading'};
                        try {
                            const m = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/matrix`);
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

                    async function pickRun(p, runId) {
                        try {
                            const run = await api(`/objects/${enc(objectId.value)}/runs/${runId}`);
                            activeRun[p.project_id] = run;
                            if (['queued', 'running', 'cancelling'].includes(run.state)) schedule('r' + p.project_id, () => pickRun(p, runId), 2000);
                            else if (runs[p.project_id] && !runs[p.project_id].some(r => r.run_id === runId)) loadRuns(p);
                        } catch (e) { fail(e); }
                    }

                    const seriesOf = p => seriesMap[p.project_id] || null;
                    const seriesBusy = p => !!(seriesOf(p) && ['queued', 'running', 'cancelling'].includes(seriesOf(p).state));
                    async function loadSeries(p, seriesId) {
                        try {
                            let s;
                            if (seriesId) s = await api(`/objects/${enc(objectId.value)}/series/${seriesId}`);
                            else s = ((await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/series`)).series || [])[0];
                            if (!s) return;
                            seriesMap[p.project_id] = s;
                            if (['queued', 'running', 'cancelling'].includes(s.state)) {
                                schedule('s' + p.project_id, () => loadSeries(p, s.series_id), 2000);
                                // Прогон текущего перехода — в строке прогона и в матрице.
                                const cur = s.pairs.find(x => x.run_state && ['queued', 'running', 'cancelling'].includes(x.run_state));
                                if (cur && (!activeRun[p.project_id] || activeRun[p.project_id].run_id !== cur.run_id)) openTransition(p, cur);
                            }
                        } catch (e) { fail(e); }
                    }
                    async function startSeries(p) {
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
                        try { seriesMap[p.project_id] = await api(`/objects/${enc(objectId.value)}/series/${s.series_id}/resume`, {method: 'POST'});
                            schedule('s' + p.project_id, () => loadSeries(p, s.series_id), 1000); } catch (e) { fail(e); }
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
                    const pairStateLabel = pr => ({queued: 'в очереди', running: 'считается…', done: 'готово', cancelled: 'отменено',
                        interrupted: 'прерван', failed: 'ошибка' + (pr.error ? ': ' + pr.error : '')}[pr.state] || pr.state);

                    function toggleProject(p) {
                        expanded[p.project_id] = !expanded[p.project_id];
                        if (expanded[p.project_id]) loadMatrix(p);
                    }

                    const manual = p => manualPairs[p.project_id] || (manualPairs[p.project_id] = []);
                    const pairReady = p => pairs[p.project_id] && pairs[p.project_id].a && pairs[p.project_id].b && pairs[p.project_id].a !== pairs[p.project_id].b;
                    const runBusy = p => activeRun[p.project_id] && ['queued', 'running', 'cancelling'].includes(activeRun[p.project_id].state);

                    function swap(p) {
                        const s = pairs[p.project_id];
                        [s.a, s.b] = [s.b, s.a];
                        manualPairs[p.project_id] = manual(p).map(x => ({a_page: x.b_page, b_page: x.a_page}));
                    }
                    function setSide(p, side, vid) {
                        const s = pairs[p.project_id];
                        if (side === 'a' && s.b === vid) s.b = s.a;
                        if (side === 'b' && s.a === vid) s.a = s.b;
                        s[side] = vid;
                        manualPairs[p.project_id] = [];
                        pending[p.project_id] = null;
                    }

                    async function startRun(p, scope) {
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
                        try { activeRun[p.project_id] = await api(`/objects/${enc(objectId.value)}/runs/${run.run_id}/resume`, {method: 'POST'});
                            schedule('r' + p.project_id, () => pickRun(p, run.run_id), 1000); } catch (e) { fail(e); }
                    }

                    function rowItem(p, row) {
                        const run = activeRun[p.project_id];
                        if (!run || run.scope !== 'all') return null;
                        const s = pairs[p.project_id];
                        if (run.a.version_id !== s.a || run.b.version_id !== s.b) return null;
                        return run.items.find(it => it.row_id === row.row_id) || null;
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
                        return {queued: 'в очереди', running: 'идёт сравнение', cancelling: 'отменяется…', done: 'готово',
                            cancelled: 'отменено', interrupted: 'прерван (можно досчитать)', failed: 'ошибка'}[run.state] || run.state;
                    }
                    function runTitle(r) {
                        const when = r.created_at ? new Date(r.created_at).toLocaleString('ru-RU') : '';
                        return `${r.a.label || r.a.file_name} → ${r.b.label || r.b.file_name} · ${r.scope === 'manual' ? 'выбранные' : 'все'} · ${when}`;
                    }

                    function cellClass(p, row, v) {
                        const s = pairs[p.project_id] || {};
                        return {'col-a': v.version_id === s.a, 'col-b': v.version_id === s.b,
                            'is-pending': pending[p.project_id] && pending[p.project_id].row === row.row_id && v.version_id === s.a};
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
                        return parts.join(' · ');
                    }
                    function thumbUrl(v, page) {
                        return `${API}/objects/${enc(objectId.value)}/sources/${v.sha256}/pages/${page}/thumb?w=160`;
                    }
                    function versionTitle(v) {
                        return `${v.index + 1}. ${v.user_label || v.file_name}`;
                    }
                    function clickCell(p, row, v, c) {
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

                    async function edit(p, body) {
                        try {
                            const m = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/links`, {method: 'POST', body});
                            matrices[p.project_id] = m;
                            if (m.state !== 'ready') schedule('m' + p.project_id, () => loadMatrix(p), 800);
                        } catch (e) { fail(e); }
                    }
                    function dragStart(vid, page, ev) {
                        dragData = {vid, page};
                        try { ev.dataTransfer.setData('text/plain', `${vid}:${page}`); } catch (e) { /* ignore */ }
                    }
                    function dropOnCell(p, row, v, ev) {
                        if (!dragData || dragData.vid !== v.version_id) { dragData = null; return; }
                        const cell = row.cells[v.version_id];
                        if (cell.state === 'matched' && cell.page === dragData.page) { dragData = null; return; }
                        edit(p, {op: 'move', page: [dragData.vid, dragData.page], to_row_anchor: row.anchor});
                        dragData = null;
                    }
                    const detach = (p, v, c) => edit(p, {op: 'detach', page: [v.version_id, c.page]});
                    const attach = (p, row, v, page) => edit(p, {op: 'move', page: [v.version_id, page], to_row_anchor: row.anchor});
                    const markAbsent = (p, row, v) => edit(p, {op: 'absent', row_anchor: row.anchor, version: v.version_id});
                    async function undoEdit(p) {
                        try { matrices[p.project_id] = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/links/undo`, {method: 'POST'});
                            if (matrices[p.project_id].state !== 'ready') schedule('m' + p.project_id, () => loadMatrix(p), 800); } catch (e) { fail(e); }
                    }
                    function failedVersions(m) {
                        return (m.versions || []).filter(v => v.profile.state === 'failed').map(v => `${v.file_name}: ${v.profile.error}`).join('; ');
                    }

                    const permitView = reactive({open: false, loading: false, data: null, title: '', project: null});
                    async function openPermit(p, run) {
                        Object.assign(permitView, {open: true, loading: true, data: null, project: p,
                            title: `${run.a.label || run.a.file_name} → ${run.b.label || run.b.file_name}`});
                        try { permitView.data = await api(`/objects/${enc(objectId.value)}/runs/${run.run_id}/permit`); }
                        catch (e) { fail(e); permitView.open = false; }
                        finally { permitView.loading = false; }
                    }
                    function openRef(ref) {
                        const p = permitView.project;
                        const run = p && activeRun[p.project_id];
                        if (!run) return;
                        const item = run.items.find(it => it.kind === 'compare' && it.status === 'done' && (it.title || '').startsWith(ref + ' '));
                        if (!item) { error.value = `Лист ${ref} не входит в сопоставленные пары прогона.`; return; }
                        permitView.open = false;
                        openViewer(p, item);
                    }
                    const compView = reactive({open: false, data: null, title: ''});
                    async function openComposition(p) {
                        Object.assign(compView, {open: true, data: null, title: p.title});
                        const poll = async () => {
                            try {
                                compView.data = await api(`/objects/${enc(objectId.value)}/projects/${p.project_id}/composition`);
                                if (compView.open && ['building', 'waiting'].includes(compView.data.state)) schedule('c' + p.project_id, poll, 2000);
                            } catch (e) { fail(e); }
                        };
                        poll();
                    }
                    const specView = reactive({open: false, loading: false, data: null, title: '', project: null, showRenumbered: false});
                    async function openSpec(p, run) {
                        Object.assign(specView, {open: true, loading: true, data: null, project: p,
                            title: `${run.a.label || run.a.file_name} → ${run.b.label || run.b.file_name}`});
                        try { specView.data = await api(`/objects/${enc(objectId.value)}/runs/${run.run_id}/spec`); }
                        catch (e) { fail(e); specView.open = false; }
                        finally { specView.loading = false; }
                    }
                    const specCount = k => (specView.data ? specView.data.changes.filter(c => c.kind === k).length : 0);
                    const specRows = () => (specView.data ? specView.data.changes.filter(c => specView.showRenumbered || c.kind !== 'renumbered') : []);
                    const specKind = k => ({changed: 'изменено', added: 'добавлено', removed: 'удалено', ambiguous: 'неоднозначно', renumbered: 'перенумерация'}[k] || k);
                    const specField = f => ({qty: 'кол.', unit: 'ед.', mark: 'марка', code: 'код', supplier: 'поставщик', note: 'примеч.', pos: 'поз.', name: 'наименование'}[f] || f);
                    function openSpecChange(c) {
                        const p = specView.project;
                        const run = p && activeRun[p.project_id];
                        if (!run) return;
                        const item = run.items.find(it => it.kind === 'compare' && it.status === 'done' &&
                            ((c.b_pages || []).includes(it.b_page) || (c.a_pages || []).includes(it.a_page)));
                        if (!item) { error.value = 'Лист с этой позицией не входит в сопоставленные пары.'; return; }
                        specView.open = false;
                        openViewer(p, item);
                    }
                    function openViewer(p, item) {
                        const run = activeRun[p.project_id];
                        viewer.run = run;
                        viewer.items = run.items.filter(it => it.kind === 'compare' && it.status === 'done');
                        viewer.spid = item.spid;
                        viewer.open = true;
                    }

                    // ------------------------------------------------ загрузка
                    function openUpload() {
                        Object.assign(upload, {open: true, step: 1, files: [], totalBytes: 0, sent: 0, busy: false, message: '', uploadId: '', proposal: null, groups: []});
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
                            upload.groups = prop.groups.map(g => ({title: g.title, code: g.code, project_id: g.existing_project_id || null,
                                warnings: g.warnings, files: g.files.map(f => Object.assign({}, f, {label: f.suggested_label}))}));
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

                    return {objects, objectId, projects, catalogLoading, error, expanded, matrices, pairs, pending, activeRun, runs,
                        viewer, upload, order, statusInfo, loadCatalog, toggleProject, manual, pairReady, runBusy, swap, setSide,
                        startRun, cancelRun, resumeRun, pickRun, rowItem, itemLabel, runCounts, progressPct, runStateLabel, runTitle,
                        cellClass, cellTitle, thumbUrl, versionTitle, clickCell, dragStart, dropOnCell, detach, attach, markAbsent,
                        undoEdit, failedVersions, openViewer, openUpload, closeUpload, pickFiles, mb, uploadPct, sendFiles, hint,
                        moveFile, moveToGroup, hasBadFiles, confirmUpload, openOrder, moveOrder, saveOrder,
                        seriesOf, seriesBusy, startSeries, cancelSeries, resumeSeries, openTransition, isOpenTransition,
                        pairStateLabel, specView, openSpec, specCount, specRows, specKind, specField, openSpecChange,
                        permitView, openPermit, openRef, compView, openComposition, partOf, partsFrom, partTitle};
                },
            });
            if (root.VersionComparisonViewer) root.VersionComparisonViewer.register(app);
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
