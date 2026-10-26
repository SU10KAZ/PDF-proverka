/* Persistent reserve calendar. All writes are confirmed by the server. */
(function (root) {
    'use strict';
    const { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } = Vue;
    const API = '/api/schedule/reserve';
    const key = value => String(value).normalize('NFC').trim().replace(/\s+/gu, ' ').toLowerCase();
    const lines = value => String(value || '').split(/[\r\n\u2028\u2029]+/u).map(s => s.trim()).filter(Boolean);
    const dayAdd = (iso, n) => {
        const d = new Date(iso + 'T12:00:00Z');
        d.setUTCDate(d.getUTCDate() + n);
        return d.toISOString().slice(0, 10);
    };
    const label = iso => iso ? new Date(iso + 'T12:00:00Z').toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', timeZone: 'UTC' }) : 'Без даты';
    const stateLabel = state => ({preparation: 'Ожидает подготовки', ready: 'Готов к проверке', in_progress: 'Проверка начата'}[state] || state);
    const normalizedVersion = value => /^v\d+$/i.test(value || '') ? 'v'+Number(String(value).slice(1)) : value;
    // randomUUID is unavailable on plain HTTP origins. The staging server is
    // intentionally reachable over HTTP, so create a standards-shaped v4 UUID
    // without depending on a secure browser context.
    const operationId = () => {
        try {
            if (root.crypto && typeof root.crypto.randomUUID === 'function') return root.crypto.randomUUID();
        } catch (_) { /* use the fallback below */ }
        const bytes = new Uint8Array(16);
        try {
            if (!root.crypto || typeof root.crypto.getRandomValues !== 'function') throw new Error('getRandomValues unavailable');
            root.crypto.getRandomValues(bytes);
        } catch (_) {
            for (let i = 0; i < bytes.length; i++) bytes[i] = Math.floor(Math.random() * 256);
        }
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        const hex = [...bytes].map(byte => byte.toString(16).padStart(2, '0')).join('');
        return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
    };
    const orderedSelection = (data, selected) => data.engineers.flatMap(e => data.tasks.filter(t => t.engineer_id === e.id && selected.includes(t.id)).sort((a, b) => a.position - b.position));
    const duplicateFields = (data, drafts) => {
        const seen = new Map(), errors = {};
        for (const e of data.engineers.filter(e => e.active !== false)) {
            for (const name of lines(drafts[e.id])) {
                const normalized = key(name);
                if (seen.has(normalized)) {
                    (errors[e.id] ||= new Set()).add(name);
                    (errors[seen.get(normalized)] ||= new Set()).add(name);
                }
                seen.set(normalized, e.id);
                if (data.tasks.some(t => !t.project_id && t.engineer_id === e.id && key(t.title) === normalized)) {
                    (errors[e.id] ||= new Set()).add(name);
                }
                if (name.length > 4000) (errors[e.id] ||= new Set()).add('Название длиннее 4000 символов');
            }
        }
        return Object.fromEntries(Object.entries(errors).map(([id, names]) => [id, [...names].join('; ')]));
    };
    async function readResponse(response) {
        const data = await response.json();
        if (!response.ok) {
            const error = new Error(data.detail?.message || (typeof data.detail === 'string' ? data.detail : 'Не удалось сохранить изменения.'));
            error.status = response.status;
            throw error;
        }
        return data;
    }
    const empty = () => ({engineers: [], tasks: [], available_projects: [], totals: {}, calendar: {}, revision: 0});

    const Calendar = {
        props: {active: Boolean}, emits: ['open-project'],
        setup(props, {emit}) {
            const data = ref(empty()), loading = ref(false), busy = ref(false), error = ref(''), status = ref('');
            const selected = ref([]), dragging = ref([]), insertion = ref(null), pending = ref(null);
            const drafts = ref({}), prepDialog = ref(null), projectDialog = ref(null), catalogDialog = ref(null);
            const prepOpen = ref(false), card = ref(null), catalogOpen = ref(false), linkTask = ref(null);
            const targetEngineer = ref(''), chosenProject = ref(''), search = ref(''), objectMenuOpen = ref(false), anchor = ref('');
            const lastChosenProject = ref('');
            const tooltip = ref(null), tooltipStyle = ref({});
            let suppressClickUntil = 0, generation = 0, timer;
            const canEdit = computed(() => !!data.value.can_edit);
            const locked = computed(() => busy.value || !!pending.value);
            const activeEngineers = computed(() => data.value.engineers.filter(e => e.active !== false));
            const selectedTasks = computed(() => orderedSelection(data.value, selected.value));
            const draftErrors = computed(() => duplicateFields(data.value, drafts.value));
            const draftItems = computed(() => activeEngineers.value.flatMap(e => lines(drafts.value[e.id]).map(title => ({engineer_id: e.id, title}))));
            const draftEngineerCount = computed(() => new Set(draftItems.value.map(i => i.engineer_id)).size);
            const grouped = computed(() => {
                const groups = new Map();
                for (const e of data.value.engineers) {
                    if (!groups.has(e.direction)) groups.set(e.direction, []);
                    groups.get(e.direction).push(e);
                }
                return [...groups].map(([name, engineers]) => {
                    const tasks = data.value.tasks.filter(t => engineers.some(e => e.id === t.engineer_id));
                    return { name, engineers, remaining: tasks.length,
                        ready: tasks.filter(t => t.state === 'ready').length,
                        preparation: tasks.filter(t => t.state === 'preparation').length,
                        running: tasks.filter(t => t.state === 'in_progress').length,
                        through: tasks.some(t => !t.forecast_date) ? null : tasks.map(t => t.forecast_date).sort().pop() };
                });
            });
            const days = computed(() => {
                if (!anchor.value) return [];
                const holidays = new Set(data.value.calendar.holidays || []), working = new Set(data.value.calendar.working_dates || []);
                return Array.from({length: 28}, (_, i) => {
                    const iso = dayAdd(anchor.value, i), d = new Date(iso+'T12:00:00Z');
                    return {iso, dom: d.getUTCDate(), label: label(iso), dow: d.toLocaleDateString('ru-RU', {weekday:'short', timeZone:'UTC'}),
                        off: holidays.has(iso) || ([0,6].includes(d.getUTCDay()) && !working.has(iso))};
                });
            });
            const columns = computed(() => [...days.value, {iso: null, label: 'Без даты', off: false}]);
            const cells = computed(() => {
                const map = new Map();
                for (const task of data.value.tasks) {
                    const cellKey = task.engineer_id + '|' + (task.forecast_date || 'none');
                    if (!map.has(cellKey)) map.set(cellKey, []);
                    map.get(cellKey).push(task);
                }
                return map;
            });
            const cell = (eid, iso) => cells.value.get(eid+'|'+(iso || 'none')) || [];
            const available = computed(() => data.value.available_projects
                .filter(p => key([p.object_name,p.title,p.code,p.version_id].join(' ')).includes(key(search.value)))
                .sort((a, b) => Number(!!b.ready) - Number(!!a.ready)
                    || String(a.object_name || '').localeCompare(String(b.object_name || ''), 'ru', {numeric:true})
                    || String(a.code || a.title || '').localeCompare(String(b.code || b.title || ''), 'ru', {numeric:true})
                    || String(a.version_id || '').localeCompare(String(b.version_id || ''), 'ru', {numeric:true})));
            const objectOptions = computed(() => {
                const objects = new Map();
                for (const project of data.value.available_projects) {
                    const name = String(project.object_name || '').trim();
                    if (name && !objects.has(key(name))) objects.set(key(name), name);
                }
                return [...objects.values()].sort((a, b) => a.localeCompare(b, 'ru', {numeric:true}));
            });
            const suggestedObjects = computed(() => {
                const query = key(search.value);
                return query ? objectOptions.value.filter(name => key(name).includes(query)) : objectOptions.value;
            });
            const chosenProjectIndexes = computed(() => {
                const values = Array.isArray(chosenProject.value) ? chosenProject.value : [chosenProject.value];
                return values.filter(value => value !== '');
            });
            const color = task => {
                const palette = ['#008e83','#6389cc','#af71b6','#d2a34b','#db7d78','#6e9b6a'];
                let n = 0;
                for (const c of task.object_id || task.engineer_id) n = ((n << 5)-n+c.charCodeAt(0)) | 0;
                return palette[Math.abs(n)%palette.length];
            };
            const tipText = task => [task.object_name, task.title, task.code && 'Шифр: '+task.code,
                task.version_id && 'Версия: '+task.version_id, data.value.engineers.find(e => e.id === task.engineer_id)?.name,
                stateLabel(task.state), (task.conditional ? 'Условная дата: ' : 'Прогноз: ') + (task.forecast_date ? label(task.forecast_date) : 'не рассчитан'),
                task.unscheduled_reason === 'missing_plan' ? 'Задайте план инженера в разделе «Выполнено».' : '',
                task.total ? 'Решений: '+task.progress+' из '+task.total : ''].filter(Boolean).join('\n');

            function accept(value) {
                data.value = value;
                if (!anchor.value) anchor.value = value.today;
                selected.value = selected.value.filter(id => value.tasks.some(t => t.id === id));
                if (card.value) card.value = value.tasks.find(t => t.id === card.value.id) || null;
                if (!card.value && projectDialog.value?.open) projectDialog.value.close();
            }
            async function load(silent = false) {
                if (busy.value || loading.value) return;
                const current = ++generation;
                if (!silent) loading.value = true;
                try {
                    const value = await readResponse(await fetch(API));
                    if (current === generation) { accept(value); if (!pending.value) error.value = ''; }
                } catch (e) { if (current === generation) error.value = e.message || 'Не удалось загрузить календарь.'; }
                finally { if (current === generation) loading.value = false; }
            }
            async function send(body = pending.value) {
                if (busy.value || !body) return false;
                generation++; loading.value = false;
                busy.value = true; error.value = ''; tooltip.value = null;
                try {
                    const response = await fetch(API+'/operations', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
                    const value = await readResponse(response);
                    pending.value = null;
                    accept(value);
                    status.value = body.kind === 'undo' ? 'Действие отменено.' : body.kind === 'start' ? 'Проверка начата. Очередь пересчитана.' : 'Изменения сохранены. Очереди и прогноз пересчитаны.';
                    if (body.kind === 'add_preparation') { drafts.value = {}; closePreparation(true); }
                    if (body.kind === 'assign') { chosenProject.value = []; lastChosenProject.value = ''; }
                    if (body.kind === 'link') closeCatalog();
                    if (body.kind === 'link') { projectDialog.value?.close(); card.value = null; }
                    return true;
                } catch (e) {
                    // With an HTTP response the result is conclusive. A lost
                    // response retains the exact key/body for a safe retry.
                    if (e.status) pending.value = null;
                    error.value = e.message || 'Связь прервана. Повторите сохранение тем же запросом.';
                    if (e.status === 409) {
                        const message = error.value;
                        busy.value = false;
                        await load();
                        error.value = message;
                    }
                    return false;
                } finally { busy.value = false; }
            }
            function operate(kind, value = {}) {
                if (locked.value || !canEdit.value) return;
                pending.value = {operation_id: operationId(), revision: data.value.revision, kind, ...value};
                return send(pending.value);
            }
            function clickTask(task, event) {
                if (Date.now() < suppressClickUntil || dragging.value.length) return;
                if (event.ctrlKey || event.metaKey) {
                    if (!canEdit.value || task.state === 'in_progress') return;
                    selected.value = selected.value.includes(task.id) ? selected.value.filter(id => id !== task.id) : [...selected.value, task.id];
                    tooltip.value = null;
                } else if (task.project_id) emit('open-project', task);
                else openCard(task);
            }
            async function openCard(task) {
                card.value = task;
                tooltip.value = null;
                await nextTick();
                projectDialog.value.showModal();
            }
            function showTip(task, event) {
                tooltip.value = task;
                const r = event.currentTarget.getBoundingClientRect();
                tooltipStyle.value = {left: Math.max(8, Math.min(r.left, window.innerWidth-490))+'px',
                    top: Math.min(r.bottom+10, window.innerHeight*0.48)+'px'};
            }
            function dragStart(task, event) {
                if (locked.value || !canEdit.value || task.state === 'in_progress') { event.preventDefault(); return; }
                dragging.value = selected.value.includes(task.id) ? selectedTasks.value.map(t => t.id) : [task.id];
                tooltip.value = null;
                event.dataTransfer.effectAllowed = 'move';
                event.dataTransfer.setData('text/plain', JSON.stringify(dragging.value));
            }
            function dragEnd() {
                dragging.value = []; insertion.value = null;
                suppressClickUntil = Date.now()+400;
            }
            function overTask(task, event) {
                if (!dragging.value.length) return;
                if (dragging.value.includes(task.id)) { event.stopPropagation(); insertion.value = null; return; }
                event.preventDefault(); event.stopPropagation();
                const r = event.currentTarget.getBoundingClientRect();
                insertion.value = {engineer_id:task.engineer_id, anchor_id:task.id, side:event.clientX < r.left+r.width/2 ? 'before' : 'after'};
            }
            function overCell(engineer, iso, event) {
                if (!dragging.value.length || engineer.active === false) return;
                event.preventDefault();
                const next = data.value.tasks.find(t => t.engineer_id === engineer.id && !dragging.value.includes(t.id) && iso && t.forecast_date && t.forecast_date >= iso);
                insertion.value = {engineer_id:engineer.id, anchor_id:next?.id || null, side:'before'};
            }
            async function drop(event) {
                if (!dragging.value.length || !insertion.value) return;
                event.preventDefault(); event.stopPropagation();
                const value = {...insertion.value, task_ids:[...dragging.value]};
                dragEnd();
                await operate('move', value);
            }
            function shift(direction) {
                const block = selectedTasks.value;
                if (!block.length) return;
                const edge = direction < 0 ? block[0] : block[block.length-1];
                const queue = data.value.tasks.filter(t => t.engineer_id === edge.engineer_id && !selected.value.includes(t.id));
                const neighbor = direction < 0 ? queue.filter(t => t.position < edge.position).pop() : queue.find(t => t.position > edge.position);
                if (!neighbor || (direction < 0 && neighbor.state === 'in_progress')) return;
                return operate('move', {task_ids: selected.value, engineer_id:edge.engineer_id, anchor_id:neighbor.id, side:direction < 0 ? 'before' : 'after'});
            }
            async function openPreparation() {
                prepOpen.value = true; tooltip.value = null;
                await nextTick(); prepDialog.value.showModal();
                prepDialog.value.querySelector('textarea')?.focus();
            }
            function closePreparation(force = false) {
                if (busy.value && !force) return;
                prepDialog.value?.close(); prepOpen.value = false;
            }
            async function openCatalog(task = null) {
                linkTask.value = task;
                targetEngineer.value = task?.engineer_id || activeEngineers.value[0]?.id || '';
                catalogOpen.value = true; chosenProject.value = task ? '' : []; lastChosenProject.value = ''; search.value = ''; objectMenuOpen.value = false; tooltip.value = null;
                await nextTick(); catalogDialog.value.showModal();
            }
            function closeCatalog() { catalogDialog.value?.close(); catalogOpen.value = false; objectMenuOpen.value = false; }
            function selectObject(name) {
                search.value = name;
                objectMenuOpen.value = false;
                chosenProject.value = linkTask.value ? '' : [];
                lastChosenProject.value = '';
            }
            function chooseProject(value, event = {}) {
                if (linkTask.value) {
                    chosenProject.value = value;
                    lastChosenProject.value = value;
                    return;
                }
                const visible = available.value.map(project => String(data.value.available_projects.indexOf(project)));
                const current = new Set(chosenProjectIndexes.value);
                if (event.shiftKey && lastChosenProject.value && visible.includes(lastChosenProject.value)) {
                    const from = visible.indexOf(lastChosenProject.value), to = visible.indexOf(value);
                    for (const item of visible.slice(Math.min(from, to), Math.max(from, to) + 1)) current.add(item);
                    chosenProject.value = [...current];
                } else if (event.ctrlKey || event.metaKey) {
                    current.has(value) ? current.delete(value) : current.add(value);
                    chosenProject.value = [...current];
                } else {
                    chosenProject.value = [value];
                }
                lastChosenProject.value = value;
            }
            function saveCatalog() {
                const items = chosenProjectIndexes.value.map(value => data.value.available_projects[Number(value)]).filter(Boolean);
                if (!items.length) return;
                return linkTask.value
                    ? operate('link', {task_id: linkTask.value.id, project:items[0]})
                    : operate('assign', {items:items.map(item => ({...item, engineer_id:targetEngineer.value}))});
            }
            function quickAddProject(value) {
                if (locked.value || !targetEngineer.value) return;
                chosenProject.value = linkTask.value ? value : [value];
                lastChosenProject.value = value;
                return saveCatalog();
            }
            function escape(event) {
                if (event.key !== 'Escape') return;
                tooltip.value = null;
                if (!prepDialog.value?.open && !projectDialog.value?.open && !catalogDialog.value?.open) selected.value = [];
            }
            watch(() => props.active, active => { if (active) load(); }, {immediate:true});
            onMounted(() => {
                window.addEventListener('keydown', escape);
                timer = setInterval(() => { if (props.active && !document.hidden && !prepOpen.value && !catalogOpen.value && !dragging.value.length && !pending.value) load(true); }, 30000);
            });
            onBeforeUnmount(() => { generation++; clearInterval(timer); window.removeEventListener('keydown', escape); });
            return {data, loading, busy, error, status, selected, dragging, insertion, pending, drafts, prepDialog, projectDialog, catalogDialog,
                prepOpen, card, catalogOpen, linkTask, targetEngineer, chosenProject, search, objectMenuOpen, anchor, tooltip, tooltipStyle,
                canEdit, locked, activeEngineers, selectedTasks, draftErrors, draftItems, draftEngineerCount, grouped, days, columns, available,
                objectOptions, suggestedObjects, chosenProjectIndexes,
                key, lines, dayAdd, label, stateLabel, cell, color, tipText, load, send, operate, clickTask, openCard, showTip,
                dragStart, dragEnd, overTask, overCell, drop, shift, openPreparation, closePreparation, openCatalog, closeCatalog, selectObject, chooseProject, saveCatalog, quickAddProject};
        },
        template: `
        <section class="reserve-calendar" aria-label="Календарь запаса" :aria-busy="loading || busy">
          <header class="rc-heading"><div><h2>Календарь запаса</h2><p>Единая очередь проектов · {{ label(data.today) }}</p></div><button type="button" :disabled="loading || locked" @click="load()">Обновить</button></header>
          <div v-if="error" class="rc-error" role="alert">{{ error }} <button v-if="pending" :disabled="busy" @click="send()">Повторить сохранение</button><button v-else :disabled="loading || busy" @click="load()">Обновить данные</button></div>
          <div v-if="loading && !data.engineers.length" class="rc-empty">Загрузка инженеров и очередей…</div>
          <div class="rc-cards"><article v-for="g in grouped" :key="g.name" class="rc-kpi"><div class="rc-kpi-head">{{ g.name }}<span>{{ g.engineers.length }} инж.</span></div><strong>{{ g.remaining }}</strong> проверок в запасе<div class="rc-muted">{{ g.ready }} готовы · {{ g.preparation }} в подготовке · {{ g.running }} начаты</div><footer>{{ g.through ? 'Прогноз до '+label(g.through) : (g.remaining ? 'Прогноз не рассчитан' : 'Очередь пуста') }}<small v-if="g.preparation">С учётом подготовки · даты условные</small></footer></article></div>
          <div class="rc-toolbar"><strong>Очередь по инженерам</strong><div class="rc-actions"><button v-if="canEdit" :disabled="locked" @click="openCatalog()">Добавить с портала</button><button v-if="canEdit" class="rc-primary-outline" :disabled="locked" @click="openPreparation()">Проекты для подготовки</button></div></div>
          <div class="rc-legend"><span><i class="rc-swatch"></i>Готов к проверке</span><span><i class="rc-swatch pending"></i>Ожидает подготовки</span><span><i class="rc-swatch running"></i>Проверка начата</span><span>Цвет — объект</span></div>
          <div class="rc-period"><span>{{ label(anchor) }} — {{ label(days[days.length-1]?.iso) }}</span><div class="rc-actions"><button @click="anchor=dayAdd(anchor,-28)" :disabled="!anchor">‹</button><button @click="anchor=data.today">Сегодня</button><button @click="anchor=dayAdd(anchor,28)" :disabled="!anchor">›</button></div></div>
          <p class="rc-help">Клик — открыть проект · Ctrl / Cmd + клик — выделить · перетаскивание — изменить очередь</p>
          <div v-if="selected.length" class="rc-selection"><span>Выбрано: {{ selected.length }}</span><button :disabled="locked" @click="shift(-1)">Раньше</button><button :disabled="locked" @click="shift(1)">Позже</button><button @click="selected=[]">Снять выделение</button></div>
          <div class="rc-scroll" @scroll="tooltip=null"><table class="rc-grid" aria-label="Очередь проектов по инженерам и датам">
            <colgroup><col class="rc-name-col"><col v-for="d in columns" :key="d.iso || 'none'" :class="{'rc-off-col':d.off, 'rc-unscheduled-col':!d.iso}"></colgroup>
            <thead><tr><th class="rc-name">Инженер / остаток</th><th v-for="d in columns" :key="d.iso || 'none'" :title="d.label" :class="{'rc-off':d.off,'rc-today':d.iso===data.today}"><template v-if="!d.off"><b>{{ d.iso ? d.dom : 'Без даты' }}</b><small>{{ d.dow }}</small></template></th></tr></thead>
            <tbody v-for="g in grouped" :key="g.name"><tr class="rc-band"><th :colspan="columns.length+1">{{ g.name }} <small>{{ g.remaining }} проверок</small></th></tr>
              <tr v-for="e in g.engineers" :key="e.id" :data-engineer-id="e.id" :class="{'rc-drop-row': insertion?.engineer_id===e.id}" @dragover="overCell(e,null,$event)" @drop="drop">
                <th class="rc-name" scope="row"><div class="rc-person"><span class="rc-avatar">{{ e.name.split(' ').map(s=>s[0]).slice(0,2).join('') }}</span><div>{{ e.name }}</div></div><button v-if="selected.length && e.active!==false" class="rc-assign" :disabled="locked" @click="operate('move',{task_ids:selected,engineer_id:e.id})">Передать сюда</button></th>
                <td v-for="d in columns" :key="d.iso || 'none'" class="rc-cell" :class="{'rc-off':d.off,'rc-today':d.iso===data.today}" @dragover.stop="overCell(e,d.iso,$event)" @drop="drop">
                  <div v-if="!d.off || cell(e.id,d.iso).length" class="rc-dots"><button v-for="t in cell(e.id,d.iso)" :key="t.id" type="button" class="rc-point" :data-task-id="t.id" :aria-label="tipText(t)" :title="tipText(t)" :aria-pressed="selected.includes(t.id)"
                    :class="{'rc-dragging':dragging.includes(t.id),'rc-insert-before':insertion?.anchor_id===t.id && insertion.side==='before','rc-insert-after':insertion?.anchor_id===t.id && insertion.side==='after'}"
                    :draggable="canEdit && !locked && t.state!=='in_progress'" @click.stop="clickTask(t,$event)" @mouseenter="showTip(t,$event)" @mouseleave="tooltip=null" @focus="showTip(t,$event)" @blur="tooltip=null"
                    @dragstart="dragStart(t,$event)" @dragend="dragEnd" @dragover="overTask(t,$event)" @drop.stop="drop"><i class="rc-swatch" :class="{pending:t.state==='preparation',running:t.state==='in_progress'}" :style="{'--rc-color':color(t)}"></i></button></div>
                  <span v-if="cell(e.id,d.iso).length" class="rc-count">{{ cell(e.id,d.iso).length }}</span>
                </td>
              </tr>
            </tbody>
          </table></div>
          <div class="rc-summary"><span>Всего: {{ data.totals.remaining || 0 }} · готовы {{ data.totals.ready || 0 }} · подготовка {{ data.totals.preparation || 0 }} · начаты {{ data.totals.running || 0 }}</span><span>Одна точка — одна проверка тома и версии</span></div>
          <p class="rc-footnote">Даты рассчитываются по очереди и плану инженера. Подготовка и следующие за ней даты условные. Начатые проверки закреплены в начале очереди.<template v-if="!data.calendar.configured"> Рабочие дни: понедельник — пятница; исключения календаря не заданы.</template></p>
          <div class="rc-status" role="status" aria-live="polite"><span>{{ busy ? 'Сохранение…' : status }}</span><button v-if="canEdit && data.undo_operation_id" :disabled="locked" @click="operate('undo',{undo_id:data.undo_operation_id})">Отменить последнее действие</button></div>
          <aside v-if="tooltip" class="rc-tooltip" role="tooltip" :style="tooltipStyle">{{ tipText(tooltip) }}</aside>

          <dialog ref="prepDialog" class="rc-dialog" aria-labelledby="rc-prep-title" @cancel.prevent="closePreparation()" @click="($event.target===prepDialog) && closePreparation()">
            <header><div><h3 id="rc-prep-title">Проекты для подготовки</h3><p>Вставьте список каждому инженеру через Ctrl + V.<br>Одна непустая строка — один проект. Добавление в конец очереди.</p></div><button aria-label="Закрыть окно подготовки" :disabled="busy" @click="closePreparation()">×</button></header>
            <div v-if="prepOpen" class="rc-fields"><section v-for="e in activeEngineers" :key="e.id" class="rc-field" :class="{'rc-invalid':draftErrors[e.id]}"><label :for="'rc-draft-'+e.id">{{ e.name }} <span>{{ e.direction }}</span></label><textarea :id="'rc-draft-'+e.id" v-model="drafts[e.id]" :disabled="locked" :aria-invalid="!!draftErrors[e.id]" :aria-describedby="draftErrors[e.id] ? 'rc-error-'+e.id : undefined" placeholder="Вставьте названия проектов, каждое с новой строки"></textarea><div class="rc-field-foot">К добавлению: <b>{{ lines(drafts[e.id]).length }}</b></div><div v-if="draftErrors[e.id]" :id="'rc-error-'+e.id" class="rc-field-error">Повторы или некорректные строки: {{ draftErrors[e.id] }}</div></section></div>
            <div v-if="error" class="rc-error" role="alert">{{ error }} <button v-if="pending" :disabled="busy" @click="send()">Повторить сохранение</button></div>
            <footer><span>К добавлению: <b>{{ draftItems.length }}</b> · инженеров: {{ draftEngineerCount }}</span><div class="rc-actions"><button :disabled="busy" @click="closePreparation()">Закрыть</button><button class="rc-primary" :disabled="locked || !draftItems.length || draftItems.length>1000 || Object.keys(draftErrors).length>0" @click="operate('add_preparation',{items:draftItems})">Добавить в очереди</button></div></footer>
          </dialog>

          <dialog ref="projectDialog" class="rc-dialog rc-card-dialog" aria-labelledby="rc-card-title" @cancel="card=null"><template v-if="card"><header><div><p>Карточка подготовки</p><h3 id="rc-card-title">{{ card.title }}</h3></div><button aria-label="Закрыть карточку" @click="projectDialog.close();card=null">×</button></header><div class="rc-card-body"><p>{{ data.engineers.find(e=>e.id===card.engineer_id)?.name }}</p><p>{{ stateLabel(card.state) }} · позиция {{ card.position }}</p><p>Условная дата: {{ label(card.forecast_date) }}</p><p>Документы ещё не связаны. После выбора проекта сохранятся ID задания, инженер и место в очереди.</p><button v-if="canEdit" :disabled="locked" @click="openCatalog(card)">Связать с проектом / версией</button></div><footer><button v-if="canEdit && data.undo_operation_id" :disabled="locked" @click="operate('undo',{undo_id:data.undo_operation_id})">Отменить последнее действие</button><button @click="projectDialog.close();card=null">В календарь</button></footer><div v-if="error" class="rc-error" role="alert">{{ error }}</div></template></dialog>

          <dialog ref="catalogDialog" class="rc-dialog rc-card-dialog" aria-labelledby="rc-catalog-title" @cancel="catalogOpen=false"><header><h3 id="rc-catalog-title">{{ linkTask ? 'Связать с документами' : 'Добавить проект с портала' }}</h3><button aria-label="Закрыть выбор проекта" :disabled="busy" @click="closeCatalog()">×</button></header><div v-if="catalogOpen" class="rc-card-body" @click="objectMenuOpen=false"><label>Найти объект, шифр или название<div class="rc-object-picker" @click.stop><input v-model="search" type="search" autocomplete="off" placeholder="Начните вводить или выберите объект" role="combobox" aria-controls="rc-object-options" :aria-expanded="objectMenuOpen" @focus="objectMenuOpen=true" @input="objectMenuOpen=true" @keydown.esc.stop="objectMenuOpen=false"><button type="button" class="rc-object-picker__toggle" aria-label="Показать объекты" :aria-expanded="objectMenuOpen" @click="objectMenuOpen=!objectMenuOpen">▾</button><div v-if="objectMenuOpen" id="rc-object-options" class="rc-object-options" role="listbox"><button v-for="name in suggestedObjects" :key="name" type="button" role="option" :aria-selected="key(search)===key(name)" @click="selectObject(name)">{{ name }}</button><span v-if="!suggestedObjects.length">Объекты не найдены</span></div></div></label><label>Проект и версия<div class="rc-project-list" role="listbox" :aria-multiselectable="!linkTask"><button v-for="p in available" :key="JSON.stringify([p.object_id,p.project_id,p.version_id])" type="button" role="option" :aria-selected="chosenProjectIndexes.includes(String(data.available_projects.indexOf(p)))" @click="chooseProject(String(data.available_projects.indexOf(p)),$event)" @dblclick="quickAddProject(String(data.available_projects.indexOf(p)))"><span class="rc-project-list__name">{{ p.object_name }} · {{ p.title }} · {{ p.version_id }}</span><span :class="p.ready ? 'rc-project-list__ready' : 'rc-project-list__pending'">{{ p.ready ? 'Проанализировано' : 'Не проанализировано' }}</span></button><span v-if="!available.length" class="rc-project-list__empty">Проекты не найдены</span></div></label><p v-if="!linkTask" class="rc-selection-hint">Выбрано: <b>{{ chosenProjectIndexes.length }}</b>. Shift — выбрать диапазон, Ctrl / Cmd — выбрать отдельные проекты. Двойной клик — сразу добавить проект.</p><label v-if="!linkTask">Инженер<select v-model="targetEngineer"><option v-for="e in activeEngineers" :key="e.id" :value="e.id">{{ e.name }}</option></select></label><p>Выберите точную версию. Совпадение названий не связывает задания автоматически.</p><div v-if="error" class="rc-error" role="alert">{{ error }}</div></div><footer><button :disabled="busy" @click="closeCatalog()">Закрыть</button><button class="rc-primary" :disabled="locked || !chosenProjectIndexes.length || !targetEngineer" @click="saveCatalog()">{{ linkTask ? 'Связать' : 'Добавить в очередь' }}<template v-if="!linkTask && chosenProjectIndexes.length"> ({{ chosenProjectIndexes.length }})</template></button></footer></dialog>
        </section>`
    };

    const ReviewStart = {
        props: ['projectId', 'versionId', 'objectId', 'canOperate'],
        setup(props) {
            const task = ref(null), data = ref(null), busy = ref(false), error = ref('');
            let sequence = 0, retryBody = null;
            async function load() {
                if (!props.projectId || !props.canOperate) { task.value = null; return; }
                const current = ++sequence;
                try {
                    const value = await readResponse(await fetch(API));
                    if (sequence !== current) return;
                    data.value = value;
                    task.value = value.tasks.find(t => t.project_id === props.projectId && normalizedVersion(t.version_id) === normalizedVersion(props.versionId) && t.object_id === props.objectId) || null;
                } catch (_) { task.value = null; }
            }
            async function start() {
                if (busy.value || !task.value) return;
                busy.value = true; error.value = '';
                retryBody ||= {operation_id: operationId(), revision:data.value.revision, kind:'start', task_id:task.value.id};
                try {
                    const value = await readResponse(await fetch(API+'/operations',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(retryBody)}));
                    data.value = value; retryBody = null;
                    task.value = value.tasks.find(t=>t.id===task.value.id) || null;
                } catch (e) { error.value = e.message; if (e.status) retryBody=null; if (e.status===409) await load(); }
                finally { busy.value=false; }
            }
            watch(() => [props.projectId, props.versionId, props.objectId, props.canOperate], load, {immediate:true});
            onBeforeUnmount(() => sequence++);
            return {task, busy, error, start};
        },
        template:`<div v-if="task" class="reserve-review-start"><span v-if="task.state==='in_progress'">Проверка начата</span><button v-else class="btn btn-sm btn-primary" :disabled="busy || !task.documents_ready || task.state!=='ready'" @click="start">{{ busy ? 'Сохранение…' : 'Взять в работу' }}</button><small v-if="!task.documents_ready">Документы в подготовке</small><span v-if="error" role="alert">{{ error }}</span></div>`
    };
    root.ReserveCalendar = {register(app) { app.component('reserve-calendar', Calendar); app.component('reserve-review-start', ReviewStart); },
        helpers: {key, lines, dayAdd, duplicateFields, orderedSelection, operationId}};
})(window);
