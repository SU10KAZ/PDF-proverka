(function (root) {
    'use strict';
    const V = root.ProjectChangeView;
    root.ProjectChangeUI = {
        register(app) {
            app.component('project-change-list', {
                props: {changes: {type: Array, default: () => []}, report: Boolean, demo: Boolean,
                    error: String, available: Boolean, persistent: Boolean, readonly: Boolean, saving: Boolean,
                    history: {type:Object, default:()=>({})}},
                emits: ['decision', 'open-evidence', 'reset-decisions', 'refresh', 'history'],
                setup(props, {emit}) {
                    const {ref, reactive, computed, nextTick} = root.Vue;
                    const filters = reactive({cipher: '', system: '', type: '', status: '', source: ''});
                    const groupBy = ref('cipher');
                    const selectedImage = ref(null);
                    const imageDialog = ref(null);
                    const failedImages = reactive({});
                    const comments = reactive({});
                    const all = computed(() => props.report ? V.report(props.changes) : props.changes);
                    const visible = computed(() => V.filter(all.value, filters));
                    const counts = computed(() => V.summary(props.changes));
                    const options = computed(() => ({
                        cipher: [...new Set(all.value.map(c => `${c.cipher} · ${c.discipline}`))].sort(),
                        system: [...new Set(all.value.map(c => c.engineering_system).filter(Boolean))].sort(),
                    }));
                    const groups = computed(() => {
                        const result = new Map();
                        for (const c of all.value) {
                            const key = c[groupBy.value] || 'Не указан';
                            result.set(key, (result.get(key) || 0) + 1);
                        }
                        return [...result.entries()];
                    });
                    async function enlarge(e) {
                        selectedImage.value = e;
                        await nextTick();
                        imageDialog.value?.showModal();
                    }
                    function open(change, e) {
                        const target = V.destination(change, e);
                        if (target) emit('open-evidence', target);
                    }
                    function clearFilters() { Object.keys(filters).forEach(k => { filters[k] = ''; }); }
                    return {filters, groupBy, selectedImage, imageDialog, failedImages, all, visible, counts,
                        options, groups, enlarge, open, clearFilters, statuses: V.STATUS, types: V.TYPES,
                        sources: V.SOURCES, destination: V.destination, comments,
                        actionLabels: {CONFIRM:'Подтверждено', NOT_A_CHANGE:'Не изменение', UNSURE:'Не могу определить', BROKEN_CASE:'Проблема'}};
                },
                template: `
                <section class="pc-workspace" :aria-label="report ? 'Отчёт' : 'Изменения проекта'">
                    <header class="pc-heading">
                        <h2>{{ report ? 'Итоговый журнал изменений' : 'Изменения проекта' }}</h2>
                        <div v-if="report" class="pc-actions" aria-label="Экспорт">
                            <button v-for="format in ['Excel', 'PDF', 'HTML']" :key="format" class="btn btn-sm btn-secondary"
                                disabled :title="'Экспорт ' + format + ' пока недоступен'">{{ format }} ↓</button>
                        </div>
                        <small v-else>{{ visible.length }} из {{ all.length }}</small>
                    </header>
                    <p v-if="demo" class="pc-notice" role="status">Исследовательские / демо-данные · объект 272 · v002.
                        Решения действуют только в этой вкладке браузера и не являются production truth.
                        <button class="pc-link" @click="$emit('reset-decisions')">Сбросить решения</button>
                    </p>
                    <p v-if="persistent" class="pc-notice" role="status">Замороженный кандидат · объект 272 · v002.
                        <span v-if="readonly">Только просмотр. Решения отключены.</span>
                        <span v-else>Решения сохраняются в отдельном журнале preview.</span>
                    </p>
                    <p v-if="!available" class="pc-notice">Данные ProjectChange ещё не опубликованы.
                        Для этой страницы нужен backend с контрактом ProjectChangeView.</p>
                    <p v-if="!demo && !persistent && available" class="pc-notice">Сохранение решений по ProjectChange ещё не подключено.</p>
                    <p v-if="error" class="sc-shell-error" role="alert">{{ error }}
                        <button v-if="!persistent && !demo" class="pc-link" @click="$emit('refresh')">Повторить загрузку</button>
                    </p>
                    <div class="pc-summary" aria-label="Сводка изменений">
                        <template v-if="report"><span>Подтверждено изменений: <b>{{ all.length }}</b></span></template>
                        <template v-else>
                            <span>Подтверждено: <b>{{ counts.confirmed }}</b></span>
                            <span>Нужно проверить: <b>{{ counts.review }}</b></span>
                            <span>Конфликты: <b>{{ counts.conflicts }}</b></span>
                            <span>Высокая важность: <b>{{ counts.high }}</b></span>
                        </template>
                    </div>
                    <div v-if="report" class="pc-report-groups">
                        <label>Группировка <select v-model="groupBy" aria-label="Группировка отчёта">
                            <option value="cipher">По шифру</option><option value="discipline">По разделу</option>
                            <option value="engineering_system">По системе</option>
                        </select></label>
                        <span v-for="[label, count] in groups" :key="label" class="pc-source">{{ label }} — {{ count }}</span>
                        <small>Экспорт пока недоступен.</small>
                    </div>
                    <div v-if="!report" class="pc-filters" aria-label="Фильтры изменений">
                        <label>Шифр / раздел<select v-model="filters.cipher"><option value="">Все разделы</option>
                            <option v-for="x in options.cipher" :key="x">{{ x }}</option></select></label>
                        <label>Инженерная система<select v-model="filters.system"><option value="">Все системы</option>
                            <option v-for="x in options.system" :key="x">{{ x }}</option></select></label>
                        <label>Тип изменения<select v-model="filters.type"><option value="">Все типы</option>
                            <option v-for="(label, key) in types" :key="key" :value="key">{{ label }}</option></select></label>
                        <label>Статус<select v-model="filters.status"><option value="">Все статусы</option>
                            <option v-for="(label, key) in statuses" :key="key" :value="key">{{ label }}</option></select></label>
                        <label>Источник<select v-model="filters.source"><option value="">Все источники</option>
                            <option v-for="x in sources" :key="x">{{ x }}</option></select></label>
                        <button class="pc-link" @click="clearFilters">Сбросить</button>
                    </div>
                    <p v-if="!visible.length" class="pc-empty">{{ report
                        ? 'Подтверждённых изменений пока нет. Примите решения на вкладке «Изменения проекта».'
                        : all.length ? 'Нет изменений с выбранными фильтрами.' : 'Список изменений пока пуст.' }}</p>
                    <div class="pc-list">
                        <article v-for="c in visible" :key="c.id" class="pc-card" :id="'pc-' + c.id"
                            :data-production-target-id="c.id" :data-status="c.status">
                            <header class="pc-card-head"><div><h3>{{ c.summary_ru }}</h3>
                                <p class="pc-meta">{{ c.cipher || 'Шифр не указан' }} · {{ c.discipline }}
                                    <span v-if="c.engineering_system"> · {{ c.engineering_system }}</span></p></div>
                                <span class="pc-status" :class="'pc-status--' + c.status.toLowerCase()">{{ statuses[c.status] }}</span>
                            </header>
                            <div class="pc-card-context"><span>{{ types[c.change_type] }}</span>
                                <span v-if="c.engineering_subject">Объект: {{ c.engineering_subject }}</span>
                                <b v-if="c.importance === 'HIGH'">Высокая важность</b></div>
                            <div class="pc-states"><div><small>Было · OLD</small><p>{{ c.old_state || 'Состояние не установлено' }}</p></div>
                                <div><small>Стало · NEW</small><p>{{ c.new_state || 'Состояние не установлено' }}</p></div></div>
                            <div class="pc-evidence-bar"><span v-for="s in sources" :key="s" class="pc-source"
                                :class="{'is-present': c.evidence.some(e => e.source_type === s)}">{{ s }} {{ c.evidence.some(e => e.source_type === s) ? '✓' : '—' }}</span>
                                <button class="pc-link sc-production-evidence-link" :disabled="!destination(c)" @click="open(c)">Открыть в PDF ↗</button>
                            </div>
                            <div v-for="(conflict, n) in c.conflicts.filter(x => !x.resolved)" :key="n" class="pc-conflict" role="status">
                                <strong>Источники противоречат друг другу.</strong><p>{{ conflict.explanation_ru }}</p>
                                <span v-for="(v, i) in conflict.values" :key="i">{{ v.source_type || 'Источник' }}: {{ v.value }} </span>
                            </div>
                            <div v-if="!report && c.status !== 'CONFIRMED' && c.status !== 'REJECTED'" class="pc-review">
                                <strong>{{ c.review_question }}</strong><p>{{ c.review_explanation_ru }}</p>
                            </div>
                            <details class="pc-evidence-details" :open="!report && !['CONFIRMED','REJECTED'].includes(c.status)">
                                <summary>Фрагменты источников · {{ c.evidence.length }}</summary>
                                <div class="pc-evidence-sides"><section v-for="side in ['OLD','NEW']" :key="side" :aria-label="side + ' доказательства'">
                                    <strong class="pc-side-label">{{ side }} · {{ c.evidence.filter(e => e.side === side).length }} фрагментов</strong>
                                    <p v-if="!c.evidence.some(e => e.side === side)" class="pc-missing">Привязка к {{ side }} не установлена. Нельзя считать отсутствие фрагмента отсутствием объекта.</p>
                                    <div class="pc-evidence-grid"><figure v-for="e in c.evidence.filter(e => e.side === side)" :key="e.id">
                                        <button v-if="e.image_url && !failedImages[e.image_url]" class="pc-crop" @click="enlarge(e)" :aria-label="'Увеличить ' + side + ', стр. ' + e.page">
                                            <img :src="e.image_url" :alt="e.short_explanation_ru" loading="lazy" @error="failedImages[e.image_url] = true">
                                            <span>{{ e.crop_precision === 'PAGE_LEVEL' ? 'Открыть страницу ↗' : 'Увеличить ↗' }}</span></button>
                                        <p v-else class="pc-missing">{{ failedImages[e.image_url] ? 'Не удалось загрузить фрагмент.' : 'Растровый фрагмент пока недоступен.' }}</p>
                                        <figcaption><b>{{ e.document.label || c.cipher }} · {{ e.page ? 'стр. ' + e.page : 'страница не указана' }} · {{ e.source_type || 'Источник' }}</b>
                                            <p>{{ e.short_explanation_ru }}</p><blockquote v-if="e.quote">{{ e.quote }}</blockquote>
                                            <button class="pc-link" :disabled="!destination(c,e)" @click="open(c,e)">Открыть в PDF</button>
                                        </figcaption>
                                    </figure></div>
                                </section></div>
                            </details>
                            <details class="pc-details"><summary>Подробности · Изменившиеся характеристики: {{ c.details.length }}</summary>
                                <table v-if="c.details.length"><thead><tr><th>Характеристика</th><th>Было</th><th>Стало</th></tr></thead>
                                    <tbody><tr v-for="(d, i) in c.details" :key="i"><td>{{ d.label }}</td><td>{{ d.old || '—' }}</td><td>{{ d.new || '—' }}</td></tr></tbody></table>
                                <p v-else>Отдельные характеристики не предоставлены.</p>
                                <details class="pc-technical"><summary>Технические подробности</summary><pre>{{ c.technical_provenance.join('\\n') }}</pre>
                                    <template v-if="persistent"><p>{{ c.decision_state }} · {{ c.decision_key }}</p>
                                        <button class="pc-link" @click="$emit('history', c)">История решений</button>
                                        <p v-if="history[c.id] && !history[c.id].length">Решений пока нет.</p>
                                        <p v-for="h in history[c.id] || []" :key="h.revision">#{{ h.revision }} · {{ actionLabels[h.decision] }} · {{ h.actor }} · {{ h.timestamp }}
                                            · {{ h.candidate_version }} / {{ h.source_run_id }}
                                            <span v-if="!h.same_decision_key"> · Другая версия события — не применяется</span>
                                            <span v-if="h.comment"> · {{ h.comment }}</span></p>
                                    </template>
                                </details>
                            </details>
                            <label v-if="persistent && !readonly && !report" class="pc-decision-comment">Комментарий к решению (необязательно)
                                <input v-model="comments[c.id]" maxlength="4000" :disabled="saving" placeholder="Что проверили в источниках"></label>
                            <footer v-if="!report" class="pc-actions" :aria-label="'Решение: ' + c.summary_ru">
                                <button v-for="s in ['CONFIRMED','REJECTED','UNDETERMINED','PROBLEM']" :key="s" class="btn btn-sm btn-secondary"
                                    :class="{'pc-selected': c.status === s}" :aria-pressed="String(c.status === s)"
                                    :disabled="readonly || saving || (!demo && !persistent) || (s === 'CONFIRMED' && c.conflicts.some(x => !x.resolved))"
                                    :title="readonly ? 'Исследовательский предпросмотр доступен только для чтения' : !demo && !persistent ? 'Backend решений ProjectChange ещё не подключён' : s === 'CONFIRMED' && c.conflicts.some(x => !x.resolved) ? 'Сначала нужно разрешить конфликт источников' : ''"
                                    @click="$emit('decision', {id:c.id, status:s, comment:comments[c.id] || ''})">{{ s === 'CONFIRMED' ? 'Подтвердить' : statuses[s] }}</button>
                                <small v-if="c.local_decision" role="status">Демо-решение сохранено в этой вкладке</small>
                                <small v-if="persistent && c.effective_decision" role="status">Сохранено в preview · {{ c.effective_decision.actor }} · {{ c.effective_decision.timestamp }}</small>
                            </footer>
                        </article>
                    </div>
                    <dialog ref="imageDialog" class="pc-image-dialog" @click="$event.target === imageDialog && imageDialog.close()">
                        <template v-if="selectedImage"><header><strong>{{ selectedImage.side }} · {{ selectedImage.document.label }} · стр. {{ selectedImage.page }}</strong>
                            <button class="btn btn-sm" @click="imageDialog.close()" autofocus>Закрыть</button></header>
                            <img :src="selectedImage.image_url" :alt="selectedImage.short_explanation_ru"><p>{{ selectedImage.short_explanation_ru }}</p></template>
                    </dialog>
                </section>`,
            });
        },
    };
}(typeof globalThis !== 'undefined' ? globalThis : this));
