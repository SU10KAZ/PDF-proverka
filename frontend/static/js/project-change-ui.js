(function (root) {
    'use strict';
    const V = root.ProjectChangeView;
    root.ProjectChangeUI = {
        register(app) {
            app.component('project-change-list', {
                props: {changes: {type: Array, default: () => []}, report: Boolean, demo: Boolean,
                    error: String, available: Boolean, persistent: Boolean, readonly: Boolean, saving: Boolean,
                    history: {type:Object, default:()=>({})}, pairs: {type:Array, default:()=>[]},
                    selectedPairId: {type:String, default:''}, pairLoading: Boolean, revealId: String},
                emits: ['decision', 'open-evidence', 'reset-decisions', 'refresh', 'history', 'select-pair'],
                setup(props, {emit}) {
                    const {ref, reactive, computed, nextTick, watch} = root.Vue;
                    const filters = reactive({cipher: '', system: '', type: '', status: '', source: ''});
                    const groupBy = ref('cipher');
                    const scope = ref('pair');
                    const expandedId = ref('');
                    const currentPair = computed(() => props.pairs.find(p => p.id === props.selectedPairId));
                    const pairLabel = p => `${p.left?.filename || 'OLD не установлен'} → ${p.right?.filename || 'NEW не установлен'}`;
                    const changePairs = c => props.pairs.filter(p => c.pair_ids.includes(p.id));
                    const presentSources = c => V.SOURCES.filter(s => c.evidence.some(e => e.source_type === s));
                    const selectedImage = ref(null);
                    const imageDialog = ref(null);
                    const failedImages = reactive({});
                    const comments = reactive({});
                    const all = computed(() => props.report ? V.report(props.changes)
                        : scope.value === 'all' ? props.changes : V.inPair(props.changes, currentPair.value?.id));
                    const bindingErrors = computed(() => props.changes.filter(c => c.pair_binding_error).length);
                    const needsPair = computed(() => !props.report && scope.value === 'pair' && !currentPair.value);
                    const visible = computed(() => V.filter(all.value, filters));
                    const counts = computed(() => V.summary(all.value));
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
                    watch(() => props.selectedPairId, () => { clearFilters(); expandedId.value = ''; });
                    watch(scope, () => { clearFilters(); expandedId.value = ''; });
                    watch(() => props.revealId, id => { if (id) expandedId.value = id; }, {immediate:true});
                    function toggle(c) { expandedId.value = expandedId.value === c.id ? '' : c.id; }
                    function clearFilters() { Object.keys(filters).forEach(k => { filters[k] = ''; }); }
                    return {filters, groupBy, selectedImage, imageDialog, failedImages, all, visible, counts,
                        scope, expandedId, currentPair, pairLabel, changePairs, presentSources, bindingErrors, needsPair, toggle,
                        compactText: V.compactText, options, groups, enlarge, open, clearFilters, statuses: V.STATUS, types: V.TYPES,
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
                        <small v-else class="pc-result-count" aria-live="polite">{{ visible.length }} из {{ all.length }} · {{ scope === 'pair' ? 'текущая пара' : 'все пары объекта' }}</small>
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
                    <div v-if="!report" class="pc-pair-toolbar">
                        <div class="pc-current-pair">
                            <strong>Текущая пара</strong>
                            <span v-if="currentPair" class="pc-pair-names" :title="pairLabel(currentPair)">
                                <span><small>OLD</small> {{ currentPair.left?.filename || 'Не установлен' }}</span>
                                <b aria-hidden="true">→</b>
                                <span><small>NEW</small> {{ currentPair.right?.filename || 'Не установлен' }}</span>
                            </span>
                            <span v-else class="pc-pair-prompt">Выберите пару документов</span>
                            <label class="pc-pair-picker"><span class="pc-sr-only">Пара документов</span>
                                <select aria-label="Пара документов" :value="selectedPairId" :disabled="pairLoading || !pairs.length"
                                    @change="$emit('select-pair', $event.target.value)">
                                    <option value="" disabled>Выберите пару документов</option>
                                    <option v-for="p in pairs" :key="p.id" :value="p.id">{{ pairLabel(p) }}</option>
                                </select>
                            </label>
                        </div>
                        <div class="pc-scope-switch" role="group" aria-label="Область изменений">
                            <button :aria-pressed="scope === 'pair'" @click="scope = 'pair'">Текущая пара</button>
                            <button :aria-pressed="scope === 'all'" @click="scope = 'all'">Все пары объекта</button>
                        </div>
                    </div>
                    <p v-if="!report && bindingErrors" class="pc-notice" role="alert">Для {{ bindingErrors }} изменений привязка к паре не установлена.
                        Они доступны в режиме «Все пары объекта» с предупреждением.</p>
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
                    <div v-if="!report && !needsPair" class="pc-filters" aria-label="Фильтры изменений">
                        <label>Шифр / раздел<select v-model="filters.cipher"><option value="">Все разделы</option>
                            <option v-for="x in options.cipher" :key="x">{{ x }}</option></select></label>
                        <label>Инженерная система<select v-model="filters.system"><option value="">Все системы</option>
                            <option v-for="x in options.system" :key="x">{{ x }}</option></select></label>
                        <label>Тип изменения<select v-model="filters.type"><option value="">Все типы</option>
                            <option v-for="(label, key) in types" :key="key" :value="key">{{ label }}</option></select></label>
                        <label>Статус<select v-model="filters.status"><option value="">Все статусы</option>
                            <option v-for="key in readonly ? ['REVIEW', 'CONFLICT'] : Object.keys(statuses)" :key="key" :value="key">{{ statuses[key] }}</option></select></label>
                        <label>Источник<select v-model="filters.source"><option value="">Все источники</option>
                            <option v-for="x in sources" :key="x">{{ x }}</option></select></label>
                        <button class="pc-link" @click="clearFilters">Сбросить</button>
                    </div>
                    <p v-if="!visible.length && !needsPair" class="pc-empty">{{ report
                        ? 'Подтверждённых изменений пока нет. Примите решения на вкладке «Изменения проекта».'
                        : all.length ? 'Нет изменений с выбранными фильтрами.' : scope === 'pair' ? 'Для этой пары изменений в preview нет.' : 'Список изменений пока пуст.' }}</p>
                    <div v-if="visible.length" class="pc-table-scroll" tabindex="0" aria-label="Таблица изменений">
                        <table class="pc-table" :class="{'pc-table--all': scope === 'all'}">
                            <colgroup><col class="pc-col-id"><col class="pc-col-importance"><col class="pc-col-cipher">
                                <col v-if="scope === 'all' && !report" class="pc-col-pair"><col class="pc-col-system"><col class="pc-col-type">
                                <col class="pc-col-summary"><col class="pc-col-states"><col class="pc-col-source"><col class="pc-col-status"><col class="pc-col-action"></colgroup>
                            <thead><tr><th scope="col">ID</th><th scope="col">Важность</th><th scope="col">Раздел / шифр</th>
                                <th v-if="scope === 'all' && !report" scope="col">Пара документов</th>
                                <th scope="col">Система</th><th scope="col">Тип</th><th scope="col">Изменение</th>
                                <th scope="col">OLD → NEW</th><th scope="col">Источник</th><th scope="col">Статус</th><th scope="col"><span class="pc-sr-only">Подробности</span></th></tr></thead>
                            <tbody><template v-for="c in visible" :key="c.id">
                                <tr class="pc-row" :id="'pc-' + c.id" :data-production-target-id="c.id" :data-status="c.status"
                                    :class="{'is-expanded': expandedId === c.id}" @click="toggle(c)">
                                    <td class="pc-row-id" :title="c.id">{{ c.display_id }}</td>
                                    <td><span class="pc-importance" :class="{'is-high': c.importance === 'HIGH'}">{{ c.importance === 'HIGH' ? 'Высокая' : 'Обычная' }}</span></td>
                                    <td><strong>{{ c.cipher || '—' }}</strong><small class="pc-cell-secondary pc-clamp">{{ c.discipline }}</small>
                                        <span v-if="c.pair_ids.length > 1" class="pc-cell-secondary">Несколько пар: {{ c.pair_ids.length }}</span>
                                        <span v-if="c.pair_binding_error" class="pc-binding-warning">Привязка не установлена</span></td>
                                    <td v-if="scope === 'all' && !report"><span v-for="p in changePairs(c)" :key="p.id" class="pc-cell-pair pc-clamp" :title="pairLabel(p)">{{ pairLabel(p) }}</span></td>
                                    <td><span class="pc-clamp" :title="c.engineering_system">{{ c.engineering_system || '—' }}</span></td>
                                    <td>{{ types[c.change_type] }}</td>
                                    <td class="pc-row-summary"><span class="pc-clamp" :title="c.summary_ru">{{ c.summary_ru }}</span></td>
                                    <td><div class="pc-table-states">
                                        <span class="pc-clamp" :title="c.old_state || 'OLD не установлен'">{{ compactText(c.old_state, 90) || 'OLD не установлен' }}</span>
                                        <b class="pc-state-arrow">→</b>
                                        <span class="pc-clamp" :title="c.new_state || 'NEW не установлен'">{{ compactText(c.new_state, 90) || 'NEW не установлен' }}</span></div></td>
                                    <td><div class="pc-row-sources"><span v-for="s in presentSources(c)" :key="s" class="pc-source is-present">{{ s }}</span></div></td>
                                    <td><span class="pc-status" :class="'pc-status--' + c.status.toLowerCase()">{{ statuses[c.status] }}</span></td>
                                    <td><button class="pc-expand" :aria-label="(expandedId === c.id ? 'Свернуть ' : 'Раскрыть ') + c.display_id"
                                        :aria-expanded="expandedId === c.id" :aria-controls="'pc-detail-' + c.id" @click.stop="toggle(c)">{{ expandedId === c.id ? '−' : '+' }}</button></td>
                                </tr>
                                <tr v-if="expandedId === c.id" class="pc-expanded-row" :id="'pc-detail-' + c.id">
                                    <td :colspan="scope === 'all' && !report ? 11 : 10">
                        <article class="pc-card" :aria-label="c.display_id + ': подробности'">
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
                                    <p v-if="!c.evidence.some(e => e.side === side)" class="pc-missing">Надёжная привязка к {{ side }} не установлена. Отсутствие объекта не доказано.</p>
                                    <div class="pc-evidence-grid"><figure v-for="e in c.evidence.filter(e => e.side === side)" :key="e.id">
                                        <button v-if="e.image_url && !failedImages[e.image_url]" class="pc-crop" @click="enlarge(e)" :aria-label="'Увеличить ' + side + ', стр. ' + e.page">
                                            <img :src="e.image_url" :alt="e.short_explanation_ru" loading="lazy" @error="failedImages[e.image_url] = true">
                                            <span>{{ e.crop_precision === 'PAGE_LEVEL' ? 'Открыть страницу ↗' : 'Увеличить ↗' }}</span></button>
                                        <p v-else class="pc-missing">{{ failedImages[e.image_url] ? 'Не удалось загрузить фрагмент.' : 'Растровый фрагмент пока недоступен.' }}</p>
                                        <figcaption><b>{{ e.document.label || c.cipher }} · {{ e.page ? 'стр. ' + e.page : 'страница не указана' }} · {{ e.source_type || 'Источник' }}</b>
                                            <p v-if="e.crop_precision === 'PAGE_LEVEL'" class="pc-page-fallback">Показана страница целиком — точная область не установлена.</p>
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
                                <p v-if="c.research" class="pc-research-meta">Исследовательская оценка: {{ c.research_status === 'PROVEN' ? 'подтверждено исследованием' : 'требует проверки' }}. Подтверждение инженера не получено.</p>
                                <details class="pc-technical"><summary>Технические подробности</summary><pre>{{ c.technical_provenance.join('\\n') }}</pre>
                                    <template v-if="persistent"><p>{{ c.decision_state }} · {{ c.decision_key }}</p>
                                        <button v-if="!readonly" class="pc-link" @click="$emit('history', c)">История решений</button>
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
                                    </td>
                                </tr>
                            </template></tbody>
                        </table>
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
