(function (root) {
    'use strict';
    const V = root.ProjectChangeView;
    root.ProjectChangeUI = {
        register(app) {
            app.component('project-change-list', {
                props: {changes: {type: Array, default: () => []}, report: Boolean, demo: Boolean,
                    error: String, available: Boolean, persistent: Boolean, readonly: Boolean, saving: Boolean,
                    history: {type:Object, default:()=>({})}, pairs: {type:Array, default:()=>[]},
                    resultHref: {type:String, default:''}, selectedPairId: {type:String, default:''}, revealId: String, objectId: {type:String, default:''}},
                emits: ['decision', 'open-evidence', 'reset-decisions', 'refresh', 'history', 'open-upload', 'expert-saved'],
                setup(props, {emit}) {
                    const {ref, reactive, computed, nextTick, watch} = root.Vue;
                    const groupBy = ref('cipher');
                    const expandedId = ref('');
                    const currentPair = computed(() => props.pairs.find(p => p.id === props.selectedPairId));
                    const presentSources = c => V.SOURCES.filter(s => c.evidence.some(e => e.source_type === s));
                    const selectedImage = ref(null);
                    const imageDialog = ref(null);
                    const pageView = ref(null), pageLoading = ref(false), pageError = ref(''), pageImageFailed = ref(false);
                    let imageRequest = 0;
                    function imageClosed() { imageRequest++; pageLoading.value = false; }
                    function pageBoxStyle(box) {
                        return {x: box.x * pageView.value.page_width, y: box.y * pageView.value.page_height,
                            width: box.width * pageView.value.page_width, height: box.height * pageView.value.page_height};
                    }
                    const failedImages = reactive({});
                    const comments = reactive({});
                    const all = computed(() => props.report ? V.report(props.changes)
                        : V.inPair(props.changes, currentPair.value?.id));
                    const needsPair = computed(() => !props.report && !currentPair.value);
                    // Human Mapping of exactly this comparison: object + opened pair.
                    const humanMappingHref = computed(() => props.resultHref || (props.objectId && props.selectedPairId
                        ? '/human-mapping/?object=' + encodeURIComponent(props.objectId)
                            + '&comparison=' + encodeURIComponent(props.selectedPairId)
                            + (all.value[0]?.candidate_version === 'projectchange_v3' && all.value[0]?.source_run_id
                                && all.value[0]?.session_id
                                ? '&session_id=' + encodeURIComponent(all.value[0].session_id)
                                  + '&run_id=' + encodeURIComponent(all.value[0].source_run_id) : '') : ''));
                    const visible = all;
                    const expertMode = ref(false), expertSaving = ref(false), expertError = ref(''), expertMessage = ref('');
                    const expertDrafts = reactive({});
                    const expertAvailable = computed(() => !props.report && all.value.some(c => c.expert_review_available));
                    const expertKey = c => [props.objectId, c.session_id, c.id].join('|');
                    const expertValue = c => expertDrafts[expertKey(c)] || c.expert_review || {decision:null, reason:''};
                    const expertDirty = c => {
                        const draft = expertDrafts[expertKey(c)], saved = c.expert_review;
                        return !!draft && (draft.decision !== (saved?.decision || null) || draft.reason !== (saved?.reason || ''));
                    };
                    const expertPending = computed(() => all.value.filter(c => c.expert_review_available && expertDirty(c)));
                    const expertInvalid = computed(() => expertPending.value.some(c => expertValue(c).decision === 'rejected' && !expertValue(c).reason.trim()));
                    const expertCounts = computed(() => ({
                        accepted: all.value.filter(c => expertValue(c).decision === 'accepted').length,
                        rejected: all.value.filter(c => expertValue(c).decision === 'rejected').length,
                    }));
                    function setExpertDecision(c, decision) {
                        if (expertSaving.value || !c.expert_review_available) return;
                        const previous = expertValue(c);
                        const next = previous.decision === decision ? null : decision;
                        expertDrafts[expertKey(c)] = {decision:next, reason:next === previous.decision ? previous.reason : '',
                            expected_revision: previous.expected_revision ?? c.expert_review?.revision ?? 0};
                        expertMessage.value = ''; expertError.value = '';
                    }
                    function setExpertReason(c, reason) {
                        if (expertSaving.value || !c.expert_review_available) return;
                        const previous = expertValue(c);
                        expertDrafts[expertKey(c)] = {...previous, reason,
                            expected_revision: previous.expected_revision ?? c.expert_review?.revision ?? 0};
                        expertMessage.value = '';
                    }
                    async function saveExpertReview() {
                        if (expertSaving.value || !expertPending.value.length || expertInvalid.value) return;
                        const objectId = props.objectId;
                        const pending = expertPending.value.map(c => ({key:expertKey(c), update:{
                            session_id:c.session_id, pair_id:c.pair_id, run_id:c.expert_review_run_id || c.source_run_id, change_id:c.projectchange_id,
                            decision:expertValue(c).decision, reason:expertValue(c).reason,
                            expected_revision:expertValue(c).expected_revision ?? c.expert_review?.revision ?? 0,
                        }}));
                        expertSaving.value = true; expertError.value = ''; expertMessage.value = '';
                        try {
                            const response = await root.fetch('/api/stage-comparison/objects/' + encodeURIComponent(objectId) + '/project-change-expert-review', {
                                method:'POST', headers:{'Content-Type':'application/json'},
                                body:JSON.stringify({updates:pending.map(p => p.update)}),
                            });
                            const data = await response.json();
                            if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : 'Не удалось сохранить решения. Повторите попытку.');
                            emit('expert-saved', {objectId, items:data.items});
                            for (const p of pending) delete expertDrafts[p.key];
                            if (props.objectId === objectId) expertMessage.value = 'Решения сохранены';
                        } catch (error) {
                            if (props.objectId === objectId) expertError.value = error.message || 'Решения не сохранены';
                        } finally { expertSaving.value = false; }
                    }
                    const statusLabel = c => ({accepted:'Принято', rejected:'Не принято'}[c.expert_review?.decision] || V.STATUS[c.status]);
                    const counts = computed(() => V.summary(all.value));
                    const groups = computed(() => {
                        const result = new Map();
                        for (const c of all.value) {
                            const key = c[groupBy.value] || 'Не указан';
                            result.set(key, (result.get(key) || 0) + 1);
                        }
                        return [...result.entries()];
                    });
                    async function enlarge(e) {
                        const requestId = ++imageRequest;
                        selectedImage.value = e;
                        pageView.value = null; pageError.value = ''; pageImageFailed.value = false;
                        pageLoading.value = !!e.page_view_url;
                        await nextTick();
                        if (requestId !== imageRequest) return;
                        if (!imageDialog.value?.open) imageDialog.value?.showModal();
                        if (!e.page_view_url) return;
                        try {
                            const response = await root.fetch(e.page_view_url);
                            if (!response.ok) throw new Error('page unavailable');
                            const data = await response.json();
                            if (data.evidence_id !== e.id || !/^\/(?!\/)/.test(data.image_url || '')
                                    || /[\s\\]/.test(data.image_url)
                                    || !Number.isFinite(data.page_width) || data.page_width <= 0
                                    || !Number.isFinite(data.page_height) || data.page_height <= 0
                                    || !Array.isArray(data.highlights) || data.highlights.some(b =>
                                        ![b.x,b.y,b.width,b.height].every(Number.isFinite) || b.x < 0 || b.y < 0
                                        || b.width <= 0 || b.height <= 0 || b.x+b.width > 1.001 || b.y+b.height > 1.001)) {
                                throw new Error('invalid page location');
                            }
                            if (requestId === imageRequest) pageView.value = data;
                        } catch (_error) {
                            if (requestId === imageRequest) pageError.value = 'Не удалось открыть лист с выделением. Показан сохранённый фрагмент.';
                        } finally {
                            if (requestId === imageRequest) pageLoading.value = false;
                        }
                    }
                    function open(change, e) {
                        const target = V.destination(change, e);
                        if (target) emit('open-evidence', target);
                    }
                    watch(() => props.selectedPairId, () => { expandedId.value = ''; });
                    watch(() => props.revealId, id => { if (id) expandedId.value = id; }, {immediate:true});
                    function toggle(c) { expandedId.value = expandedId.value === c.id ? '' : c.id; }
                    return {humanMappingHref, groupBy, selectedImage, imageDialog, pageView, pageLoading, pageError, pageImageFailed, pageBoxStyle, imageClosed, failedImages, all, visible, counts,
                        expertMode, expertAvailable, expertSaving, expertError, expertMessage, expertPending, expertInvalid, expertCounts,
                        expertValue, expertDirty, setExpertDecision, setExpertReason, saveExpertReview, statusLabel,
                        expandedId, presentSources, needsPair, toggle,
                        compactText: V.compactText, groups, enlarge, open, statuses: V.STATUS, types: V.TYPES,
                        sources: V.SOURCES, destination: V.destination, comments,
                        actionLabels: {CONFIRM:'Подтверждено', NOT_A_CHANGE:'Не изменение', UNSURE:'Не могу определить', BROKEN_CASE:'Проблема'}};
                },
                template: `
                <section class="pc-workspace" :class="{'pc-workspace--changes': !report}" :aria-label="report ? 'Отчёт' : 'Изменения проекта'">
                    <header class="pc-heading">
                        <h2>{{ report ? 'Итоговый журнал изменений' : 'Изменения проекта' }}</h2>
                        <button v-if="expertAvailable" class="btn-expert-toggle" :class="{active:expertMode}" :aria-pressed="String(expertMode)"
                            @click="expertMode = !expertMode">{{ expertMode ? 'Скрыть оценку' : 'Экспертная оценка' }}</button>
                        <div v-if="humanMappingHref" class="pc-actions" style="margin-left:auto" aria-label="Human Mapping">
                            <a class="btn btn-sm btn-secondary" id="pc-human-mapping-link"
                               :href="humanMappingHref" target="_blank" rel="noopener">Human Mapping</a>
                        </div>
                        <div v-if="report" class="pc-actions" aria-label="Экспорт">
                            <button v-for="format in ['Excel', 'PDF', 'HTML']" :key="format" class="btn btn-sm btn-secondary"
                                disabled :title="'Экспорт ' + format + ' пока недоступен'">{{ format }} ↓</button>
                        </div>
                    </header>
                    <div v-if="expertAvailable && expertMode" class="pc-expert-toolbar">
                        <span>Принято: <b>{{ expertCounts.accepted }}</b> · Не принято: <b>{{ expertCounts.rejected }}</b></span>
                        <span v-if="expertPending.length">Не сохранено: {{ expertPending.length }}</span>
                        <button class="btn-expert-save" :disabled="expertSaving || !expertPending.length || expertInvalid" @click="saveExpertReview">
                            {{ expertSaving ? 'Сохранение…' : 'Сохранить решения' }}</button>
                        <span v-if="expertInvalid" role="status">Укажите причину каждого отказа.</span>
                    </div>
                    <p v-if="expertError" class="sc-shell-error" role="alert">{{ expertError }}</p>
                    <p v-if="expertMessage" class="pc-expert-message" role="status">{{ expertMessage }}</p>
                    <p v-if="demo" class="pc-notice" role="status">Исследовательские / демо-данные · объект 272 · v002.
                        Решения действуют только в этой вкладке браузера и не являются production truth.
                        <button class="pc-link" @click="$emit('reset-decisions')">Сбросить решения</button>
                    </p>
                    <p v-if="persistent && report" class="pc-notice" role="status">Опубликованные результаты доступны только для просмотра.</p>
                    <p v-if="error" class="sc-shell-error" role="alert">{{ error }}
                        <button v-if="!persistent && !demo" class="pc-link" @click="$emit('refresh')">Повторить загрузку</button>
                    </p>
                    <div v-if="needsPair" class="pc-empty pc-pair-prompt">
                        <p>Сначала откройте пару документов на вкладке «Загрузка документации».</p>
                        <button class="btn btn-sm btn-secondary" @click="$emit('open-upload')">Перейти к загрузке документации</button>
                    </div>
                    <div v-if="!needsPair" class="pc-summary" aria-label="Сводка изменений" aria-live="polite">
                        <template v-if="report"><span>Подтверждено изменений: <b>{{ all.length }}</b></span></template>
                        <template v-else>
                            <span>Всего изменений: <b>{{ all.length }}</b></span>
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
                    <p v-if="!visible.length && !needsPair" class="pc-empty">{{ report
                        ? 'Подтверждённых изменений пока нет.'
                        : 'Для выбранной пары пока нет результатов анализа изменений.' }}</p>
                    <div v-if="visible.length" class="pc-table-scroll" tabindex="0" aria-label="Таблица изменений">
                        <table class="pc-table" :class="{'pc-table--expert': expertMode && expertAvailable}">
                            <colgroup><col class="pc-col-id">
                                <col class="pc-col-summary"><col class="pc-col-old"><col class="pc-col-new"><col class="pc-col-source"><col class="pc-col-status">
                                <template v-if="expertMode && expertAvailable"><col class="pc-col-decision"><col class="pc-col-reason"></template><col class="pc-col-action"></colgroup>
                            <thead><tr><th scope="col">ID</th><th scope="col">Изменение</th>
                                <th scope="col">Было</th><th scope="col">Стало</th><th scope="col">Источник</th><th scope="col">Статус</th>
                                <template v-if="expertMode && expertAvailable"><th scope="col">Решение</th><th scope="col">Причина / комментарий</th></template>
                                <th scope="col"><span class="pc-sr-only">Подробности</span></th></tr></thead>
                            <tbody><template v-for="c in visible" :key="c.id">
                                <tr class="pc-row" :id="'pc-' + c.id" :data-production-target-id="c.id" :data-status="c.status"
                                    :class="{'is-expanded': expandedId === c.id}" @click="toggle(c)">
                                    <td class="pc-row-id" :title="c.id">{{ c.display_id }}</td>
                                    <td class="pc-row-summary"><span class="pc-cell-text" :title="c.summary_ru">{{ c.summary_ru }}</span>
                                        <span v-if="c.pair_ids.length > 1" class="pc-cell-secondary">Несколько пар: {{ c.pair_ids.length }}</span>
                                        <span v-if="c.pair_binding_error" class="pc-binding-warning">Привязка не установлена</span></td>
                                    <td><span class="pc-cell-text" :title="c.old_state || 'Не установлено'">{{ c.old_state || 'Не установлено' }}</span></td>
                                    <td><span class="pc-cell-text" :title="c.new_state || 'Не установлено'">{{ c.new_state || 'Не установлено' }}</span></td>
                                    <td><div class="pc-row-sources"><span v-for="s in presentSources(c)" :key="s" class="pc-source is-present">{{ s }}</span></div></td>
                                    <td><span class="pc-status" :class="'pc-status--' + c.status.toLowerCase()">{{ statusLabel(c) }}</span></td>
                                    <template v-if="expertMode && expertAvailable">
                                        <td @click.stop><div v-if="c.expert_review_available" class="decision-toggle">
                                            <button v-for="d in ['accepted','rejected']" :key="d" class="btn-decision" :class="[d === 'accepted' ? 'btn-accept' : 'btn-reject', {active:expertValue(c).decision === d}]"
                                                :disabled="expertSaving" :aria-pressed="String(expertValue(c).decision === d)"
                                                :aria-label="(d === 'accepted' ? 'Принято: ' : 'Не принято: ') + c.display_id"
                                                :title="d === 'accepted' ? 'Принято' : 'Не принято'" @click="setExpertDecision(c,d)">
                                                <span class="decision-glyph">{{ d === 'accepted' ? '✓' : '✕' }}</span></button>
                                        </div><small v-else>Только просмотр</small><small v-if="expertDirty(c)" class="pc-cell-secondary">Не сохранено</small></td>
                                        <td @click.stop><textarea v-if="c.expert_review_available && expertValue(c).decision" class="pc-expert-reason"
                                            :value="expertValue(c).reason" @input="setExpertReason(c,$event.target.value)" :disabled="expertSaving"
                                            :required="expertValue(c).decision === 'rejected'" :aria-label="'Причина / комментарий: ' + c.display_id"
                                            :placeholder="expertValue(c).decision === 'rejected' ? 'Причина отказа (обязательно)' : 'Комментарий (необязательно)'" rows="2" maxlength="4000"></textarea></td>
                                    </template>
                                    <td><button class="pc-expand" :aria-label="(expandedId === c.id ? 'Свернуть ' : 'Раскрыть ') + c.display_id"
                                        :aria-expanded="expandedId === c.id" :aria-controls="'pc-detail-' + c.id" @click.stop="toggle(c)">{{ expandedId === c.id ? '−' : '+' }}</button></td>
                                </tr>
                                <tr v-if="expandedId === c.id" class="pc-expanded-row" :id="'pc-detail-' + c.id">
                                    <td :colspan="expertMode && expertAvailable ? 9 : 7">
                        <article class="pc-card" :aria-label="c.display_id + ': подробности'">
                            <header class="pc-card-head"><div><h3>{{ c.summary_ru }}</h3>
                                <p class="pc-meta">{{ c.cipher || 'Шифр не указан' }} · {{ c.discipline }}
                                    <span v-if="c.engineering_system"> · {{ c.engineering_system }}</span></p></div>
                                <span class="pc-status" :class="'pc-status--' + c.status.toLowerCase()">{{ statusLabel(c) }}</span>
                            </header>
                            <div class="pc-card-context"><span>{{ types[c.change_type] }}</span>
                                <span v-if="c.engineering_subject">Объект: {{ c.engineering_subject }}</span>
                                <b v-if="c.importance === 'HIGH'">Высокая важность</b></div>
                            <div class="pc-states"><div><small>Было</small><p>{{ c.old_state || 'Состояние не установлено' }}</p></div>
                                <div><small>Стало</small><p>{{ c.new_state || 'Состояние не установлено' }}</p></div></div>
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
                                            <span>{{ e.page_view_url ? 'Лист с выделением ↗' : e.crop_precision === 'PAGE_LEVEL' ? 'Открыть страницу ↗' : 'Увеличить ↗' }}</span></button>
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
                            <p v-if="c.expert_review?.decision" class="pc-expert-saved">{{ statusLabel(c) }} · {{ c.expert_review.actor }} · {{ c.expert_review.timestamp }}
                                <span v-if="c.expert_review.reason"> · {{ c.expert_review.reason }}</span></p>
                            <footer v-if="!report && !c.expert_review_available" class="pc-actions" :aria-label="'Решение: ' + c.summary_ru">
                                <button v-for="s in ['CONFIRMED','REJECTED','UNDETERMINED','PROBLEM']" :key="s" class="btn btn-sm btn-secondary"
                                    :class="{'pc-selected': c.status === s}" :aria-pressed="String(c.status === s)"
                                    :disabled="readonly || saving || (!demo && !persistent) || (s === 'CONFIRMED' && c.conflicts.some(x => !x.resolved))"
                                    :title="readonly ? 'Опубликованные результаты доступны только для чтения' : !demo && !persistent ? 'Сохранение решений пока недоступно' : s === 'CONFIRMED' && c.conflicts.some(x => !x.resolved) ? 'Сначала нужно разрешить конфликт источников' : ''"
                                    @click="$emit('decision', {id:c.id, status:s, comment:comments[c.id] || ''})">{{ s === 'CONFIRMED' ? 'Подтвердить' : statuses[s] }}</button>
                                <small v-if="c.local_decision" role="status">Демо-решение сохранено в этой вкладке</small>
                                <small v-if="persistent && c.effective_decision" role="status">Решение сохранено · {{ c.effective_decision.actor }} · {{ c.effective_decision.timestamp }}</small>
                            </footer>
                        </article>
                                    </td>
                                </tr>
                            </template></tbody>
                        </table>
                    </div>
                    <dialog ref="imageDialog" class="pc-image-dialog" @close="imageClosed" @click="$event.target === imageDialog && imageDialog.close()">
                        <template v-if="selectedImage"><header><strong>{{ selectedImage.side }} · {{ selectedImage.document.label }} · стр. {{ selectedImage.page }}</strong>
                            <button class="btn btn-sm" @click="imageDialog.close()" autofocus>Закрыть</button></header>
                            <p v-if="pageLoading" role="status">Поиск фрагмента на листе…</p>
                            <template v-else-if="pageView && !pageImageFailed">
                                <svg class="pc-evidence-page" :viewBox="'0 0 ' + pageView.page_width + ' ' + pageView.page_height"
                                    role="img" :aria-label="pageView.message" :key="selectedImage.id">
                                    <image :href="pageView.image_url" x="0" y="0" :width="pageView.page_width" :height="pageView.page_height"
                                        @error="pageImageFailed = true" />
                                    <rect v-for="(box, index) in pageView.highlights" :key="index" v-bind="pageBoxStyle(box)"
                                        class="pc-source-highlight" :class="{'pc-source-highlight--block': pageView.kind === 'SOURCE_BLOCK'}" />
                                </svg>
                                <p class="pc-location-message" role="status">{{ pageView.message }}</p>
                            </template>
                            <template v-else>
                                <p class="pc-location-message" role="status">{{ pageError || (pageImageFailed ? 'Не удалось загрузить лист. Показан сохранённый фрагмент.' : 'Для этого результата доступен сохранённый фрагмент без точного выделения цитаты.') }}</p>
                                <img :src="selectedImage.image_url" :alt="selectedImage.short_explanation_ru">
                            </template>
                            <p>{{ selectedImage.short_explanation_ru }}</p>
                            <blockquote v-if="selectedImage.quote" class="pc-image-quote"><strong>Цитата, на которую ссылается программа:</strong>{{ selectedImage.quote }}</blockquote>
                        </template>
                    </dialog>
                </section>`,
            });
        },
    };
}(typeof globalThis !== 'undefined' ? globalThis : this));


/* Human Mapping production entry: always an explicit object + comparison. */
(function () {
  if (typeof window === 'undefined') return;
  window.openHumanMapping = function (objectId, pairId) {
    if (!objectId || !pairId) return;
    window.open('/human-mapping/?object=' + encodeURIComponent(objectId)
      + '&comparison=' + encodeURIComponent(pairId), '_blank');
  };
})();
