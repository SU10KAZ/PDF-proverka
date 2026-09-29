(function (root) {
    'use strict';

    function capabilities() {
        const node = root.document && root.document.getElementById('project-assemblies-caps');
        try { return JSON.parse(node ? node.textContent : '{}'); }
        catch (_) { return {}; }
    }

    function register(app) {
        const {ref, computed, watch} = Vue;
        app.component('project-assemblies', {
            props: {objectId: {type: String, default: ''}},
            emits: ['attached'],
            setup(props, {emit}) {
                const sources = ref([]), assemblies = ref([]), selected = ref([]);
                const name = ref(''), section = ref('OTHER'), compositionCompleteness = ref('UNKNOWN');
                const loading = ref(false), error = ref('');
                const preview = ref(null), created = ref(null);
                const enabled = computed(() => capabilities().enabled === true);
                const selectedRows = computed(() => selected.value.map(id => sources.value.find(row => row.source_ref === id)).filter(Boolean));
                const ready = row => Object.values(row.readiness || {}).every(Boolean);
                const message = data => typeof data?.detail === 'string' ? data.detail
                    : data?.detail?.message || data?.message || 'Операция не выполнена';
                async function api(url, options) {
                    const response = await fetch(url, options);
                    const data = await response.json().catch(() => ({}));
                    if (!response.ok) throw new Error(message(data));
                    return data;
                }
                async function load() {
                    if (!enabled.value || !props.objectId) return;
                    loading.value = true; error.value = '';
                    try {
                        const query = '?object_id=' + encodeURIComponent(props.objectId);
                        const [sourceData, assemblyData] = await Promise.all([
                            api('/api/project-assemblies/sources' + query), api('/api/project-assemblies' + query),
                        ]);
                        sources.value = sourceData.items || [];
                        assemblies.value = assemblyData.items || [];
                    } catch (e) { error.value = String(e.message || e); }
                    finally { loading.value = false; }
                }
                function toggle(row) {
                    if (!ready(row)) return;
                    const index = selected.value.indexOf(row.source_ref);
                    if (index >= 0) selected.value.splice(index, 1); else selected.value.push(row.source_ref);
                    preview.value = null;
                }
                function move(index, delta) {
                    const target = index + delta;
                    if (target < 0 || target >= selected.value.length) return;
                    const copy = selected.value.slice();
                    [copy[index], copy[target]] = [copy[target], copy[index]];
                    selected.value = copy; preview.value = null;
                }
                async function runPreview() {
                    error.value = ''; preview.value = null;
                    try { preview.value = await api('/api/project-assemblies/preview', {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({object_id: props.objectId, source_refs: selected.value}),
                    }); } catch (e) { error.value = String(e.message || e); }
                }
                async function create() {
                    if (!name.value.trim() || !selected.value.length) return;
                    loading.value = true; error.value = '';
                    try {
                        created.value = await api('/api/project-assemblies', {
                            method: 'POST', headers: {'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID()},
                            body: JSON.stringify({object_id: props.objectId, name: name.value, section: section.value, composition_completeness: compositionCompleteness.value, source_refs: selected.value}),
                        });
                        selected.value = []; preview.value = null; name.value = '';
                        await load();
                    } catch (e) { error.value = String(e.message || e); }
                    finally { loading.value = false; }
                }
                async function attach(row, side) {
                    error.value = '';
                    try {
                        await api(`/api/project-assemblies/${encodeURIComponent(row.assembly_id)}/versions/${encodeURIComponent(row.current_version)}/attachment`, {
                            method: 'POST', headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({object_id: props.objectId, side}),
                        });
                        emit('attached', {side});
                    } catch (e) { error.value = String(e.message || e); }
                }
                async function jobAction(row, action) {
                    error.value = '';
                    try {
                        await api(`/api/project-assemblies/${encodeURIComponent(row.assembly_id)}/versions/${encodeURIComponent(row.current_version)}/${action}?object_id=${encodeURIComponent(props.objectId)}`, {method: 'POST'});
                        await load();
                    } catch (e) { error.value = String(e.message || e); }
                }
                watch(() => props.objectId, load, {immediate: true});
                return {enabled, sources, assemblies, selected, selectedRows, name, section, loading, error,
                    compositionCompleteness, preview, created, ready, load, toggle, move, runPreview, create, attach, jobAction};
            },
            template: `
              <section class="card" style="padding:18px" aria-label="Сборки проектов">
                <header style="display:flex;justify-content:space-between;gap:16px;align-items:center">
                  <div><h3 style="margin:0">Сборки проектов</h3><small>Объединение готовых OCR-комплектов для одного сравнения</small></div>
                  <button class="btn btn-sm" @click="load" :disabled="loading">Обновить</button>
                </header>
                <p v-if="error" class="alert alert-error">{{ error }}</p>
                <div v-if="!objectId" class="empty-state">Выберите объект.</div>
                <template v-else>
                  <div style="display:grid;grid-template-columns:minmax(0,1fr) minmax(320px,.8fr);gap:18px;margin-top:16px">
                    <div>
                      <h4>1. Источники и версии</h4>
                      <label v-for="row in sources" :key="row.source_ref" style="display:flex;gap:9px;padding:7px;border-bottom:1px solid var(--border-color,#ddd)">
                        <input type="checkbox" :checked="selected.includes(row.source_ref)" :disabled="!ready(row)" @change="toggle(row)">
                        <span><b>{{ row.document_code }}</b> · {{ row.version_id }} · {{ row.discipline || row.stage }}
                          <small v-if="!ready(row)" style="display:block;color:#b45309">Нужна совместимая выгрузка OCR: PDF, MD, blocks.json и HTML</small>
                        </span>
                      </label>
                    </div>
                    <div>
                      <h4>2. Порядок и название</h4>
                      <div v-for="(row,index) in selectedRows" :key="row.source_ref" style="display:flex;gap:6px;align-items:center;margin:5px 0">
                        <button class="btn btn-sm" @click="move(index,-1)" :disabled="index===0">↑</button>
                        <button class="btn btn-sm" @click="move(index,1)" :disabled="index===selectedRows.length-1">↓</button>
                        <span>{{ index+1 }}. {{ row.document_code }} · {{ row.version_id }}</span>
                      </div>
                      <input v-model="name" class="form-input" placeholder="Название сборного проекта" style="width:100%;margin-top:10px">
                      <input v-model="section" class="form-input" placeholder="Раздел" style="width:100%;margin-top:8px">
                      <label style="display:block;margin-top:8px">Полнота состава относительно OLD
                        <select v-model="compositionCompleteness" class="form-input" style="width:100%"><option value="UNKNOWN">Неизвестно</option><option value="COMPLETE">Полный состав</option><option value="INCOMPLETE">Неполный состав</option></select>
                      </label>
                      <div style="display:flex;gap:8px;margin-top:10px">
                        <button class="btn" @click="runPreview" :disabled="!selected.length || loading">Проверить состав</button>
                        <button class="btn btn-primary" @click="create" :disabled="!selected.length || !name.trim() || loading">Собрать проект</button>
                      </div>
                      <p v-if="preview">{{ preview.totals.page_count }} стр. · {{ preview.totals.block_count }} блоков · {{ preview.totals.graphic_count }} графических</p>
                    </div>
                  </div>
                  <h4 style="margin-top:22px">Готовые сборки</h4>
                  <div v-if="!assemblies.length" class="empty-state">Сборок пока нет.</div>
                  <div v-for="row in assemblies" :key="row.assembly_id" class="card" style="padding:12px;margin:8px 0;display:flex;justify-content:space-between;align-items:center;gap:12px">
                    <span><b>{{ row.name }}</b> · {{ row.current_version || '—' }} · {{ row.status }}<small v-if="row.totals" style="display:block">{{ row.totals.page_count }} стр., {{ row.totals.block_count }} блоков<span v-if="row.artifact_bytes"> · {{ (row.artifact_bytes/1048576).toFixed(1) }} МБ</span></small><small v-if="row.error" style="display:block;color:#b91c1c">{{ row.error }}</small></span>
                    <span style="display:flex;gap:6px"><button v-if="['FAILED','CANCELLED','INTERRUPTED'].includes(row.status)" class="btn btn-sm" @click="jobAction(row,'retry')">Повторить</button><button v-if="['PREPARING','RUNNING'].includes(row.status)" class="btn btn-sm" @click="jobAction(row,'cancel')">Отменить</button><button class="btn btn-sm" @click="attach(row,'stage_1')" :disabled="row.status!=='READY'">В OLD</button><button class="btn btn-primary btn-sm" @click="attach(row,'stage_2')" :disabled="row.status!=='READY'">В NEW</button></span>
                  </div>
                </template>
              </section>`,
        });
    }
    root.ProjectAssemblies = {register, capabilities};
})(globalThis);
