(function (root) {
    'use strict';
    const labels = {uploading: 'Загрузка', queued: 'Ожидает разбора', detecting: 'Разбор PDF',
        review: 'Требуется проверка', ready: 'Ожидает обработки', preparing: 'Подготовка версий',
        comparing: 'Сравнение версий', done: 'Готово', failed: 'Ошибка', skipped: 'Исключён'};
    const id = prefix => prefix + root.crypto.randomUUID().replace(/-/g, '');
    const natural = (a, b) => a.localeCompare(b, 'ru', {numeric: true});

    function groupFiles(entries, parent = false) {
        const groups = new Map();
        for (const entry of entries) {
            const file = entry.file || entry;
            const path = entry.path || file.webkitRelativePath || '';
            const parts = path.split('/');
            const depth = parent ? 2 : 1;
            if (parts.length <= depth || !/\.pdf$/i.test(file.name)) continue;
            const rootName = parts[depth - 1];
            const key = parts.slice(0, depth).join('/');
            if (!groups.has(key)) groups.set(key, {source_root: rootName, title: rootName, selected: true, files: []});
            groups.get(key).files.push({file, relative_path: parts.slice(depth).join('/'),
                size: file.size, last_modified: file.lastModified || 0});
        }
        for (const group of groups.values()) group.files.sort((a, b) => natural(a.relative_path, b.relative_path));
        return [...groups.values()].sort((a,b)=>natural(a.source_root,b.source_root));
    }

    function fingerprint(group) {
        return JSON.stringify([group.source_root, group.files.map(f => [f.relative_path, f.size, f.last_modified || 0])
            .sort((a, b) => natural(a[0], b[0]))]);
    }

    async function readEntry(entry, prefix = '') {
        const path = prefix + entry.name;
        if (entry.isFile) {
            const file = await new Promise((resolve, reject) => entry.file(resolve, reject));
            return [{file, path}];
        }
        if (!entry.isDirectory) return [];
        const reader = entry.createReader();
        const files = [];
        for (;;) {
            const chunk = await new Promise((resolve, reject) => reader.readEntries(resolve, reject));
            if (!chunk.length) break;
            for (const child of chunk) files.push(...await readEntry(child, path + '/'));
        }
        return files;
    }

    const template = `
<div class="vc-batch">
  <div class="vc-batch-bar">
    <button class="btn btn-outline" @click="openNew" :disabled="busy">Загрузить несколько проектов</button>
    <label v-if="saved.length">Пакеты
      <select v-model="selectedId" :disabled="busy" @change="openSaved">
        <option value="">Выберите пакет…</option>
        <option v-for="b in saved" :key="b.batch_id" :value="b.batch_id">{{ batchLabel(b) }}</option>
      </select>
    </label>
    <span v-if="current">{{ batchLabel(current) }}</span>
    <button v-if="current" class="btn btn-outline btn-sm" @click="open=true">Показать очередь</button>
    <button v-if="busy && current && !current.started" class="btn btn-outline btn-sm" @click="stopUpload">Остановить загрузку</button>
    <button v-if="current && !current.started && !busy" class="btn btn-outline btn-sm" @click="removeBatch">Удалить пакет</button>
  </div>
  <div v-if="open" class="modal-overlay" @click.self="close">
    <div class="modal vc-modal vc-batch-modal" role="dialog" aria-modal="true" aria-label="Загрузка нескольких проектов">
      <div class="vc-modal-head"><h3>Загрузка нескольких проектов</h3><button class="btn btn-outline btn-sm" @click="close" aria-label="Закрыть">✕</button></div>
      <p class="vc-muted">Одна папка — один проект. Все подпапки и PDF относятся к этому проекту.</p>
      <div v-if="error" class="vc-error" role="alert">{{ error }}</div>
      <div v-if="notice" class="vc-warning" role="status">{{ notice }}</div>
      <template v-if="!current || !current.started">
        <div class="vc-batch-drop" @dragover.prevent @drop.prevent="drop">
          <b>Перетащите сюда папки проектов, выделенные через Ctrl в Проводнике</b>
          <p class="vc-muted">Или добавьте папки по одной / выберите общую папку с проектами.</p>
          <div class="vc-upload-pick">
            <label class="btn btn-outline">Добавить папку<input type="file" webkitdirectory directory multiple hidden :disabled="busy || reading" @change="pick($event, false)"></label>
            <label class="btn btn-outline">Общая папка с проектами<input type="file" webkitdirectory directory multiple hidden :disabled="busy || reading" @change="pick($event, true)"></label>
          </div>
        </div>
        <p v-if="current" class="vc-muted">Для продолжения выберите те же исходные папки. Уже принятые файлы сохраняются на сервере.</p>
        <p v-if="reading">Чтение списка файлов…</p>
        <table v-if="!current && groups.length" class="vc-files vc-batch-list">
          <thead><tr><th>Выбрать</th><th>Проект</th><th>PDF</th><th>Размер</th><th>Порядок</th></tr></thead>
          <tbody><tr v-for="(g, i) in groups" :key="g.item_id">
            <td><input type="checkbox" v-model="g.selected" :disabled="locked"></td>
            <td><input class="vc-input" v-model="g.title" :disabled="locked" aria-label="Название проекта"><div class="vc-muted">{{g.source_root}}</div></td>
            <td>{{g.files.length}}</td><td>{{mb(g.files.reduce((n,f)=>n+f.size,0))}}</td>
            <td><button class="btn btn-outline btn-xs" @click="move(groups,i,-1)" :disabled="locked || !i" aria-label="Проект выше">↑</button>
                <button class="btn btn-outline btn-xs" @click="move(groups,i,1)" :disabled="locked || i===groups.length-1" aria-label="Проект ниже">↓</button>
                <button class="btn btn-outline btn-xs" @click="groups.splice(i,1)" :disabled="locked" aria-label="Убрать проект">×</button></td>
          </tr></tbody>
        </table>
        <label v-if="!current" class="vc-batch-option"><input type="checkbox" v-model="compare" :disabled="locked"> После подготовки сравнить все соседние версии</label>
        <p v-if="!current && groups.length">Выбрано проектов: {{groups.filter(g=>g.selected).length}} · {{mb(totalSelected())}}</p>
        <div v-if="busy || sent" class="vc-progress wide"><span :style="{width: uploadPercent()+'%'}"></span></div>
        <p v-if="message" role="status">{{message}}</p>
        <p class="vc-muted">До завершения передачи файлов оставьте вкладку открытой. После запуска очереди обработка продолжится на сервере.</p>
        <button class="btn btn-primary" :disabled="busy || reading || (!current && !groups.some(g=>g.selected))" @click="send">{{busy ? 'Загрузка…' : current ? 'Продолжить загрузку' : 'Загрузить и поставить в очередь'}}</button>
        <button v-if="busy" class="btn btn-outline" @click="stopUpload">Остановить загрузку</button>
        <button v-if="current && !busy" class="btn btn-outline" @click="removeBatch">Удалить пакет</button>
      </template>
      <template v-if="current">
        <div v-if="current.started" class="vc-batch-bar">
          <b>{{batchLabel(current)}}</b>
          <button class="btn btn-outline btn-sm" :disabled="busy" @click="pause">{{current.paused ? 'Продолжить очередь' : 'Пауза после текущего проекта'}}</button>
          <button v-if="current.items.some(i=>i.state==='review')" class="btn btn-primary btn-sm" :disabled="busy" @click="confirmAll">Подтвердить порядок во всех проектах</button>
        </div>
        <p v-if="current.paused" class="vc-warning">Очередь на паузе. Текущий проект завершит свой этап.</p>
        <div v-for="item in current.items" :key="item.item_id" class="vc-batch-item">
          <div class="vc-batch-bar"><b>{{item.title}}</b><span :class="{'vc-error':item.state==='failed'}">{{labels[item.state] || item.state}}</span>
            <span class="vc-muted">{{fileProgress(item)}}</span>
            <button v-if="['queued','review','ready','failed'].includes(item.state)" class="btn btn-outline btn-xs" :disabled="busy" @click="act(item,'skip')">Исключить</button>
            <button v-if="item.state==='failed'" class="btn btn-outline btn-xs" :disabled="busy" @click="act(item,'retry')">Повторить</button>
          </div>
          <p v-if="item.error" class="vc-error">{{item.error}}</p>
          <p v-if="item.waiting_reason && !['done','failed','skipped'].includes(item.state)" class="vc-warning">{{item.waiting_reason}}</p>
          <p v-for="w in item.warnings || []" :key="w" class="vc-warning">{{w}}</p>
          <details v-if="item.state==='review'" :open="item.item_id === current.items.find(i=>i.state==='review')?.item_id">
            <summary>Проверьте состав и порядок версий (сверху вниз)</summary>
            <p class="vc-muted">Номер в имени, «Изм.» и дата выпуска — подсказки. Порядок задаёте вы.</p>
            <table class="vc-files vc-batch-list"><thead><tr><th>PDF / подпапка</th><th>Метка версии</th><th>Подсказки</th><th>Порядок</th></tr></thead>
              <tbody><tr v-for="(f,i) in drafts[item.item_id] || []" :key="f.file_id">
                <td>{{f.relative_path}}</td><td><input class="vc-input" v-model="f.label" :disabled="busy" aria-label="Метка версии"></td>
                <td>{{hint(f)}}</td><td><button class="btn btn-outline btn-xs" :disabled="busy || !i" @click="move(drafts[item.item_id],i,-1)" aria-label="Версия выше">↑</button>
                <button class="btn btn-outline btn-xs" :disabled="busy || i===item.files.length-1" @click="move(drafts[item.item_id],i,1)" aria-label="Версия ниже">↓</button></td>
              </tr></tbody>
            </table>
            <button class="btn btn-primary btn-sm" :disabled="busy" @click="confirmOne(item)">Подтвердить состав и порядок</button>
          </details>
        </div>
      </template>
    </div>
  </div>
</div>`;

    root.VersionComparisonBatch = {groupFiles, fingerprint, readEntry, register(app) {
        app.component('vc-batch-upload', {template, props: {objectId: {type: String, required: true}}, emits: ['updated'],
            setup(props, {emit}) {
                const {ref, reactive, onMounted, onBeforeUnmount} = root.Vue;
                const open = ref(false), busy = ref(false), reading = ref(false), locked = ref(false);
                const groups = ref([]), current = ref(null), saved = ref([]), selectedId = ref('');
                const error = ref(''), notice = ref(''), compare = ref(false), message = ref(''), sent = ref(0);
                const drafts = reactive({});
                const files = new Map();
                let manifest = null, timer = null, alive = true, xhr = null, published = new Set();
                let cancelled = false;
                const base = `/objects/${encodeURIComponent(props.objectId)}/batches`;
                const api = (path, options) => root.VersionComparison.api(base + path, options);
                const mb = n => (n / 1048576).toFixed(1) + ' МБ';
                const totalSelected = () => groups.value.filter(g => g.selected).reduce((n,g) => n+g.files.reduce((s,f)=>s+f.size,0),0);
                const total = () => current.value ? current.value.items.reduce((n,i)=>n+i.files.reduce((s,f)=>s+f.size,0),0) : totalSelected();
                const uploadPercent = () => total() ? Math.min(100,Math.round(sent.value*100/total())) : 0;
                const batchLabel = b => `${b.created_at.slice(0,16).replace('T',' ')} · готово ${b.items.filter(i=>i.state==='done').length} из ${b.items.length}`;
                const fileProgress = i => i.compare_progress ? `переходов: ${i.compare_progress.done} из ${i.compare_progress.total}` :
                    i.state === 'uploading' ? `${i.files.filter(f=>f.received).length} из ${i.files.length} PDF загружено` :
                    i.state === 'preparing' ? `${i.files.filter(f=>f.profiled).length} из ${i.files.length} PDF подготовлено` : `${i.files.length} PDF`;
                function hint(f) {
                    const d=f.detected || {}, parts=[];
                    if (d.file_number != null) parts.push('№ '+d.file_number);
                    if (d.izm_max != null) parts.push('Изм. '+d.izm_max);
                    if (d.release_dates?.length) parts.push(d.release_dates.join(', '));
                    if (d.page_count) parts.push(d.page_count+' стр.');
                    return parts.join(' · ') || '—';
                }
                function adopt(batch) {
                    current.value=batch; selectedId.value=batch.batch_id;
                    for(const item of batch.items) if(item.state==='review' && !drafts[item.item_id])
                        drafts[item.item_id]=item.files.map(f=>({...f,label:f.label || f.suggested_label || f.file_name}));
                }
                async function refresh() {
                    try {
                        const data=await api('');
                        if(!alive) return;
                        saved.value=data.batches || [];
                        if(current.value) {
                            const b=saved.value.find(b=>b.batch_id===current.value.batch_id);
                            // Keep the object used by an ongoing transfer stable.
                            if(b && !busy.value) adopt(b);
                            if(!b && !busy.value) reset();
                        }
                        const next=new Set(saved.value.flatMap(b=>b.items.filter(i=>i.published).map(i=>i.project_id)));
                        if([...next].some(p=>!published.has(p))) emit('updated');
                        published=next;
                    } catch(e) { if(open.value) error.value='Не удалось обновить очередь: '+e.message; }
                    finally { if(alive) timer=setTimeout(refresh,2500); }
                }
                function openNew() {
                    if(busy.value) return;
                    if(current.value) reset();
                    open.value=true; error.value='';
                }
                function reset() {
                    current.value=null; selectedId.value=''; groups.value=[]; manifest=null;
                    locked.value=false; files.clear(); sent.value=0; message.value=''; notice.value='';
                    for(const key of Object.keys(drafts)) delete drafts[key];
                }
                function stopUpload() {cancelled=true; if(xhr) xhr.abort();}
                async function removeBatch() {
                    if(busy.value || !current.value || current.value.started) return;
                    if(!root.confirm('Удалить этот пакет из списка загрузок? Его можно будет отправить заново.')) return;
                    busy.value=true; error.value='';
                    const bid=current.value.batch_id;
                    try {
                        await api('/'+bid,{method:'DELETE'});
                        saved.value=saved.value.filter(b=>b.batch_id!==bid);
                        reset(); open.value=false;
                    } catch(e) {error.value=e.message; open.value=true;}
                    finally {busy.value=false;}
                }
                function openSaved() {
                    if(busy.value) return;
                    const b=saved.value.find(b=>b.batch_id===selectedId.value);
                    if(b) {adopt(b); open.value=true; manifest=null; files.clear(); groups.value=[]; error.value=''; notice.value=''; sent.value=0;}
                }
                function close() {open.value=false;}
                function move(list,index,delta) {const next=index+delta; if(next<0 || next>=list.length) return; list.splice(next,0,list.splice(index,1)[0]);}
                function append(incoming) {
                    if(current.value && !current.value.started) {
                        let matched=0;
                        for(const group of incoming) {
                            const matches=current.value.items.filter(i=>fingerprint(i)===fingerprint(group));
                            if(matches.length!==1) {notice.value='Некоторые папки не совпадают с исходным списком. Проверьте названия, размер и даты файлов.'; continue;}
                            const item=matches[0];
                            for(const file of group.files) {
                                const target=item.files.find(f=>f.relative_path===file.relative_path);
                                files.set(item.item_id+'/'+target.file_id,file.file);
                            }
                            matched++;
                        }
                        message.value=`Подключено папок для продолжения: ${matched}`;
                        return;
                    }
                    if(locked.value) return;
                    let added=0;
                    for(const group of incoming) {
                        if(groups.value.some(g=>fingerprint(g)===fingerprint(group))) continue;
                        group.item_id=id('i'); group.files.forEach(f=>f.file_id=id('f')); groups.value.push(group); added++;
                    }
                    notice.value=added ? '' : 'Новых папок с PDF не найдено. Пустые папки и другие типы файлов пропускаются.';
                }
                async function pick(event,parent) {
                    if(busy.value || reading.value) return;
                    reading.value=true;
                    try {append(groupFiles([...event.target.files],parent));} catch(e){error.value=e.message;}
                    finally {reading.value=false; event.target.value='';}
                }
                async function drop(event) {
                    if(busy.value || reading.value) return;
                    // Capture the entries synchronously while the drop data store is readable.
                    const entries=[...event.dataTransfer.items].map(i=>i.webkitGetAsEntry?.()).filter(e=>e?.isDirectory);
                    reading.value=true;
                    try {for(const entry of entries) append(groupFiles(await readEntry(entry))); if(!entries.length) notice.value='Перетащите именно папки проектов.';}
                    catch(e){error.value='Не удалось прочитать папку: '+e.message;}
                    finally {reading.value=false;}
                }
                function transfer(path,file,onProgress) {
                    return new Promise((resolve,reject)=>{
                        const request=new XMLHttpRequest(); xhr=request;
                        let timer, settled=false;
                        const finish=(error,result)=>{
                            if(settled) return; settled=true; clearTimeout(timer);
                            if(xhr===request) xhr=null;
                            if(error) reject(error); else resolve(result);
                        };
                        const activity=()=>{clearTimeout(timer);timer=setTimeout(()=>{
                            finish(new Error('Передача не отвечает более двух минут. Принятые части сохранены; нажмите «Продолжить загрузку».'));
                            request.abort();
                        },120000);};
                        request.open('POST',root.VersionComparison.API+base+path);
                        request.upload.onprogress=e=>{activity();if(e.lengthComputable) onProgress(Math.min(file.size,e.loaded));};
                        request.upload.onload=activity;
                        request.onerror=()=>finish(new Error('Передача прервана. Нажмите «Продолжить загрузку».'));
                        request.onabort=()=>finish(new Error('Загрузка остановлена. Принятые части сохранены; можно продолжить или удалить пакет.'));
                        request.onload=()=>{let result; try{result=JSON.parse(request.responseText);}catch(e){result={};}
                            if(request.status>=200 && request.status<300) finish(null,result);
                            else finish(new Error(result.detail || 'Ошибка загрузки '+request.status));};
                        const form=new FormData(); form.append('file',file,'part.pdf');
                        activity();
                        try {request.send(form);} catch(e) {finish(e);}
                    });
                }
                async function send() {
                    if(busy.value || reading.value) return;
                    busy.value=true; locked.value=true; cancelled=false; error.value=''; notice.value='';
                    try {
                        if(!current.value) {
                            if(!manifest) {
                                const chosen=groups.value.filter(g=>g.selected);
                                if(!chosen.length || chosen.some(g=>!g.title.trim())) throw new Error('Выберите проекты и укажите названия.');
                                manifest={batch_id:id('b'),compare_neighbors:compare.value,items:chosen.map(g=>({item_id:g.item_id,title:g.title.trim(),source_root:g.source_root,
                                    files:g.files.map(f=>({file_id:f.file_id,relative_path:f.relative_path,size:f.size,last_modified:f.last_modified}))}))};
                                for(const g of chosen) for(const f of g.files) files.set(g.item_id+'/'+f.file_id,f.file);
                            }
                            adopt(await api('',{method:'POST',body:manifest}));
                        } else adopt(await api('/'+current.value.batch_id));
                        const b=current.value;
                        if(b.started) return;
                        const checkStopped=()=>{if(cancelled || !alive) throw new Error('Загрузка остановлена. Можно продолжить или удалить пакет.');};
                        checkStopped();
                        const chunkSize=b.chunk_size || 8*1024*1024;
                        let completed=b.items.reduce((s,i)=>s+i.files.filter(f=>f.received).reduce((n,f)=>n+f.size,0),0);
                        sent.value=completed;
                        for(const item of b.items) for(const file of item.files) {
                            if(file.received) continue;
                            checkStopped();
                            const local=files.get(item.item_id+'/'+file.file_id);
                            if(!local) throw new Error('Повторно выберите исходную папку «'+item.source_root+'» для продолжения.');
                            const count=Math.ceil(file.size/chunkSize);
                            for(let index=0; index<count && !file.received; index++) {
                                checkStopped();
                                if(file.chunks?.[index] && index<count-1) continue;
                                const known=Object.entries(file.chunks || {}).reduce((n,[key,c])=>n+(Number(key)===index ? 0 : c.size),0);
                                message.value=item.title+' · '+file.relative_path+' · часть '+(index+1)+' из '+count;
                                const result=await transfer('/'+b.batch_id+'/items/'+item.item_id+'/files/'+file.file_id+'/chunks/'+index,
                                    local.slice(index*chunkSize,Math.min(file.size,(index+1)*chunkSize)),n=>sent.value=completed+known+n);
                                Object.assign(file,result);
                            }
                            if(!file.received) throw new Error('Файл ещё не собран на сервере. Нажмите «Продолжить загрузку».');
                            completed+=file.size; sent.value=completed;
                        }
                        checkStopped();
                        adopt(await api('/'+b.batch_id+'/start',{method:'POST'}));
                        message.value='Все файлы переданы. Очередь сохранена на сервере.';
                        files.clear(); groups.value=[];
                    } catch(e){if(alive) error.value=e.message;
                        if(!current.value && [400,422].includes(e.status)) manifest=null;
                        if(!manifest) locked.value=false;}
                    finally {busy.value=false; xhr=null;}
                }
                async function command(path,options) {
                    if(busy.value) return;
                    busy.value=true; error.value='';
                    try {adopt(await api('/'+current.value.batch_id+path,options));} catch(e){error.value=e.message;} finally {busy.value=false;}
                }
                const pause=()=>command('',{method:'PATCH',body:{paused:!current.value.paused}});
                const act=(item,commandName)=>command('/items/'+item.item_id+'/action',{method:'POST',body:{command:commandName}});
                const choices=item=>(drafts[item.item_id] || []).map(f=>({file_id:f.file_id,label:f.label}));
                const confirmOne=item=>command('/items/'+item.item_id+'/confirm',{method:'POST',body:{files:choices(item)}});
                async function confirmAll() {
                    const items=current.value.items.filter(i=>i.state==='review');
                    for(const item of items) {await confirmOne(item); if(error.value) break;}
                }
                function beforeUnload(event) {if(busy.value && xhr){event.preventDefault(); event.returnValue='';}}
                onMounted(()=>{refresh(); root.addEventListener('beforeunload',beforeUnload);});
                onBeforeUnmount(()=>{alive=false; clearTimeout(timer); if(xhr) xhr.abort(); root.removeEventListener('beforeunload',beforeUnload);});
                return {open,busy,reading,locked,groups,current,saved,selectedId,error,notice,compare,message,sent,drafts,labels,
                    mb,totalSelected,uploadPercent,batchLabel,fileProgress,hint,openNew,openSaved,close,move,pick,drop,send,stopUpload,removeBatch,pause,act,confirmOne,confirmAll};
            }
        });
    }};
})(typeof window !== 'undefined' ? window : globalThis);
