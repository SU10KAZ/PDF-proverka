(function (root) {
    'use strict';
    // Просмотр результата пары листов: наложение по тайлам, A, B, рядом;
    // рамки регионов отличий и переход между ними; каналы проверки и
    // служебные зоны. Координаты холста — пиксели полного разрешения
    // сравнения (result.canvas), уровень L — уменьшение в 2^L раз.
    const TILE = 512;
    // Сдвиг надписи и перенос блока — не изменение (решение 15.10), в списке скрыты.
    const SHIFT_CLASSES = ['text_moved', 'block_moved', 'shifted'];
    const CLASS_LABELS = {
        text: 'Текст', text_moved: 'Сдвиг надписи', added: 'Графика: добавлено (B)', removed: 'Графика: удалено (A)',
        changed: 'Графика: изменено', color: 'Изменение цвета', light: 'Светлая заливка / штриховка',
        style: 'Оформление линии: толщина, тон, наложение', block_moved: 'Блок перемещён', shifted: 'Сдвиг: тот же элемент на другом месте',
    };
    const LAYERS = [
        {key: 'main', label: 'Основное', names: ['ink_any', 'common', 'a_only', 'b_only'], on: true},
        {key: 'light', label: 'Светлые элементы', names: ['light_a', 'light_b'], on: true},
        {key: 'color', label: 'Цвет', names: ['color'], on: true},
        {key: 'style', label: 'Оформление линий', names: ['style'], on: true},
        {key: 'moved', label: 'Перемещено (сдвиг — не изменение)', names: ['moved'], on: false},
        {key: 'tol', label: 'В пределах допуска', names: ['within_tol'], on: false},
        {key: 'noise', label: 'Шум', names: ['noise'], on: false},
        {key: 'zones', label: 'Служебные зоны', names: ['zones'], on: true},
    ];

    const TEMPLATE = `
<div class="vcv-overlay" @keydown="onKey" tabindex="0" ref="rootEl">
  <div class="vcv-window">
    <div class="vcv-head">
      <div class="vcv-title">
        <b>{{ title }}</b>
        <span class="vc-chip vc-chip-a">A</span> {{ sideLabel('a') }}<span v-if="result && result.a.scan" class="vc-scan" title="Страница A — скан: текст не извлекается и не сравнивается">скан</span>
        <span class="vcv-arrow">→</span>
        <span class="vc-chip vc-chip-b">B</span> {{ sideLabel('b') }}<span v-if="result && result.b.scan" class="vc-scan" title="Страница B — скан: текст не извлекается и не сравнивается">скан</span>
        <span v-if="result" class="vc-badge" :class="'b-' + statusInfo(result.status).cls">{{ statusInfo(result.status).icon }} {{ result.status_label }}</span>
      </div>
      <div class="vcv-nav">
        <button class="btn btn-outline btn-sm" :disabled="sheetIndex <= 0" @click="gotoSheet(-1)">‹ лист</button>
        <span class="vc-muted">{{ sheetIndex + 1 }} / {{ items.length }}</span>
        <button class="btn btn-outline btn-sm" :disabled="sheetIndex >= items.length - 1" @click="gotoSheet(1)">лист ›</button>
        <button class="btn btn-outline btn-sm" @click="$emit('close')">✕ Закрыть</button>
      </div>
    </div>
    <div v-if="loading" class="vc-empty">Загрузка результата…</div>
    <div v-else-if="loadError" class="vc-error">{{ loadError }}</div>
    <template v-else-if="result">
      <div v-if="result.a.scan || result.b.scan" class="vcv-scan-note">
        <span class="vc-scan">скан</span> {{ result.a.scan && result.b.scan ? 'Обе страницы' : (result.a.scan ? 'Страница A' : 'Страница B') }} —
        изображение без текста: надписи не читаются и сравниваются только как картинка.
      </div>
      <div class="vcv-channels">
        <span class="vc-muted">Проверено:</span>
        <span v-for="c in channelsBy('checked')" :key="c.name" class="vcv-ch ok">{{ c.label }}<template v-if="c.diffs"> · {{ c.diffs }}</template></span>
        <template v-if="channelsBy('unchecked').length">
          <span class="vc-muted">Не проверено:</span>
          <span v-for="c in channelsBy('unchecked')" :key="c.name" class="vcv-ch bad" :title="c.reason">{{ c.label }}: {{ c.reason }}</span>
        </template>
        <span v-if="result.reasons && result.reasons.length" class="vcv-reasons" :title="result.reasons.join(', ')">причины: {{ reasonText() }}</span>
      </div>
      <div class="vcv-toolbar">
        <div class="vcv-modes">
          <button v-for="mo in modes" :key="mo.key" class="btn btn-sm" :class="mode === mo.key ? 'btn-primary' : 'btn-outline'" @click="setMode(mo.key)" :aria-pressed="mode === mo.key">{{ mo.label }}</button>
        </div>
        <div class="vcv-swipe-ctl" :class="{off: mode === 'split'}">
          <label title="Ползунок под листом: тяните вправо — верхний лист скрывается, открывается нижний"><input type="checkbox" v-model="swipeOn" :disabled="mode === 'split'"> шторка</label>
          <label v-if="swipeOn && mode !== 'split'">под ней:
            <select :value="underMode" @change="under = $event.target.value">
              <option v-for="mo in underModes" :key="mo.key" :value="mo.key">{{ mo.label }}</option>
            </select>
          </label>
        </div>
        <div class="vcv-zoom">
          <button class="btn btn-outline btn-sm" @click="zoomBy(1/1.5)">−</button>
          <span class="vc-muted">{{ Math.round(view.zoom * 100) }}%</span>
          <button class="btn btn-outline btn-sm" @click="zoomBy(1.5)">+</button>
          <button class="btn btn-outline btn-sm" @click="fitView">Вписать</button>
          <button class="btn btn-outline btn-sm" @click="oneToOne">1:1</button>
        </div>
        <div v-if="(mode === 'overlay' || (swipeActive && underMode === 'overlay')) && !result.identical_content" class="vcv-layers">
          <label v-for="l in layers" :key="l.key"><input type="checkbox" v-model="l.on"> {{ l.label }}</label>
        </div>
        <label class="vcv-boxes"><input type="checkbox" v-model="showBoxes"> рамки отличий</label>
        <label class="vcv-boxes" title="Совместить уверенно распознанные блоки B с A без изменения PDF. Снимите галочку, чтобы вернуть исходное положение."><input type="checkbox" v-model="alignBlocks"> Совмещать блоки<span v-if="result && result.aligned_blocks"> ({{ result.aligned_blocks }})</span></label>
      </div>
      <div class="vcv-body">
        <div class="vcv-stage" :class="{split: mode === 'split'}">
          <div v-for="pane in panes" :key="pane.key" class="vcv-pane" :data-mode="pane.mode" ref="paneEls"
               @wheel.prevent="onWheel($event)" @pointerdown="onDown($event)" @pointermove="onMove($event)" @pointerup="onUp($event)" @pointercancel="onUp($event)">
            <template v-if="swipeActive">
              <!-- Шторка: снизу underMode, сверху выбранный режим; ползунок срезает верхний слева. -->
              <img v-for="t in tiles(underMode, 'blue')" :key="'u' + t.key" :src="t.src" class="vcv-tile" :style="t.style" draggable="false" alt="">
              <div class="vcv-swipe-top" :style="{clipPath: 'inset(0 0 0 ' + swipeX() + 'px)'}">
                <img v-for="t in tiles(pane.mode, 'red')" :key="t.key" :src="t.src" class="vcv-tile" :style="t.style" draggable="false" alt="">
              </div>
            </template>
            <template v-else>
              <img v-for="t in tiles(pane.mode)" :key="t.key" :src="t.src" class="vcv-tile" :style="t.style" draggable="false" alt="">
            </template>
            <svg class="vcv-svg" :width="paneSize.w" :height="paneSize.h">
              <!-- Граница листа: холст сравнения = лист (при сдвиге/масштабе B — объединение листов A и B). -->
              <rect v-if="result" class="vcv-sheet-edge" :x="sx(0)" :y="sy(0)" :width="canvas().width_px * view.zoom" :height="canvas().height_px * view.zoom"></rect>
              <template v-if="showBoxes">
              <rect v-for="r in listed" :key="r.id" :x="sx(r.bbox_px[0]) - 3" :y="sy(r.bbox_px[1]) - 3"
                    :width="Math.max(6, (r.bbox_px[2] - r.bbox_px[0]) * view.zoom + 6)" :height="Math.max(6, (r.bbox_px[3] - r.bbox_px[1]) * view.zoom + 6)"
                    :class="['vcv-box', 'k-' + r.class, {sel: r.id === selected}]" @click.stop="select(r.id, false)"></rect>
              </template>
            </svg>
            <div class="vcv-pane-label">{{ pane.label }}</div>
            <template v-if="swipeActive">
              <div class="vcv-swipe-line" :style="{left: swipeX() + 'px', top: '0px', height: paneSize.h + 'px'}"></div>
              <div class="vcv-swipe-handle" :style="{left: swipeX() + 'px', top: handleY() + 'px'}" title="Тяните вправо — лист B (сверху) скрывается, открывается A"
                   @pointerdown.stop="swipeDown($event)" @pointermove.stop="swipeMove($event)" @pointerup.stop="swipeUp" @pointercancel.stop="swipeUp"></div>
            </template>
          </div>
        </div>
        <aside class="vcv-side">
          <div class="vcv-diffnav">
            <b>Отличия: {{ mainCount }}</b>
            <button class="btn btn-outline btn-xs" :disabled="!listed.length" @click="step(-1)">◀ пред.</button>
            <button class="btn btn-outline btn-xs" :disabled="!listed.length" @click="step(1)">след. ▶</button>
            <span class="vc-muted">←/→</span>
          </div>
          <label v-if="styleCount" class="vcv-style-toggle" title="Линия есть в обеих версиях, но изменились толщина, тон или порядок наложения на пересечениях">
            <input type="checkbox" v-model="showStyle"> <span class="vcv-dot k-style"></span>оформление линий: {{ styleCount }} — показать в списке
          </label>
          <label v-if="shiftCount" class="vcv-style-toggle" title="Те же надписи или блок на другом месте листа. Изменением не считается">
            <input type="checkbox" v-model="showShifts"> <span class="vcv-dot k-text_moved"></span>сдвиги (не изменение): {{ shiftCount }} — показать в списке
          </label>
          <div class="vcv-list">
            <div v-for="(r, i) in listed" :key="r.id" class="vcv-item" :data-change-id="r.id" :class="{sel: r.id === selected}" @click="select(r.id, true)">
              <div><span class="vcv-dot" :class="'k-' + r.class"></span>{{ i + 1 }}. {{ classLabel(r.class) }}<span v-if="r.text_unreadable" class="vc-flag" title="Надпись без текстового слоя">⚠</span></div>
              <template v-if="regionChanges(r).length">
                <div v-for="tc in regionChanges(r).slice(0, 6)" :key="tc.id" class="vcv-text">
                  <template v-if="tc.kind === 'moved'">сдвиг: {{ tc.before }} <span class="vc-muted">({{ fmtShift(tc) }})</span></template>
                  <template v-else>
                    <span class="was"><span v-for="(sg, k) in segments(tc.before, tc, 'before')" :key="k" :class="{num: sg.hl}">{{ sg.t }}</span><template v-if="!tc.before">—</template></span>
                    → <span class="now"><span v-for="(sg, k) in segments(tc.after, tc, 'after')" :key="k" :class="{num: sg.hl}">{{ sg.t }}</span><template v-if="!tc.after">—</template></span>
                    <span v-if="tc.norm" class="vcv-norm">{{ normLabel(tc.norm) }}</span>
                  </template>
                </div>
                <div v-if="regionChanges(r).length > 6" class="vc-muted">… ещё {{ regionChanges(r).length - 6 }}</div>
              </template>
              <template v-else>
                <div v-if="r.text_before.length || r.text_after.length" class="vcv-text">
                  <span class="was">{{ r.text_before.join(' ') || '—' }}</span> → <span class="now">{{ r.text_after.join(' ') || '—' }}</span>
                </div>
                <div v-if="r.moved_text.length" class="vcv-text">сдвиг: {{ r.moved_text.join(' ') }}</div>
              </template>
              <div v-if="r.class === 'block_moved'" class="vcv-text">сдвиг {{ fmtShift(r) }}, слов: {{ r.words }}; изменения внутри — отдельными отличиями</div>
            </div>
            <div v-if="!result.regions.length" class="vc-muted">В основной области отличий нет.</div>
            <div v-else-if="!listed.length" class="vc-muted">Кроме оформления линий и сдвигов, отличий нет.</div>
          </div>
          <div v-if="result.part" class="vcv-details vc-muted">
            <template v-if="result.part.kind === 'split'">Лист B — часть листа A: сравнивается только область B (рамка {{ fmtBox(result.part.area_a_pt) }} pt в A); доля содержимого A в этой области — {{ result.part.coverage_a != null ? Math.round(result.part.coverage_a * 100) + '%' : '—' }}. Покрытие листа A всеми частями — в строке прогона.</template>
            <template v-else>{{ result.part.note }}</template>
          </div>
          <details v-if="result.spec_page" class="vcv-details" open>
            <summary>Спецификация по позициям<template v-if="result.spec_page.basis === 'positions'">: {{ result.spec_page.changes }} изм. на листе</template></summary>
            <div v-if="result.spec_page.basis !== 'positions'" class="vc-warning">{{ result.spec_page.error }}</div>
            <template v-else>
              <div class="vc-muted">Статус листа — по позициям: строки, переехавшие между листами, не считаются изменением. Наложение страниц — справочно<template v-if="result.overlay_status"> (по наложению: {{ statusInfo(result.overlay_status).label }})</template>.</div>
              <div v-for="c in result.spec_page.items.filter(x => x.kind !== 'renumbered')" :key="c.id" class="vcv-tchange">
                <span class="vcv-tkind">{{ {changed: 'изменено', added: 'добавлено', removed: 'удалено', ambiguous: 'неоднозначно'}[c.kind] || c.kind }}</span> {{ c.label }}
                <div v-for="(v, f) in c.fields" :key="f"><span class="vc-muted">{{ f }}:</span> <span class="was">{{ v[0] || '—' }}</span> → <span class="now">{{ v[1] || '—' }}</span></div>
                <div class="vc-muted vcv-coords">стр. A: {{ c.a_pages.join(', ') || '—' }} · стр. B: {{ c.b_pages.join(', ') || '—' }}</div>
              </div>
            </template>
          </details>
          <details v-if="textChanges.length" class="vcv-details">
            <summary>Текст: {{ textCounts.main }} изменений<template v-if="textCounts.moved">, сдвигов (не изменение) {{ textCounts.moved }}</template><template v-if="textCounts.norm">, нормализация {{ textCounts.norm }}</template></summary>
            <div v-for="tc in textChanges.filter(c => showShifts || c.kind !== 'moved')" :key="tc.id" class="vcv-tchange" @click="focusBox(tc.a_bbox || tc.b_in_a_bbox)">
              <span class="vcv-tkind">{{ textKindLabel(tc.kind) }}</span>
              <template v-if="tc.kind === 'moved'">{{ tc.before }} <span class="vc-muted">({{ fmtShift(tc) }})</span></template>
              <template v-else>
                <span class="was"><span v-for="(sg, k) in segments(tc.before, tc, 'before')" :key="k" :class="{num: sg.hl}">{{ sg.t }}</span></span>
                <template v-if="tc.kind === 'changed'"> → </template>
                <span class="now"><span v-for="(sg, k) in segments(tc.after, tc, 'after')" :key="k" :class="{num: sg.hl}">{{ sg.t }}</span></span>
                <span v-if="tc.norm" class="vcv-norm">{{ normLabel(tc.norm) }}</span>
              </template>
            </div>
          </details>
          <details class="vcv-details">
            <summary>Служебные зоны и пометки</summary>
            <div v-for="(f, i) in (result.service || [])" :key="'f' + i" class="vcv-zone vcv-field">
              <b>{{ f.label }}</b>: {{ changeLabel(f.change) }}
              <div class="vcv-text"><span class="was">{{ f.before || '—' }}</span> → <span class="now">{{ f.after || '—' }}</span></div>
            </div>
            <div v-for="o in result.overlays" :key="o.kind" class="vcv-zone">
              <b>{{ overlayLabel(o.kind) }}</b>: {{ changeLabel(o.change) }}
              <div v-if="o.before && o.before.length" class="vcv-text was">было: {{ o.before.join(' | ') || '(картинка)' }}</div>
              <div v-if="o.after && o.after.length" class="vcv-text now">стало: {{ o.after.join(' | ') || '(картинка)' }}</div>
              <div v-if="o.kind === 'warning'" class="vc-warning">Исчезновение предупреждения не подтверждает согласования.</div>
            </div>
            <div v-for="z in result.zones" :key="z.type" class="vcv-zone">
              <b>{{ zoneLabel(z.type) }}</b>: {{ z.changed ? 'изменено' : 'без изменений' }}
              <span v-if="z.words_only_a.length" class="vcv-text was"> было: {{ z.words_only_a.join(' ') }}</span>
              <span v-if="z.words_only_b.length" class="vcv-text now"> стало: {{ z.words_only_b.join(' ') }}</span>
              <span v-if="z.words_moved && z.words_moved.length" class="vcv-text"> сдвиг надписи: {{ z.words_moved.join(' ') }}</span>
              <span v-if="z.changed && !z.words_only_a.length && !z.words_only_b.length && !(z.words_moved && z.words_moved.length)" class="vc-muted"> (надписи те же, изменена графика)</span>
            </div>
            <div v-if="annotChanges()" class="vcv-zone">
              <b>PDF-пометки</b>:
              <div v-for="a in result.annotations.removed" :key="'r' + a.rect.join()" class="vcv-text was">удалена: {{ a.type }} {{ a.subject }} {{ a.author }} «{{ a.content }}»</div>
              <div v-for="a in result.annotations.added" :key="'a' + a.rect.join()" class="vcv-text now">добавлена: {{ a.type }} {{ a.subject }} {{ a.author }} «{{ a.content }}»</div>
              <div v-for="(c, i) in result.annotations.changed" :key="'c' + i" class="vcv-text">изменена: «{{ c.before.content }}» → «{{ c.after.content }}»</div>
            </div>
          </details>
          <details class="vcv-details">
            <summary>Как сравнивалось</summary>
            <div class="vc-muted">Совмещение: {{ transformText() }}</div>
            <div class="vc-muted">Разрешение {{ result.params.dpi }} dpi, допуск {{ result.params.tol_pt }} pt.</div>
            <div v-if="result.within_tolerance" class="vc-muted">В пределах допуска: {{ result.within_tolerance.components }} мелких отличий; шум: {{ result.within_tolerance.noise_components || 0 }}. {{ result.within_tolerance.note || '' }}</div>
            <div v-for="s in separatedAll()" :key="s.side + s.kind + (s.stream_xref || s.image_xref)" class="vc-muted">
              Отделено от {{ s.side.toUpperCase() }}: {{ overlayLabel(s.kind) }} (контроль: {{ s.safety_check === 'passed' ? 'пройден' : s.safety_check }})
            </div>
            <div class="vc-muted">Исключено из основного наложения: основная надпись и поля за рамкой (сравниваются отдельно), отделённые объекты, PDF-аннотации (сравниваются списком).</div>
          </details>
        </aside>
      </div>
    </template>
  </div>
</div>`;

    root.VersionComparisonViewer = {
        TILE, CLASS_LABELS,
        register(app) {
            app.component('vc-viewer', {
                template: TEMPLATE,
                props: {objectId: String, run: Object, items: Array, startSpid: String},
                emits: ['close'],
                setup(props) {
                    const {ref, reactive, computed, onMounted, onBeforeUnmount, nextTick, watch} = root.Vue;
                    const VC = root.VersionComparison;
                    const rootEl = ref(null);
                    const paneEls = ref([]);
                    const spid = ref(props.startSpid);
                    const result = ref(null);
                    const alignBlocks = ref(true);
                    const loading = ref(false);
                    const loadError = ref('');
                    const mode = ref('b');
                    let previousMode = 'b';
                    function setMode(next) {
                        if (next === 'overlay') {
                            if (mode.value === 'overlay') { mode.value = previousMode; return; }
                            previousMode = mode.value;
                        }
                        mode.value = next;
                    }
                    const showBoxes = ref(true);
                    const selected = ref(null);
                    const showStyle = ref(false);
                    const showShifts = ref(false);
                    // Пока пользователь не двигал вид, лист вписывается заново при
                    // изменении размера панели (первая раскладка, «Рядом», окно).
                    let userMoved = false;
                    const layers = reactive(LAYERS.map(l => Object.assign({}, l)));
                    const view = reactive({zoom: 0.1, ox: 0, oy: 0});
                    const paneSize = reactive({w: 800, h: 600});
                    const modes = [{key: 'overlay', label: 'Наложение'}, {key: 'a', label: 'A'}, {key: 'b', label: 'B'}, {key: 'split', label: 'Рядом'}];
                    // Шторка — отдельный переключатель поверх режима (кроме «Рядом»): сверху выбранный
                    // режим, под ним — under (по умолчанию: под B — A, под A — B, под наложением — A).
                    const swipeOn = ref(true);
                    const under = ref('');
                    const LAYER_LABEL = {overlay: 'Наложение', a: 'A', b: 'B'};
                    const swipeActive = computed(() => swipeOn.value && mode.value !== 'split');
                    const underMode = computed(() => (under.value && under.value !== mode.value) ? under.value
                        : ({b: 'a', a: 'b', overlay: 'a'}[mode.value] || 'a'));
                    const underModes = computed(() => modes.filter(m => m.key !== 'split' && m.key !== mode.value));
                    // Шторка: положение линии — доля ширины листа (0 — левый край листа, B виден целиком);
                    // линия ходит только в пределах листа и следует за ним при сдвиге и масштабе.
                    const swipe = ref(0);
                    let swipeDrag = null;
                    const swipeX = () => Math.round(Math.max(Math.min(16, paneSize.w / 2),
                        Math.min(Math.max(paneSize.w / 2, paneSize.w - 16), sx(swipe.value * canvas().width_px))));
                    // Ручка закреплена у нижнего края окна независимо от масштаба и положения листа.
                    const handleY = () => Math.max(0, paneSize.h - 30);
                    function swipeDown(ev) {
                        swipeDrag = ev.currentTarget.parentElement.getBoundingClientRect();
                        ev.currentTarget.setPointerCapture(ev.pointerId);
                    }
                    function swipeMove(ev) {
                        if (!swipeDrag) return;
                        const w = canvas().width_px * view.zoom;
                        swipe.value = Math.min(1, Math.max(0, (ev.clientX - swipeDrag.left - sx(0)) / (w || 1)));
                    }
                    function swipeUp() { swipeDrag = null; }
                    let drag = null;
                    let resizeObs = null;

                    const sheetIndex = computed(() => props.items.findIndex(it => it.spid === spid.value));
                    const item = computed(() => props.items[sheetIndex.value] || null);
                    const title = computed(() => (item.value && item.value.title) || (result.value && (result.value.b.title || result.value.a.title)) || 'Лист');
                    const panes = computed(() => mode.value === 'split'
                        ? [{key: 'pa', mode: 'a', label: 'A'}, {key: 'pb', mode: 'b', label: 'B'}]
                        : [{key: 'p1', mode: mode.value, label: swipeActive.value ? `Шторка: слева ${LAYER_LABEL[underMode.value]}, справа ${LAYER_LABEL[mode.value]}` : LAYER_LABEL[mode.value]}]);
                    const base = () => `${VC.API}/objects/${encodeURIComponent(props.objectId)}/runs/${props.run.run_id}/sheets/${spid.value}`;

                    async function load() {
                        loading.value = true;
                        loadError.value = '';
                        selected.value = null;
                        userMoved = false;
                        swipe.value = 0;   // шторка нового листа — снова у его левого края
                        try {
                            const data = await VC.api(`/objects/${encodeURIComponent(props.objectId)}/runs/${props.run.run_id}/sheets/${spid.value}?align_blocks=${alignBlocks.value}`);
                            result.value = data.result;
                            if (!data.result) loadError.value = 'Результата для этого листа нет.';
                        } catch (e) { loadError.value = e.message; }
                        finally { loading.value = false; }
                        await nextTick();
                        observePanes();
                        measure();
                    }
                    function measure() {
                        const el = paneEls.value && paneEls.value[0];
                        if (!el || !el.clientWidth || !el.clientHeight) return;
                        paneSize.w = el.clientWidth;
                        paneSize.h = el.clientHeight;
                        if (!userMoved) fit();
                    }
                    function observePanes() {
                        if (!resizeObs) return;
                        resizeObs.disconnect();
                        (paneEls.value || []).forEach(el => resizeObs.observe(el));
                    }
                    const isShift = r => SHIFT_CLASSES.includes(r.class);
                    const listed = computed(() => !result.value ? [] :
                        result.value.regions.filter(r => (showStyle.value || r.class !== 'style') && (showShifts.value || !isShift(r))));
                    const styleCount = computed(() => !result.value ? 0 : result.value.regions.filter(r => r.class === 'style').length);
                    const shiftCount = computed(() => !result.value ? 0 : result.value.regions.filter(isShift).length);
                    const mainCount = computed(() => !result.value ? 0 : result.value.regions.length - styleCount.value - shiftCount.value);
                    const canvas = () => (result.value && result.value.canvas) || {width_px: 1, height_px: 1};
                    function fit() {
                        const c = canvas();
                        const z = Math.min(paneSize.w / c.width_px, paneSize.h / c.height_px) * 0.98;
                        view.zoom = z;
                        view.ox = (c.width_px - paneSize.w / z) / 2;
                        view.oy = (c.height_px - paneSize.h / z) / 2;
                    }
                    function fitView() { userMoved = false; fit(); }
                    function oneToOne() { zoomAt(1 / view.zoom, paneSize.w / 2, paneSize.h / 2); }
                    function zoomAt(factor, px, py) {
                        userMoved = true;
                        const nz = Math.min(8, Math.max(0.005, view.zoom * factor));
                        const cx = view.ox + px / view.zoom;
                        const cy = view.oy + py / view.zoom;
                        view.zoom = nz;
                        view.ox = cx - px / nz;
                        view.oy = cy - py / nz;
                    }
                    const zoomBy = f => zoomAt(f, paneSize.w / 2, paneSize.h / 2);
                    const sx = x => (x - view.ox) * view.zoom;
                    const sy = y => (y - view.oy) * view.zoom;

                    function level() {
                        const c = canvas();
                        const maxL = Math.max(0, Math.ceil(Math.log2(Math.max(c.width_px, c.height_px) / TILE)));
                        return Math.max(0, Math.min(maxL, Math.floor(Math.log2(1 / Math.max(view.zoom, 1e-6)))));
                    }
                    function layerParam() {
                        return layers.filter(l => l.on).flatMap(l => l.names).join(',');
                    }
                    function tiles(paneMode, textColor = 'original') {
                        if (!result.value) return [];
                        const c = canvas();
                        const L = level();
                        const f = Math.pow(2, L);
                        const span = TILE * f;
                        const x0 = Math.max(0, Math.floor(view.ox / span));
                        const y0 = Math.max(0, Math.floor(view.oy / span));
                        const x1 = Math.min(Math.ceil(c.width_px / span) - 1, Math.floor((view.ox + paneSize.w / view.zoom) / span));
                        const y1 = Math.min(Math.ceil(c.height_px / span) - 1, Math.floor((view.oy + paneSize.h / view.zoom) / span));
                        const out = [];
                        const lp = '&render=aligned-blocks-1&align_blocks=' + alignBlocks.value + '&text_color=' + textColor
                            + (paneMode === 'overlay' ? '&layers=' + encodeURIComponent(layerParam()) : '');
                        for (let ty = y0; ty <= y1; ty++) {
                            for (let tx = x0; tx <= x1; tx++) {
                                const w = Math.min(span, c.width_px - tx * span) * view.zoom;
                                const h = Math.min(span, c.height_px - ty * span) * view.zoom;
                                out.push({key: `${paneMode}-${L}-${tx}-${ty}-${lp}`,
                                    src: `${base()}/tile?mode=${paneMode}&level=${L}&x=${tx}&y=${ty}${lp}`,
                                    style: {left: sx(tx * span) + 'px', top: sy(ty * span) + 'px', width: Math.ceil(w) + 1 + 'px', height: Math.ceil(h) + 1 + 'px'}});
                            }
                        }
                        return out;
                    }

                    function onWheel(ev) {
                        userMoved = true;
                        const rect = ev.currentTarget.getBoundingClientRect();
                        if (ev.ctrlKey || ev.metaKey) {
                            zoomAt(ev.deltaY < 0 ? 1.15 : 1 / 1.15, ev.clientX - rect.left, ev.clientY - rect.top);
                        } else {
                            view.ox += (ev.shiftKey ? ev.deltaY : ev.deltaX) / view.zoom;
                            view.oy += (ev.shiftKey ? 0 : ev.deltaY) / view.zoom;
                        }
                    }
                    function onDown(ev) { userMoved = true; drag = {x: ev.clientX, y: ev.clientY, ox: view.ox, oy: view.oy, moved: false}; ev.currentTarget.setPointerCapture(ev.pointerId); }
                    function onMove(ev) {
                        if (!drag) return;
                        if (Math.hypot(ev.clientX - drag.x, ev.clientY - drag.y) > 4) drag.moved = true;
                        view.ox = drag.ox - (ev.clientX - drag.x) / view.zoom;
                        view.oy = drag.oy - (ev.clientY - drag.y) / view.zoom;
                    }
                    function onUp(ev) {
                        const click = drag && !drag.moved && ev && ev.type !== 'pointercancel'
                            && Math.hypot(ev.clientX - drag.x, ev.clientY - drag.y) <= 4;
                        drag = null;
                        if (click) pickChangeAt(ev);
                    }
                    function pickChangeAt(ev) {
                        if (!result.value) return;
                        const rect = ev.currentTarget.getBoundingClientRect();
                        const x = view.ox + (ev.clientX - rect.left) / view.zoom;
                        const y = view.oy + (ev.clientY - rect.top) / view.zoom;
                        const c = canvas(), scale = c.dpi / 72;
                        const hit = b => b && x >= b[0] && y >= b[1] && x <= b[2] && y <= b[3];
                        const pixels = b => b && [(b[0] - c.origin_pt[0]) * scale, (b[1] - c.origin_pt[1]) * scale,
                            (b[2] - c.origin_pt[0]) * scale, (b[3] - c.origin_pt[1]) * scale];
                        const visibleMode = swipeActive.value && ev.clientX - rect.left < swipeX()
                            ? underMode.value : ((ev.currentTarget.dataset || {}).mode || mode.value);
                        const textHits = new Set(((result.value.text || {}).changes || [])
                            .filter(t => (visibleMode !== 'b' && hit(pixels(t.a_bbox)))
                                || (visibleMode !== 'a' && hit(pixels(t.b_in_a_bbox)))).map(t => t.id));
                        const candidates = listed.value.filter(r => hit(r.bbox_px)
                            || (r.text_changes || []).some(id => textHits.has(id)));
                        candidates.sort((a, b) => {
                            const text = r => (r.text_changes || []).some(id => textHits.has(id)) ? 0 : 1;
                            const area = r => (r.bbox_px[2] - r.bbox_px[0]) * (r.bbox_px[3] - r.bbox_px[1]);
                            return text(a) - text(b) || area(a) - area(b);
                        });
                        if (candidates.length) select(candidates[0].id, false);
                    }

                    function select(id, center) {
                        selected.value = id;
                        nextTick().then(() => {
                            const rows = rootEl.value && rootEl.value.querySelectorAll('[data-change-id]');
                            const row = rows && Array.from(rows).find(el => el.dataset.changeId === String(id));
                            if (row) row.scrollIntoView({block: 'nearest', inline: 'nearest'});
                        });
                        const r = result.value.regions.find(x => x.id === id);
                        if (!r || !center) return;
                        userMoved = true;
                        const [x0, y0, x1, y1] = r.bbox_px;
                        const w = Math.max(x1 - x0, 1), h = Math.max(y1 - y0, 1);
                        const z = Math.min(2, Math.max(view.zoom, Math.min(paneSize.w / (w * 4), paneSize.h / (h * 4))));
                        view.zoom = z;
                        view.ox = (x0 + x1) / 2 - paneSize.w / (2 * z);
                        view.oy = (y0 + y1) / 2 - paneSize.h / (2 * z);
                    }
                    function step(d) {
                        const list = listed.value;
                        const n = list.length;
                        if (!n) return;
                        const cur = list.findIndex(r => r.id === selected.value);
                        select(list[((cur < 0 ? (d > 0 ? -1 : 0) : cur) + d + n) % n].id, true);
                    }
                    function onKey(ev) {
                        if (ev.target && /input|select|textarea/i.test(ev.target.tagName)) return;
                        if (ev.key === 'ArrowRight') { step(1); ev.preventDefault(); }
                        else if (ev.key === 'ArrowLeft') { step(-1); ev.preventDefault(); }
                        else if (ev.key === 'Escape') { ev.preventDefault(); }
                    }
                    function gotoSheet(d) {
                        const it = props.items[sheetIndex.value + d];
                        if (it) { spid.value = it.spid; load(); }
                    }

                    function sideLabel(side) {
                        if (!result.value) return '';
                        const s = result.value[side];
                        const run = props.run[side];
                        return `${run.label || run.file_name} · стр. ${s.page}${s.sheet_no ? ' · лист ' + s.sheet_no : ''}`;
                    }
                    function channelsBy(state) {
                        if (!result.value) return [];
                        return Object.entries(result.value.channels || {}).filter(([, c]) => c.state === state)
                            .map(([name, c]) => ({name, label: VC.CHANNEL_LABELS[name] || name, diffs: c.diffs, reason: c.reason}));
                    }
                    function reasonText() {
                        const map = {MATCH_LOW_CONFIDENCE: 'сопоставление листов не уверенное', DUPLICATE_SHEET: 'дубль листа',
                            ALIGNMENT_UNRELIABLE: 'совмещение ненадёжно', ALIGNMENT_PARTIAL: 'совмещена только часть листа', PARTIAL_COVERAGE: 'часть листа не проверена',
                            TEXT_UNREADABLE_IN_DIFF_AREA: 'надписи без текстового слоя', SPEC_NOT_PARSED: 'таблица СО не разобрана', NO_TEXT_LAYER: 'нет текстового слоя',
                            RENDER_FAILED: 'ошибка рендера'};
                        return (result.value.reasons || []).filter(r => !r.startsWith('UNCHECKED_CHANNEL')).map(r => map[r] || r).join('; ') || 'есть непроверенные каналы';
                    }
                    const classLabel = c => CLASS_LABELS[c] || c;
                    const textChanges = computed(() => (result.value && result.value.text && result.value.text.changes) || []);
                    const textById = computed(() => Object.fromEntries(textChanges.value.map(c => [c.id, c])));
                    const textCounts = computed(() => {
                        const all = textChanges.value;
                        return {main: all.filter(c => c.kind !== 'moved' && c.norm !== 'spaces').length,
                            moved: all.filter(c => c.kind === 'moved').length,
                            norm: all.filter(c => c.norm).length};
                    });
                    const regionChanges = r => (r.text_changes || []).map(id => textById.value[id])
                        .filter(Boolean);
                    const normLabel = n => ({spaces: 'только пробелы', symbol: 'замена символа лат./кир.', number_format: 'формат числа , / .'}[n] || n);
                    const textKindLabel = k => ({changed: 'изменено', added: 'добавлено', removed: 'удалено', moved: 'сдвиг'}[k] || k);
                    const fmtShift = tc => (tc.shift_pt || []).map(v => (v > 0 ? '+' : '') + v.toFixed(1)).join('; ') + ' pt';
                    // Текст по кускам: изменённые числа подсвечиваются (без v-html — текст из PDF).
                    function segments(text, tc, side) {
                        if (!text) return [];
                        const nums = new Set((tc.numbers || []).map(n => n[side]).filter(Boolean));
                        const out = [];
                        let last = 0;
                        const re = /\d+(?:[.,]\d+)?/g;
                        let m;
                        while ((m = re.exec(text)) !== null) {
                            if (m.index > last) out.push({t: text.slice(last, m.index), hl: false});
                            out.push({t: m[0], hl: nums.has(m[0])});
                            last = m.index + m[0].length;
                        }
                        if (last < text.length) out.push({t: text.slice(last), hl: false});
                        return out;
                    }
                    function focusBox(box) {
                        if (!box || !result.value) return;
                        const c = canvas();
                        const s = (c.dpi || 200) / 72;
                        const [ox, oy] = c.origin_pt || [0, 0];
                        const x0 = (box[0] - ox) * s, y0 = (box[1] - oy) * s, x1 = (box[2] - ox) * s, y1 = (box[3] - oy) * s;
                        const hit = result.value.regions.find(r => !(r.bbox_px[2] < x0 || r.bbox_px[0] > x1 || r.bbox_px[3] < y0 || r.bbox_px[1] > y1));
                        if (hit) {
                            if (hit.class === 'style') showStyle.value = true;
                            if (isShift(hit)) showShifts.value = true;
                            select(hit.id, true); return;
                        }
                        userMoved = true;
                        const w = Math.max(x1 - x0, 1), h = Math.max(y1 - y0, 1);
                        const z = Math.min(2, Math.max(view.zoom, Math.min(paneSize.w / (w * 4), paneSize.h / (h * 4))));
                        view.zoom = z;
                        view.ox = (x0 + x1) / 2 - paneSize.w / (2 * z);
                        view.oy = (y0 + y1) / 2 - paneSize.h / (2 * z);
                    }
                    const fmtBox = b => (b || []).map(v => Math.round(v)).join(', ');
                    const overlayLabel = k => ({release_stamp: 'Отметка «В производство работ»', warning: 'Предупреждение', qr: 'QR-код'}[k] || k);
                    const changeLabel = c => ({added: 'появилось', removed: 'исчезло', changed: 'изменилось', same_text: 'текст тот же'}[c] || c);
                    const zoneLabel = t => ({stamp: 'Основная надпись (штамп)', margin: 'Поля за рамкой', release_stamp: 'Отметка выпуска (не отделена)'}[t] || t);
                    const annotChanges = () => result.value && result.value.annotations &&
                        (result.value.annotations.added.length + result.value.annotations.removed.length + result.value.annotations.changed.length);
                    function transformText() {
                        const t = result.value.transform || {};
                        if (t.identity) return `тождественное (${t.method === 'identity' ? 'тождество содержимого' : 'по ' + (t.inliers || 0) + ' опорным словам'})`;
                        const parts = [`метод: ${t.method === 'anchors' ? 'опорные слова' : (t.method === 'phase' ? 'растровая корреляция' : t.method)}`];
                        if (t.scale !== undefined) parts.push(`масштаб ${t.scale.toFixed(4)}`, `поворот ${t.rot_deg.toFixed(2)}°`, `сдвиг ${t.dx.toFixed(1)}; ${t.dy.toFixed(1)} pt`);
                        parts.push(t.reliable ? 'надёжно' : 'НЕНАДЁЖНО');
                        return parts.join(', ');
                    }
                    function separatedAll() {
                        if (!result.value || !result.value.separated) return [];
                        return (result.value.separated.a || []).map(s => Object.assign({side: 'a'}, s))
                            .concat((result.value.separated.b || []).map(s => Object.assign({side: 'b'}, s)));
                    }

                    watch(alignBlocks, load);
                    watch(mode, async () => { await nextTick(); observePanes(); measure(); });
                    onMounted(() => {
                        if (root.ResizeObserver) resizeObs = new ResizeObserver(() => measure());
                        load();
                        if (rootEl.value) rootEl.value.focus();
                    });
                    onBeforeUnmount(() => { if (resizeObs) resizeObs.disconnect(); });

                    return {rootEl, paneEls, result, canvas, swipeOn, under, swipeActive, underMode, underModes, swipeX, handleY, swipeDown, swipeMove, swipeUp, loading, loadError, mode, modes, setMode, showBoxes, selected, layers, view, paneSize,
                        showStyle, showShifts, alignBlocks, shiftCount, listed, styleCount, mainCount, textChanges, textCounts, regionChanges, normLabel,
                        textKindLabel, fmtShift, segments, focusBox,
                        sheetIndex, title, panes, statusInfo: VC.statusInfo, tiles, fitView, oneToOne, zoomBy, sx, sy, onWheel, onDown,
                        onMove, onUp, select, step, onKey, gotoSheet, sideLabel, channelsBy, reasonText, classLabel, fmtBox,
                        overlayLabel, changeLabel, zoneLabel, annotChanges, transformText, separatedAll};
                },
            });
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
