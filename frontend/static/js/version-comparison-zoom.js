(function (root) {
    'use strict';
    // Лист в окне «лист крупно» и «рядом» (матрица сравнения версий): Ctrl+колесо — масштаб у курсора,
    // перетаскивание и колесо — сдвиг, двойной щелчок — вписать. Снизу превью (видно сразу), сверху резкие
    // тайлы страницы (backend: …/sources/{sha}/pages/{page}/tile). Листы «рядом» двигаются вместе через
    // общий объект ``sync``: центр вида в долях листа и масштаб относительно «вписать».
    const TILE = 512;

    const template = `
<div class="vc-zoom" ref="el" @wheel.prevent="onWheel" @pointerdown="onDown" @pointermove="onMove" @pointerup="onUp"
     @pointercancel="onUp" @dblclick="fit(true)" title="Ctrl+колесо — масштаб · перетаскивание — сдвиг · двойной щелчок — вписать">
  <template v-if="canvas && view.zoom">
    <img v-if="thumbUrl" class="vc-zoom-img" :src="thumbUrl" :style="baseStyle()" draggable="false" alt="">
    <img v-for="t in tiles()" :key="t.key" class="vc-zoom-img" :src="t.src" :style="t.style" draggable="false" alt="">
    <span class="vc-zoom-pct">{{ Math.round(relZoom() * 100) }}%</span>
  </template>
  <div v-else-if="error" class="vc-error">{{ error }}</div>
  <img v-else-if="thumbUrl" class="vc-zoom-fallback" :src="thumbUrl" draggable="false" alt="">
</div>`;

    root.VersionComparisonZoom = {
        register(app) {
            app.component('vc-zoom-pane', {
                template,
                // page — путь страницы без префикса API: /objects/{o}/sources/{sha}/pages/{n}
                props: {page: {type: String, required: true}, thumbUrl: {type: String, default: ''},
                    sync: {type: Object, default: null}},
                setup(props) {
                    const {ref, reactive, onMounted, onBeforeUnmount, watch} = root.Vue;
                    const VC = root.VersionComparison;
                    const el = ref(null), canvas = ref(null), error = ref('');
                    const size = reactive({w: 0, h: 0});
                    const view = reactive({zoom: 0, ox: 0, oy: 0});
                    const id = Math.random().toString(36).slice(2);
                    let drag = null, observer = null, alive = true;

                    const fitZoom = () => Math.min(size.w / canvas.value.width_px, size.h / canvas.value.height_px) * 0.98;
                    const relZoom = () => view.zoom / fitZoom();
                    const sx = x => (x - view.ox) * view.zoom;
                    const sy = y => (y - view.oy) * view.zoom;
                    const ready = () => !!(canvas.value && size.w && size.h);

                    // Вид в долях листа — общий язык для листов разного размера.
                    function share() {
                        if (!props.sync || !props.sync.on || !ready()) return;
                        const c = canvas.value;
                        Object.assign(props.sync, {from: id, rev: (props.sync.rev || 0) + 1, k: relZoom(),
                            cx: (view.ox + size.w / 2 / view.zoom) / c.width_px, cy: (view.oy + size.h / 2 / view.zoom) / c.height_px});
                    }
                    function apply(s) {
                        if (!ready()) return;
                        const c = canvas.value;
                        view.zoom = s.k * fitZoom();
                        view.ox = s.cx * c.width_px - size.w / 2 / view.zoom;
                        view.oy = s.cy * c.height_px - size.h / 2 / view.zoom;
                    }
                    function fit(broadcast) {
                        if (!ready()) return;
                        const c = canvas.value, z = fitZoom();
                        view.zoom = z;
                        view.ox = (c.width_px - size.w / z) / 2;
                        view.oy = (c.height_px - size.h / z) / 2;
                        if (broadcast) share();
                    }
                    function zoomAt(factor, px, py, broadcast = true) {
                        if (!ready()) return;
                        const nz = Math.min(fitZoom() * 64, Math.max(fitZoom() / 4, view.zoom * factor));
                        const cx = view.ox + px / view.zoom, cy = view.oy + py / view.zoom;
                        view.zoom = nz;
                        view.ox = cx - px / nz;
                        view.oy = cy - py / nz;
                        if (broadcast) share();
                    }
                    function start() {
                        if (!ready() || view.zoom) return;
                        if (props.sync && props.sync.on && props.sync.rev) apply(props.sync); else fit(false);
                    }

                    function baseStyle() {
                        const c = canvas.value;
                        return {left: sx(0) + 'px', top: sy(0) + 'px', width: c.width_px * view.zoom + 'px', height: c.height_px * view.zoom + 'px'};
                    }
                    function tiles() {
                        const c = canvas.value;
                        const maxL = Math.max(0, Math.ceil(Math.log2(Math.max(c.width_px, c.height_px) / TILE)));
                        const L = Math.max(0, Math.min(maxL, Math.floor(Math.log2(1 / Math.max(view.zoom, 1e-6)))));
                        const span = TILE * Math.pow(2, L);
                        const x0 = Math.max(0, Math.floor(view.ox / span)), y0 = Math.max(0, Math.floor(view.oy / span));
                        const x1 = Math.min(Math.ceil(c.width_px / span) - 1, Math.floor((view.ox + size.w / view.zoom) / span));
                        const y1 = Math.min(Math.ceil(c.height_px / span) - 1, Math.floor((view.oy + size.h / view.zoom) / span));
                        const out = [];
                        for (let ty = y0; ty <= y1; ty++) {
                            for (let tx = x0; tx <= x1; tx++) {
                                const w = Math.min(span, c.width_px - tx * span) * view.zoom;
                                const h = Math.min(span, c.height_px - ty * span) * view.zoom;
                                out.push({key: `${L}-${tx}-${ty}`, src: `${VC.API}${props.page}/tile?level=${L}&x=${tx}&y=${ty}`,
                                    style: {left: sx(tx * span) + 'px', top: sy(ty * span) + 'px', width: Math.ceil(w) + 1 + 'px', height: Math.ceil(h) + 1 + 'px'}});
                            }
                        }
                        return out;
                    }

                    function onWheel(ev) {
                        if (!ready()) return;
                        const rect = el.value.getBoundingClientRect();
                        if (ev.ctrlKey || ev.metaKey) {
                            zoomAt(ev.deltaY < 0 ? 1.15 : 1 / 1.15, ev.clientX - rect.left, ev.clientY - rect.top);
                        } else {
                            view.ox += (ev.shiftKey ? ev.deltaY : ev.deltaX) / view.zoom;
                            view.oy += (ev.shiftKey ? 0 : ev.deltaY) / view.zoom;
                            share();
                        }
                    }
                    function onDown(ev) {
                        if (ev.button !== 0 || !ready()) return;
                        drag = {x: ev.clientX, y: ev.clientY, ox: view.ox, oy: view.oy};
                        ev.currentTarget.setPointerCapture(ev.pointerId);
                    }
                    function onMove(ev) {
                        if (!drag) return;
                        view.ox = drag.ox - (ev.clientX - drag.x) / view.zoom;
                        view.oy = drag.oy - (ev.clientY - drag.y) / view.zoom;
                        share();
                    }
                    function onUp() { drag = null; }

                    // Кнопки окна: «Вписать», «+», «−» — команда всем листам (sync.cmd).
                    watch(() => props.sync && props.sync.cmd && props.sync.cmd.n, () => {
                        const cmd = props.sync.cmd;
                        if (cmd.kind === 'fit') fit(false);
                        else zoomAt(cmd.kind === 'in' ? 1.5 : 1 / 1.5, size.w / 2, size.h / 2, false);
                    });
                    watch(() => props.sync && props.sync.rev, () => {
                        if (props.sync.on && props.sync.from !== id) apply(props.sync);
                    });

                    onMounted(async () => {
                        observer = new ResizeObserver(() => {
                            if (!el.value) return;
                            const had = ready(), k = had && view.zoom ? relZoom() : 0;
                            const c = canvas.value, cx = had ? view.ox + size.w / 2 / view.zoom : 0, cy = had ? view.oy + size.h / 2 / view.zoom : 0;
                            size.w = el.value.clientWidth;
                            size.h = el.value.clientHeight;
                            if (had && k) apply({k, cx: cx / c.width_px, cy: cy / c.height_px}); else start();
                        });
                        observer.observe(el.value);
                        try {
                            const c = await VC.api(props.page + '/canvas');
                            if (!alive) return;
                            canvas.value = c;
                            start();
                        } catch (e) { error.value = 'Лист не загрузился: ' + (e.message || e); }
                    });
                    onBeforeUnmount(() => { alive = false; if (observer) observer.disconnect(); });

                    return {el, canvas, error, view, relZoom, baseStyle, tiles, fit, onWheel, onDown, onMove, onUp};
                },
            });
        },
    };
})(window);
