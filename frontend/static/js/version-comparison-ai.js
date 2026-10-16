(function (root) {
    'use strict';
    // ИИ-описание изменений пары версий (docs/version_comparison_ai_plan.md §9).
    // Панель открывает сохранённый результат задания без нового вызова модели;
    // запуск нового описания — только с явным подтверждением передачи документов.
    // Переход к доказательствам: рамки берёт сервер по ID на текущем результате
    // движка (…/focus), панель только передаёт их просмотрщику.

    const BUCKETS = ['ai_reviewed', 'engine_classified', 'linked_unverified', 'not_reviewed'];
    const AI_STATUS = {
        done: 'описано ИИ', partial: 'описано ИИ не полностью', failed: 'ошибка ИИ', cancelled: 'отменено',
        stale: 'устарело', engine_only: 'без ИИ — по данным движка', queued: 'в очереди', running: 'идёт',
    };
    const JOB_STATE = {
        queued: 'в очереди', running: 'идёт', done: 'готово', partial: 'готово не полностью', failed: 'ошибка',
        cancelling: 'отменяется…', cancelled: 'отменено', interrupted: 'прервано — можно досчитать',
        paused_quota: 'пауза: исчерпана квота подписки', paused_auth: 'пауза: нет авторизации исполнителя',
    };
    const TYPE_LABEL = {text: 'текст', region: 'графика', block: 'блок', spec: 'позиция СО', service: 'служебное поле',
        zone: 'служебная зона', overlay: 'наложенный объект', annotation: 'пометка'};
    const FATE_LABEL = {described: 'описана', merged: 'объединена', service: 'служебная', formatting: 'оформление',
        needs_check: 'неоднозначна', not_reviewed: 'не просмотрена', linked: 'через связь'};

    const TEMPLATE = `
<div class="vcai-overlay" @keydown.esc="$emit('close')" tabindex="0">
  <div class="vcai-window">
    <div class="vcai-head">
      <div>
        <div class="vcai-title">ИИ-описание изменений</div>
        <div class="vcai-passport" v-if="run">
          <span v-if="view && view.project.title"><b>{{ view.project.title }}</b> · </span>
          <span class="vc-chip vc-chip-a">A</span> {{ run.a.label }} <span class="vc-muted">(файл «{{ run.a.file_name }}»)</span>
          <span class="vcv-arrow">→</span>
          <span class="vc-chip vc-chip-b">B</span> {{ run.b.label }} <span class="vc-muted">(файл «{{ run.b.file_name }}»)</span>
        </div>
      </div>
      <div class="vcai-head-actions">
        <a v-if="job" class="btn btn-outline btn-sm" :href="mdUrl()" download>Выгрузить MD</a>
        <button class="btn btn-outline btn-sm" @click="$emit('close')">✕ Закрыть</button>
      </div>
    </div>

    <div class="vcai-body">
      <div v-if="loading" class="vc-muted">Загрузка…</div>
      <div v-if="error" class="vc-error">{{ error }}</div>

      <p v-if="!loading && !job">Для этого сравнения ИИ-описания ещё нет.</p>
      <div v-if="!loading && (!job || newJob)" class="vcai-estimate">
        <p v-if="!enabled" class="vc-muted">Запуск ИИ-описаний на этой установке выключен (передача документов внешнему исполнителю не разрешена).</p>
        <template v-else>
          <div class="vcai-estimate-head"><b>Новое ИИ-описание</b>
            <button v-if="job" class="btn btn-outline btn-xs" @click="newJob = false; estimate = null">✕</button></div>
          <button v-if="!estimate" class="btn btn-outline btn-sm" :disabled="estimating" @click="loadEstimate">{{ estimating ? 'Оценка… (пакеты собираются без отправки)' : 'Оценить объём' }}</button>
          <template v-if="estimate">
            <p>Листов с записями для ИИ: <b>{{ estimate.sheets_to_ai }}</b> из {{ estimate.sheets_total }}. Исполнитель: {{ estimate.executor.transport }}, модель {{ estimate.executor.model }}<template v-if="estimate.executor.effort">, усилие {{ estimate.executor.effort }}</template>.</p>
            <table class="vcai-table">
              <tr><th></th><th>Лист</th><th>Стр. PDF A → B</th><th>Записей</th><th>Обращений</th><th>Изображений</th></tr>
              <tr v-for="x in estimate.sheets.filter(x => x.ai_records)" :key="x.spid">
                <td><input type="checkbox" v-model="pick[x.spid]"></td>
                <td>{{ x.title }}<span v-if="x.scope === 'added' || x.scope === 'removed'" class="vc-muted"> — лист {{ x.scope === 'added' ? 'добавлен' : 'удалён' }}, описать целиком</span></td><td title="Номера страниц PDF в файлах A и B (не номера листов из штампа)">{{ x.a_page || '—' }} → {{ x.b_page || '—' }}</td><td>{{ x.ai_records }}</td>
                <td>{{ x.reusable_from ? '0 — готов (задание ' + x.reusable_from + ')' : x.chunks }}</td><td>{{ x.reusable_from ? '—' : x.images }}</td>
              </tr>
            </table>
            <p>Выбрано листов: <b>{{ picked.length }}</b>; новых обращений к модели: <b>{{ pickedCalls }}</b> (при ответе не по схеме — до {{ estimate.retries_max }} повтора на часть); изображений: {{ pickedImages }}.
              <span class="vc-muted">Листы без записей для ИИ описываются по данным движка без обращений.</span></p>
            <p class="vc-muted">{{ estimate.transfer }}</p>
            <label><input type="checkbox" v-model="consent"> Подтверждаю передачу документов выбранных листов исполнителю</label>
            <div><button class="btn btn-primary btn-sm" :disabled="!consent || starting || !picked.length" @click="start">Передать и описать</button></div>
          </template>
        </template>
      </div>

      <template v-if="job">
        <div class="vcai-jobline">
          <span>Задание {{ job.job_id }} · {{ jobState(job.state) }}</span>
          <span v-if="view && view.job.executor" class="vc-muted"> · {{ view.job.executor.transport }}, модель {{ view.job.executor.model }}<template v-if="view.job.executor.effort">, усилие {{ view.job.executor.effort }}</template></span>
          <span v-if="view && view.job.finished_at" class="vc-muted"> · {{ fmtTime(view.job.finished_at) }}</span>
          <span v-if="job.reason" class="vc-error"> · {{ job.reason }}</span>
          <span v-if="['queued','running'].includes(job.state) && job.progress" class="vc-muted"> · частей {{ job.progress.chunks_done }} из {{ job.progress.chunks_total }}</span>
        </div>
        <p class="vcai-disclaimer" v-if="view">{{ view.disclaimer }}</p>
        <p class="vcai-scope" v-if="view && view.scope_text">{{ view.scope_text }}</p>
        <button v-if="enabled && !newJob && ['interrupted','cancelled','paused_quota','paused_auth'].includes(job.state)" class="btn btn-primary btn-sm" :disabled="starting"
                title="Продолжить это задание: готовые части не отправляются заново, только оставшиеся" @click="resume">Досчитать</button>
        <button v-if="enabled && !newJob && !['queued','running','cancelling'].includes(job.state)" class="btn btn-outline btn-sm" @click="newJob = true">Новое описание…</button>

        <div v-if="view" class="vcai-counters" title="Учёт записей сравнения. Полный учёт записей не означает полного анализа всего листа.">
          <div v-for="k in buckets" :key="k" class="vcai-counter" :class="'b-' + k">
            <div class="vcai-counter-n">{{ view.coverage[k] || 0 }}</div>
            <div class="vcai-counter-l">{{ view.coverage_labels[k] }}</div>
          </div>
        </div>

        <div v-if="view" class="vcai-sheets">
          <div v-for="s in aiSheets" :key="s.spid" class="vcai-sheet">
            <div class="vcai-sheet-head">
              <b>{{ s.title }}</b>
              <span class="vc-muted"> · лист {{ s.b.sheet_no || s.a.sheet_no || '—' }} · стр. PDF {{ s.a.page || '—' }} → {{ s.b.page || '—' }}</span>
              <span class="vcai-chip vcai-chip-engine" title="Статус детерминированного сравнения — не меняется ИИ">Движок: {{ s.engine_status_label }}</span>
              <span class="vcai-chip" :class="'vcai-ai-' + s.ai_status" title="Статус ИИ-описания листа">ИИ: {{ aiStatus(s.ai_status) }}</span>
            </div>
            <div v-if="s.stale" class="vcai-stale">⚠ {{ s.stale_text }}</div>
            <div v-if="s.reused_text" class="vc-muted">{{ s.reused_text }}</div>

            <div v-for="(le, li) in (s.layer_errors || [])" :key="'le' + li" class="vcai-limit">⚠ {{ le }}</div>
            <div v-if="s.summary" class="vcai-summary">
              <div class="vcai-summary-head"><b>Кратко</b>
                <span class="vc-muted"> · пунктов {{ s.summary.coverage.items_total }} → групп {{ s.summary.groups.length }} ·
                  {{ s.summary.mode === 'editorial' ? 'группы сведены ИИ по готовым пунктам (без повторного анализа чертежей) и проверены программой' : 'программное объединение одинаковых изменений' }}</span>
                <span v-if="!s.summary.coverage.ok" class="vc-error"> · учёт пунктов в сводке нарушен — смотрите подробный список</span>
              </div>
              <template v-for="c in summaryCats(s)" :key="c.key">
                <div class="vcai-group" :class="'vcai-g-' + c.key">
                  <div class="vcai-group-title">{{ c.title }} <span class="vc-muted">· групп {{ c.groups.length }}</span></div>
                  <div v-for="g in c.groups" :key="g.group_id" class="vcai-entry vcai-sgroup" :data-group="g.group_id">
                    <div class="vcai-brief">
                      <button class="vcai-toggle" @click="toggle(s.spid + g.group_id)">{{ isOpen(s.spid + g.group_id) ? '▾' : '▸' }}</button>
                      <span class="vcai-brief-text"><b v-if="g.title">{{ g.title }}.</b> {{ g.brief }}
                        <span class="vcai-mini">{{ g.item_ids.length }} п.</span>
                        <span v-if="g.limitations.length" class="vcai-mini vcai-mini-warn" title="Ограничения пунктов показаны при раскрытии">ограничения: {{ g.limitations.length }}</span>
                        <span v-for="k in checkClasses(g)" :key="k.cls" class="vcai-mini" :class="'vcai-cls-' + k.cls">{{ k.label }}<template v-if="k.n > 1"> ×{{ k.n }}</template></span>
                        <span v-if="g.verified_items.length" class="vcai-mini vcai-cls-resolved" title="Предупреждение проверки снято: основание найдено в исходном PDF">проверено при разборе: {{ g.verified_items.length }}</span>
                      </span>
                      <button class="btn btn-outline btn-xs vcai-go" :disabled="s.stale" title="Открыть A и B рядом: первое доказательство крупно, далее «след.»" @click="goto(s, g.focus_ids, g.title || g.brief)">к области →</button>
                    </div>
                    <div v-if="isOpen(s.spid + g.group_id)" class="vcai-detail">
                      <div v-for="l in g.limitations" :key="'l' + l.item_id" class="vcai-limit">Ограничение {{ l.item_id }}: {{ l.text }}</div>
                      <div class="vc-muted">Исходные пункты ({{ g.members.length }}) — без изменений:</div>
                      <vc-ai-item v-for="m in g.members" :key="m.id" :s="s" :e="m" :open="open" @goto="goto" @goto-ref="gotoRef"></vc-ai-item>
                    </div>
                  </div>
                </div>
              </template>
            </div>

            <details class="vcai-more">
              <summary>Подробно: все пункты по категориям ({{ s.groups.reduce((n, g) => n + g.entries.length, 0) }})</summary>
            <template v-for="g in s.groups" :key="g.key">
              <div v-if="g.entries.length" class="vcai-group" :class="'vcai-g-' + g.key">
                <div class="vcai-group-title">{{ g.title }}</div>
                <vc-ai-item v-for="e in g.entries" :key="e.id" :s="s" :e="e" :open="open" @goto="goto" @goto-ref="gotoRef"></vc-ai-item>
              </div>
            </template>
            </details>

            <details class="vcai-more">
              <summary>Служебные изменения: {{ s.service.length }}</summary>
              <div v-for="x in s.service" :key="x.id" class="vcai-related-row">{{ x.id }} · {{ x.label }}: {{ x.before || '—' }} → {{ x.after || '—' }}
                <span class="vc-muted"> · {{ x.ai ? 'передано ИИ' : 'по данным движка' }}</span></div>
            </details>
            <details class="vcai-more" :open="s.not_reviewed.length > 0">
              <summary>Не просмотрено: {{ s.not_reviewed.length }}</summary>
              <div v-for="x in s.not_reviewed" :key="x.id" class="vcai-related-row">{{ x.id }} · {{ x.reason }}
                <button class="btn btn-outline btn-xs" :disabled="s.stale" @click="goto(s, [x.id], x.id)">→</button></div>
            </details>
            <details class="vcai-more">
              <summary>Классифицировано движком (ИИ не передавалось): {{ s.coverage.engine_classified }}</summary>
              <div v-for="x in s.engine_classified" :key="x.fate" class="vcai-related-row">{{ x.label }} — {{ x.ids.length }}: <span class="vc-muted">{{ x.ids.join(', ') }}</span></div>
            </details>
            <details class="vcai-more">
              <summary>Учтено через связь, отдельно не проверялось: {{ s.linked.length }}</summary>
              <div v-for="x in s.linked" :key="x.id" class="vcai-related-row">{{ x.id }} → {{ x.links.join(', ') }}</div>
            </details>
          </div>

          <details v-if="engineOnly.length" class="vcai-more">
            <summary>Листы без ИИ (по данным движка): {{ engineOnly.length }}</summary>
            <div v-for="s in engineOnly" :key="s.spid" class="vcai-related-row">{{ s.title }}: {{ s.engine_only_text }}</div>
          </details>
        </div>
      </template>
      <div v-if="notice" class="vc-error">{{ notice }}</div>
    </div>
  </div>
</div>`;

    const ITEM_TEMPLATE = `
                <div class="vcai-entry" :data-entry="e.id">
                  <div class="vcai-brief">
                    <button class="vcai-toggle" @click="toggle(s.spid + e.id)">{{ isOpen(s.spid + e.id) ? '▾' : '▸' }}</button>
                    <span class="vcai-brief-text">{{ e.brief }}</span>
                    <button class="btn btn-outline btn-xs vcai-go" :disabled="s.stale" title="Открыть страницы A и B на этом месте" @click="$emit('goto', s, e.focus_ids, e.brief)">к области →</button>
                  </div>
                  <div v-if="isOpen(s.spid + e.id)" class="vcai-detail">
                    <template v-if="e.kind === 'group'">
                      <table class="vcai-table">
                        <tr><th>Позиция</th><th>Было (A)</th><th>Стало (B)</th><th>Изменение</th><th>ID</th><th></th></tr>
                        <tr v-for="p in e.positions" :key="p.spec_id">
                          <td>{{ p.position }}</td>
                          <td class="was">{{ p.changes.map(c => c.before || '—').join('; ') }}</td>
                          <td class="now">{{ p.changes.map(c => c.after || '—').join('; ') }}</td>
                          <td>{{ p.changes.map(c => c.field_label + ': ' + c.delta).join('; ') }}</td>
                          <td class="vc-muted">{{ p.evidence_ids.join(', ') }}</td>
                          <td><button class="btn btn-outline btn-xs" :disabled="s.stale" @click="$emit('goto', s, p.evidence_ids, p.position)">→</button></td>
                        </tr>
                      </table>
                      <div class="vc-muted">Подробные пункты ИИ: {{ e.members.map(m => m.id).join(', ') }}</div>
                      <div v-for="m in e.members" :key="m.id" class="vcai-member">{{ m.id }}: {{ m.summary }}</div>
                    </template>
                    <template v-else>
                      <div v-if="e.summary !== e.brief">{{ e.summary }}</div>
                      <div v-if="e.limitation" class="vcai-limit">Ограничение: {{ e.limitation }}</div>
                      <div v-if="e.flags.length && !e.verification" class="vcai-limit">Переведено в «Нужна проверка» проверкой: {{ e.flags.join(', ') }}</div>
                      <table class="vcai-table">
                        <tr><th>ID</th><th>Вид</th><th>Было (A)</th><th>Стало (B)</th><th>Учёт</th><th></th></tr>
                        <tr v-for="ev in e.evidence" :key="ev.id">
                          <td>{{ ev.id }}</td><td>{{ typeLabel(ev) }}</td>
                          <td class="was">{{ ev.before || '—' }}</td><td class="now">{{ ev.after || '—' }}</td>
                          <td>{{ ev.bucket_label }}<span v-if="ev.fate" class="vc-muted"> · {{ fateLabel(ev.fate) }}</span></td>
                          <td><button class="btn btn-outline btn-xs" :disabled="s.stale" @click="$emit('goto', s, [ev.id], ev.id)">→</button></td>
                        </tr>
                      </table>

                      <div v-if="e.spec_refs && e.spec_refs.length" class="vcai-related">
                        <div class="vcai-related-title">Позиции СО, переданные ИИ как межлистовой контекст</div>
                        <div v-for="x in e.spec_refs" :key="x.id" class="vcai-related-row">{{ x.id }} · {{ x.side }}, стр. PDF {{ x.page }} · {{ x.mark }}<span v-if="x.note" class="vc-muted"> · примечание: {{ x.note }}</span>
                          <button class="btn btn-outline btn-xs" @click="$emit('goto-ref', s, x.id, x.mark)">к строке СО →</button></div>
                      </div>
                      <div v-if="e.verification" class="vcai-check">
                        <div class="vcai-check-head">Проверка предупреждения
                          <span class="vcai-mini" :class="'vcai-cls-' + e.verification.class">{{ e.verification.class_label }}</span>
                          <span class="vc-muted"> · {{ e.verification.set_by === 'program' ? 'предупреждение поставила проверка программы' : 'неоднозначность отметил сам ИИ' }} · добавлено при проверке, модели не отправлялось</span></div>
                        <div v-if="e.verification.prior.issues.length" class="vc-muted">Причина предупреждения: {{ e.verification.prior.issues.join('; ') }}</div>
                        <div v-if="e.verification.final_category !== 'needs_check'" class="vcai-ok">Основание найдено для каждого сомнительного поля — пункт возвращён в «{{ e.category_label }}».</div>
                        <div v-else-if="e.verification.field_doubts.length" class="vcai-limit">Сомнение в полях: {{ e.verification.field_doubts.join(', ') }}<template v-if="e.verification.main_conclusion_affected"> — от них зависит, какой объект или значение изменены, поэтому пункт остаётся в «Нужна проверка»</template>.</div>
                        <table v-if="e.verification.tokens.length" class="vcai-table">
                          <tr><th>Значение</th><th>Итог</th><th>Основание</th></tr>
                          <tr v-for="t in e.verification.tokens" :key="t.token"><td>{{ t.token }}</td><td>{{ t.status_label }}</td><td class="vc-muted">{{ tokenBasis(t) }}</td></tr>
                        </table>
                        <div v-for="(f, fi) in e.verification.findings" :key="fi" class="vcai-finding">
                          <div>{{ f.text }}</div>
                          <div v-if="f.b_text" class="vc-muted">В B (стр. PDF {{ f.b_page }}): «{{ f.b_text.join(' / ') }}»</div>
                          <div v-for="(o, oi) in (f.overlaps || [])" :key="oi" class="vc-muted">В B, стр. PDF {{ o.added.page }}: «{{ o.added.text }}» поверх «{{ o.covers.map(c => c.text).join(' ') }}»</div>
                          <table v-if="f.rows" class="vcai-table">
                            <tr><th>Объект</th><th>Схема A → B</th><th>СО B: строка</th><th>Основание</th><th>Расхождение</th><th></th></tr>
                            <tr v-for="r in f.rows" :key="r.record">
                              <td>{{ r.object }}</td>
                              <td><span class="was">{{ r.schema_before }}</span> → <span class="now">{{ r.schema_after }}</span> <span class="vc-muted">({{ r.record }})</span></td>
                              <td><template v-if="r.spec_b">стр. PDF {{ r.spec_b.page }} · {{ r.spec_b.mark }} · кол. {{ r.spec_b.qty }}<span class="vc-muted"> · примечание: {{ r.spec_b.note || (r.spec_b.note_from_page_text && r.spec_b.note_from_page_text.text) || '—' }}</span></template><template v-else>не найдена</template></td>
                              <td class="vc-muted">{{ r.spec_b ? r.spec_b.source + (r.spec_b.sent ? '' : ' — добавлено при проверке') : '—' }}</td>
                              <td>{{ r.object_confirmed ? 'подтверждено: в СО «ПЧ» есть' : 'не подтверждено' }}</td>
                              <td class="vcai-nowrap"><button class="btn btn-outline btn-xs" :disabled="s.stale" @click="$emit('goto', s, [r.record], r.object + ' на схеме')">к схеме</button>
                                <button v-if="r.spec_b" class="btn btn-outline btn-xs" @click="$emit('goto-ref', s, 'V:' + e.id + ':' + r.object, r.object)">к строке СО</button></td>
                            </tr>
                          </table>
                        </div>
                        <div v-if="e.verification.crops.length" class="vcai-crops">
                          <a v-for="c in e.verification.crops" :key="c.file" :href="c.url" target="_blank" class="vcai-crop">
                            <img :src="c.url" loading="lazy" :alt="c.file"><span>{{ c.side }}, стр. PDF {{ c.page }}, {{ c.dpi }} dpi</span></a>
                          <div class="vc-muted">{{ e.verification.crops[0].purpose }}</div>
                        </div>
                      </div>
                      <div v-if="e.related_graphics.length" class="vcai-related">
                        <div class="vcai-related-title">Связанное оформление (графика, отнесённая ИИ к этому пункту, — не самостоятельное изменение)</div>
                        <div v-for="ev in e.related_graphics" :key="ev.id" class="vcai-related-row">
                          {{ ev.id }} · {{ ev.reason || 'фрагмент графики' }}
                          <span class="vc-muted"> · {{ ev.image_check ? ev.image_check.text : '' }}</span>
                          <button class="btn btn-outline btn-xs" :disabled="s.stale" @click="$emit('goto', s, [ev.id], ev.id)">→</button>
                        </div>
                      </div>
                    </template>
                  </div>
                </div>`;

    const CLASS_LABEL = {contradiction: 'противоречие источников', unreadable: 'нечитаемо', image_only: 'прочитано с изображения',
        counted: 'подсчёт не подтверждён', no_basis: 'основание не найдено', resolved: 'основание найдено при проверке'};
    const SUMMARY_CATS = [['content', 'Изменения содержания'], ['formatting', 'Оформление'], ['service', 'Служебное'],
        ['needs_check', 'Нужна проверка']];

    function tokenBasis(t) {
        if (t.kind === 'mark' && t.sides) {
            return Object.entries(t.sides).map(([side, x]) => {
                const c = x.context;
                return `${side}, стр. PDF ${x.page}: ` + (c ? `«${c.text}» в ${c.distance_pt} pt` : 'рядом нет') + ` (${x.coverage})`;
            }).join('; ');
        }
        if (t.kind === 'number' && t.unique_places !== undefined) {
            return `уникальных мест ${t.unique_places} из ${t.records} записей (сторона ${t.side})`;
        }
        if (t.context) return `${t.context.side}, стр. PDF ${t.context.page}: «${t.context.text}» в ${t.context.distance_pt} pt`;
        return '—';
    }

    root.VersionComparisonAI = {
        AI_STATUS, BUCKETS,
        register(app) {
            app.component('vc-ai-item', {
                template: ITEM_TEMPLATE,
                props: {s: Object, e: Object, open: Object},
                emits: ['goto', 'goto-ref'],
                setup(props) {
                    const toggle = k => { props.open[k] = !props.open[k]; };
                    const isOpen = k => !!props.open[k];
                    const typeLabel = ev => (TYPE_LABEL[ev.type] || ev.type) + (ev.label ? ': ' + ev.label : '');
                    return {toggle, isOpen, typeLabel, tokenBasis, fateLabel: f => FATE_LABEL[f] || f};
                },
            });
            app.component('vc-ai-report', {
                template: TEMPLATE,
                props: {objectId: String, run: Object},
                emits: ['close', 'open-evidence'],
                setup(props, {emit}) {
                    const {ref, reactive, computed, onMounted, onBeforeUnmount} = root.Vue;
                    const VC = root.VersionComparison;
                    const loading = ref(true), error = ref(''), notice = ref('');
                    const job = ref(null), view = ref(null), enabled = ref(false);
                    const estimate = ref(null), consent = ref(false), starting = ref(false), estimating = ref(false);
                    const newJob = ref(false), pick = reactive({});
                    const picked = computed(() => estimate.value ? estimate.value.sheets.filter(x => x.ai_records && pick[x.spid]) : []);
                    const pickedCalls = computed(() => picked.value.reduce((n, x) => n + (x.reusable_from ? 0 : x.chunks), 0));
                    const pickedImages = computed(() => picked.value.reduce((n, x) => n + (x.reusable_from ? 0 : x.images), 0));
                    const open = reactive({});
                    let timer = null;
                    const base = () => `/objects/${encodeURIComponent(props.objectId)}/runs/${props.run.run_id}/ai`;

                    async function load() {
                        loading.value = true; error.value = '';
                        try {
                            const latest = await VC.api(`${base()}/latest`);
                            enabled.value = !!latest.enabled;
                            job.value = latest.job;
                            if (job.value) await refresh();
                        } catch (e) { error.value = e.message; }
                        finally { loading.value = false; }
                    }
                    async function refresh() {
                        job.value = await VC.api(`${base()}/jobs/${job.value.job_id}`);
                        if (['queued', 'running', 'cancelling'].includes(job.value.state)) {
                            timer = setTimeout(refresh, 2000);
                            return;
                        }
                        view.value = await VC.api(`${base()}/jobs/${job.value.job_id}/view`);
                    }
                    async function loadEstimate() {
                        estimating.value = true;
                        try {
                            estimate.value = await VC.api(`${base()}/estimate`, {method: 'POST', body: {}});
                            estimate.value.sheets.forEach(x => { pick[x.spid] = !!x.ai_records; });
                        } catch (e) { error.value = e.message; }
                        finally { estimating.value = false; }
                    }
                    async function start() {
                        starting.value = true;
                        try {
                            job.value = await VC.api(`${base()}/jobs`, {method: 'POST', body: {consent: consent.value, spids: picked.value.map(x => x.spid)}});
                            newJob.value = false; estimate.value = null; consent.value = false; view.value = null;
                            await refresh();
                        } catch (e) { error.value = e.message; }
                        finally { starting.value = false; }
                    }
                    async function resume() {
                        starting.value = true;
                        try {
                            job.value = await VC.api(`${base()}/jobs/${job.value.job_id}/resume`, {method: 'POST'});
                            view.value = null;
                            await refresh();
                        } catch (e) { error.value = e.message; }
                        finally { starting.value = false; }
                    }
                    async function goto(s, ids, label) {
                        notice.value = '';
                        try {
                            const f = await VC.api(`${base()}/jobs/${job.value.job_id}/sheets/${s.spid}/focus?ids=${encodeURIComponent(ids.join(','))}`);
                            if (!f.available) { notice.value = f.reason || 'Область для перехода не найдена.'; return; }
                            emit('open-evidence', {spid: s.spid, focus: {spid: s.spid, ids, label, boxes: f.boxes, union_px: f.union_px,
                                index: 0, all: false, where: {a: s.a, b: s.b}}});
                        } catch (e) { notice.value = e.message; }
                    }
                    // Переход на ДРУГОЙ лист пары (строка СО): рамки и лист выбирает сервер по сохранённым данным.
                    async function gotoRef(s, ref, label) {
                        notice.value = '';
                        try {
                            const f = await VC.api(`${base()}/jobs/${job.value.job_id}/sheets/${s.spid}/ref-focus?ref=${encodeURIComponent(ref)}`);
                            if (!f.available) { notice.value = f.reason || 'Область для перехода не найдена.'; return; }
                            emit('open-evidence', {spid: f.spid, focus: {spid: f.spid, ids: [ref], label: f.label || label, boxes: f.boxes,
                                union_px: f.union_px, index: 0, all: false,
                                where: {a: {label: props.run.a.label, page: f.a_page}, b: {label: props.run.b.label, page: f.b_page}, title: f.title}}});
                        } catch (e) { notice.value = e.message; }
                    }
                    function summaryCats(s) {
                        const groups = (s.summary && s.summary.groups) || [];
                        return SUMMARY_CATS.map(([key, title]) => ({key, title, groups: groups.filter(g => g.category === key)}))
                            .filter(c => c.groups.length);
                    }
                    function checkClasses(g) {
                        const n = {};
                        (g.checks || []).forEach(c => { n[c.class] = (n[c.class] || 0) + 1; });
                        return Object.entries(n).map(([cls, k]) => ({cls, n: k, label: CLASS_LABEL[cls] || cls}));
                    }
                    const aiSheets = computed(() => (view.value ? view.value.sheets : []).filter(s => !s.pending && s.ai_status !== 'engine_only'));
                    const engineOnly = computed(() => (view.value ? view.value.sheets : []).filter(s => s.ai_status === 'engine_only'));
                    const mdUrl = () => `${VC.API}${base()}/jobs/${job.value.job_id}/view.md`;
                    const toggle = k => { open[k] = !open[k]; };
                    const isOpen = k => !!open[k];
                    const typeLabel = ev => (TYPE_LABEL[ev.type] || ev.type) + (ev.label ? ': ' + ev.label : '');
                    const fmtTime = t => (t || '').replace('T', ' ').slice(0, 16);
                    onMounted(load);
                    onBeforeUnmount(() => clearTimeout(timer));
                    return {loading, error, notice, job, view, enabled, resume, estimate, consent, starting, estimating, newJob, pick, picked, pickedCalls, pickedImages, buckets: BUCKETS,
                        aiSheets, engineOnly, loadEstimate, start, goto, gotoRef, summaryCats, checkClasses, open, mdUrl, toggle, isOpen, typeLabel, fmtTime,
                        fateLabel: f => FATE_LABEL[f] || f, aiStatus: s => AI_STATUS[s] || s,
                        jobState: s => JOB_STATE[s] || s};
                },
            });
        },
    };
})(typeof window !== 'undefined' ? window : globalThis);
