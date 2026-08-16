(function initDistributedPage(root) {
    'use strict';

    root.DistributedPage = {
        props: { distributed: { type: Object, required: true } },
        template: `
        <div class="container distributed-page">
            <header class="distributed-page-header">
                <div>
                    <div class="distributed-page-header__eyebrow">Диспетчерский центр</div>
                    <h1>Распределённые вычисления</h1>
                    <p>Мощности, лимиты провайдеров и распределение проверок проектной документации.</p>
                </div>
                <div class="distributed-page-header__status">
                    <span class="distributed-demo-badge" title="Все действия изменяют только локальные mock-данные">Демо-данные · без запуска задач</span>
                    <span class="distributed-system-badge"><i></i>Система в норме</span>
                </div>
            </header>

            <nav class="distributed-tabs" aria-label="Разделы распределённых вычислений">
                <button v-for="tab in distributed.tabs" :key="tab.key"
                        class="distributed-tab" :class="{active: distributed.activeTab === tab.key}"
                        :aria-current="distributed.activeTab === tab.key ? 'page' : null"
                        @click="distributed.goToTab(tab.key)">{{ tab.label }}</button>
            </nav>

            <div v-if="distributed.loading" class="distributed-loading" aria-live="polite">
                <div class="distributed-loading__mark"></div>
                <p>Загружаем состояние вычислительных узлов…</p>
            </div>
            <div v-else-if="distributed.error" class="distributed-error" role="alert">
                <div class="distributed-error__icon">!</div>
                <h3>Не удалось получить данные</h3>
                <p>{{ distributed.error }}</p>
                <button class="btn btn-primary" @click="distributed.refresh()">Повторить</button>
            </div>

            <template v-else-if="distributed.loaded">
                <!-- Обзор -->
                <div v-if="distributed.activeTab === 'overview'">
                    <template v-if="distributed.overview && distributed.overview.workers.length">
                        <section class="distributed-kpis" aria-label="Основные показатели">
                            <article class="distributed-kpi distributed-kpi--online">
                                <span class="distributed-kpi__icon">●</span><strong>{{ distributed.overview.kpis.online }} / {{ distributed.overview.kpis.totalWorkers }}</strong><span>Серверы онлайн</span>
                            </article>
                            <article class="distributed-kpi"><span class="distributed-kpi__icon">▶</span><strong>{{ distributed.overview.kpis.active }}</strong><span>Выполняется</span></article>
                            <article class="distributed-kpi"><span class="distributed-kpi__icon">≡</span><strong>{{ distributed.overview.kpis.queued }}</strong><span>В очереди</span></article>
                            <article class="distributed-kpi distributed-kpi--error"><span class="distributed-kpi__icon">!</span><strong>{{ distributed.overview.kpis.errors }}</strong><span>Ошибки</span></article>
                            <article class="distributed-kpi"><span class="distributed-kpi__icon">◇</span><strong>{{ distributed.overview.kpis.freeSlots }} / {{ distributed.overview.kpis.totalSlots }}</strong><span>Свободные слоты</span></article>
                            <article class="distributed-kpi"><span class="distributed-kpi__icon">✓</span><strong>{{ distributed.overview.kpis.completedToday }}</strong><span>Завершено сегодня</span></article>
                        </section>

                        <section class="distributed-section">
                            <div class="distributed-section-heading">
                                <div><h2>Вычислительные узлы</h2><p>Ресурсы, доступные слоты и примерные остатки лимитов</p></div>
                                <button class="distributed-section-heading__action" @click="distributed.goToTab('workers')">Все воркеры →</button>
                            </div>
                            <div class="distributed-workers-grid">
                                <distributed-worker-card v-for="worker in distributed.overview.workers" :key="worker.id" :worker="worker" :detailed="false" @task="distributed.openTask"></distributed-worker-card>
                            </div>
                        </section>

                        <section v-if="distributed.overview.recommendation && distributed.recommendationProject && distributed.recommendationWorker" class="distributed-section">
                            <div class="distributed-section-heading"><div><h2>Следующая задача</h2><p>Рекомендация на основе свободных мощностей и лимитов</p></div></div>
                            <article class="distributed-recommendation">
                                <div>
                                    <div class="distributed-recommendation__eyebrow">Готова к назначению</div>
                                    <h3>{{ distributed.recommendationProject.project }} → {{ distributed.recommendationProject.packageName }}</h3>
                                    <span class="distributed-recommendation__mode">{{ distributed.modeLabel(distributed.recommendationProject.mode) }}</span>
                                </div>
                                <div class="distributed-recommendation__target">
                                    <span>Система предлагает</span><strong>VPS {{ distributed.recommendationWorker.name }}</strong>
                                    <div class="distributed-recommendation__facts">
                                        <span>{{ distributed.overview.recommendation.freeSlots }} свободный слот</span><span>GPU {{ distributed.overview.recommendation.gpu }}%</span><span>Claude {{ distributed.overview.recommendation.claude }}%</span><span>Codex {{ distributed.overview.recommendation.codex }}%</span>
                                    </div>
                                </div>
                                <div class="distributed-recommendation__actions">
                                    <button class="btn btn-primary btn-sm" @click="distributed.openSend(distributed.recommendationProject, distributed.recommendationWorker.id)">Отправить сейчас</button>
                                    <button class="btn btn-outline btn-sm" @click="distributed.openAlternative()">Другой VPS</button>
                                    <button class="btn btn-ghost btn-sm" @click="distributed.openWhy()">Почему выбран?</button>
                                </div>
                            </article>
                        </section>

                        <section class="distributed-section distributed-overview-split">
                            <article class="distributed-panel">
                                <header class="distributed-panel__header"><div><h2>Проекты для проверки</h2><p>Ручное назначение или автоматический выбор VPS</p></div></header>
                                <div class="distributed-table-wrap">
                                    <table class="distributed-table">
                                        <thead><tr><th>Проект / комплект</th><th>Режим</th><th>Размер</th><th>Приоритет</th><th>Статус</th><th>Назначение</th><th></th></tr></thead>
                                        <tbody>
                                            <tr v-for="project in distributed.overview.projects" :key="project.id">
                                                <td class="distributed-table__project"><strong>{{ project.project }}</strong><span>{{ project.packageName }}</span></td>
                                                <td><span class="distributed-mode">{{ distributed.modeLabel(project.mode) }}</span></td>
                                                <td>{{ project.pageCount }} стр. / {{ project.blockCount }} блоков</td>
                                                <td><span class="distributed-priority" :class="'distributed-priority--' + project.priority">{{ distributed.priorityLabel(project.priority) }}</span></td>
                                                <td><span class="distributed-chip" :class="project.status === 'Добавлен в очередь' ? 'distributed-chip--green' : ''">{{ project.status }}</span></td>
                                                <td>
                                                    <select class="form-input" :value="distributed.assignmentByProject[project.id]" @change="distributed.setAssignment(project.id, $event.target.value)" :aria-label="'Назначение для ' + project.project + ' ' + project.packageName">
                                                        <option value="auto">Автоматически</option>
                                                        <option v-for="worker in distributed.workers" :key="worker.id" :value="worker.id" :disabled="worker.status === 'offline'">VPS {{ worker.name }}{{ worker.status === 'offline' ? ' · offline' : '' }}</option>
                                                    </select>
                                                </td>
                                                <td class="distributed-table__actions"><button class="btn btn-primary btn-sm" @click="distributed.openSend(project)">Отправить</button></td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </div>
                            </article>

                            <article class="distributed-panel">
                                <header class="distributed-panel__header"><div><h2>Очередь</h2><p>Ближайшие пять проверок</p></div><button class="distributed-section-heading__action" @click="distributed.goToTab('queue')">Открыть всю →</button></header>
                                <div class="distributed-queue-preview">
                                    <div v-for="item in distributed.overview.queuePreview" :key="item.id" class="distributed-queue-preview__item">
                                        <span class="distributed-queue-preview__number">{{ item.position }}</span>
                                        <div><div class="distributed-queue-preview__project">{{ item.project }} → {{ item.packageName }}</div><div class="distributed-queue-preview__meta"><span>{{ distributed.modeLabel(item.mode) }}</span><span>VPS {{ distributed.workerName(item.suggestedWorkerId) }}</span><span>Старт {{ item.expectedStart }}</span></div></div>
                                    </div>
                                </div>
                            </article>
                        </section>

                        <section class="distributed-section">
                            <div class="distributed-section-heading"><div><h2>Требует внимания</h2><p>Проблемы, для которых может понадобиться решение оператора</p></div></div>
                            <div v-if="distributed.overview.attention.length" class="distributed-attention">
                                <article v-for="issue in distributed.overview.attention" :key="issue.id" class="distributed-attention-card" :class="'distributed-attention-card--' + issue.severity">
                                    <span class="distributed-attention-card__icon">!</span>
                                    <div><h3>VPS {{ distributed.workerName(issue.workerId) }} · {{ issue.title }}</h3><p>{{ issue.description }}</p></div>
                                    <div class="distributed-attention-card__actions">
                                        <button class="btn btn-outline btn-sm" @click="distributed.openAttentionTask(issue.taskId)">Подробнее</button>
                                        <button class="btn btn-outline btn-sm" @click="distributed.retryAttentionTask(issue.taskId)">Повторить</button>
                                        <button class="btn btn-outline btn-sm" @click="distributed.transferAttentionTask(issue.taskId)">Перенести</button>
                                    </div>
                                </article>
                            </div>
                            <div v-else class="distributed-all-good">✓ Проблем не обнаружено</div>
                        </section>
                    </template>
                    <div v-else class="distributed-empty"><div class="distributed-empty__icon">◇</div><h3>Вычислительные узлы не добавлены</h3><p>После подключения первого VPS здесь появятся ресурсы, лимиты и доступные слоты.</p></div>
                </div>

                <!-- Очередь -->
                <div v-else-if="distributed.activeTab === 'queue'">
                    <div class="distributed-section-heading"><div><h2>Очередь проверок</h2><p>{{ distributed.queue.length }} комплектов ожидают запуска · изменения сохраняются только в демо-состоянии</p></div></div>
                    <article v-if="distributed.queue.length" class="distributed-panel">
                        <div class="distributed-table-wrap">
                            <table class="distributed-table distributed-table--queue">
                                <thead><tr><th>Позиция</th><th>Проект</th><th>Комплект</th><th>Режим</th><th>Приоритет</th><th>Размер</th><th>Предполагаемый VPS</th><th>Ожидаемый старт</th><th>Статус</th><th>Порядок</th></tr></thead>
                                <tbody>
                                    <tr v-for="(item, index) in distributed.queue" :key="item.id">
                                        <td><span class="distributed-position">{{ item.position }}</span></td><td class="distributed-table__project"><strong>{{ item.project }}</strong></td><td>{{ item.packageName }}</td><td><span class="distributed-mode">{{ distributed.modeLabel(item.mode) }}</span></td>
                                        <td><select class="form-input" :value="item.priority" @change="distributed.changePriority(item, $event.target.value)" :aria-label="'Приоритет ' + item.project + ' ' + item.packageName"><option value="critical">Критический</option><option value="high">Высокий</option><option value="normal">Обычный</option><option value="low">Низкий</option></select></td>
                                        <td>{{ item.pageCount }} стр. / {{ item.blockCount }} блоков</td>
                                        <td>VPS {{ distributed.workerName(item.suggestedWorkerId) }}<button class="distributed-info-button" @click="distributed.openQueueWhy(item)" aria-label="Почему выбран этот VPS?">i</button></td>
                                        <td>{{ item.expectedStart }}</td><td><span class="distributed-chip">{{ item.status }}</span></td>
                                        <td class="distributed-table__actions">
                                            <button class="distributed-icon-action" :disabled="index === 0" @click="distributed.moveQueue(item, 'first')" title="На первое место">⇈</button><button class="distributed-icon-action" :disabled="index === 0" @click="distributed.moveQueue(item, 'up')" title="Выше">↑</button><button class="distributed-icon-action" :disabled="index === distributed.queue.length - 1" @click="distributed.moveQueue(item, 'down')" title="Ниже">↓</button><button class="distributed-icon-action" :disabled="index === distributed.queue.length - 1" @click="distributed.moveQueue(item, 'last')" title="В конец">⇊</button>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </article>
                    <div v-else class="distributed-empty"><div class="distributed-empty__icon">≡</div><h3>Очередь пуста</h3><p>Все подготовленные комплекты уже назначены или завершены.</p></div>
                </div>

                <!-- Задачи -->
                <div v-else-if="distributed.activeTab === 'tasks'">
                    <div class="distributed-toolbar">
                        <div class="distributed-subtabs" role="tablist" aria-label="Состояние задач">
                            <button class="distributed-subtab" :class="{active: distributed.taskSubtab === 'active'}" @click="distributed.taskSubtab = 'active'">Активные <b>{{ distributed.tasks.active.length }}</b></button><button class="distributed-subtab" :class="{active: distributed.taskSubtab === 'completed'}" @click="distributed.taskSubtab = 'completed'">Завершённые <b>{{ distributed.tasks.completed.length }}</b></button><button class="distributed-subtab" :class="{active: distributed.taskSubtab === 'errors'}" @click="distributed.taskSubtab = 'errors'">Ошибки <b>{{ distributed.tasks.errors.length }}</b></button>
                        </div>
                        <div v-if="distributed.taskSubtab === 'completed'" class="distributed-periods">
                            <button v-for="period in [{key:'today',label:'Сегодня'},{key:'7d',label:'7 дней'},{key:'30d',label:'30 дней'},{key:'custom',label:'Период'}]" :key="period.key" class="distributed-period" :class="{active: distributed.historyPeriod === period.key}" @click="distributed.historyPeriod = period.key">{{ period.label }}</button>
                        </div>
                    </div>
                    <div v-if="distributed.taskSubtab === 'completed' && distributed.historyPeriod === 'custom'" class="distributed-date-range" style="margin-bottom:10px"><input type="date" class="form-input" v-model="distributed.historyFrom" aria-label="Начало периода"><span>—</span><input type="date" class="form-input" v-model="distributed.historyTo" aria-label="Конец периода"></div>

                    <article v-if="distributed.taskSubtab === 'active' && distributed.tasks.active.length" class="distributed-panel">
                        <div class="distributed-table-wrap"><table class="distributed-table distributed-table--tasks">
                            <thead><tr><th>Проект</th><th>VPS</th><th>Режим</th><th>Прогресс</th><th>Время</th><th>Последняя активность</th><th>Статус</th></tr></thead>
                            <tbody><tr v-for="task in distributed.tasks.active" :key="task.id" tabindex="0" class="distributed-table__row-button" @click="distributed.openTask(task)" @keyup.enter="distributed.openTask(task)">
                                <td class="distributed-table__project"><strong>{{ task.project }}</strong><span>{{ task.packageName }}</span></td><td>VPS {{ distributed.workerName(task.workerId) }}</td><td>{{ distributed.modeLabel(task.mode) }}</td><td><div class="distributed-task-progress"><div class="distributed-progress distributed-progress--task"><span :style="distributed.progressStyle(task.progress)"></span></div><b>{{ task.progress }}%</b></div></td><td>{{ task.duration }}</td><td>{{ task.lastActivity }}</td><td><span class="distributed-chip distributed-chip--green">{{ task.status }}</span></td>
                            </tr></tbody>
                        </table></div>
                    </article>
                    <article v-else-if="distributed.taskSubtab === 'completed' && distributed.visibleCompletedTasks.length" class="distributed-panel">
                        <div class="distributed-table-wrap"><table class="distributed-table distributed-table--tasks">
                            <thead><tr><th>Проект</th><th>VPS</th><th>Режим</th><th>Время проверки</th><th>Результат</th><th>Завершено</th><th>Статус</th></tr></thead>
                            <tbody><tr v-for="task in distributed.visibleCompletedTasks" :key="task.id" tabindex="0" class="distributed-table__row-button" @click="distributed.openTask(task)" @keyup.enter="distributed.openTask(task)"><td class="distributed-table__project"><strong>{{ task.project }}</strong><span>{{ task.packageName }}</span></td><td>VPS {{ distributed.workerName(task.workerId) }}</td><td>{{ distributed.modeLabel(task.mode) }}</td><td>{{ task.duration }}</td><td>{{ task.result }}</td><td>{{ task.completedAt }}</td><td><span class="distributed-chip distributed-chip--green">Готово</span></td></tr></tbody>
                        </table></div>
                    </article>
                    <article v-else-if="distributed.taskSubtab === 'errors' && distributed.tasks.errors.length" class="distributed-panel">
                        <div class="distributed-table-wrap"><table class="distributed-table distributed-table--tasks">
                            <thead><tr><th>Проект</th><th>VPS</th><th>Этап</th><th>Что произошло</th><th>Когда</th><th>Действия</th></tr></thead>
                            <tbody><tr v-for="task in distributed.tasks.errors" :key="task.id"><td class="distributed-table__project"><strong>{{ task.project }}</strong><span>{{ task.packageName }}</span></td><td>VPS {{ distributed.workerName(task.workerId) }}</td><td><span class="distributed-chip distributed-chip--red">{{ task.status }}</span></td><td class="distributed-error-message">{{ task.errorMessage }}</td><td>{{ task.lastActivity }}</td><td class="distributed-table__actions"><button class="btn btn-outline btn-sm" @click="distributed.openTask(task)">Подробнее</button><button class="btn btn-outline btn-sm" @click="distributed.retryTask(task)">Повторить</button><button class="btn btn-outline btn-sm" @click="distributed.openTransfer(task)">Перенести</button></td></tr></tbody>
                        </table></div>
                    </article>
                    <div v-else class="distributed-empty"><div class="distributed-empty__icon">✓</div><h3>{{ distributed.taskSubtab === 'errors' ? 'Ошибок нет' : 'Задач пока нет' }}</h3><p>{{ distributed.taskSubtab === 'errors' ? 'Все проверки выполняются штатно.' : 'В выбранном периоде нет задач.' }}</p></div>
                </div>

                <!-- Воркеры -->
                <div v-else-if="distributed.activeTab === 'workers'">
                    <div class="distributed-section-heading"><div><h2>Воркеры</h2><p>Детальное состояние VPS и демо-управление приёмом новых задач</p></div></div>
                    <div v-if="distributed.workers.length" class="distributed-workers-grid">
                        <distributed-worker-card v-for="worker in distributed.workers" :key="worker.id" :worker="worker" :detailed="true" @detail="distributed.openWorker" @toggle-intake="distributed.toggleWorkerIntake(worker, $event)" @task="distributed.openTask"></distributed-worker-card>
                    </div>
                    <div v-else class="distributed-empty"><div class="distributed-empty__icon">◇</div><h3>Воркеры не подключены</h3><p>Сведения появятся после регистрации вычислительного узла.</p></div>
                </div>

                <!-- Лимиты -->
                <div v-else-if="distributed.activeTab === 'limits'">
                    <div v-if="distributed.limits.length">
                        <div class="distributed-limits-summary">
                            <article class="distributed-summary-card distributed-summary-card--accent"><span>Claude · средний остаток</span><strong>{{ distributed.limitsSummary.claude }}%</strong></article><article class="distributed-summary-card distributed-summary-card--accent"><span>Codex · средний остаток</span><strong>{{ distributed.limitsSummary.codex }}%</strong></article><article class="distributed-summary-card"><span>Ближайший сброс</span><strong>{{ distributed.limitsSummary.nextReset }}</strong></article><article class="distributed-summary-card"><span>Больше всего Claude</span><strong>VPS {{ distributed.limitsSummary.bestClaude }}</strong></article><article class="distributed-summary-card"><span>Больше всего Codex</span><strong>VPS {{ distributed.limitsSummary.bestCodex }}</strong></article>
                        </div>
                        <div class="distributed-provider-columns">
                            <article v-for="provider in [{key:'claude',label:'Claude'},{key:'codex',label:'Codex'}]" :key="provider.key" class="distributed-panel">
                                <header class="distributed-panel__header"><div><h2>{{ provider.label }}</h2><p>Примерная доступность подписки по VPS</p></div><span class="distributed-demo-badge">≈ оценка</span></header>
                                <div class="distributed-provider-list">
                                    <div v-for="item in distributed.limits" :key="provider.key + item.workerId" class="distributed-provider-row">
                                        <div class="distributed-provider-row__worker"><strong>{{ item.workerName }}</strong><span>{{ item.online ? (item.stale ? 'последние данные' : 'online') : 'offline · последние данные' }}</span></div>
                                        <div class="distributed-provider-row__quota"><div class="distributed-progress" :class="item[provider.key].status === 'critical' ? 'distributed-progress--danger' : item[provider.key].status === 'warning' ? 'distributed-progress--warning' : ''"><span :style="distributed.progressStyle(item[provider.key].percentageRemaining)"></span></div><strong>{{ item[provider.key].percentageRemaining }}%</strong></div>
                                        <div class="distributed-provider-row__reset">{{ item.stale ? 'Последний известный reset: ' : 'Сброс через ' }}{{ item[provider.key].resetIn }}</div><div class="distributed-provider-row__used">Использовано сегодня {{ item[provider.key].usedToday }}%</div>
                                    </div>
                                </div>
                            </article>
                        </div>
                    </div>
                    <div v-else class="distributed-empty"><div class="distributed-empty__icon">%</div><h3>Нет данных о лимитах</h3><p>Провайдеры ещё не передали оценку доступного остатка.</p></div>
                </div>

                <!-- Диагностика -->
                <div v-else-if="distributed.activeTab === 'diagnostics'">
                    <div class="distributed-diagnostics-intro"><div><h3>Безопасная техническая метаинформация</h3><p>Секреты, ключи, данные авторизации и закрытые материалы сертификатов сюда не попадают.</p></div><button class="btn btn-outline btn-sm" @click="distributed.copyDiagnostics()">Копировать диагностику</button></div>
                    <article v-if="distributed.diagnostics.length" class="distributed-panel">
                        <div class="distributed-table-wrap"><table class="distributed-table distributed-table--diagnostics">
                            <thead><tr><th>Worker</th><th>Instance</th><th>Transport</th><th>gRPC stream</th><th>mTLS</th><th>Heartbeat</th><th>Agent / Executor</th><th>EventOutbox</th><th>Версия</th><th></th></tr></thead>
                            <tbody><tr v-for="row in distributed.diagnostics" :key="row.diagnostic.workerId"><td class="distributed-table__project"><strong>{{ row.workerName }}</strong><span class="distributed-mono">{{ row.diagnostic.workerId }}</span></td><td class="distributed-mono">{{ row.diagnostic.instanceId }}</td><td>{{ row.diagnostic.transport }}</td><td><span class="distributed-chip" :class="row.diagnostic.grpcStream === 'connected' ? 'distributed-chip--green' : row.diagnostic.grpcStream === 'disconnected' ? 'distributed-chip--red' : 'distributed-chip--amber'">{{ row.diagnostic.grpcStream }}</span></td><td>{{ row.diagnostic.mtls }}</td><td>{{ row.diagnostic.heartbeat }}</td><td>{{ row.diagnostic.agentStatus }} / {{ row.diagnostic.executorStatus }}</td><td class="distributed-mono">{{ row.diagnostic.eventOutbox.lastAckedSeq }} / {{ row.diagnostic.eventOutbox.lastWrittenSeq }} · pending {{ row.diagnostic.eventOutbox.pending }}</td><td>{{ row.diagnostic.workerVersion }}</td><td><button class="btn btn-outline btn-sm" @click="distributed.openDiagnostic(row)">Подробнее</button></td></tr></tbody>
                        </table></div>
                    </article>
                    <div v-else class="distributed-empty"><div class="distributed-empty__icon">⌁</div><h3>Диагностика недоступна</h3><p>Нет безопасной метаинформации о подключённых воркерах.</p></div>
                </div>
            </template>

            <!-- Task details drawer -->
            <div v-if="distributed.selectedTask" class="distributed-drawer-overlay" @click.self="distributed.closeTask()">
                <aside class="distributed-drawer" role="dialog" aria-modal="true" aria-label="Подробности задачи">
                    <header class="distributed-drawer__header"><div><div class="distributed-drawer__eyebrow">Задача аудита</div><h2>{{ distributed.selectedTask.project }}</h2><p>{{ distributed.selectedTask.packageName }}</p></div><button class="distributed-close" @click="distributed.closeTask()" aria-label="Закрыть">×</button></header>
                    <div class="distributed-detail-grid"><div class="distributed-detail"><span>VPS</span><strong>{{ distributed.workerName(distributed.selectedTask.workerId) }}</strong></div><div class="distributed-detail"><span>Режим</span><strong>{{ distributed.modeLabel(distributed.selectedTask.mode) }}</strong></div><div class="distributed-detail"><span>Прогресс</span><strong>{{ distributed.selectedTask.progress }}%</strong></div><div class="distributed-detail"><span>Время</span><strong>{{ distributed.selectedTask.duration }}</strong></div></div>
                    <section class="distributed-drawer-section"><h3>Этапы</h3><div class="distributed-stage-list"><div v-for="stage in ['transfer','preparing','auditing','collecting','returning','done']" :key="stage" class="distributed-stage" :class="'distributed-stage--' + distributed.taskStageState(distributed.selectedTask, stage)"><i>{{ distributed.taskStageText(distributed.selectedTask, stage) }}</i><span>{{ distributed.stageLabel(stage) }}</span><b v-if="stage === distributed.selectedTask.stage && stage === 'auditing'">{{ distributed.selectedTask.progress }}%</b></div></div></section>
                    <section v-if="distributed.selectedTask.errorMessage" class="distributed-drawer-section"><h3>Что произошло</h3><p class="distributed-error-message">{{ distributed.selectedTask.errorMessage }}</p></section>
                    <section class="distributed-drawer-section"><h3>Использование моделей</h3><div class="distributed-detail-grid" style="padding:0"><div class="distributed-detail"><span>Claude</span><strong>{{ distributed.selectedTask.modelUsage.claude }}</strong></div><div class="distributed-detail"><span>Codex</span><strong>{{ distributed.selectedTask.modelUsage.codex }}</strong></div></div></section>
                    <section class="distributed-drawer-section"><h3>Краткие события</h3><div class="distributed-events"><div v-for="event in distributed.selectedTask.events" :key="event.at + event.text" class="distributed-event"><time>{{ event.at }}</time><span>{{ event.text }}</span></div></div></section>
                    <details class="distributed-technical"><summary>Техническая информация</summary><pre><span>task_id: {{ distributed.selectedTask.id }}</span>\n<span>stage: {{ distributed.selectedTask.stage }}</span>\n<span>worker_id: {{ distributed.selectedTask.workerId }}</span><span v-if="distributed.selectedTask.technicalCode">\ncode: {{ distributed.selectedTask.technicalCode }}</span></pre></details>
                    <footer v-if="distributed.selectedTask.stage === 'error'" class="distributed-drawer__actions"><button class="btn btn-outline" @click="distributed.openTransfer(distributed.selectedTask)">Перенести</button><button class="btn btn-primary" @click="distributed.retryTask(distributed.selectedTask)">Повторить</button></footer>
                </aside>
            </div>

            <!-- Worker details drawer -->
            <div v-if="distributed.selectedWorker" class="distributed-drawer-overlay" @click.self="distributed.closeWorker()">
                <aside class="distributed-drawer distributed-drawer--wide" role="dialog" aria-modal="true" aria-label="Подробности воркера">
                    <header class="distributed-drawer__header"><div><div class="distributed-drawer__eyebrow">Вычислительный узел</div><h2>VPS {{ distributed.selectedWorker.name }}</h2><p>{{ distributed.selectedWorker.diagnostic.workerId }} · uptime {{ distributed.selectedWorker.uptime }}</p></div><button class="distributed-close" @click="distributed.closeWorker()">×</button></header>
                    <section class="distributed-drawer-section"><div class="distributed-detail-grid"><div class="distributed-detail"><span>Состояние</span><strong>{{ distributed.selectedWorker.status }}</strong></div><div class="distributed-detail"><span>Последний heartbeat</span><strong>{{ distributed.selectedWorker.lastHeartbeat }}</strong></div><div class="distributed-detail"><span>Слоты</span><strong>{{ distributed.selectedWorker.slots.used }} занято / {{ distributed.selectedWorker.slots.total }} всего</strong></div><div class="distributed-detail"><span>OpenRouter</span><strong>{{ distributed.selectedWorker.openRouter.status }} · {{ distributed.selectedWorker.openRouter.usedToday }}</strong></div><div class="distributed-detail"><span>Диск</span><strong>{{ distributed.selectedWorker.resources.disk }}%</strong></div><div class="distributed-detail"><span>VRAM</span><strong>{{ distributed.selectedWorker.resources.vramUsedGb }} / {{ distributed.selectedWorker.resources.vramTotalGb }} ГБ</strong></div></div></section>
                    <section class="distributed-drawer-section"><h3>Лимиты провайдеров</h3><div class="distributed-quota-grid"><distributed-quota-bar label="Claude" :quota="distributed.selectedWorker.quotas.claude" :stale="distributed.selectedWorker.quotaDataStale"></distributed-quota-bar><distributed-quota-bar label="Codex" :quota="distributed.selectedWorker.quotas.codex" :stale="distributed.selectedWorker.quotaDataStale"></distributed-quota-bar></div></section>
                    <section class="distributed-drawer-section"><h3>Текущие задачи</h3><div v-if="distributed.selectedWorker.currentTasks.length" class="distributed-events"><button v-for="task in distributed.selectedWorker.currentTasks" :key="task.id" class="distributed-worker-task" @click="distributed.openTask(task)"><span><strong>{{ task.project }}</strong> → {{ task.packageName }}</span><span>{{ task.progress }}%</span></button></div><div v-else class="distributed-all-good">Активных задач нет</div></section>
                    <footer class="distributed-drawer__actions"><button class="btn btn-outline" @click="distributed.openWorkerDiagnostic()">Открыть диагностику</button></footer>
                </aside>
            </div>

            <!-- Diagnostic details drawer -->
            <div v-if="distributed.selectedDiagnostic" class="distributed-drawer-overlay" @click.self="distributed.closeDiagnostic()">
                <aside class="distributed-drawer" role="dialog" aria-modal="true" aria-label="Диагностика воркера">
                    <header class="distributed-drawer__header"><div><div class="distributed-drawer__eyebrow">Безопасная диагностика</div><h2>VPS {{ distributed.selectedDiagnostic.workerName }}</h2><p>Секретные поля исключены из модели</p></div><button class="distributed-close" @click="distributed.closeDiagnostic()">×</button></header>
                    <section class="distributed-drawer-section"><dl class="distributed-diagnostic-list"><template v-for="row in distributed.diagnosticRows(distributed.selectedDiagnostic.diagnostic)" :key="row[0]"><dt>{{ row[0] }}</dt><dd>{{ row[1] }}</dd></template></dl></section>
                    <footer class="distributed-drawer__actions"><button class="btn btn-primary" @click="distributed.copyDiagnostics()">Копировать безопасный JSON</button></footer>
                </aside>
            </div>

            <!-- Distributed demo modals -->
            <div v-if="distributed.modal" class="modal-overlay" @click.self="distributed.closeModal()">
                <div class="modal-content distributed-modal" role="dialog" aria-modal="true">
                    <button class="modal-close" @click="distributed.closeModal()">×</button>
                    <template v-if="distributed.modal.type === 'send'">
                        <div class="distributed-modal__icon">→</div><h3>Отправить проект на проверку?</h3><p class="distributed-modal__lead">Действие обновит только демо-состояние. Реальная задача и scheduler запущены не будут.</p>
                        <div class="distributed-confirm-card"><div class="distributed-confirm-row"><span>Проект</span><strong>{{ distributed.modal.project.project }} → {{ distributed.modal.project.packageName }}</strong></div><div class="distributed-confirm-row"><span>VPS</span><strong>{{ distributed.modal.workerId === 'auto' ? 'Автоматически' : 'VPS ' + distributed.workerName(distributed.modal.workerId) }}</strong></div><div class="distributed-confirm-row"><span>Режим</span><strong>{{ distributed.modeLabel(distributed.modal.project.mode) }}</strong></div></div>
                        <div class="distributed-modal__actions"><button class="btn btn-outline" @click="distributed.closeModal()">Отмена</button><button class="btn btn-primary" @click="distributed.confirmSend()">Отправить</button></div>
                    </template>
                    <template v-else-if="distributed.modal.type === 'why'">
                        <div class="distributed-modal__icon">i</div><h3>Почему выбран VPS {{ distributed.recommendationWorker.name }}?</h3><p class="distributed-modal__lead">Рекомендация объясняется понятными оператору факторами; техническая формула score здесь не используется.</p><ul class="distributed-reasons"><li v-for="reason in distributed.modal.recommendation.reasons" :key="reason">{{ reason }}</li></ul><div class="distributed-modal__actions"><button class="btn btn-primary" @click="distributed.closeModal()">Понятно</button></div>
                    </template>
                    <template v-else-if="distributed.modal.type === 'alternative'">
                        <div class="distributed-modal__icon">◇</div><h3>Выбрать другой VPS</h3><p class="distributed-modal__lead">Для {{ distributed.modal.project.project }} → {{ distributed.modal.project.packageName }}</p><label class="form-row"><span>Вычислительный узел</span><select class="form-input" v-model="distributed.modal.workerId"><option v-for="worker in distributed.workers" :key="worker.id" :value="worker.id" :disabled="worker.status === 'offline'">VPS {{ worker.name }} · {{ worker.slots.total - worker.slots.used }} свободно · Claude {{ worker.quotas.claude.percentageRemaining }}% · Codex {{ worker.quotas.codex.percentageRemaining }}%</option></select></label><div class="distributed-modal__actions"><button class="btn btn-outline" @click="distributed.closeModal()">Отмена</button><button class="btn btn-primary" @click="distributed.applyAlternative()">Выбрать</button></div>
                    </template>
                    <template v-else-if="distributed.modal.type === 'queue-why'">
                        <div class="distributed-modal__icon">i</div><h3>Почему VPS {{ distributed.modal.worker.name }}?</h3><p class="distributed-modal__lead">Предполагаемое назначение для {{ distributed.modal.item.project }} → {{ distributed.modal.item.packageName }}</p><div class="distributed-confirm-card"><div class="distributed-confirm-row"><span>Claude</span><strong>{{ distributed.modal.worker.quotas.claude.percentageRemaining }}% осталось</strong></div><div class="distributed-confirm-row"><span>Codex</span><strong>{{ distributed.modal.worker.quotas.codex.percentageRemaining }}% осталось</strong></div><div class="distributed-confirm-row"><span>Слоты</span><strong>{{ distributed.modal.worker.slots.total - distributed.modal.worker.slots.used }} свободно</strong></div><div class="distributed-confirm-row"><span>GPU</span><strong>{{ distributed.modal.worker.resources.gpu }}%</strong></div></div><div class="distributed-modal__actions"><button class="btn btn-primary" @click="distributed.closeModal()">Закрыть</button></div>
                    </template>
                    <template v-else-if="distributed.modal.type === 'transfer'">
                        <div class="distributed-modal__icon">⇄</div><h3>Перенести задачу</h3><p class="distributed-modal__lead">{{ distributed.modal.task.project }} → {{ distributed.modal.task.packageName }}. Изменение остаётся в demo state.</p><label class="form-row"><span>Новый VPS</span><select class="form-input" v-model="distributed.transferWorkerId"><option v-for="worker in distributed.workers" :key="worker.id" :value="worker.id" :disabled="worker.status === 'offline'">VPS {{ worker.name }} · {{ worker.slots.total - worker.slots.used }} свободно</option></select></label><div class="distributed-modal__actions"><button class="btn btn-outline" @click="distributed.closeModal()">Отмена</button><button class="btn btn-primary" @click="distributed.confirmTransfer()">Перенести</button></div>
                    </template>
                </div>
            </div>

            <transition name="fade"><div v-if="distributed.toast" class="distributed-toast" :class="'distributed-toast--' + distributed.toast.tone" role="status"><i>{{ distributed.toast.tone === 'error' ? '!' : '✓' }}</i><span>{{ distributed.toast.message }}</span></div></transition>
        </div>`,
    };
})(typeof window !== 'undefined' ? window : globalThis);
