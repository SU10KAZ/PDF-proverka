// @ts-check
/**
 * Typed demo data/service layer for the "Distributed computing" UI.
 *
 * The UI only talks to the interface implemented by MockDistributedService.
 * A production adapter can replace it later without changing page templates.
 */
(function initDistributedData(root) {
    'use strict';

    /** @typedef {'full_codex'|'hybrid'} AuditMode */
    /** @typedef {'critical'|'high'|'normal'|'low'} TaskPriority */
    /** @typedef {'queued'|'transfer'|'preparing'|'auditing'|'collecting'|'returning'|'done'|'error'} TaskStage */
    /** @typedef {'online'|'busy'|'offline'} WorkerStatus */
    /** @typedef {'ok'|'warning'|'critical'|'unknown'} QuotaStatus */

    /**
     * @typedef {Object} ProviderQuota
     * @property {number} percentageRemaining
     * @property {string} resetAt
     * @property {string} resetIn
     * @property {QuotaStatus} status
     * @property {string} source
     * @property {boolean} isEstimated
     * @property {number} usedToday
     */

    /**
     * @typedef {Object} WorkerResources
     * @property {number} cpu
     * @property {number} ram
     * @property {number} gpu
     * @property {number} vramUsedGb
     * @property {number} vramTotalGb
     * @property {number} disk
     */

    /**
     * @typedef {Object} WorkerTaskSummary
     * @property {string} id
     * @property {string} project
     * @property {string} packageName
     * @property {AuditMode} mode
     * @property {number} progress
     * @property {string} duration
     * @property {TaskStage} stage
     */

    /**
     * @typedef {Object} WorkerDiagnostic
     * @property {string} workerId
     * @property {string} instanceId
     * @property {string} transport
     * @property {string} grpcStream
     * @property {string} connectionId
     * @property {string} mtls
     * @property {string} heartbeat
     * @property {string} gatewayTarget
     * @property {string} sourceHost
     * @property {string} resultHost
     * @property {string} nginx
     * @property {string} agentStatus
     * @property {string} executorStatus
     * @property {{lastWrittenSeq:number,lastAckedSeq:number,pending:number}} eventOutbox
     * @property {string} resultAck
     * @property {string} workerVersion
     * @property {string} runtimeVersion
     * @property {string} uptime
     * @property {string} certExpiry
     */

    /**
     * @typedef {Object} WorkerNode
     * @property {string} id
     * @property {string} name
     * @property {string} location
     * @property {WorkerStatus} status
     * @property {string} lastHeartbeat
     * @property {string} uptime
     * @property {WorkerResources} resources
     * @property {{used:number,total:number}} slots
     * @property {{claude:ProviderQuota,codex:ProviderQuota}} quotas
     * @property {{status:string,usedToday:string}} openRouter
     * @property {WorkerTaskSummary[]} currentTasks
     * @property {boolean} acceptsNewTasks
     * @property {boolean} quotaDataStale
     * @property {WorkerDiagnostic} diagnostic
     */

    /**
     * @typedef {Object} AuditProject
     * @property {string} id
     * @property {string} project
     * @property {string} packageName
     * @property {AuditMode} mode
     * @property {number} pageCount
     * @property {number} blockCount
     * @property {number} packageSizeBytes
     * @property {TaskPriority} priority
     * @property {string} status
     * @property {string} assignment
     */

    /**
     * @typedef {Object} AuditQueueItem
     * @property {string} id
     * @property {number} position
     * @property {string} project
     * @property {string} packageName
     * @property {AuditMode} mode
     * @property {TaskPriority} priority
     * @property {number} pageCount
     * @property {number} blockCount
     * @property {string} suggestedWorkerId
     * @property {string} expectedStart
     * @property {string} status
     */

    /**
     * @typedef {Object} TaskEvent
     * @property {string} at
     * @property {string} text
     */

    /**
     * @typedef {Object} AuditTask
     * @property {string} id
     * @property {string} project
     * @property {string} packageName
     * @property {string} workerId
     * @property {AuditMode} mode
     * @property {number} progress
     * @property {string} duration
     * @property {string} lastActivity
     * @property {TaskStage} stage
     * @property {string} status
     * @property {string=} result
     * @property {string=} completedAt
     * @property {string=} completedAtIso
     * @property {string=} errorMessage
     * @property {string=} technicalCode
     * @property {TaskEvent[]} events
     * @property {{claude:string,codex:string}} modelUsage
     */

    /**
     * @typedef {Object} NextTaskRecommendation
     * @property {string} projectId
     * @property {string} workerId
     * @property {string[]} reasons
     * @property {number} freeSlots
     * @property {number} gpu
     * @property {number} claude
     * @property {number} codex
     */

    /** @param {unknown} value */
    function clone(value) {
        return JSON.parse(JSON.stringify(value));
    }

    /** @param {number} percentageRemaining */
    function quotaStatus(percentageRemaining) {
        if (percentageRemaining < 15) return 'critical';
        if (percentageRemaining < 25) return 'warning';
        return 'ok';
    }

    /** @returns {{workers:WorkerNode[],projects:AuditProject[],queue:AuditQueueItem[],tasks:{active:AuditTask[],completed:AuditTask[],errors:AuditTask[]},attention:Array<Record<string,unknown>>,recommendation:NextTaskRecommendation}} */
    function createDemoDataset() {
        /** @type {WorkerNode[]} */
        const workers = [
            {
                id: 'worker-mow-01', name: 'Москва-01', location: 'Москва', status: 'busy',
                lastHeartbeat: 'сейчас', uptime: '18 д 7 ч', acceptsNewTasks: false, quotaDataStale: false,
                resources: { cpu: 47, ram: 61, gpu: 82, vramUsedGb: 36, vramTotalGb: 48, disk: 54 },
                slots: { used: 2, total: 2 },
                quotas: {
                    claude: { percentageRemaining: 34, resetAt: '2026-08-16T18:18:00+03:00', resetIn: '6 ч 18 мин', status: 'ok', source: 'provider_estimate', isEstimated: true, usedToday: 66 },
                    codex: { percentageRemaining: 71, resetAt: '2026-08-18T16:00:00+03:00', resetIn: '2 д 4 ч', status: 'ok', source: 'provider_estimate', isEstimated: true, usedToday: 29 },
                },
                openRouter: { status: 'Доступен', usedToday: '$1.84' },
                currentTasks: [
                    { id: 'task-active-01', project: 'ЖК Алия', packageName: 'АР1.2-К6', mode: 'hybrid', progress: 68, duration: '18 мин', stage: 'auditing' },
                    { id: 'task-active-04', project: 'Мосфильмовская', packageName: 'КР-К2', mode: 'full_codex', progress: 54, duration: '21 мин', stage: 'auditing' },
                ],
                diagnostic: {
                    workerId: 'worker-mow-01', instanceId: 'mow01-a-7f21', transport: 'AgentStream v1', grpcStream: 'connected', connectionId: 'conn-mow01-3', mtls: 'verified', heartbeat: '4 сек назад', gatewayTarget: 'gateway.internal:443', sourceHost: 'central-source', resultHost: 'central-results', nginx: 'healthy', agentStatus: 'running', executorStatus: 'busy', eventOutbox: { lastWrittenSeq: 14582, lastAckedSeq: 14582, pending: 0 }, resultAck: 'confirmed', workerVersion: '1.12.4', runtimeVersion: '2026.08.15', uptime: '18 д 7 ч', certExpiry: 'через 71 день',
                },
            },
            {
                id: 'worker-mow-02', name: 'Москва-02', location: 'Москва', status: 'busy',
                lastHeartbeat: '6 сек назад', uptime: '11 д 3 ч', acceptsNewTasks: true, quotaDataStale: false,
                resources: { cpu: 58, ram: 69, gpu: 74, vramUsedGb: 27, vramTotalGb: 48, disk: 63 },
                slots: { used: 1, total: 2 },
                quotas: {
                    claude: { percentageRemaining: 82, resetAt: '2026-08-17T16:00:00+03:00', resetIn: '1 д 4 ч', status: 'ok', source: 'provider_estimate', isEstimated: true, usedToday: 18 },
                    codex: { percentageRemaining: 18, resetAt: '2026-08-16T19:00:00+03:00', resetIn: '7 ч', status: 'warning', source: 'provider_estimate', isEstimated: true, usedToday: 82 },
                },
                openRouter: { status: 'Доступен', usedToday: '$0.96' },
                currentTasks: [
                    { id: 'task-active-02', project: 'ЗИЛАРТ', packageName: 'ЭОМ-К3', mode: 'full_codex', progress: 31, duration: '7 мин', stage: 'auditing' },
                    { id: 'task-active-06', project: 'ЖК Алия', packageName: 'ОВ-К4', mode: 'hybrid', progress: 43, duration: '29 мин', stage: 'collecting' },
                ],
                diagnostic: {
                    workerId: 'worker-mow-02', instanceId: 'mow02-b-19ad', transport: 'AgentStream v1', grpcStream: 'degraded', connectionId: 'conn-mow02-8', mtls: 'verified', heartbeat: '6 сек назад', gatewayTarget: 'gateway.internal:443', sourceHost: 'central-source', resultHost: 'central-results', nginx: 'healthy', agentStatus: 'running', executorStatus: 'busy', eventOutbox: { lastWrittenSeq: 9971, lastAckedSeq: 9968, pending: 3 }, resultAck: 'waiting', workerVersion: '1.12.4', runtimeVersion: '2026.08.15', uptime: '11 д 3 ч', certExpiry: 'через 64 дня',
                },
            },
            {
                id: 'worker-mow-03', name: 'Москва-03', location: 'Москва', status: 'online',
                lastHeartbeat: '2 сек назад', uptime: '26 д 11 ч', acceptsNewTasks: true, quotaDataStale: false,
                resources: { cpu: 21, ram: 38, gpu: 16, vramUsedGb: 8, vramTotalGb: 48, disk: 42 },
                slots: { used: 1, total: 2 },
                quotas: {
                    claude: { percentageRemaining: 63, resetAt: '2026-08-16T16:12:00+03:00', resetIn: '4 ч 12 мин', status: 'ok', source: 'provider_estimate', isEstimated: true, usedToday: 37 },
                    codex: { percentageRemaining: 48, resetAt: '2026-08-17T12:00:00+03:00', resetIn: '1 день', status: 'ok', source: 'provider_estimate', isEstimated: true, usedToday: 52 },
                },
                openRouter: { status: 'Доступен', usedToday: '$0.42' },
                currentTasks: [
                    { id: 'task-active-03', project: 'ЖК Примавера', packageName: 'АР-К1', mode: 'hybrid', progress: 92, duration: '34 мин', stage: 'returning' },
                ],
                diagnostic: {
                    workerId: 'worker-mow-03', instanceId: 'mow03-a-c041', transport: 'AgentStream v1', grpcStream: 'connected', connectionId: 'conn-mow03-6', mtls: 'verified', heartbeat: '2 сек назад', gatewayTarget: 'gateway.internal:443', sourceHost: 'central-source', resultHost: 'central-results', nginx: 'healthy', agentStatus: 'running', executorStatus: 'ready', eventOutbox: { lastWrittenSeq: 18002, lastAckedSeq: 18002, pending: 0 }, resultAck: 'confirmed', workerVersion: '1.12.4', runtimeVersion: '2026.08.15', uptime: '26 д 11 ч', certExpiry: 'через 83 дня',
                },
            },
            {
                id: 'worker-mow-04', name: 'Москва-04', location: 'Москва', status: 'busy',
                lastHeartbeat: '5 сек назад', uptime: '7 д 19 ч', acceptsNewTasks: true, quotaDataStale: false,
                resources: { cpu: 66, ram: 72, gpu: 89, vramUsedGb: 41, vramTotalGb: 48, disk: 71 },
                slots: { used: 1, total: 2 },
                quotas: {
                    claude: { percentageRemaining: 12, resetAt: '2026-08-16T14:11:00+03:00', resetIn: '2 ч 11 мин', status: 'critical', source: 'provider_estimate', isEstimated: true, usedToday: 88 },
                    codex: { percentageRemaining: 84, resetAt: '2026-08-19T12:00:00+03:00', resetIn: '3 дня', status: 'ok', source: 'provider_estimate', isEstimated: true, usedToday: 16 },
                },
                openRouter: { status: 'Доступен', usedToday: '$2.11' },
                currentTasks: [
                    { id: 'task-active-05', project: 'Покровское-Стрешнево', packageName: 'ЭОМ-К1', mode: 'full_codex', progress: 76, duration: '26 мин', stage: 'auditing' },
                    { id: 'task-active-07', project: 'ЗИЛАРТ', packageName: 'ВК-К2', mode: 'full_codex', progress: 12, duration: '4 мин', stage: 'preparing' },
                ],
                diagnostic: {
                    workerId: 'worker-mow-04', instanceId: 'mow04-c-4b90', transport: 'AgentStream v1', grpcStream: 'connected', connectionId: 'conn-mow04-4', mtls: 'verified', heartbeat: '5 сек назад', gatewayTarget: 'gateway.internal:443', sourceHost: 'central-source', resultHost: 'central-results', nginx: 'healthy', agentStatus: 'running', executorStatus: 'busy', eventOutbox: { lastWrittenSeq: 6211, lastAckedSeq: 6211, pending: 0 }, resultAck: 'confirmed', workerVersion: '1.12.3', runtimeVersion: '2026.08.14', uptime: '7 д 19 ч', certExpiry: 'через 58 дней',
                },
            },
            {
                id: 'worker-mow-05', name: 'Москва-05', location: 'Москва', status: 'offline',
                lastHeartbeat: '17 мин назад', uptime: '—', acceptsNewTasks: false, quotaDataStale: true,
                resources: { cpu: 0, ram: 0, gpu: 0, vramUsedGb: 0, vramTotalGb: 24, disk: 49 },
                slots: { used: 0, total: 0 },
                quotas: {
                    claude: { percentageRemaining: 45, resetAt: '2026-08-17T08:00:00+03:00', resetIn: 'последние данные', status: 'unknown', source: 'last_known', isEstimated: true, usedToday: 55 },
                    codex: { percentageRemaining: 22, resetAt: '2026-08-17T20:00:00+03:00', resetIn: 'последние данные', status: 'unknown', source: 'last_known', isEstimated: true, usedToday: 78 },
                },
                openRouter: { status: 'Последний статус: доступен', usedToday: '$0.00' },
                currentTasks: [],
                diagnostic: {
                    workerId: 'worker-mow-05', instanceId: 'mow05-a-8e12', transport: 'AgentStream v1', grpcStream: 'disconnected', connectionId: 'conn-mow05-1', mtls: 'last verified', heartbeat: '17 мин назад', gatewayTarget: 'gateway.internal:443', sourceHost: 'central-source', resultHost: 'central-results', nginx: 'unknown', agentStatus: 'unreachable', executorStatus: 'unknown', eventOutbox: { lastWrittenSeq: 4103, lastAckedSeq: 4103, pending: 0 }, resultAck: 'not applicable', workerVersion: '1.12.3', runtimeVersion: '2026.08.14', uptime: '—', certExpiry: 'через 52 дня',
                },
            },
        ];

        /** @type {AuditProject[]} */
        const projects = [
            { id: 'project-alia-ar', project: 'ЖК Алия', packageName: 'АР1.2-К6', mode: 'hybrid', pageCount: 28, blockCount: 214, packageSizeBytes: 48300000, priority: 'high', status: 'Готов к проверке', assignment: 'worker-mow-01' },
            { id: 'project-primavera-eom', project: 'ЖК Примавера', packageName: 'ЭОМ-К3', mode: 'full_codex', pageCount: 12, blockCount: 87, packageSizeBytes: 21500000, priority: 'normal', status: 'Готов к проверке', assignment: 'auto' },
            { id: 'project-zilart-vk', project: 'ЗИЛАРТ', packageName: 'ВК-К2', mode: 'hybrid', pageCount: 19, blockCount: 146, packageSizeBytes: 31100000, priority: 'critical', status: 'Готов к проверке', assignment: 'auto' },
            { id: 'project-mosfilm-ar', project: 'Мосфильмовская', packageName: 'АР-К5', mode: 'full_codex', pageCount: 34, blockCount: 276, packageSizeBytes: 57200000, priority: 'low', status: 'Подготовлен', assignment: 'worker-mow-03' },
            { id: 'project-pokrov-eom', project: 'Покровское-Стрешнево', packageName: 'ЭОМ-К1', mode: 'hybrid', pageCount: 16, blockCount: 119, packageSizeBytes: 26400000, priority: 'normal', status: 'Готов к проверке', assignment: 'auto' },
        ];

        const queueSeed = [
            ['ЖК Примавера', 'ЭОМ-К3', 'full_codex', 'high', 12, 87, 'worker-mow-03', '~12 мин'],
            ['ЖК Алия', 'ВК-К2', 'hybrid', 'critical', 21, 153, 'worker-mow-01', '~26 мин'],
            ['ЗИЛАРТ', 'АР-К4', 'full_codex', 'normal', 31, 241, 'worker-mow-02', '~34 мин'],
            ['Мосфильмовская', 'ОВ-К2', 'hybrid', 'high', 18, 132, 'worker-mow-03', '~41 мин'],
            ['Покровское-Стрешнево', 'ЭОМ-К4', 'full_codex', 'normal', 14, 104, 'worker-mow-04', '~53 мин'],
            ['ЖК Алия', 'СС-К1', 'hybrid', 'normal', 17, 126, 'worker-mow-02', '~1 ч 08 мин'],
            ['ЖК Примавера', 'КР-К2', 'full_codex', 'low', 42, 315, 'worker-mow-03', '~1 ч 22 мин'],
            ['ЗИЛАРТ', 'ОВ-К5', 'hybrid', 'high', 23, 178, 'worker-mow-01', '~1 ч 37 мин'],
            ['Мосфильмовская', 'АР-К5', 'full_codex', 'normal', 34, 276, 'worker-mow-03', '~1 ч 51 мин'],
            ['Покровское-Стрешнево', 'ВК-К1', 'hybrid', 'normal', 16, 119, 'worker-mow-02', '~2 ч 04 мин'],
            ['ЖК Алия', 'КЖ-К7', 'full_codex', 'critical', 38, 298, 'worker-mow-04', '~2 ч 18 мин'],
            ['ЖК Примавера', 'АР-К3', 'hybrid', 'low', 24, 188, 'worker-mow-03', '~2 ч 33 мин'],
            ['ЗИЛАРТ', 'ЭОМ-К6', 'full_codex', 'normal', 15, 112, 'worker-mow-01', '~2 ч 49 мин'],
            ['Мосфильмовская', 'ВК-К3', 'hybrid', 'high', 20, 147, 'worker-mow-02', '~3 ч 05 мин'],
            ['Покровское-Стрешнево', 'АР-К2', 'full_codex', 'normal', 29, 226, 'worker-mow-03', '~3 ч 24 мин'],
            ['ЖК Алия', 'ОВ-К6', 'hybrid', 'low', 13, 98, 'worker-mow-04', '~3 ч 41 мин'],
            ['ЖК Примавера', 'СС-К2', 'full_codex', 'normal', 11, 81, 'worker-mow-01', '~3 ч 55 мин'],
            ['ЗИЛАРТ', 'КР-К3', 'hybrid', 'high', 36, 284, 'worker-mow-02', '~4 ч 18 мин'],
        ];
        /** @type {AuditQueueItem[]} */
        const queue = queueSeed.map((row, index) => ({
            id: `queue-${String(index + 1).padStart(2, '0')}`,
            position: index + 1,
            project: /** @type {string} */ (row[0]),
            packageName: /** @type {string} */ (row[1]),
            mode: /** @type {AuditMode} */ (row[2]),
            priority: /** @type {TaskPriority} */ (row[3]),
            pageCount: /** @type {number} */ (row[4]),
            blockCount: /** @type {number} */ (row[5]),
            suggestedWorkerId: /** @type {string} */ (row[6]),
            expectedStart: /** @type {string} */ (row[7]),
            status: index < 2 ? 'Назначение рассчитано' : 'Ожидает',
        }));

        /** @type {AuditTask[]} */
        const active = [
            ['task-active-01', 'ЖК Алия', 'АР1.2-К6', 'worker-mow-01', 'hybrid', 68, '18 мин', '4 сек назад', 'auditing', 'Проверка'],
            ['task-active-02', 'ЗИЛАРТ', 'ЭОМ-К3', 'worker-mow-02', 'full_codex', 31, '7 мин', '8 сек назад', 'auditing', 'Проверка'],
            ['task-active-03', 'ЖК Примавера', 'АР-К1', 'worker-mow-03', 'hybrid', 92, '34 мин', 'сейчас', 'returning', 'Возврат результата'],
            ['task-active-04', 'Мосфильмовская', 'КР-К2', 'worker-mow-01', 'full_codex', 54, '21 мин', '6 сек назад', 'auditing', 'Проверка'],
            ['task-active-05', 'Покровское-Стрешнево', 'ЭОМ-К1', 'worker-mow-04', 'full_codex', 76, '26 мин', '5 сек назад', 'auditing', 'Проверка'],
            ['task-active-06', 'ЖК Алия', 'ОВ-К4', 'worker-mow-02', 'hybrid', 43, '29 мин', '17 мин назад', 'collecting', 'Сбор результата'],
            ['task-active-07', 'ЗИЛАРТ', 'ВК-К2', 'worker-mow-04', 'full_codex', 12, '4 мин', '5 сек назад', 'preparing', 'Подготовка'],
        ].map((row) => ({
            id: /** @type {string} */ (row[0]), project: /** @type {string} */ (row[1]), packageName: /** @type {string} */ (row[2]), workerId: /** @type {string} */ (row[3]), mode: /** @type {AuditMode} */ (row[4]), progress: /** @type {number} */ (row[5]), duration: /** @type {string} */ (row[6]), lastActivity: /** @type {string} */ (row[7]), stage: /** @type {TaskStage} */ (row[8]), status: /** @type {string} */ (row[9]),
            events: [{ at: '11:42', text: 'Комплект получен воркером' }, { at: '11:44', text: 'Подготовка контекста завершена' }, { at: 'сейчас', text: /** @type {string} */ (row[9]) }],
            modelUsage: { claude: row[4] === 'hybrid' ? 'контекст и проверка' : 'не используется', codex: 'анализ блоков и свод результата' },
        }));

        /** @type {AuditTask[]} */
        const completed = [
            { id: 'task-done-01', project: 'ЖК Алия', packageName: 'АР-К1', workerId: 'worker-mow-01', mode: 'hybrid', progress: 100, duration: '24 мин', lastActivity: 'сегодня, 07:31', stage: 'done', status: 'Готово', result: '17 замечаний', completedAt: 'Сегодня 07:31', completedAtIso: '2026-08-16T07:31:00+03:00', events: [{ at: '07:31', text: 'Результат принят центральным узлом' }], modelUsage: { claude: '41%', codex: '59%' } },
            { id: 'task-done-02', project: 'ЖК Примавера', packageName: 'ВК-К1', workerId: 'worker-mow-03', mode: 'full_codex', progress: 100, duration: '19 мин', lastActivity: 'сегодня, 09:18', stage: 'done', status: 'Готово', result: '9 замечаний', completedAt: 'Сегодня 09:18', completedAtIso: '2026-08-16T09:18:00+03:00', events: [{ at: '09:18', text: 'Результат принят центральным узлом' }], modelUsage: { claude: 'не использовался', codex: '100%' } },
            { id: 'task-done-03', project: 'ЗИЛАРТ', packageName: 'СС-К4', workerId: 'worker-mow-02', mode: 'hybrid', progress: 100, duration: '31 мин', lastActivity: 'вчера, 18:42', stage: 'done', status: 'Готово', result: '22 замечания', completedAt: 'Вчера 18:42', completedAtIso: '2026-08-15T18:42:00+03:00', events: [{ at: '18:42', text: 'Результат принят центральным узлом' }], modelUsage: { claude: '46%', codex: '54%' } },
            { id: 'task-done-04', project: 'Мосфильмовская', packageName: 'ЭОМ-К2', workerId: 'worker-mow-04', mode: 'full_codex', progress: 100, duration: '27 мин', lastActivity: '3 дня назад', stage: 'done', status: 'Готово', result: '13 замечаний', completedAt: '13.08.2026 14:26', completedAtIso: '2026-08-13T14:26:00+03:00', events: [{ at: '14:26', text: 'Результат принят центральным узлом' }], modelUsage: { claude: 'не использовался', codex: '100%' } },
        ];

        /** @type {AuditTask[]} */
        const errors = [
            { id: 'task-error-01', project: 'ЖК Алия', packageName: 'ЭОМ-К2', workerId: 'worker-mow-02', mode: 'hybrid', progress: 43, duration: '28 мин', lastActivity: '17 мин назад', stage: 'error', status: 'Требует внимания', errorMessage: 'Воркeр давно не передавал прогресс. Соединение доступно, задача сохранена и может быть продолжена.', technicalCode: 'HEARTBEAT_PROGRESS_STALE', events: [{ at: '11:19', text: 'Последний подтверждённый прогресс — 43%' }, { at: '11:36', text: 'Система отметила отсутствие активности' }], modelUsage: { claude: 'доступен', codex: 'доступен' } },
            { id: 'task-error-02', project: 'Покровское-Стрешнево', packageName: 'АР-К3', workerId: 'worker-mow-05', mode: 'full_codex', progress: 8, duration: '6 мин', lastActivity: '42 мин назад', stage: 'error', status: 'Ошибка соединения', errorMessage: 'VPS Москва-05 потерял соединение. Текущая задача сохранена локально; можно перенести её на другой узел.', technicalCode: 'WORKER_CONNECTION_LOST', events: [{ at: '10:54', text: 'Передача комплекта завершена' }, { at: '11:02', text: 'Соединение с воркером потеряно' }], modelUsage: { claude: 'не использовался', codex: 'последний статус: доступен' } },
        ];

        return {
            workers,
            projects,
            queue,
            tasks: { active, completed, errors },
            attention: [
                { id: 'attention-01', workerId: 'worker-mow-02', taskId: 'task-error-01', title: 'Нет активности 17 минут', description: 'Прогресс ЖК Алия → ЭОМ-К2 остановился на 43%. Связь и оба провайдера доступны.', severity: 'warning' },
                { id: 'attention-02', workerId: 'worker-mow-05', taskId: 'task-error-02', title: 'Воркeр недоступен', description: 'Москва-05 не выходил на связь 17 минут. Задачу можно перенести без потери исходных данных.', severity: 'error' },
            ],
            recommendation: {
                projectId: 'project-primavera-eom', workerId: 'worker-mow-03', freeSlots: 1, gpu: 16, claude: 63, codex: 48,
                reasons: ['свободен 1 из 2 слотов', 'GPU загружен только на 16%', 'Claude: осталось примерно 63%', 'Codex: осталось примерно 48%', 'лимит Claude сбросится через 4 ч 12 мин', 'нет тяжёлых активных проектов', 'узел подходит под режим Full Codex'],
            },
        };
    }

    class MockDistributedService {
        /** @param {{scenario?:'loaded'|'empty'|'error',latency?:number}=} options */
        constructor(options = {}) {
            this.scenario = options.scenario || 'loaded';
            this.latency = options.latency ?? 70;
            this.state = createDemoDataset();
        }

        /** @returns {Promise<void>} */
        async wait() {
            if (this.latency > 0) await new Promise((resolve) => setTimeout(resolve, this.latency));
            if (this.scenario === 'error') throw new Error('Не удалось загрузить демо-данные распределённых вычислений');
        }

        /** @param {'loaded'|'empty'|'error'} scenario */
        setScenario(scenario) { this.scenario = scenario; }

        async getOverview() {
            await this.wait();
            if (this.scenario === 'empty') {
                return { kpis: { online: 0, totalWorkers: 0, active: 0, queued: 0, errors: 0, freeSlots: 0, totalSlots: 0, completedToday: 0 }, workers: [], recommendation: null, projects: [], queuePreview: [], attention: [] };
            }
            const online = this.state.workers.filter((worker) => worker.status !== 'offline').length;
            const totalSlots = this.state.workers.reduce((sum, worker) => sum + worker.slots.total, 0);
            const usedSlots = this.state.workers.reduce((sum, worker) => sum + worker.slots.used, 0);
            return clone({
                kpis: { online, totalWorkers: this.state.workers.length, active: this.state.tasks.active.length, queued: this.state.queue.length, errors: this.state.tasks.errors.length, freeSlots: totalSlots - usedSlots, totalSlots, completedToday: 2 },
                workers: this.state.workers,
                recommendation: this.state.recommendation,
                projects: this.state.projects,
                queuePreview: this.state.queue.slice(0, 5),
                attention: this.state.attention,
            });
        }

        async getWorkers() { await this.wait(); return clone(this.scenario === 'empty' ? [] : this.state.workers); }
        async getQueue() { await this.wait(); return clone(this.scenario === 'empty' ? [] : this.state.queue); }
        async getTasks() { await this.wait(); return clone(this.scenario === 'empty' ? { active: [], completed: [], errors: [] } : this.state.tasks); }
        async getProviderLimits() { await this.wait(); return clone(this.scenario === 'empty' ? [] : this.state.workers.map((worker) => ({ workerId: worker.id, workerName: worker.name, online: worker.status !== 'offline', stale: worker.quotaDataStale, claude: worker.quotas.claude, codex: worker.quotas.codex }))); }
        async getDiagnostics() { await this.wait(); return clone(this.scenario === 'empty' ? [] : this.state.workers.map((worker) => ({ workerName: worker.name, online: worker.status !== 'offline', diagnostic: worker.diagnostic }))); }
        async getNextRecommendation() { await this.wait(); return clone(this.scenario === 'empty' ? null : this.state.recommendation); }

        /** @param {string} projectId @param {string} workerId */
        async assignTask(projectId, workerId) {
            await this.wait();
            const project = this.state.projects.find((item) => item.id === projectId);
            if (!project) throw new Error('Проект не найден');
            project.assignment = workerId;
            return clone(project);
        }

        /** @param {string} projectId @param {string} workerId */
        async sendTask(projectId, workerId) {
            await this.wait();
            const project = this.state.projects.find((item) => item.id === projectId);
            if (!project) throw new Error('Проект не найден');
            project.assignment = workerId;
            project.status = 'Добавлен в очередь';
            return clone(project);
        }

        /** @param {string} queueItemId @param {TaskPriority} priority */
        async changePriority(queueItemId, priority) {
            await this.wait();
            const item = this.state.queue.find((row) => row.id === queueItemId);
            if (!item) throw new Error('Элемент очереди не найден');
            item.priority = priority;
            return clone(item);
        }

        /** @param {string} queueItemId @param {'first'|'up'|'down'|'last'} direction */
        async moveQueueItem(queueItemId, direction) {
            await this.wait();
            const index = this.state.queue.findIndex((row) => row.id === queueItemId);
            if (index < 0) throw new Error('Элемент очереди не найден');
            let target = index;
            if (direction === 'first') target = 0;
            if (direction === 'up') target = Math.max(0, index - 1);
            if (direction === 'down') target = Math.min(this.state.queue.length - 1, index + 1);
            if (direction === 'last') target = this.state.queue.length - 1;
            const [item] = this.state.queue.splice(index, 1);
            this.state.queue.splice(target, 0, item);
            this.state.queue.forEach((row, rowIndex) => { row.position = rowIndex + 1; });
            return clone(this.state.queue);
        }

        /** @param {string} workerId @param {boolean} acceptsNewTasks */
        async setWorkerIntake(workerId, acceptsNewTasks) {
            await this.wait();
            const worker = this.state.workers.find((item) => item.id === workerId);
            if (!worker) throw new Error('Воркeр не найден');
            worker.acceptsNewTasks = acceptsNewTasks;
            return clone(worker);
        }

        /** @param {string} taskId */
        async retryTask(taskId) {
            await this.wait();
            const index = this.state.tasks.errors.findIndex((item) => item.id === taskId);
            if (index < 0) throw new Error('Задача не найдена');
            const [task] = this.state.tasks.errors.splice(index, 1);
            task.stage = 'transfer';
            task.status = 'Повторная передача';
            task.progress = 3;
            task.lastActivity = 'сейчас';
            delete task.errorMessage;
            delete task.technicalCode;
            task.events.push({ at: 'сейчас', text: 'Запущена повторная попытка в демо-режиме' });
            this.state.tasks.active.push(task);
            this.state.attention = this.state.attention.filter((item) => item.taskId !== taskId);
            return clone(task);
        }

        /** @param {string} taskId @param {string} workerId */
        async transferTask(taskId, workerId) {
            await this.wait();
            const task = [...this.state.tasks.active, ...this.state.tasks.errors].find((item) => item.id === taskId);
            if (!task) throw new Error('Задача не найдена');
            task.workerId = workerId;
            task.stage = 'transfer';
            task.status = 'Перенос на другой VPS';
            task.lastActivity = 'сейчас';
            task.events.push({ at: 'сейчас', text: 'Выбран новый вычислительный узел' });
            return clone(task);
        }

        /** @returns {string} */
        getSafeDiagnosticsText() {
            return JSON.stringify(this.state.workers.map((worker) => ({ name: worker.name, online: worker.status !== 'offline', ...worker.diagnostic })), null, 2);
        }
    }

    root.DistributedData = Object.freeze({
        createMockService: (options) => new MockDistributedService(options),
        createDemoDataset,
        quotaStatus,
    });
})(typeof window !== 'undefined' ? window : globalThis);
