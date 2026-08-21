(function stageComparisonDifferencesModule(root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.StageComparisonDifferences = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function buildModule() {
    'use strict';

    const DISCREPANCY_BUCKETS = ['changed', 'removed', 'added', 'uncertain'];
    const PREVIEW_LIMIT = 5;

    function bucketItems(group, bucket) {
        return Array.isArray(group && group[bucket]) ? group[bucket] : [];
    }

    function hasDiscrepancy(group) {
        return DISCREPANCY_BUCKETS.some(bucket => bucketItems(group, bucket).length > 0);
    }

    function sourceGroups(finalResult, deterministicResult) {
        if (finalResult && !finalResult.stale) return finalResult.sheet_groups || [];
        if (deterministicResult && !deterministicResult.stale) {
            return deterministicResult.sheet_groups || [];
        }
        return [];
    }

    function firstPage(group, side) {
        const pages = (group && group[`${side}_pages`] || [])
            .map(Number)
            .filter(Number.isFinite);
        return pages.length ? Math.min(...pages) : Number.MAX_SAFE_INTEGER;
    }

    function searchableText(group) {
        const values = [
            ...(group.left_labels || []),
            ...(group.right_labels || []),
        ];
        for (const bucket of DISCREPANCY_BUCKETS) {
            for (const item of bucketItems(group, bucket)) {
                values.push(item.summary, item.before, item.after, item.reason);
            }
        }
        return values.filter(Boolean).join('\n').toLocaleLowerCase('ru-RU');
    }

    function buildRows(finalResult, deterministicResult, options) {
        const selected = options || {};
        const filter = DISCREPANCY_BUCKETS.includes(selected.filter)
            ? selected.filter
            : 'all';
        const query = String(selected.query || '').trim().toLocaleLowerCase('ru-RU');
        return sourceGroups(finalResult, deterministicResult)
            .filter(hasDiscrepancy)
            .filter(group => filter === 'all' || bucketItems(group, filter).length > 0)
            .filter(group => !query || searchableText(group).includes(query))
            .slice()
            .sort((left, right) => (
                firstPage(left, 'left') - firstPage(right, 'left')
                || firstPage(left, 'right') - firstPage(right, 'right')
                || String(left.id || '').localeCompare(String(right.id || ''))
            ));
    }

    function visibleItems(group, bucket, expanded, limit) {
        const items = bucketItems(group, bucket);
        return expanded ? items : items.slice(0, limit || PREVIEW_LIMIT);
    }

    function remainingCount(group, bucket, expanded, limit) {
        if (expanded) return 0;
        return Math.max(0, bucketItems(group, bucket).length - (limit || PREVIEW_LIMIT));
    }

    function transitionLabel(status) {
        return String(status || 'MIXED').replaceAll('_', ' + ');
    }

    function groupAiDiagnostics(review, groupId) {
        const group = (review && review.sheet_groups || [])
            .find(item => String(item.id || '') === String(groupId || ''));
        if (!group || group.status !== 'completed') return null;
        const decisions = group.decisions || [];
        const transitions = new Map();
        let confirmed = 0;
        let corrected = 0;
        let uncertain = 0;
        for (const decision of decisions) {
            const before = String(decision.deterministic_status || 'MIXED');
            const after = String(decision.final_status || 'UNCERTAIN');
            if (before === after) confirmed += 1;
            else corrected += 1;
            if (after === 'UNCERTAIN') uncertain += 1;
            if (before !== after) {
                const key = `${transitionLabel(before)} → ${after}`;
                transitions.set(key, (transitions.get(key) || 0) + 1);
            }
        }
        return {
            confirmed,
            corrected,
            uncertain,
            transitions: [...transitions.entries()].map(([transition, count]) => ({
                transition,
                count,
            })),
        };
    }

    function uncertainReasonLabel(item) {
        const policy = String(item && item.policy_reason || '');
        if (policy.startsWith('unsupported_model_')) return 'VALIDATOR_REJECTED';
        if (policy === 'same_conflicts_with_deterministic_change') return 'OCR_NOISE';
        if (item && item.final_status === 'UNCERTAIN') return 'OTHER';
        return '';
    }

    return {
        DISCREPANCY_BUCKETS,
        PREVIEW_LIMIT,
        bucketItems,
        buildRows,
        groupAiDiagnostics,
        hasDiscrepancy,
        remainingCount,
        uncertainReasonLabel,
        visibleItems,
    };
}));
