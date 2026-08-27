(function stageComparisonReviewModule(root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.StageComparisonReview = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function buildModule() {
    'use strict';

    const REVIEW_DECISIONS = ['PENDING_REVIEW', 'APPROVED', 'REJECTED'];
    const QUESTION_CATEGORIES = ['SHEET', 'ENTITY', 'CHANGE'];

    function array(value) {
        return Array.isArray(value) ? value : [];
    }

    function text(value, fallback) {
        if (value === null || value === undefined || value === '') return fallback || '—';
        if (typeof value === 'string') return value;
        if (typeof value === 'number' || typeof value === 'boolean') return String(value);
        try { return JSON.stringify(value); } catch (_) { return String(value); }
    }

    function decision(value) {
        const normalized = String(value || '').toUpperCase();
        return REVIEW_DECISIONS.includes(normalized) ? normalized : 'PENDING_REVIEW';
    }

    function confidence(value) {
        if (value && typeof value === 'object') {
            return String(value.level || value.confidence || 'UNKNOWN').toUpperCase();
        }
        return String(value || 'UNKNOWN').toUpperCase();
    }

    function uniqueNumbers(values) {
        return [...new Set(array(values)
            .map(Number)
            .filter(value => Number.isInteger(value) && value > 0))]
            .sort((left, right) => left - right);
    }

    function pagesForSide(change, side) {
        const upper = String(side || '').toUpperCase();
        const lower = upper.toLowerCase();
        const found = [];
        const visited = new Set();

        function add(value) {
            const page = Number(value);
            if (Number.isInteger(page) && page > 0) found.push(page);
        }

        function visit(value) {
            if (!value || typeof value !== 'object' || visited.has(value)) return;
            visited.add(value);
            if (Array.isArray(value)) {
                value.forEach(visit);
                return;
            }
            array(value[`${lower}_pages`]).forEach(add);
            if (String(value.side || '').toUpperCase() === upper) add(value.page);
            const locations = value.locations;
            if (locations && typeof locations === 'object' && !Array.isArray(locations)) {
                array(locations[upper] || locations[lower]).forEach(location => {
                    if (location && typeof location === 'object') add(location.page);
                });
            }
            Object.values(value).forEach(visit);
        }

        visit(change);
        return uniqueNumbers(found);
    }

    function changeLabel(change) {
        const parts = [change.facet_ref, change.dimension, change.direction]
            .filter(value => value !== null && value !== undefined && value !== '')
            .map(String);
        return [...new Set(parts)].join(' · ') || text(change.outcome || change.type);
    }

    function normalizeRows(payload) {
        const rows = Array.isArray(payload) ? payload : array(payload && payload.rows);
        return rows.map((source, index) => {
            const change = source && typeof source.change === 'object'
                ? source.change
                : (source || {});
            const engineer = source && typeof source.engineer_decision === 'object'
                ? source.engineer_decision
                : (change.engineer_decision || {});
            const targetId = String(
                source.target_id || change.change_id || change.review_evidence_id || `row-${index + 1}`
            );
            const leftPages = pagesForSide(change, 'LEFT');
            const rightPages = pagesForSide(change, 'RIGHT');
            const groupId = source.presentation_group_id || null;
            return {
                target_id: targetId,
                target_kind: String(source.target_kind || (change.review_evidence_id ? 'REVIEW_EVIDENCE' : 'CHANGE')),
                object_ref: text(change.project_entity_ref || change.subject_ref || change.scope_ref),
                left_pages: leftPages,
                right_pages: rightPages,
                sheets_label: `LEFT ${leftPages.join(', ') || '—'} → RIGHT ${rightPages.join(', ') || '—'}`,
                change_label: changeLabel(change),
                before: text(change.before_value),
                after: text(change.after_value),
                source: String(change.source_mode || change.source || 'UNKNOWN').toUpperCase(),
                status: String(change.review_status || change.outcome || 'CONFIRMED').toUpperCase(),
                confidence: confidence(change.confidence),
                decision: decision(engineer.decision),
                author: engineer.author || '',
                comment: engineer.comment || '',
                reason_code: engineer.reason_code || '',
                target_input_signature: engineer.input_signature || '',
                decision_revision: Number.isInteger(Number(engineer.revision))
                    ? Number(engineer.revision)
                    : 0,
                stale: Boolean(engineer.stale),
                presentation_group_id: groupId,
                presentation_group_label: groupId ? `Группа ${groupId}` : '',
                reason_codes: array(change.reason_codes).map(String),
                raw_change: change,
                raw: source,
            };
        });
    }

    function reviewCounts(value) {
        const rows = Array.isArray(value) && value.every(item => item && item.target_id)
            ? value
            : normalizeRows(value);
        const counts = {total: rows.length, APPROVED: 0, REJECTED: 0, PENDING_REVIEW: 0};
        rows.forEach(row => { counts[decision(row.decision)] += 1; });
        return counts;
    }

    function questionsFrom(payload) {
        if (Array.isArray(payload)) return payload;
        return array(payload && payload.questions);
    }

    function normalizeQuestionCounts(payload) {
        const source = payload && typeof payload.counts === 'object' ? payload.counts : {};
        const counts = {SHEET: 0, ENTITY: 0, CHANGE: 0, total: 0};
        const hasServerCounts = QUESTION_CATEGORIES.some(category =>
            Number.isFinite(Number(source[category] !== undefined ? source[category] : source[category.toLowerCase()]))
        );
        if (hasServerCounts) {
            QUESTION_CATEGORIES.forEach(category => {
                const value = source[category] !== undefined ? source[category] : source[category.toLowerCase()];
                counts[category] = Math.max(0, Number(value) || 0);
            });
        } else {
            questionsFrom(payload).forEach(question => {
                const category = String(question && question.category || '').toUpperCase();
                if (QUESTION_CATEGORIES.includes(category)) counts[category] += 1;
            });
        }
        counts.total = QUESTION_CATEGORIES.reduce((sum, category) => sum + counts[category], 0);
        return counts;
    }

    function normalizeQuestions(payload) {
        return questionsFrom(payload).map((question, index) => {
            const answerRecord = question && typeof question.answer === 'object'
                ? question.answer
                : (question && typeof question.human_answer === 'object' ? question.human_answer : {});
            const answerValue = question && typeof question.answer === 'string'
                ? question.answer
                : (answerRecord.answer || question.selected_answer || '');
            return {
                question_id: String(question.question_id || `question-${index + 1}`),
                category: String(question.category || 'CHANGE').toUpperCase(),
                question_type: String(question.question_type || ''),
                prompt: text(question.prompt || question.question),
                options: array(question.options || question.answer_options).map(option => {
                    if (option && typeof option === 'object') {
                        return {
                            value: String(option.value || option.id || option.code || option.label || ''),
                            label: text(option.label || option.title || option.value || option.id),
                        };
                    }
                    return {value: String(option), label: text(option)};
                }).filter(option => option.value),
                status: String(question.status || 'PENDING').toUpperCase(),
                answer: answerValue,
                author: answerRecord.author || '',
                comment: answerRecord.comment || '',
                selected_refs: array(answerRecord.selected_refs).map(String),
                explicit_candidate: answerRecord.explicit_candidate
                    && typeof answerRecord.explicit_candidate === 'object'
                    ? {...answerRecord.explicit_candidate}
                    : null,
                typed_resolution: answerRecord.typed_resolution
                    && typeof answerRecord.typed_resolution === 'object'
                    ? {...answerRecord.typed_resolution}
                    : null,
                context: question.context || {},
                input_signature: question.input_signature || '',
                raw: question,
            };
        });
    }

    function point(value) {
        if (Array.isArray(value) && value.length >= 2) {
            return {x: Number(value[0]), y: Number(value[1])};
        }
        if (value && typeof value === 'object') {
            return {x: Number(value.x), y: Number(value.y)};
        }
        return null;
    }

    function finitePoint(value) {
        return value && Number.isFinite(value.x) && Number.isFinite(value.y);
    }

    function unitsFor(values) {
        return values.length && values.every(value => Number.isFinite(value) && value >= 0 && value <= 1)
            ? 'normalized'
            : 'absolute';
    }

    function regionFromBox(value) {
        let x;
        let y;
        let width;
        let height;
        if (Array.isArray(value) && value.length >= 4) {
            const numbers = value.slice(0, 4).map(Number);
            if (!numbers.every(Number.isFinite)) return null;
            x = numbers[0];
            y = numbers[1];
            width = numbers[2] - numbers[0];
            height = numbers[3] - numbers[1];
        } else if (value && typeof value === 'object') {
            x = Number(value.x !== undefined ? value.x : value.x0);
            y = Number(value.y !== undefined ? value.y : value.y0);
            width = Number(value.width !== undefined ? value.width : Number(value.x1) - x);
            height = Number(value.height !== undefined ? value.height : Number(value.y1) - y);
        }
        if (![x, y, width, height].every(Number.isFinite) || width <= 0 || height <= 0) return null;
        return {kind: 'BBOX', x, y, width, height, units: unitsFor([x, y, x + width, y + height])};
    }

    function regionFromPolygon(value) {
        const points = array(value).map(point).filter(finitePoint);
        if (points.length < 3) return null;
        const xs = points.map(item => item.x);
        const ys = points.map(item => item.y);
        const x = Math.min(...xs);
        const y = Math.min(...ys);
        const width = Math.max(...xs) - x;
        const height = Math.max(...ys) - y;
        if (!(width > 0 && height > 0)) return null;
        return {
            kind: 'POLYGON', x, y, width, height,
            units: unitsFor(points.flatMap(item => [item.x, item.y])),
            polygon: points.map(item => ({x: (item.x - x) / width, y: (item.y - y) / height})),
        };
    }

    function pageSize(location) {
        const value = location && location.page_size;
        if (Array.isArray(value) && value.length >= 2) {
            const width = Number(value[0]);
            const height = Number(value[1]);
            return width > 0 && height > 0 ? {width, height} : null;
        }
        if (value && typeof value === 'object') {
            const width = Number(value.width);
            const height = Number(value.height);
            return width > 0 && height > 0 ? {width, height} : null;
        }
        return null;
    }

    function normalizeRegionCoordinates(region, location) {
        if (!region) return null;
        const coordinateSpace = String(location && location.coordinate_space || '').toUpperCase();
        if (coordinateSpace === 'NORMALIZED_PAGE_TOP_LEFT') {
            if (region.units !== 'normalized') return null;
            return region;
        }
        if (coordinateSpace === 'PDF_VISUAL_PT' || region.units === 'absolute') {
            const size = pageSize(location);
            // Absolute PDF points without their exact page size cannot be
            // placed honestly in the normalized browser viewer.
            if (!size) return null;
            return {
                ...region,
                x: region.x / size.width,
                y: region.y / size.height,
                width: region.width / size.width,
                height: region.height / size.height,
                units: 'normalized',
            };
        }
        return region.units === 'normalized' ? region : null;
    }

    function regionsFromHighlight(highlight, location) {
        if (!highlight || typeof highlight !== 'object') return [];
        const kind = String(highlight.kind || '').toUpperCase();
        if (kind === 'POLYGON') {
            const region = normalizeRegionCoordinates(regionFromPolygon(highlight.polygon), location);
            return region ? [region] : [];
        }
        if (kind === 'BBOX_SET') {
            return array(highlight.bboxes)
                .map(regionFromBox)
                .map(region => normalizeRegionCoordinates(region, location))
                .filter(Boolean);
        }
        const region = normalizeRegionCoordinates(regionFromBox(highlight.bbox || highlight), location);
        return region ? [region] : [];
    }

    function normalizeEvidence(payload) {
        const sourceSides = payload && payload.sides && typeof payload.sides === 'object'
            ? payload.sides
            : {};
        const result = {
            target_id: String(payload && payload.target_id || ''),
            source_mode: String(payload && payload.source_mode || 'UNKNOWN').toUpperCase(),
            layout: String(payload && payload.layout || 'SIDE_BY_SIDE').toUpperCase(),
            sides: {},
            trace: array(payload && payload.trace),
            raw: payload || {},
        };
        for (const upper of ['LEFT', 'RIGHT']) {
            const locations = array(sourceSides[upper] || sourceSides[upper.toLowerCase()]);
            const hasPage = location => location && location.page !== null
                && location.page !== undefined && location.page !== ''
                && Number.isInteger(Number(location.page)) && Number(location.page) > 0;
            const firstPageLocation = locations.find(hasPage);
            const page = firstPageLocation ? Number(firstPageLocation.page) : null;
            const pages = uniqueNumbers(locations.filter(hasPage).map(location => location.page));
            const overlays = [];
            locations.forEach((location, locationIndex) => {
                const locationPage = hasPage(location)
                    ? Number(location.page)
                    : null;
                regionsFromHighlight(location && location.highlight, location).forEach((region, regionIndex) => {
                    overlays.push({
                        ...region,
                        id: `${upper}-${locationIndex}-${regionIndex}`,
                        page: locationPage,
                        source: String(location.source || 'UNKNOWN').toUpperCase(),
                        block_id: location.block_id || null,
                        node_id: location.node_id || null,
                        fragment_id: location.fragment_id || null,
                    });
                });
            });
            result.sides[upper.toLowerCase()] = {
                side: upper,
                page,
                pages,
                locations,
                overlays,
                has_evidence: locations.length > 0,
                coordinates_available: overlays.length > 0,
                coordinates_missing: locations.length > 0 && overlays.length === 0,
            };
        }
        result.has_any_coordinates = result.sides.left.coordinates_available
            || result.sides.right.coordinates_available;
        result.has_both_sides = result.sides.left.has_evidence && result.sides.right.has_evidence;
        return result;
    }

    function evidenceFocus(side) {
        const page = side && side.page;
        const overlays = array(side && side.overlays).filter(region => (
            page === null || page === undefined || Number(region.page) === Number(page)
        ));
        if (!overlays.length) return null;
        const units = overlays[0].units;
        const compatible = overlays.filter(region => region.units === units);
        const x0 = Math.min(...compatible.map(region => region.x));
        const y0 = Math.min(...compatible.map(region => region.y));
        const x1 = Math.max(...compatible.map(region => region.x + region.width));
        const y1 = Math.max(...compatible.map(region => region.y + region.height));
        return {x: (x0 + x1) / 2, y: (y0 + y1) / 2, width: x1 - x0, height: y1 - y0, units};
    }

    function normalizeFinalRows(report) {
        const approved = array(report && report.approved_atomic_changes);
        return normalizeRows(approved.map(change => ({
            target_id: change.change_id,
            target_kind: 'CHANGE',
            change,
            engineer_decision: {
                ...(change.engineer_decision || {}),
                decision: 'APPROVED',
            },
        })));
    }

    function normalizeSheetSuggestions(state) {
        const raw = state && state.sheet_suggestions;
        const suggestions = Array.isArray(raw) ? raw : array(raw && raw.suggestions);
        return suggestions.map((item, index) => {
            const selectedLeftPages = uniqueNumbers(item.selected_left_pages || []);
            const selectedRightPages = uniqueNumbers(item.selected_right_pages || []);
            const leftPages = uniqueNumbers(
                item.suggested_left_pages || item.left_pages
                || item.selected_left_pages || [item.left_page]
            );
            const rightPages = uniqueNumbers(
                item.suggested_right_pages || item.right_pages || item.candidate_right_pages
                || item.selected_right_pages || [item.right_page || item.candidate_right_page]
            );
            return {
                id: String(item.suggestion_id || item.question_id || `sheet-suggestion-${index + 1}`),
                question_id: item.question_id || null,
                selected_left_pages: selectedLeftPages,
                selected_right_pages: selectedRightPages,
                left_pages: leftPages,
                right_pages: rightPages,
                confidence: confidence(item.confidence || item.status),
                message: text(item.message || item.recommendation,
                    `Для LEFT ${leftPages.join(', ') || '—'} найден кандидат RIGHT ${rightPages.join(', ') || '—'}`),
                raw: item,
            };
        });
    }

    function normalizePageGroup(value) {
        if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
        const leftPages = uniqueNumbers(value.left_pages || value.selected_left_pages);
        const rightPages = uniqueNumbers(value.right_pages || value.selected_right_pages);
        if (!leftPages.length || !rightPages.length) return null;
        return {left_pages: leftPages, right_pages: rightPages};
    }

    function firstBoolean(values) {
        const found = values.find(value => typeof value === 'boolean');
        return typeof found === 'boolean' ? found : null;
    }

    function normalizeSuggestionApplication(payload, suggestionId) {
        const wrapper = payload && typeof payload === 'object' ? payload : {};
        const state = wrapper.state && typeof wrapper.state === 'object'
            ? wrapper.state
            : wrapper;
        const questions = wrapper.review_questions && typeof wrapper.review_questions === 'object'
            ? wrapper.review_questions
            : (wrapper.production_review_questions
                && typeof wrapper.production_review_questions === 'object'
                ? wrapper.production_review_questions
                : {});
        const application = wrapper.application && typeof wrapper.application === 'object'
            ? wrapper.application
            : (questions.application && typeof questions.application === 'object'
                ? questions.application
                : {});
        const diagnostics = application.diagnostics && typeof application.diagnostics === 'object'
            ? application.diagnostics
            : (wrapper.diagnostics && typeof wrapper.diagnostics === 'object'
                ? wrapper.diagnostics
                : {});
        const stages = state.stages && typeof state.stages === 'object' ? state.stages : {};
        const sheetScope = stages.sheet_scope && typeof stages.sheet_scope === 'object'
            ? stages.sheet_scope
            : {};
        const generationScope = state.generation_scope
            && typeof state.generation_scope === 'object'
            ? state.generation_scope
            : {};
        const semantics = wrapper.suggestion_action_semantics
            && typeof wrapper.suggestion_action_semantics === 'object'
            ? wrapper.suggestion_action_semantics
            : (questions.suggestion_action_semantics
                && typeof questions.suggestion_action_semantics === 'object'
                ? questions.suggestion_action_semantics
                : (state.suggestion_action_semantics
                    && typeof state.suggestion_action_semantics === 'object'
                    ? state.suggestion_action_semantics
                    : {}));
        const actions = wrapper.suggestion_actions && typeof wrapper.suggestion_actions === 'object'
            ? wrapper.suggestion_actions
            : (questions.suggestion_actions && typeof questions.suggestion_actions === 'object'
                ? questions.suggestion_actions
                : (state.suggestion_actions && typeof state.suggestion_actions === 'object'
                    ? state.suggestion_actions
                    : {}));
        const outcome = array(semantics.outcomes).find(item => (
            item && String(item.suggestion_id || '') === String(suggestionId || '')
        )) || {};
        const rawGroupSources = [
            wrapper.effective_page_groups,
            diagnostics.effective_page_groups,
            diagnostics.page_groups,
            semantics.effective_page_groups,
            state.effective_page_groups,
            state.page_groups,
            sheetScope.effective_page_groups,
            generationScope.page_groups,
            Array.isArray(sheetScope.groups) ? sheetScope.groups : null,
        ];
        let groups = [];
        for (const source of rawGroupSources) {
            const normalized = array(source).map(normalizePageGroup).filter(Boolean);
            if (normalized.length) {
                groups = normalized;
                break;
            }
        }
        if (!groups.length) {
            const selection = diagnostics.effective_selection
                || wrapper.effective_selection
                || state.effective_selection
                || state.selection;
            const group = normalizePageGroup(selection);
            if (group) groups = [group];
        }
        return {
            action: String(actions[String(suggestionId || '')] || outcome.action || ''),
            state: String(outcome.state || semantics.state || diagnostics.state || ''),
            scope_applied: outcome.state === 'IGNORED' ? false : firstBoolean([
                outcome.scope_applied,
                wrapper.scope_applied,
                diagnostics.scope_applied,
                semantics.scope_applied,
                sheetScope.scope_applied,
            ]),
            pipeline_rerun: firstBoolean([
                outcome.pipeline_rerun,
                wrapper.pipeline_rerun,
                diagnostics.pipeline_rerun,
                semantics.pipeline_rerun,
                sheetScope.pipeline_rerun,
            ]),
            this_update_reran: firstBoolean([
                outcome.this_update_reran,
                wrapper.this_update_reran,
                diagnostics.this_update_reran,
                semantics.this_update_reran,
                sheetScope.this_update_reran,
            ]),
            generation_was_materialized: firstBoolean([
                wrapper.generation_was_materialized,
                diagnostics.generation_was_materialized,
                semantics.generation_was_materialized,
                sheetScope.generation_was_materialized,
            ]),
            generation_run_id: String(
                diagnostics.generation_run_id || wrapper.generation_run_id
                || semantics.generation_run_id || state.generation_run_id
                || state.run_id || '',
            ),
            groups,
        };
    }

    return {
        QUESTION_CATEGORIES,
        REVIEW_DECISIONS,
        decision,
        evidenceFocus,
        normalizeEvidence,
        normalizeFinalRows,
        normalizeQuestionCounts,
        normalizeQuestions,
        normalizeRows,
        normalizeSheetSuggestions,
        normalizeSuggestionApplication,
        pagesForSide,
        reviewCounts,
        text,
    };
}));
