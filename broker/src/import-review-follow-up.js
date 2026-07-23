const MAX_FOLLOW_UP_ITEMS = 5000;
const MAX_REAUTHORISE_ITEMS = 5000;
const AUTHORISE_CHUNK_SIZE = 100;
const VALID_COMPONENT_STATUSES = new Set(['PENDING', 'COMPLETE', 'FAILED_RETRYABLE', 'NOT_REQUIRED']);
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function objectValue(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function uniqueBoundedStrings(value, max = MAX_FOLLOW_UP_ITEMS) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((item) => String(item || '').trim()).filter(Boolean))].slice(0, max);
}

function componentStatus(storedResponse, component) {
  const key = component === 'EMAIL'
    ? 'review_email_follow_up_status'
    : 'review_tsfin_follow_up_status';
  const status = String(objectValue(storedResponse)[key] || 'NOT_REQUIRED').trim().toUpperCase();
  if (!VALID_COMPONENT_STATUSES.has(status)) throw new Error('IMPORT_REVIEW_FOLLOW_UP_COMPONENT_STATE_INVALID');
  return status;
}

function safeComponentFailure(component, code, message) {
  return {
    component,
    expectedStatus: 'PENDING',
    newStatus: 'FAILED_RETRYABLE',
    errorCode: code,
    errorMessage: message
  };
}

function reauthorisationCompleted(result, expectedTimesheetIds) {
  if (result?.ok !== true) return false;
  const expected = new Set(expectedTimesheetIds);
  const rows = Array.isArray(result?.results) ? result.results : [];
  if (result?.all_success === true && Number(result?.success_count || 0) >= expected.size) return true;
  const completed = new Set();
  for (const row of rows) {
    const id = String(row?.requested_timesheet_id || row?.current_timesheet_id || '').trim().toLowerCase();
    if (!expected.has(id)) continue;
    if (row?.success === true || String(row?.error_code || '').trim().toUpperCase() === 'ALREADY_AUTHORISED') {
      completed.add(id);
    }
  }
  return completed.size === expected.size;
}

function isTransientFollowUpRpcFailure(error) {
  const status = Number(error?.status || 0);
  const detail = [error?.code, error?.json?.code, error?.json?.message, error?.json?.details, error?.message]
    .filter(Boolean).map(String).join(' ').toUpperCase();
  return [408, 429, 500, 502, 503, 504].includes(status)
    || /TIMEOUT|TIMED OUT|ECONNRESET|ECONNABORTED|UPSTREAM|PLDBGAPI2/.test(detail);
}

export async function reconcileTimesheetQueryDeliveryAfterProviderAcceptance({ row, markDelivery, reconcileDelivery } = {}) {
  if (String(row?.type || '').trim().toUpperCase() !== 'TIMESHEET_QUERY') {
    return { applicable: false, history_marked: false, reconcile_attempted: false };
  }
  if (typeof markDelivery !== 'function' || typeof reconcileDelivery !== 'function') {
    throw new TypeError('Timesheet-query delivery reconciliation dependencies are required');
  }
  try {
    await markDelivery(row);
    return { applicable: true, history_marked: true, reconcile_attempted: false };
  } catch {
    let reconciled = false;
    try {
      await reconcileDelivery(row);
      reconciled = true;
    } catch {}
    return {
      applicable: true,
      history_marked: false,
      reconcile_attempted: true,
      reconcile_completed: reconciled,
      error_code: 'TIMESHEET_QUERY_DELIVERY_MARK_FAILED'
    };
  }
}

export function createImportReviewPostCommitRunner({ sbRpc, unwrapRpcJsonb, runTsfinWorkerOnce, wait } = {}) {
  if (typeof sbRpc !== 'function' || typeof unwrapRpcJsonb !== 'function' || typeof runTsfinWorkerOnce !== 'function') {
    throw new TypeError('Import-review follow-up dependencies are required');
  }
  const waitForConcurrentTsfin = typeof wait === 'function'
    ? wait
    : (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs));

  async function updateComponent(env, details, transition) {
    const raw = await sbRpc(env, 'import_review_follow_up_component_update_v1', {
      p_import_id: details.importId,
      p_operation_id: details.operationId,
      p_request_hash: details.requestHash || null,
      p_component: transition.component,
      p_expected_component_status: transition.expectedStatus,
      p_new_component_status: transition.newStatus,
      p_error_code: transition.errorCode || null,
      p_error_message: transition.errorMessage || null,
      p_actor_user_id: details.actorUserId
    }, { timeoutMs: 15000 });
    return unwrapRpcJsonb(raw, 'import_review_follow_up_component_update_v1') || {};
  }

  async function failComponent(env, details, transition) {
    try {
      return await updateComponent(env, details, transition);
    } catch (recordError) {
      const error = new Error('IMPORT_REVIEW_FOLLOW_UP_FAILURE_RECORDING_FAILED');
      error.cause = recordError;
      throw error;
    }
  }

  return async function runImportReviewPostCommit(env, details = {}) {
    const statusRaw = await sbRpc(env, 'import_review_apply_status_get_v1', {
      p_import_id: details.importId,
      p_operation_id: details.operationId,
      p_request_hash: details.requestHash || null
    }, { timeoutMs: 8000 });
    const status = unwrapRpcJsonb(statusRaw, 'import_review_apply_status_get_v1') || {};
    if (status.ok === false || !String(status.outcome || '').startsWith('COMMITTED_')) {
      throw new Error('IMPORT_REVIEW_FOLLOW_UP_SOURCE_NOT_COMMITTED');
    }
    const committedAtUtc = String(status.committed_at_utc || '').trim();
    if (!committedAtUtc || !Number.isFinite(Date.parse(committedAtUtc))) {
      throw new Error('IMPORT_REVIEW_FOLLOW_UP_COMMIT_FENCE_INVALID');
    }

    let storedResponse = objectValue(status.stored_response);
    if (!Object.keys(storedResponse).length) storedResponse = objectValue(details.applyResult);

    const emailActionIds = uniqueBoundedStrings(storedResponse.post_commit_email_action_ids);
    const affectedTimesheetIds = uniqueBoundedStrings(storedResponse.affected_timesheet_ids);
    const reauthoriseTargets = Array.isArray(storedResponse.post_commit_reauthorise_timesheet_ids)
      ? storedResponse.post_commit_reauthorise_timesheet_ids
      : [];
    const autoAuthoriseTargets = Array.isArray(storedResponse.auto_authorise_timesheet_ids)
      ? storedResponse.auto_authorise_timesheet_ids
      : [];
    const authoriseTimesheetIds = uniqueBoundedStrings(
      [...reauthoriseTargets, ...autoAuthoriseTargets],
      MAX_REAUTHORISE_ITEMS + 1
    ).map((value) => value.toLowerCase());
    if (reauthoriseTargets.length > MAX_REAUTHORISE_ITEMS
      || autoAuthoriseTargets.length > MAX_REAUTHORISE_ITEMS
      || authoriseTimesheetIds.length > MAX_REAUTHORISE_ITEMS
      || authoriseTimesheetIds.some((value) => !UUID_PATTERN.test(value))) {
      throw new Error('IMPORT_REVIEW_AUTHORISE_TARGET_SET_INVALID');
    }
    const componentStates = {
      EMAIL: componentStatus(storedResponse, 'EMAIL'),
      TSFIN: componentStatus(storedResponse, 'TSFIN')
    };

    if (['COMPLETE', 'NOT_REQUIRED'].includes(componentStates.EMAIL)
      && ['COMPLETE', 'NOT_REQUIRED'].includes(componentStates.TSFIN)) {
      return {
        ok: true,
        source_committed: true,
        email_follow_up_status: componentStates.EMAIL,
        tsfin_follow_up_status: componentStates.TSFIN
      };
    }

    if (componentStates.EMAIL === 'FAILED_RETRYABLE') {
      await updateComponent(env, details, {
        component: 'EMAIL', expectedStatus: 'FAILED_RETRYABLE', newStatus: 'PENDING'
      });
      componentStates.EMAIL = 'PENDING';
    }
    if (componentStates.TSFIN === 'FAILED_RETRYABLE') {
      await updateComponent(env, details, {
        component: 'TSFIN', expectedStatus: 'FAILED_RETRYABLE', newStatus: 'PENDING'
      });
      componentStates.TSFIN = 'PENDING';
    }

    let emailFailure = null;
    if (componentStates.EMAIL === 'PENDING') {
      if (!emailActionIds.length) {
        emailFailure = new Error('IMPORT_REVIEW_EMAIL_ACTION_SET_MISSING');
        await failComponent(env, details, safeComponentFailure(
          'EMAIL',
          'EMAIL_ACTION_SET_MISSING',
          'Query email follow-up has no persisted action set and requires review.'
        ));
      } else {
        try {
          await sbRpc(env, 'timesheet_query_email_enqueue_v1', {
            p_import_id: details.importId,
            p_operation_id: details.operationId,
            p_selected_action_ids: emailActionIds,
            p_actor_user_id: details.actorUserId,
            p_max_actions: MAX_FOLLOW_UP_ITEMS,
            p_max_groups: 100
          }, { timeoutMs: 30000 });
        } catch (error) {
          emailFailure = error;
          await failComponent(env, details, safeComponentFailure(
            'EMAIL',
            'EMAIL_ENQUEUE_FAILED',
            'Query email enqueue failed after source commit and can be retried safely.'
          ));
        }
      }
    }

    // Reconciliation repairs provider-accepted deliveries whose history marker
    // did not complete. It never sends or re-enqueues an email.
    if (componentStates.EMAIL === 'PENDING') {
      try {
        await sbRpc(env, 'timesheet_query_email_delivery_reconcile_v1', {
          p_after_delivery_id: null,
          p_limit: 100,
          p_actor_user_id: details.actorUserId
        }, { timeoutMs: 15000 });
      } catch {}
    }

    let tsfinFailure = null;
    if (componentStates.TSFIN === 'PENDING') {
      if (!affectedTimesheetIds.length) {
        tsfinFailure = new Error('IMPORT_REVIEW_TSFIN_TARGET_SET_MISSING');
        await failComponent(env, details, safeComponentFailure(
          'TSFIN',
          'TSFIN_TARGET_SET_MISSING',
          'TSFIN follow-up has no persisted timesheet set and requires review.'
        ));
      } else {
        let targetsSettled = false;
        try {
          const loadTargetSummary = async () => {
            const raw = await sbRpc(env, 'tsfin_follow_up_target_summary_v1', {
              p_timesheet_ids: affectedTimesheetIds,
              p_not_before_utc: committedAtUtc
            }, { timeoutMs: 8000 });
            const summary = unwrapRpcJsonb(raw, 'tsfin_follow_up_target_summary_v1') || {};
            const targetCount = Number(summary.target_count);
            const freshCount = Number(summary.fresh_target_count);
            const pendingTotal = Number(summary.pending_total);
            const completeEvidence = summary.ok === true
              && targetCount === affectedTimesheetIds.length
              && freshCount === affectedTimesheetIds.length
              && pendingTotal === 0
              && summary.all_targets_fresh === true
              && summary.all_targets_settled === true;
            return { ...summary, targetCount, freshCount, pendingTotal, completeEvidence };
          };

          let summary = await loadTargetSummary();
          targetsSettled = summary.completeEvidence;

          if (!targetsSettled) {
            // Wake only the persisted post-commit targets. This idempotent outbox
            // operation cannot repeat source apply. A bounded recheck allows the
            // scheduled TSFIN worker to finish rows that it leased concurrently.
            await sbRpc(env, 'enqueue_ts_financials_priority', {
              _timesheet_ids: affectedTimesheetIds,
              _reason: 'CONTEXT_CHANGED'
            }, { timeoutMs: 8000 });

            for (let loop = 0; loop < 10; loop += 1) {
              const run = await runTsfinWorkerOnce(env, {
                limit: 50,
                onlyTimesheetIds: affectedTimesheetIds
              });
              summary = await loadTargetSummary();
              targetsSettled = summary.completeEvidence;
              if (targetsSettled) break;
              if (Number(run?.picked || 0) === 0 && summary.pendingTotal > 0 && loop < 9) {
                await waitForConcurrentTsfin(200);
              }
            }
          }

          if (targetsSettled) {
            if (authoriseTimesheetIds.length) {
              for (let offset = 0; offset < authoriseTimesheetIds.length; offset += AUTHORISE_CHUNK_SIZE) {
                const chunk = authoriseTimesheetIds.slice(offset, offset + AUTHORISE_CHUNK_SIZE);
                let authoriseResult = null;
                for (let attempt = 0; attempt < 2; attempt += 1) {
                  try {
                    const authoriseRaw = await sbRpc(env, 'timesheet_authorise_bulk_atomic', {
                      p_items: chunk.map((timesheetId) => ({
                        timesheet_id: timesheetId,
                        expected_timesheet_id: timesheetId
                      })),
                      p_actor_user_id: details.actorUserId,
                      p_now_utc: null
                    }, { timeoutMs: 30000 });
                    authoriseResult = unwrapRpcJsonb(authoriseRaw, 'timesheet_authorise_bulk_atomic') || {};
                    break;
                  } catch (error) {
                    if (attempt > 0 || !isTransientFollowUpRpcFailure(error)) throw error;
                    await waitForConcurrentTsfin(200);
                  }
                }
                if (!reauthorisationCompleted(authoriseResult, chunk)) {
                  throw new Error('IMPORT_REVIEW_AUTHORISE_INCOMPLETE');
                }
              }
            }

            if (authoriseTimesheetIds.length) {
              let postAuthoriseSummary = await loadTargetSummary();
              let postAuthoriseSettled = postAuthoriseSummary.completeEvidence;
              for (let loop = 0; !postAuthoriseSettled && loop < 10; loop += 1) {
                const run = await runTsfinWorkerOnce(env, {
                  limit: 50,
                  onlyTimesheetIds: authoriseTimesheetIds
                });
                postAuthoriseSummary = await loadTargetSummary();
                postAuthoriseSettled = postAuthoriseSummary.completeEvidence;
                if (!postAuthoriseSettled
                    && Number(run?.picked || 0) === 0
                    && postAuthoriseSummary.pendingTotal > 0
                    && loop < 9) {
                  await waitForConcurrentTsfin(200);
                }
              }
              if (!postAuthoriseSettled) {
                throw new Error('IMPORT_REVIEW_POST_AUTHORISE_TSFIN_INCOMPLETE');
              }
            }

            await updateComponent(env, details, {
              component: 'TSFIN', expectedStatus: 'PENDING', newStatus: 'COMPLETE'
            });
          } else {
            tsfinFailure = new Error('IMPORT_REVIEW_TSFIN_FOLLOW_UP_INCOMPLETE');
            await failComponent(env, details, safeComponentFailure(
              'TSFIN',
              'TSFIN_FOLLOW_UP_INCOMPLETE',
              'TSFIN refresh remains pending and can be retried safely.'
            ));
          }
        } catch (error) {
          try {
            console.warn('[IMPORT_REVIEW][TSFIN_FOLLOW_UP_FAILED]', {
              error: String(error?.message || error || 'UNKNOWN').slice(0, 500),
              cause: String(error?.cause?.message || error?.cause || '').slice(0, 500)
            });
          } catch {}

          tsfinFailure = error;
          if (!String(error?.message || '').includes('FOLLOW_UP_FAILURE_RECORDING_FAILED')) {
            await failComponent(env, details, safeComponentFailure(
              'TSFIN',
              authoriseTimesheetIds.length ? 'TSFIN_AUTHORISE_FAILED' : 'TSFIN_FOLLOW_UP_FAILED',
              authoriseTimesheetIds.length
                ? 'TSFIN refresh, configured auto-authorisation or authorised-state restoration failed after source commit and can be retried safely.'
                : 'TSFIN refresh failed after source commit and can be retried safely.'
            ));
          }
        }
      }
    }

    if (emailFailure || tsfinFailure) {
      const error = new Error('IMPORT_REVIEW_FOLLOW_UP_FAILED_RETRYABLE');
      error.cause = emailFailure || tsfinFailure;
      throw error;
    }

    return {
      ok: true,
      source_committed: true,
      email_follow_up_status: componentStates.EMAIL,
      tsfin_follow_up_status: componentStates.TSFIN
    };
  };
}
