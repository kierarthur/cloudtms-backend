const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function upper(value) {
  return text(value).toUpperCase();
}

function unwrap(value, functionName) {
  let result = value;
  if (Array.isArray(result) && result.length === 1) [result] = result;
  if (result && typeof result === 'object' && !Array.isArray(result)
      && Object.prototype.hasOwnProperty.call(result, functionName)) {
    result = result[functionName];
  }
  if (Array.isArray(result) && result.length === 1) [result] = result;
  return result;
}

async function rpc(dependencies, functionName, request, timeoutMs = 30_000) {
  if (typeof dependencies.rpc !== 'function') {
    throw new Error('WEEKLY_COMPLETED_PACK_RPC_UNAVAILABLE');
  }
  return unwrap(await dependencies.rpc(
    functionName, { p_request: request }, { timeoutMs }
  ), functionName);
}

function safeErrorCode(error) {
  const code = upper(error?.code || error?.message);
  return /^[A-Z][A-Z0-9_]{2,119}$/.test(code)
    ? code : 'WEEKLY_COMPLETED_PACK_COPY_FAILED';
}

function validateJob(job) {
  if (!job || typeof job !== 'object' || Array.isArray(job)
      || !UUID_RE.test(text(job.workflow_id))
      || !UUID_RE.test(text(job.timesheet_id))
      || !['CHECK_ONLY', 'INVOICE_EVIDENCE_REQUIRED'].includes(upper(job.document_mode))
      || !/^[0-9a-f]{64}$/.test(text(job.render_input_sha256))) {
    throw new Error('WEEKLY_COMPLETED_PACK_JOB_INVALID');
  }
  return job;
}

export async function runWeeklySourceCompletedPackCopies(
  env, dependencies = {}, options = {}
) {
  const limit = Math.max(1, Math.min(100, Number(options.limit || 25)));
  if (typeof dependencies.renderCompletedPack !== 'function') {
    throw new Error('WEEKLY_COMPLETED_PACK_RENDERER_UNAVAILABLE');
  }

  const status = await rpc(
    dependencies, 'weekly_source_completed_pack_copy_status_sync_v1', { limit: 500 }
  );
  const due = await rpc(
    dependencies, 'weekly_source_completed_pack_copy_due_list_v1', { limit }
  );
  const items = Array.isArray(due?.items) ? due.items : [];
  let committed = 0;
  let replayed = 0;
  let failed = 0;
  const failures = [];

  for (const rawJob of items) {
    let workflowId = null;
    try {
      const job = validateJob(rawJob);
      workflowId = job.workflow_id;
      const artifact = await dependencies.renderCompletedPack(env, job);
      const result = await rpc(
        dependencies,
        'weekly_source_completed_pack_copy_commit_atomic_v1',
        artifact,
        45_000
      );
      if (result?.ok !== true) throw new Error('WEEKLY_COMPLETED_PACK_COMMIT_FAILED');
      if (result.idempotent_replay === true) replayed += 1;
      else committed += 1;
    } catch (error) {
      failed += 1;
      const errorCode = safeErrorCode(error);
      failures.push({ workflow_id: workflowId, error_code: errorCode });
      console.warn('[weekly-source-completed-pack] copy not queued', {
        workflow_id: workflowId,
        error_code: errorCode
      });
    }
  }

  return {
    ok: failed === 0,
    status_updates: Number(status?.updated_count || 0),
    due: items.length,
    committed,
    replayed,
    failed,
    failures
  };
}

export const weeklySourceCompletedPackCopyInternals = Object.freeze({
  safeErrorCode,
  validateJob
});
