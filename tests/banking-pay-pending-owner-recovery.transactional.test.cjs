const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { spawn, spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const enabled = process.env.BANKING_PAY_OWNER_RECOVERY_TRANSACTIONAL === '1';
const containerName = `cloudtms-owner-recovery-${process.pid}`;
const repoRoot = path.resolve(__dirname, '..');
const postgresImage = 'postgres:17.6-alpine';

const completionHashes = {
  pay_workbench_repair_orphaned_pending_source_build:
    '78d2a4ac9dd7b8309ed5c77112d981f0',
  pay_workbench_session_get_progress_light:
    '9f7489d1242697dea393fab3a1d748e3',
  pay_workbench_session_recompute_progress_counters:
    '3ba446a42bab9f8d25dd165a77b0af82'
};

const preDeltaHashes = {
  pay_workbench_repair_orphaned_pending_source_build:
    '977f2aa68b33a10649c69e308cf86e16',
  pay_workbench_session_get_progress_light:
    '64a227e561acf1be8bf434b13dd253c7',
  pay_workbench_session_recompute_progress_counters:
    '0830bcf4a7895de0cfee6960120580df'
};

const commonFunctionMetadata = {
  pay_workbench_repair_orphaned_pending_source_build: {
    identity_args:
      'p_session_id uuid, p_candidate_id uuid, p_limit integer, ' +
      'p_now_utc timestamp with time zone, p_reason text',
    full_args:
      'p_session_id uuid DEFAULT NULL::uuid, ' +
      'p_candidate_id uuid DEFAULT NULL::uuid, p_limit integer DEFAULT 10, ' +
      'p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, ' +
      "p_reason text DEFAULT 'PENDING_SCOPE_OWNER_REPAIR'::text",
    owner: 'postgres',
    security_definer: true,
    proconfig: ['search_path=public'],
    acl: '{postgres=X/postgres,service_role=X/postgres}',
    comment: null
  },
  pay_workbench_session_get_progress_light: {
    identity_args: 'p_session_id uuid',
    full_args: 'p_session_id uuid',
    owner: 'postgres',
    security_definer: true,
    proconfig: ['search_path=public'],
    acl: '{postgres=X/postgres,authenticated=X/postgres,service_role=X/postgres}',
    comment: null
  },
  pay_workbench_session_recompute_progress_counters: {
    identity_args:
      'p_session_id uuid, p_apply boolean, p_reason text, ' +
      'p_write_progress_json boolean',
    full_args:
      'p_session_id uuid, p_apply boolean DEFAULT true, ' +
      "p_reason text DEFAULT 'AUTHORITATIVE_COUNTER_RECOMPUTE'::text, " +
      'p_write_progress_json boolean DEFAULT true',
    owner: 'postgres',
    security_definer: true,
    proconfig: [
      'search_path=public',
      'plpgsql_check.mode=disabled',
      'plpgsql_check.profiler=off',
      'plpgsql_check.tracer=off',
      'plpgsql_check.constants_tracing=off',
      'plpgsql_check.cursors_leaks=off',
      'plpgsql_check.strict_cursors_leaks=off',
      'plpgsql_check.fatal_errors=off'
    ],
    acl: '{postgres=X/postgres,authenticated=X/postgres,service_role=X/postgres}',
    comment: null
  }
};

const expectedManifest = (hashes) => Object.fromEntries(
  Object.entries(commonFunctionMetadata).map(([name, metadata]) => [
    name,
    { ...metadata, definition_md5: hashes[name] }
  ])
);

const sleepSync = (milliseconds) => {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
};

const readSql = (name) => fs.readFileSync(
  path.resolve(repoRoot, 'supabase/repeatable', name),
  'utf8'
);

const extractFunction = (source, name) => {
  const startPattern = new RegExp(
    `^CREATE OR REPLACE FUNCTION public\\.${name}\\s*\\(`,
    'm'
  );
  const match = startPattern.exec(source);
  assert.ok(match, `${name} must have a canonical definition`);
  const tail = source.slice(match.index);
  const endMatch = /\r?\n\$function\$;/.exec(tail);
  assert.ok(endMatch, `${name} must have a complete function delimiter`);
  // pg_get_functiondef MD5 includes stored function-body line endings. TEST
  // stores LF, so normalise the Windows checkout before reproducibility checks.
  return tail
    .slice(0, endMatch.index + endMatch[0].length)
    .replaceAll('\r\n', '\n');
};

const canonicalSql = readSql('26052026_2100HRS_NEW_FUNCTIONS.sql');
const repairSql = readSql(
  '04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql'
);
const progressSql = extractFunction(
  canonicalSql,
  'pay_workbench_session_get_progress_light'
);
const recomputeSql = extractFunction(
  readSql('07082026_2155_pay_workbench_session_recompute_progress_counters.sql'),
  'pay_workbench_session_recompute_progress_counters'
);
const helperSql = extractFunction(
  repairSql,
  'pay_workbench_repair_orphaned_pending_source_build'
);
const rollbackSql = fs.readFileSync(
  path.resolve(
    repoRoot,
    'codex_outputs',
    'banking-pay-pending-owner-recovery',
    '31072026_1122_banking_pay_source_build_owner_recovery_completion_rollback.sql'
  ),
  'utf8'
);

const rollbackFailureMarker =
  'CREATE OR REPLACE FUNCTION public.pay_workbench_session_recompute_progress_counters';
const rollbackFailureSql = rollbackSql.replace(
  rollbackFailureMarker,
  `DO $forced_failure$
  BEGIN
    RAISE EXCEPTION 'FORCED_MID_ROLLBACK_FAILURE' USING ERRCODE = 'P0001';
  END
  $forced_failure$;

  ${rollbackFailureMarker}`
);
assert.notEqual(
  rollbackFailureSql,
  rollbackSql,
  'replacement rollback must expose the deterministic failure injection point'
);

const run = (command, args, options = {}) => {
  const result = spawnSync(command, args, {
    encoding: 'utf8',
    timeout: 120000,
    ...options
  });
  if (result.status !== 0) {
    throw new Error(
      `${command} failed (${result.status}): ${String(result.stderr || '').slice(0, 2000)}`
    );
  }
  return String(result.stdout || '');
};

const psql = (sql) => run(
  'docker',
  [
    'exec',
    '-i',
    containerName,
    'psql',
    '-X',
    '-v',
    'ON_ERROR_STOP=1',
    '-qAt',
    '-U',
    'postgres',
    '-d',
    'postgres'
  ],
  { input: sql }
);

const psqlExpectFailure = (sql) => {
  const result = spawnSync(
    'docker',
    [
      'exec',
      '-i',
      containerName,
      'psql',
      '-X',
      '-v',
      'ON_ERROR_STOP=1',
      '-qAt',
      '-U',
      'postgres',
      '-d',
      'postgres'
    ],
    { input: sql, encoding: 'utf8', timeout: 120000 }
  );
  assert.notEqual(result.status, 0, 'SQL was expected to fail');
  return String(result.stderr || '');
};

const psqlAsync = (sql) => new Promise((resolve, reject) => {
  const child = spawn(
    'docker',
    [
      'exec',
      '-i',
      containerName,
      'psql',
      '-X',
      '-v',
      'ON_ERROR_STOP=1',
      '-qAt',
      '-U',
      'postgres',
      '-d',
      'postgres'
    ],
    { stdio: ['pipe', 'pipe', 'pipe'] }
  );
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  child.on('error', reject);
  child.on('close', (code) => {
    if (code === 0) resolve(stdout);
    else reject(new Error(`psql failed (${code}): ${stderr.slice(0, 2000)}`));
  });
  child.stdin.end(sql);
});

const parseLastJson = (output) => {
  const lines = output.trim().split(/\r?\n/).filter(Boolean);
  assert.ok(lines.length > 0, 'expected a JSON result');
  return JSON.parse(lines.at(-1));
};

const completionMetadataSql = `
  ALTER FUNCTION public.pay_workbench_session_get_progress_light(uuid)
    OWNER TO postgres;
  REVOKE ALL ON FUNCTION public.pay_workbench_session_get_progress_light(uuid)
    FROM PUBLIC, anon, authenticated, service_role;
  GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_progress_light(uuid)
    TO postgres, authenticated, service_role;
  COMMENT ON FUNCTION public.pay_workbench_session_get_progress_light(uuid)
    IS NULL;

  ALTER FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean)
    OWNER TO postgres;
  REVOKE ALL ON FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean)
    FROM PUBLIC, anon, authenticated, service_role;
  GRANT EXECUTE ON FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean)
    TO postgres, authenticated, service_role;
  COMMENT ON FUNCTION public.pay_workbench_session_recompute_progress_counters(uuid, boolean, text, boolean)
    IS NULL;

  ALTER FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text)
    OWNER TO postgres;
  REVOKE ALL ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text)
    FROM PUBLIC, anon, authenticated, service_role;
  GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text)
    TO postgres, service_role;
  COMMENT ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text)
    IS NULL;
`;

const installCompletionFunctions = () => {
  psql(progressSql);
  psql(recomputeSql);
  psql(helperSql);
  psql(completionMetadataSql);
};

const getFunctionManifest = () => parseLastJson(psql(`
  SELECT jsonb_object_agg(manifest.function_name, manifest.details)::text
  FROM (
    SELECT
      p.proname AS function_name,
      jsonb_build_object(
        'identity_args', pg_get_function_identity_arguments(p.oid),
        'full_args', pg_get_function_arguments(p.oid),
        'owner', pg_get_userbyid(p.proowner),
        'security_definer', p.prosecdef,
        'proconfig', to_jsonb(p.proconfig),
        'acl', p.proacl::text,
        'comment', obj_description(p.oid, 'pg_proc'),
        'definition_md5', md5(pg_get_functiondef(p.oid))
      ) AS details
    FROM pg_proc AS p
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
      AND (
        (
          p.proname = 'pay_workbench_repair_orphaned_pending_source_build'
          AND pg_get_function_identity_arguments(p.oid) =
            'p_session_id uuid, p_candidate_id uuid, p_limit integer, ' ||
            'p_now_utc timestamp with time zone, p_reason text'
        )
        OR (
          p.proname = 'pay_workbench_session_get_progress_light'
          AND pg_get_function_identity_arguments(p.oid) = 'p_session_id uuid'
        )
        OR (
          p.proname = 'pay_workbench_session_recompute_progress_counters'
          AND pg_get_function_identity_arguments(p.oid) =
            'p_session_id uuid, p_apply boolean, p_reason text, ' ||
            'p_write_progress_json boolean'
        )
      )
  ) AS manifest;
`));

const assertFunctionManifest = (hashes) => {
  assert.deepEqual(getFunctionManifest(), expectedManifest(hashes));
};

const sqlString = (value) => `'${String(value).replaceAll("'", "''")}'`;

const resetDatabase = () => {
  psql(`
    TRUNCATE TABLE
      public.banking_pay_workbench_preview_rows,
      public.banking_pay_workbench_candidate_line_work,
      public.banking_pay_workbench_candidate_source_lines,
      public.banking_pay_workbench_jobs,
      public.banking_pay_workbench_session_scope,
      public.app_change_counters,
      public.test_control,
      public.banking_pay_workbench_sessions;
  `);
};

const insertSession = (sessionId, version = 1) => {
  psql(`
    INSERT INTO public.banking_pay_workbench_sessions (
      id, status, version, scope_seed_complete, scope_next_cursor_json,
      section_counts_json, candidate_sample_rows_json, progress_json,
      created_at_utc, updated_at_utc
    ) VALUES (
      '${sessionId}', 'OPEN', ${version}, true, '{}'::jsonb,
      '{}'::jsonb, '[]'::jsonb, '{}'::jsonb, now(), now()
    );
  `);
};

const insertOwnerFixture = ({
  sessionId,
  candidateId,
  scopeId = randomUUID(),
  ownerId = randomUUID(),
  sessionVersion = 1,
  liveSeq = 11,
  ownerSeq = 10,
  ownerStatus = 'FAILED',
  attemptCount = 0,
  maxAttempts = 8,
  scopeOrdinal = 1
}) => {
  const runId = randomUUID();
  psql(`
    INSERT INTO public.app_change_counters(entity_key, seq, updated_at)
    VALUES ('pay_candidate:${candidateId}', ${liveSeq}, now());

    INSERT INTO public.banking_pay_workbench_jobs (
      id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
      session_id, candidate_id, payload_json, created_at_utc, updated_at_utc,
      completed_at_utc, failed_at_utc
    ) VALUES (
      '${ownerId}', 'WORKBENCH_CANDIDATE_SOURCE_BUILD', '${ownerStatus}', 100,
      now(), ${attemptCount}, ${maxAttempts}, '${sessionId}', '${candidateId}',
      jsonb_build_object(
        'session_version', ${sessionVersion},
        'source_change_seq', ${ownerSeq},
        'source_build_run_id', '${runId}',
        'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
        'pay_channel_scope', 'ALL'
      ),
      now(), now(),
      CASE WHEN '${ownerStatus}' = 'SUCCEEDED' THEN now() ELSE NULL END,
      CASE WHEN '${ownerStatus}' IN ('FAILED', 'DEAD') THEN now() ELSE NULL END
    );

    INSERT INTO public.banking_pay_workbench_session_scope (
      id, session_id, candidate_id, scope_ordinal, status, pending_job_id,
      seeded, dirty, error_json, created_at_utc, updated_at_utc
    ) VALUES (
      '${scopeId}', '${sessionId}', '${candidateId}', ${scopeOrdinal},
      'SOURCE_BUILD_PENDING', '${ownerId}', false, true, NULL, now(), now()
    );
  `);
  return { ownerId, runId, scopeId };
};

const insertSuccessfulBuild = ({
  sessionId,
  candidateId,
  sessionVersion = 1,
  sourceSeq = 11,
  includeDirty = false
}) => {
  const jobId = randomUUID();
  const runId = randomUUID();
  psql(`
    INSERT INTO public.banking_pay_workbench_jobs (
      id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
      session_id, candidate_id, payload_json, created_at_utc, updated_at_utc,
      completed_at_utc, failed_at_utc
    ) VALUES (
      '${jobId}', 'WORKBENCH_CANDIDATE_SOURCE_BUILD', 'SUCCEEDED', 100,
      now(), 1, 8, '${sessionId}', '${candidateId}',
      jsonb_build_object(
        'session_version', ${sessionVersion},
        'source_change_seq', ${sourceSeq},
        'source_build_run_id', '${runId}',
        'refresh_scope_kind', 'CANDIDATE_FULL_LIVE'
      ),
      now(), now(), now(), NULL
    );

    INSERT INTO public.banking_pay_workbench_candidate_source_lines (
      id, session_id, candidate_id, session_version, source_change_seq,
      source_build_run_id, source_ordinal, line_key, status,
      created_at_utc, updated_at_utc
    ) VALUES (
      gen_random_uuid(), '${sessionId}', '${candidateId}', ${sessionVersion},
      ${sourceSeq}, '${runId}', 1, 'CURRENT-LINE', 'CURRENT', now(), now()
    );

    ${includeDirty ? `
      INSERT INTO public.banking_pay_workbench_candidate_source_lines (
        id, session_id, candidate_id, session_version, source_change_seq,
        source_build_run_id, source_ordinal, line_key, status,
        created_at_utc, updated_at_utc
      ) VALUES (
        gen_random_uuid(), '${sessionId}', '${candidateId}', ${sessionVersion},
        ${sourceSeq}, '${runId}', 2, 'DIRTY-LINE', 'DIRTY', now(), now()
      );
    ` : ''}
  `);
  return { jobId, runId };
};

const insertActiveSuccessor = ({
  sessionId,
  candidateId,
  sessionVersion = 1,
  sourceSeq = 11,
  status = 'QUEUED',
  createdAt = 'now()'
}) => {
  const jobId = randomUUID();
  const runId = randomUUID();
  psql(`
    INSERT INTO public.banking_pay_workbench_jobs (
      id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
      session_id, candidate_id, payload_json, created_at_utc, updated_at_utc,
      started_at_utc
    ) VALUES (
      '${jobId}', 'WORKBENCH_CANDIDATE_SOURCE_BUILD', '${status}', 100,
      now(), 0, 8, '${sessionId}', '${candidateId}',
      jsonb_build_object(
        'session_version', ${sessionVersion},
        'source_change_seq', ${sourceSeq},
        'source_build_run_id', '${runId}'
      ),
      ${createdAt}, now(),
      CASE WHEN '${status}' = 'RUNNING' THEN now() ELSE NULL END
    );
  `);
  return { jobId, runId };
};

const setControl = (candidateId, {
  reconcileMode = 'SUCCESS',
  enqueueMode = 'NORMAL',
  suppressScopeUpdates = false
} = {}) => {
  psql(`
    INSERT INTO public.test_control(
      candidate_id, reconcile_mode, enqueue_mode, suppress_scope_updates
    ) VALUES (
      '${candidateId}', ${sqlString(reconcileMode)}, ${sqlString(enqueueMode)},
      ${suppressScopeUpdates}
    )
    ON CONFLICT (candidate_id) DO UPDATE
    SET reconcile_mode = EXCLUDED.reconcile_mode,
        enqueue_mode = EXCLUDED.enqueue_mode,
        suppress_scope_updates = EXCLUDED.suppress_scope_updates;
  `);
};

const callHelper = ({
  sessionId = null,
  candidateId = null,
  limit = 10
} = {}) => parseLastJson(psql(`
  SELECT public.pay_workbench_repair_orphaned_pending_source_build(
    ${sessionId ? `'${sessionId}'::uuid` : 'NULL::uuid'},
    ${candidateId ? `'${candidateId}'::uuid` : 'NULL::uuid'},
    ${limit},
    now(),
    'TRANSACTIONAL_TEST'
  )::text;
`));

const setupSql = `
  CREATE EXTENSION IF NOT EXISTS pgcrypto;
  DO $roles$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
      CREATE ROLE anon;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
      CREATE ROLE authenticated;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
      CREATE ROLE service_role;
    END IF;
  END
  $roles$;

  CREATE TABLE public.banking_pay_workbench_sessions (
    id uuid PRIMARY KEY,
    actor_user_id uuid,
    pay_date date,
    week_ending_cutoff date,
    filters_json jsonb DEFAULT '{}'::jsonb,
    session_signature text,
    source_snapshot_run_id uuid,
    status text,
    version bigint DEFAULT 1,
    server_selected_preview_row_ids jsonb DEFAULT '[]'::jsonb,
    created_at_utc timestamptz DEFAULT now(),
    updated_at_utc timestamptz DEFAULT now(),
    discarded_at_utc timestamptz,
    server_selected_preview_row_ids_provided boolean DEFAULT false,
    scope_next_cursor_json jsonb DEFAULT '{}'::jsonb,
    scope_seed_complete boolean DEFAULT true,
    scope_total_count integer DEFAULT 0,
    scope_seeded_count integer DEFAULT 0,
    scope_ready_count integer DEFAULT 0,
    scope_pending_count integer DEFAULT 0,
    scope_failed_count integer DEFAULT 0,
    line_units_total integer DEFAULT 0,
    line_units_ready integer DEFAULT 0,
    line_units_pending integer DEFAULT 0,
    line_units_failed integer DEFAULT 0,
    preview_row_count integer DEFAULT 0,
    selected_row_count integer DEFAULT 0,
    section_counts_json jsonb DEFAULT '{}'::jsonb,
    candidate_sample_rows_json jsonb DEFAULT '[]'::jsonb,
    progress_state text DEFAULT 'REFRESHING_CANDIDATES',
    progress_json jsonb DEFAULT '{}'::jsonb,
    progress_counter_version bigint DEFAULT 0,
    progress_updated_at_utc timestamptz,
    scope_candidate_ids uuid[] DEFAULT ARRAY[]::uuid[],
    replacement_session_id uuid,
    replacement_idempotency_key text,
    scope_discovery_checked_at_utc timestamptz
  );

  CREATE TABLE public.banking_pay_workbench_jobs (
    id uuid PRIMARY KEY,
    job_type text,
    status text,
    priority integer DEFAULT 100,
    run_at_utc timestamptz DEFAULT now(),
    attempt_count integer DEFAULT 0,
    max_attempts integer DEFAULT 8,
    dedupe_key text,
    snapshot_run_id uuid,
    session_id uuid,
    candidate_id uuid,
    payload_json jsonb DEFAULT '{}'::jsonb,
    created_at_utc timestamptz DEFAULT now(),
    updated_at_utc timestamptz DEFAULT now(),
    started_at_utc timestamptz,
    completed_at_utc timestamptz,
    failed_at_utc timestamptz,
    last_error_json jsonb
  );

  CREATE TABLE public.banking_pay_workbench_session_scope (
    id uuid PRIMARY KEY,
    session_id uuid,
    candidate_id uuid,
    scope_ordinal bigint,
    status text,
    pending_job_id uuid,
    seeded boolean DEFAULT false,
    dirty boolean DEFAULT false,
    error_json jsonb,
    created_at_utc timestamptz DEFAULT now(),
    updated_at_utc timestamptz DEFAULT now(),
    UNIQUE(session_id, candidate_id)
  );

  CREATE TABLE public.banking_pay_workbench_candidate_source_lines (
    id uuid PRIMARY KEY,
    session_id uuid,
    candidate_id uuid,
    session_version bigint,
    source_change_seq bigint,
    source_build_run_id uuid,
    source_ordinal bigint,
    line_key text,
    parent_line_key text,
    split_suffix text,
    timesheet_id uuid,
    section text,
    source_row_json jsonb,
    economic_key_json jsonb,
    contract_json jsonb,
    pay_channel_scope text,
    refresh_scope_kind text,
    status text,
    created_at_utc timestamptz DEFAULT now(),
    updated_at_utc timestamptz DEFAULT now()
  );

  CREATE TABLE public.banking_pay_workbench_candidate_line_work (
    id uuid PRIMARY KEY,
    session_id uuid,
    candidate_id uuid,
    timesheet_id uuid,
    line_key text,
    line_ordinal bigint,
    status text,
    work_payload_json jsonb,
    result_row_json jsonb,
    error_json jsonb,
    created_at_utc timestamptz DEFAULT now(),
    updated_at_utc timestamptz DEFAULT now()
  );

  CREATE TABLE public.banking_pay_workbench_preview_rows (
    id uuid PRIMARY KEY,
    session_id uuid,
    candidate_id uuid,
    section text,
    row_key text,
    row_ordinal bigint,
    row_json jsonb,
    timesheet_id uuid,
    key_type text,
    key_value text,
    selected boolean,
    selection_state text,
    status text,
    session_version bigint,
    created_at_utc timestamptz DEFAULT now(),
    updated_at_utc timestamptz DEFAULT now()
  );

  CREATE TABLE public.app_change_counters (
    entity_key text PRIMARY KEY,
    seq bigint NOT NULL DEFAULT 0,
    updated_at timestamptz DEFAULT now()
  );

  CREATE TABLE public.test_control (
    candidate_id uuid PRIMARY KEY,
    reconcile_mode text NOT NULL DEFAULT 'SUCCESS',
    enqueue_mode text NOT NULL DEFAULT 'NORMAL',
    suppress_scope_updates boolean NOT NULL DEFAULT false
  );

  CREATE OR REPLACE FUNCTION public.banking_pay_hot_path_budget_apply(text)
  RETURNS void
  LANGUAGE sql
  AS $$ SELECT NULL::void $$;

  CREATE OR REPLACE FUNCTION public.pay_workbench_session_compact_progress_json(
    p_progress_json jsonb DEFAULT '{}'::jsonb,
    p_keep_active_jobs boolean DEFAULT true
  )
  RETURNS jsonb
  LANGUAGE sql
  IMMUTABLE
  AS $$ SELECT COALESCE(p_progress_json, '{}'::jsonb) $$;

  CREATE OR REPLACE FUNCTION public._audit_insert(
    text, text, text, jsonb, jsonb, text, uuid
  )
  RETURNS void
  LANGUAGE sql
  AS $$ SELECT NULL::void $$;

  CREATE OR REPLACE FUNCTION public.pay_workbench_reconcile_successful_source_build(
    p_session_id uuid,
    p_candidate_id uuid,
    p_source_build_run_id uuid,
    p_source_change_seq bigint,
    p_session_version bigint DEFAULT NULL::bigint,
    p_success_job_id uuid DEFAULT NULL::uuid,
    p_refresh_scope_kind text DEFAULT NULL::text,
    p_targeted_timesheet_ids jsonb DEFAULT '[]'::jsonb,
    p_linked_timesheet_ids jsonb DEFAULT '[]'::jsonb,
    p_recompute_session_progress boolean DEFAULT true
  )
  RETURNS jsonb
  LANGUAGE plpgsql
  AS $stub$
  DECLARE
    v_mode text := 'SUCCESS';
  BEGIN
    SELECT COALESCE(control.reconcile_mode, 'SUCCESS')
    INTO v_mode
    FROM public.test_control AS control
    WHERE control.candidate_id = p_candidate_id;
    v_mode := COALESCE(v_mode, 'SUCCESS');

    IF v_mode = 'RAISE' THEN
      RAISE EXCEPTION 'CONTROLLED_RECONCILIATION_FAILURE' USING ERRCODE = 'P0001';
    ELSIF v_mode = 'SKIP' THEN
      RETURN jsonb_build_object('ok', true, 'skipped', true, 'deferred', false);
    ELSIF v_mode = 'BAD_POST' THEN
      RETURN jsonb_build_object('ok', true, 'skipped', false, 'deferred', false);
    END IF;

    UPDATE public.banking_pay_workbench_session_scope
    SET status = 'SOURCE_READY',
        pending_job_id = NULL,
        dirty = false,
        error_json = NULL,
        updated_at_utc = now()
    WHERE session_id = p_session_id
      AND candidate_id = p_candidate_id;

    RETURN jsonb_build_object(
      'ok', true,
      'skipped', false,
      'deferred', false,
      'progress_recomputed', p_recompute_session_progress
    );
  END
  $stub$;

  CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_candidate_refresh(
    p_snapshot_run_id uuid,
    p_candidate_id uuid,
    p_reason text DEFAULT NULL::text,
    p_actor_user_id uuid DEFAULT NULL::uuid,
    p_payload_json jsonb DEFAULT '{}'::jsonb
  )
  RETURNS jsonb
  LANGUAGE plpgsql
  AS $stub$
  DECLARE
    v_mode text := 'NORMAL';
    v_job_id uuid := gen_random_uuid();
    v_run_id uuid := gen_random_uuid();
    v_session_id uuid := (p_payload_json->>'session_id')::uuid;
  BEGIN
    SELECT COALESCE(control.enqueue_mode, 'NORMAL')
    INTO v_mode
    FROM public.test_control AS control
    WHERE control.candidate_id = p_candidate_id;
    v_mode := COALESCE(v_mode, 'NORMAL');

    IF v_mode = 'RAISE' THEN
      RAISE EXCEPTION 'CONTROLLED_ENQUEUE_FAILURE' USING ERRCODE = 'P0001';
    ELSIF v_mode = 'INVALID' THEN
      RETURN jsonb_build_object('job_id', v_job_id::text);
    END IF;

    INSERT INTO public.banking_pay_workbench_jobs (
      id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
      snapshot_run_id, session_id, candidate_id, payload_json,
      created_at_utc, updated_at_utc
    ) VALUES (
      v_job_id, 'WORKBENCH_CANDIDATE_SOURCE_BUILD', 'QUEUED', 100, now(), 0, 8,
      p_snapshot_run_id, v_session_id, p_candidate_id,
      COALESCE(p_payload_json, '{}'::jsonb)
        || jsonb_build_object('source_build_run_id', v_run_id::text),
      now(), now()
    );

    UPDATE public.banking_pay_workbench_session_scope
    SET status = 'SOURCE_BUILD_PENDING',
        pending_job_id = v_job_id,
        dirty = true,
        error_json = NULL,
        updated_at_utc = now()
    WHERE session_id = v_session_id
      AND candidate_id = p_candidate_id;

    RETURN jsonb_build_object('job_id', v_job_id::text);
  END
  $stub$;

  CREATE OR REPLACE FUNCTION public.test_suppress_scope_update()
  RETURNS trigger
  LANGUAGE plpgsql
  AS $trigger$
  BEGIN
    IF EXISTS (
      SELECT 1
      FROM public.test_control AS control
      WHERE control.candidate_id = OLD.candidate_id
        AND control.suppress_scope_updates
    ) THEN
      RETURN NULL;
    END IF;
    RETURN NEW;
  END
  $trigger$;

  CREATE TRIGGER test_suppress_scope_update
  BEFORE UPDATE ON public.banking_pay_workbench_session_scope
  FOR EACH ROW
  EXECUTE FUNCTION public.test_suppress_scope_update();
`;

if (enabled) test.before(() => {
  run('docker', [
    'run',
    '--rm',
    '-d',
    '--name',
    containerName,
    '-e',
    'POSTGRES_HOST_AUTH_METHOD=trust',
    postgresImage
  ]);

  let ready = false;
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const logs = spawnSync(
      'docker',
      ['logs', containerName],
      { encoding: 'utf8', timeout: 5000 }
    );
    const logText = `${String(logs.stdout || '')}\n${String(logs.stderr || '')}`;
    const check = spawnSync(
      'docker',
      ['exec', containerName, 'pg_isready', '-U', 'postgres', '-d', 'postgres'],
      { encoding: 'utf8', timeout: 5000 }
    );
    if (
      logText.includes('PostgreSQL init process complete; ready for start up.')
      && check.status === 0
    ) {
      ready = true;
      break;
    }
    sleepSync(250);
  }
  assert.ok(ready, 'disposable PostgreSQL did not become ready');

  psql(setupSql);
  installCompletionFunctions();
});

if (enabled) test.after(() => {
  spawnSync('docker', ['rm', '-f', containerName], {
    encoding: 'utf8',
    timeout: 30000
  });
});

test('M00 exact PostgreSQL 17.6 compilation reproduces the verified completion manifest', { skip: !enabled }, () => {
  assert.equal(
    run('docker', ['inspect', '--format', '{{.Config.Image}}', containerName]).trim(),
    postgresImage
  );
  assert.equal(psql('SHOW server_version;').trim(), '17.6');
  assert.equal(psql("SELECT current_setting('server_version_num');").trim(), '170006');
  assertFunctionManifest(completionHashes);
});

test('R01 a forced mid-rollback failure is atomic and preserves the completion manifest', { skip: !enabled }, () => {
  const errorText = psqlExpectFailure(rollbackFailureSql);
  assert.match(errorText, /FORCED_MID_ROLLBACK_FAILURE/);
  assertFunctionManifest(completionHashes);
});

test('R02 replacement rollback is exact and completion definitions reinstall cleanly', { skip: !enabled }, () => {
  psql(rollbackSql);
  assertFunctionManifest(preDeltaHashes);

  installCompletionFunctions();
  assertFunctionManifest(completionHashes);
});

test('T01 numeric stale active owner is selected and replaced by one current successor', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  const { ownerId } = insertOwnerFixture({
    sessionId,
    candidateId,
    ownerStatus: 'QUEUED',
    ownerSeq: 10,
    liveSeq: 11
  });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.repaired_count, 1);
  assert.equal(result.enqueued_count, 1);
  assert.equal(result.unresolved_count, 0);
  assert.equal(result.results[0].state_transition_proven, true);

  const state = parseLastJson(psql(`
    SELECT jsonb_build_object(
      'old_retained', scope_row.pending_job_id = '${ownerId}'::uuid,
      'current_successors', COUNT(*) FILTER (
        WHERE job_row.id = scope_row.pending_job_id
          AND job_row.status IN ('QUEUED', 'RUNNING')
          AND (job_row.payload_json->>'source_change_seq')::bigint >= 11
          AND (job_row.payload_json->>'session_version')::bigint = 1
          AND job_row.payload_json->>'source_build_run_id' ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      )
    )::text
    FROM public.banking_pay_workbench_session_scope AS scope_row
    LEFT JOIN public.banking_pay_workbench_jobs AS job_row
      ON job_row.session_id = scope_row.session_id
     AND job_row.candidate_id = scope_row.candidate_id
    WHERE scope_row.session_id = '${sessionId}'
      AND scope_row.candidate_id = '${candidateId}'
    GROUP BY scope_row.pending_job_id;
  `));
  assert.equal(state.old_retained, false);
  assert.equal(state.current_successors, 1);
});

test('T02 a current active owner is not mutated or recomputed', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  const { ownerId } = insertOwnerFixture({
    sessionId,
    candidateId,
    ownerStatus: 'QUEUED',
    ownerSeq: 11,
    liveSeq: 11
  });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.repaired_count, 0);
  assert.equal(result.progress_recomputed_count, 0);
  assert.equal(result.unresolved_count, 0);
  assert.equal(result.examined_count, 0);
  assert.equal(psql(`
    SELECT pending_job_id::text
    FROM public.banking_pay_workbench_session_scope
    WHERE session_id = '${sessionId}' AND candidate_id = '${candidateId}';
  `).trim(), ownerId);
});

test('T03 a genuinely usable completed build is reconciled and then recomputed', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });
  insertSuccessfulBuild({ sessionId, candidateId });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.reconciled_count, 1);
  assert.equal(result.enqueued_count, 0);
  assert.equal(result.results[0].progress_recomputed, true);
  assert.equal(result.results[0].state_transition_proven, true);
  assert.equal(psql(`
    SELECT status || ':' || dirty::text || ':' || COALESCE(pending_job_id::text, 'NULL')
    FROM public.banking_pay_workbench_session_scope
    WHERE session_id = '${sessionId}' AND candidate_id = '${candidateId}';
  `).trim(), 'SOURCE_READY:false:NULL');
});

test('T04 mixed CURRENT and DIRTY evidence is not reused and remains untouched', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });
  insertSuccessfulBuild({ sessionId, candidateId, includeDirty: true });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.reconciled_count, 0);
  assert.equal(result.enqueued_count, 1);
  assert.equal(psql(`
    SELECT string_agg(status, ',' ORDER BY status)
    FROM public.banking_pay_workbench_candidate_source_lines
    WHERE session_id = '${sessionId}' AND candidate_id = '${candidateId}';
  `).trim(), 'CURRENT,DIRTY');
});

test('T05 ok=true skipped=true reconciliation continues to the real recovery branch', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });
  insertSuccessfulBuild({ sessionId, candidateId });
  setControl(candidateId, { reconcileMode: 'SKIP' });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.reconciled_count, 0);
  assert.equal(result.enqueued_count, 1);
  assert.equal(result.results[0].action, 'ENQUEUED_CANONICAL_SUCCESSOR');
});

test('T06 reconciliation exception rolls back locally and does not leak raw errors', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });
  insertSuccessfulBuild({ sessionId, candidateId });
  setControl(candidateId, { reconcileMode: 'RAISE' });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.reconciled_count, 0);
  assert.equal(result.enqueued_count, 1);
  assert.doesNotMatch(JSON.stringify(result), /CONTROLLED_RECONCILIATION_FAILURE/);
  assert.equal(result.results[0].action, 'ENQUEUED_CANONICAL_SUCCESSOR');
});

test('T07 active-successor rebind is counted only after the exact postcondition', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });
  const successor = insertActiveSuccessor({ sessionId, candidateId, sourceSeq: 11 });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.rebound_count, 1);
  assert.equal(result.results[0].successor_job_id, successor.jobId);
  assert.equal(result.results[0].progress_recomputed, true);
  assert.equal(psql(`
    SELECT pending_job_id::text || ':' || dirty::text || ':' || COALESCE(error_json::text, 'NULL')
    FROM public.banking_pay_workbench_session_scope
    WHERE session_id = '${sessionId}' AND candidate_id = '${candidateId}';
  `).trim(), `${successor.jobId}:true:NULL`);
});

test('T08 maximum-attempt fail close proves the safe terminal state', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({
    sessionId,
    candidateId,
    ownerStatus: 'FAILED',
    attemptCount: 8,
    maxAttempts: 8
  });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.failed_closed_count, 1);
  assert.equal(result.results[0].action, 'FAILED_CLOSED_MAX_ATTEMPTS');
  assert.equal(result.results[0].progress_recomputed, true);
  assert.equal(psql(`
    SELECT status || ':' || COALESCE(pending_job_id::text, 'NULL') || ':' ||
           (error_json->>'code')
    FROM public.banking_pay_workbench_session_scope
    WHERE session_id = '${sessionId}' AND candidate_id = '${candidateId}';
  `).trim(), 'SOURCE_BUILD_ERROR:NULL:WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB');
});

test('T09 canonical enqueue proves returned job identity and exact scope binding', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.enqueued_count, 1);
  const successorId = result.results[0].successor_job_id;
  const proof = parseLastJson(psql(`
    SELECT jsonb_build_object(
      'bound', scope_row.pending_job_id = job_row.id,
      'status', job_row.status,
      'type', job_row.job_type,
      'session_match', job_row.session_id = scope_row.session_id,
      'candidate_match', job_row.candidate_id = scope_row.candidate_id,
      'version', (job_row.payload_json->>'session_version')::bigint,
      'sequence', (job_row.payload_json->>'source_change_seq')::bigint,
      'run_id_valid', job_row.payload_json->>'source_build_run_id' ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )::text
    FROM public.banking_pay_workbench_session_scope AS scope_row
    JOIN public.banking_pay_workbench_jobs AS job_row
      ON job_row.id = scope_row.pending_job_id
    WHERE job_row.id = '${successorId}';
  `));
  assert.deepEqual(proof, {
    bound: true,
    status: 'QUEUED',
    type: 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    session_match: true,
    candidate_match: true,
    version: 1,
    sequence: 11,
    run_id_valid: true
  });
});

test('T10 one candidate postcondition failure does not undo an earlier proven repair', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateA = randomUUID();
  const candidateB = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId: candidateA, ownerStatus: 'FAILED', scopeOrdinal: 1 });
  const fixtureB = insertOwnerFixture({
    sessionId,
    candidateId: candidateB,
    ownerStatus: 'FAILED',
    scopeOrdinal: 2
  });
  setControl(candidateB, { suppressScopeUpdates: true });

  const result = callHelper({ sessionId, limit: 2 });
  assert.equal(result.repaired_count, 1);
  assert.equal(result.unresolved_count, 1);
  assert.equal(result.partial, true);
  assert.equal(result.all_state_transitions_proven, false);

  const resultA = result.results.find((row) => row.candidate_id === candidateA);
  const resultB = result.results.find((row) => row.candidate_id === candidateB);
  assert.equal(resultA.repaired, true);
  assert.equal(resultB.repaired, false);
  assert.equal(resultB.state_transition_proven, false);
  assert.equal(resultB.action, 'UNRESOLVED_POSTCONDITION_NOT_PROVEN');
  assert.equal(resultB.failure_code, 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB');
  assert.equal(resultB.unresolved_reason, 'POSTCONDITION_NOT_PROVEN');
  assert.doesNotMatch(JSON.stringify(resultB), /CONTROLLED|SQLSTATE|CONTEXT|DETAIL/);

  assert.equal(psql(`
    SELECT pending_job_id::text
    FROM public.banking_pay_workbench_session_scope
    WHERE session_id = '${sessionId}' AND candidate_id = '${candidateB}';
  `).trim(), fixtureB.ownerId);

  const light = parseLastJson(psql(`
    SELECT public.pay_workbench_session_get_progress_light('${sessionId}')::text;
  `));
  assert.equal(
    light.pending_owner_failures.some((row) => row.candidate_id === candidateB),
    true
  );

  setControl(candidateB, { suppressScopeUpdates: false });
  const retry = callHelper({ sessionId, candidateId: candidateB });
  assert.equal(retry.repaired_count, 1);
  assert.equal(retry.unresolved_count, 0);
});

test('T11 all four proven branches report successful separate recomputation', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const reconciled = randomUUID();
  const rebound = randomUUID();
  const enqueued = randomUUID();
  const failedClosed = randomUUID();
  insertSession(sessionId);

  insertOwnerFixture({ sessionId, candidateId: reconciled, ownerStatus: 'FAILED', scopeOrdinal: 1 });
  insertSuccessfulBuild({ sessionId, candidateId: reconciled });
  insertOwnerFixture({ sessionId, candidateId: rebound, ownerStatus: 'FAILED', scopeOrdinal: 2 });
  insertActiveSuccessor({ sessionId, candidateId: rebound, sourceSeq: 11 });
  insertOwnerFixture({ sessionId, candidateId: enqueued, ownerStatus: 'FAILED', scopeOrdinal: 3 });
  insertOwnerFixture({
    sessionId,
    candidateId: failedClosed,
    ownerStatus: 'FAILED',
    attemptCount: 8,
    maxAttempts: 8,
    scopeOrdinal: 4
  });

  const result = callHelper({ sessionId, limit: 4 });
  assert.equal(result.repaired_count, 4);
  assert.equal(result.reconciled_count, 1);
  assert.equal(result.rebound_count, 1);
  assert.equal(result.enqueued_count, 1);
  assert.equal(result.failed_closed_count, 1);
  assert.equal(result.progress_recomputed_count, 4);
  assert.equal(result.progress_recompute_failed_count, 0);
  assert.equal(result.all_progress_recomputed, true);
  for (const row of result.results) {
    assert.equal(row.progress_recomputed, true);
    assert.equal(row.progress_recompute_error_code, null);
  }
});

test('T12 recomputation failure preserves the proven ownership transition', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });

  psql(`
    CREATE OR REPLACE FUNCTION public.pay_workbench_session_recompute_progress_counters(
      p_session_id uuid,
      p_apply boolean DEFAULT true,
      p_reason text DEFAULT 'AUTHORITATIVE_COUNTER_RECOMPUTE'::text,
      p_write_progress_json boolean DEFAULT true
    )
    RETURNS jsonb
    LANGUAGE plpgsql
    AS $raising$
    BEGIN
      UPDATE public.banking_pay_workbench_sessions
      SET scope_total_count = 999
      WHERE id = p_session_id;
      RAISE EXCEPTION 'CONTROLLED_RECOMPUTE_FAILURE' USING ERRCODE = 'P0001';
    END
    $raising$;
  `);

  const result = callHelper({ sessionId, candidateId });
  assert.equal(result.repaired_count, 1);
  assert.equal(result.progress_recompute_failed_count, 1);
  assert.equal(result.all_progress_recomputed, false);
  assert.equal(result.partial, true);
  assert.equal(result.results[0].repaired, true);
  assert.equal(result.results[0].progress_recomputed, false);
  assert.equal(result.results[0].progress_recompute_error_code, 'P0001');
  assert.equal(psql(`
    SELECT scope_total_count::text
    FROM public.banking_pay_workbench_sessions
    WHERE id = '${sessionId}';
  `).trim(), '0');

  const successorId = result.results[0].successor_job_id;
  assert.equal(psql(`
    SELECT pending_job_id::text
    FROM public.banking_pay_workbench_session_scope
    WHERE session_id = '${sessionId}' AND candidate_id = '${candidateId}';
  `).trim(), successorId);

  psql(recomputeSql);
  const convergence = parseLastJson(psql(`
    SELECT public.pay_workbench_session_recompute_progress_counters(
      '${sessionId}', true, 'TRANSACTIONAL_TEST_CONVERGENCE', true
    )::text;
  `));
  assert.equal(convergence.ok, true);

  const retry = callHelper({ sessionId, candidateId });
  assert.equal(retry.repaired_count, 0);
  assert.equal(psql(`
    SELECT COUNT(*)::text
    FROM public.banking_pay_workbench_jobs
    WHERE session_id = '${sessionId}'
      AND candidate_id = '${candidateId}'
      AND status IN ('QUEUED', 'RUNNING');
  `).trim(), '1');
});

test('T13 successor diagnostics are deterministic and identical in light/recompute', { skip: !enabled }, () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });
  insertActiveSuccessor({
    sessionId,
    candidateId,
    sourceSeq: 99,
    status: 'QUEUED',
    createdAt: "now() - interval '3 minutes'"
  });
  insertActiveSuccessor({
    sessionId,
    candidateId,
    sourceSeq: 12,
    status: 'RUNNING',
    createdAt: "now() - interval '2 minutes'"
  });
  const expected = insertActiveSuccessor({
    sessionId,
    candidateId,
    sourceSeq: 15,
    status: 'RUNNING',
    createdAt: "now() - interval '1 minute'"
  });

  const light = parseLastJson(psql(`
    SELECT public.pay_workbench_session_get_progress_light('${sessionId}')::text;
  `));
  const recompute = parseLastJson(psql(`
    SELECT public.pay_workbench_session_recompute_progress_counters(
      '${sessionId}', false, 'TRANSACTIONAL_TEST_PARITY', false
    )::text;
  `));

  const lightSample = light.pending_owner_failures[0];
  const recomputeSample = recompute.pending_owner_failures[0];
  assert.equal(lightSample.successor_job_id, expected.jobId);
  assert.equal(lightSample.successor_job_status, 'RUNNING');
  assert.equal(recomputeSample.successor_job_id, expected.jobId);
  assert.equal(recomputeSample.successor_job_status, 'RUNNING');

  for (const field of [
    'recovery_required',
    'recovery_required_count',
    'recovery_scheduled',
    'recovery_scheduled_count',
    'session_blocker_codes',
    'draft_blocker_codes',
    'phase'
  ]) {
    assert.deepEqual(recompute[field], light[field], `${field} must remain in parity`);
  }
});

test('T14 concurrent repair converges without duplicate authority or false counters', { skip: !enabled }, async () => {
  resetDatabase();
  const sessionId = randomUUID();
  const candidateId = randomUUID();
  insertSession(sessionId);
  insertOwnerFixture({ sessionId, candidateId, ownerStatus: 'FAILED' });

  const callSql = `
    SELECT public.pay_workbench_repair_orphaned_pending_source_build(
      '${sessionId}', '${candidateId}', 1, now(), 'CONCURRENT_TEST'
    )::text;
  `;
  const [firstRaw, secondRaw] = await Promise.all([
    psqlAsync(callSql),
    psqlAsync(callSql)
  ]);
  const first = parseLastJson(firstRaw);
  const second = parseLastJson(secondRaw);

  assert.equal(first.repaired_count + second.repaired_count, 1);
  assert.equal(first.unresolved_count + second.unresolved_count, 0);
  assert.equal(psql(`
    SELECT COUNT(*)::text
    FROM public.banking_pay_workbench_jobs AS job_row
    JOIN public.banking_pay_workbench_session_scope AS scope_row
      ON scope_row.pending_job_id = job_row.id
    WHERE job_row.session_id = '${sessionId}'
      AND job_row.candidate_id = '${candidateId}'
      AND job_row.status IN ('QUEUED', 'RUNNING');
  `).trim(), '1');
});
