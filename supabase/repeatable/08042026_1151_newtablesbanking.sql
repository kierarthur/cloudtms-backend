BEGIN;

-- Phase 1 - Banking Pay workbench schema + provenance
-- Safe to rerun.
-- Notes:
--  * Existing-table columns are only appended via ADD COLUMN IF NOT EXISTS.
--  * New tables are created with IF NOT EXISTS, then self-healed with ADD COLUMN IF NOT EXISTS.
--  * Constraints on existing/new objects are added via pg_constraint checks so reruns are safe.
--  * This migration intentionally does not change any existing pre-draft/post-draft authority rules.

--------------------------------------------------------------------------------
-- 1) Alter public.pay_batches (append-only changes)
--------------------------------------------------------------------------------

ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS source_workbench_session_id uuid,
  ADD COLUMN IF NOT EXISTS source_snapshot_run_id uuid,
  ADD COLUMN IF NOT EXISTS source_session_version bigint,
  ADD COLUMN IF NOT EXISTS execution_commit_state text,
  ADD COLUMN IF NOT EXISTS execution_commit_ref text,
  ADD COLUMN IF NOT EXISTS execution_committed_at_utc timestamptz;

UPDATE public.pay_batches
SET execution_commit_state = 'NOT_SUBMITTED'
WHERE execution_commit_state IS NULL;

ALTER TABLE public.pay_batches
  ALTER COLUMN execution_commit_state SET DEFAULT 'NOT_SUBMITTED',
  ALTER COLUMN execution_commit_state SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_batches_execution_commit_state_chk'
      AND conrelid = 'public.pay_batches'::regclass
  ) THEN
    ALTER TABLE public.pay_batches
      ADD CONSTRAINT pay_batches_execution_commit_state_chk
      CHECK (execution_commit_state IN ('NOT_SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_pay_batches_source_workbench_session_id
  ON public.pay_batches (source_workbench_session_id);

CREATE INDEX IF NOT EXISTS idx_pay_batches_source_snapshot_run_id
  ON public.pay_batches (source_snapshot_run_id);

CREATE INDEX IF NOT EXISTS idx_pay_batches_execution_commit_state
  ON public.pay_batches (execution_commit_state);

--------------------------------------------------------------------------------
-- 2) Create public.banking_pay_snapshot_runs
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_snapshot_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pay_date date NOT NULL,
  week_ending_cutoff date NOT NULL,
  pay_week_start date NOT NULL,
  eligibility_from_date date NOT NULL,
  eligibility_to_date date NOT NULL,
  status text NOT NULL DEFAULT 'OPEN',
  is_active boolean NOT NULL DEFAULT false,
  summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  paye_guardrails_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  ready_at_utc timestamptz,
  failed_at_utc timestamptz,
  last_error_json jsonb
);

ALTER TABLE public.banking_pay_snapshot_runs
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS pay_date date,
  ADD COLUMN IF NOT EXISTS week_ending_cutoff date,
  ADD COLUMN IF NOT EXISTS pay_week_start date,
  ADD COLUMN IF NOT EXISTS eligibility_from_date date,
  ADD COLUMN IF NOT EXISTS eligibility_to_date date,
  ADD COLUMN IF NOT EXISTS status text,
  ADD COLUMN IF NOT EXISTS is_active boolean,
  ADD COLUMN IF NOT EXISTS summary_json jsonb,
  ADD COLUMN IF NOT EXISTS paye_guardrails_json jsonb,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS ready_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS failed_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS last_error_json jsonb;

UPDATE public.banking_pay_snapshot_runs
SET status = COALESCE(status, 'OPEN'),
    is_active = COALESCE(is_active, false),
    summary_json = COALESCE(summary_json, '{}'::jsonb),
    paye_guardrails_json = COALESCE(paye_guardrails_json, '{}'::jsonb),
    created_at_utc = COALESCE(created_at_utc, now()),
    updated_at_utc = COALESCE(updated_at_utc, now())
WHERE status IS NULL
   OR is_active IS NULL
   OR summary_json IS NULL
   OR paye_guardrails_json IS NULL
   OR created_at_utc IS NULL
   OR updated_at_utc IS NULL;

ALTER TABLE public.banking_pay_snapshot_runs
  ALTER COLUMN id SET DEFAULT gen_random_uuid(),
  ALTER COLUMN status SET DEFAULT 'OPEN',
  ALTER COLUMN is_active SET DEFAULT false,
  ALTER COLUMN summary_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN paye_guardrails_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN updated_at_utc SET DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_runs_pkey'
      AND conrelid = 'public.banking_pay_snapshot_runs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_runs
      ADD CONSTRAINT banking_pay_snapshot_runs_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_runs_status_chk'
      AND conrelid = 'public.banking_pay_snapshot_runs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_runs
      ADD CONSTRAINT banking_pay_snapshot_runs_status_chk
      CHECK (status IN ('OPEN', 'READY', 'FAILED', 'ARCHIVED'));
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_snapshot_runs_active_cycle
  ON public.banking_pay_snapshot_runs (pay_date, week_ending_cutoff)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_bpay_snapshot_runs_status_cycle
  ON public.banking_pay_snapshot_runs (status, pay_date, week_ending_cutoff);

--------------------------------------------------------------------------------
-- 3) Create public.banking_pay_snapshot_candidate_state
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_snapshot_candidate_state (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_run_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  candidate_fragment_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  summary_fragment_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  paye_candidate_json jsonb,
  non_paye_payee_json jsonb,
  payees_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  case_resolution_states_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  canonical_preview_lines_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  source_change_seq bigint NOT NULL DEFAULT 0,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  last_refreshed_at_utc timestamptz,
  last_error_json jsonb
);

ALTER TABLE public.banking_pay_snapshot_candidate_state
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS snapshot_run_id uuid,
  ADD COLUMN IF NOT EXISTS candidate_id uuid,
  ADD COLUMN IF NOT EXISTS status text,
  ADD COLUMN IF NOT EXISTS candidate_fragment_json jsonb,
  ADD COLUMN IF NOT EXISTS summary_fragment_json jsonb,
  ADD COLUMN IF NOT EXISTS paye_candidate_json jsonb,
  ADD COLUMN IF NOT EXISTS non_paye_payee_json jsonb,
  ADD COLUMN IF NOT EXISTS payees_json jsonb,
  ADD COLUMN IF NOT EXISTS case_resolution_states_json jsonb,
  ADD COLUMN IF NOT EXISTS canonical_preview_lines_json jsonb,
  ADD COLUMN IF NOT EXISTS source_change_seq bigint,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS last_refreshed_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS last_error_json jsonb;

UPDATE public.banking_pay_snapshot_candidate_state
SET status = COALESCE(status, 'PENDING'),
    candidate_fragment_json = COALESCE(candidate_fragment_json, '{}'::jsonb),
    summary_fragment_json = COALESCE(summary_fragment_json, '{}'::jsonb),
    payees_json = COALESCE(payees_json, '[]'::jsonb),
    case_resolution_states_json = COALESCE(case_resolution_states_json, '[]'::jsonb),
    canonical_preview_lines_json = COALESCE(canonical_preview_lines_json, '[]'::jsonb),
    source_change_seq = COALESCE(source_change_seq, 0),
    created_at_utc = COALESCE(created_at_utc, now()),
    updated_at_utc = COALESCE(updated_at_utc, now())
WHERE status IS NULL
   OR candidate_fragment_json IS NULL
   OR summary_fragment_json IS NULL
   OR payees_json IS NULL
   OR case_resolution_states_json IS NULL
   OR canonical_preview_lines_json IS NULL
   OR source_change_seq IS NULL
   OR created_at_utc IS NULL
   OR updated_at_utc IS NULL;

ALTER TABLE public.banking_pay_snapshot_candidate_state
  ALTER COLUMN id SET DEFAULT gen_random_uuid(),
  ALTER COLUMN status SET DEFAULT 'PENDING',
  ALTER COLUMN candidate_fragment_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN summary_fragment_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN payees_json SET DEFAULT '[]'::jsonb,
  ALTER COLUMN case_resolution_states_json SET DEFAULT '[]'::jsonb,
  ALTER COLUMN canonical_preview_lines_json SET DEFAULT '[]'::jsonb,
  ALTER COLUMN source_change_seq SET DEFAULT 0,
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN updated_at_utc SET DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_candidate_state_pkey'
      AND conrelid = 'public.banking_pay_snapshot_candidate_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_candidate_state
      ADD CONSTRAINT banking_pay_snapshot_candidate_state_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_candidate_state_status_chk'
      AND conrelid = 'public.banking_pay_snapshot_candidate_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_candidate_state
      ADD CONSTRAINT banking_pay_snapshot_candidate_state_status_chk
      CHECK (status IN ('PENDING', 'READY', 'FAILED'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_candidate_state_snapshot_run_id_fkey'
      AND conrelid = 'public.banking_pay_snapshot_candidate_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_candidate_state
      ADD CONSTRAINT banking_pay_snapshot_candidate_state_snapshot_run_id_fkey
      FOREIGN KEY (snapshot_run_id) REFERENCES public.banking_pay_snapshot_runs(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_candidate_state_candidate_id_fkey'
      AND conrelid = 'public.banking_pay_snapshot_candidate_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_candidate_state
      ADD CONSTRAINT banking_pay_snapshot_candidate_state_candidate_id_fkey
      FOREIGN KEY (candidate_id) REFERENCES public.candidates(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_snapshot_candidate_state_run_candidate
  ON public.banking_pay_snapshot_candidate_state (snapshot_run_id, candidate_id);

CREATE INDEX IF NOT EXISTS idx_bpay_snapshot_candidate_state_run_status
  ON public.banking_pay_snapshot_candidate_state (snapshot_run_id, status);

--------------------------------------------------------------------------------
-- 4) Create public.banking_pay_snapshot_case_state
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_snapshot_case_state (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_run_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  case_key text NOT NULL,
  finance_case_id uuid,
  case_state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  is_blocked boolean NOT NULL DEFAULT false,
  is_resolved boolean NOT NULL DEFAULT false,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.banking_pay_snapshot_case_state
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS snapshot_run_id uuid,
  ADD COLUMN IF NOT EXISTS candidate_id uuid,
  ADD COLUMN IF NOT EXISTS case_key text,
  ADD COLUMN IF NOT EXISTS finance_case_id uuid,
  ADD COLUMN IF NOT EXISTS case_state_json jsonb,
  ADD COLUMN IF NOT EXISTS is_blocked boolean,
  ADD COLUMN IF NOT EXISTS is_resolved boolean,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz;

UPDATE public.banking_pay_snapshot_case_state
SET case_state_json = COALESCE(case_state_json, '{}'::jsonb),
    is_blocked = COALESCE(is_blocked, false),
    is_resolved = COALESCE(is_resolved, false),
    created_at_utc = COALESCE(created_at_utc, now()),
    updated_at_utc = COALESCE(updated_at_utc, now())
WHERE case_state_json IS NULL
   OR is_blocked IS NULL
   OR is_resolved IS NULL
   OR created_at_utc IS NULL
   OR updated_at_utc IS NULL;

ALTER TABLE public.banking_pay_snapshot_case_state
  ALTER COLUMN id SET DEFAULT gen_random_uuid(),
  ALTER COLUMN case_state_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN is_blocked SET DEFAULT false,
  ALTER COLUMN is_resolved SET DEFAULT false,
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN updated_at_utc SET DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_case_state_pkey'
      AND conrelid = 'public.banking_pay_snapshot_case_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_case_state
      ADD CONSTRAINT banking_pay_snapshot_case_state_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_case_state_snapshot_run_id_fkey'
      AND conrelid = 'public.banking_pay_snapshot_case_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_case_state
      ADD CONSTRAINT banking_pay_snapshot_case_state_snapshot_run_id_fkey
      FOREIGN KEY (snapshot_run_id) REFERENCES public.banking_pay_snapshot_runs(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_case_state_candidate_id_fkey'
      AND conrelid = 'public.banking_pay_snapshot_case_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_case_state
      ADD CONSTRAINT banking_pay_snapshot_case_state_candidate_id_fkey
      FOREIGN KEY (candidate_id) REFERENCES public.candidates(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_snapshot_case_state_run_candidate_case
  ON public.banking_pay_snapshot_case_state (snapshot_run_id, candidate_id, case_key);

--------------------------------------------------------------------------------
-- 5) Create public.banking_pay_snapshot_case_component_state
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_snapshot_case_component_state (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_run_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  case_key text NOT NULL,
  timesheet_id uuid,
  component_fingerprint text,
  source_basis_fingerprint text,
  source_family_key text,
  bucket_code text,
  component_key_type text,
  component_key_value text,
  component_state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.banking_pay_snapshot_case_component_state
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS snapshot_run_id uuid,
  ADD COLUMN IF NOT EXISTS candidate_id uuid,
  ADD COLUMN IF NOT EXISTS case_key text,
  ADD COLUMN IF NOT EXISTS timesheet_id uuid,
  ADD COLUMN IF NOT EXISTS component_fingerprint text,
  ADD COLUMN IF NOT EXISTS source_basis_fingerprint text,
  ADD COLUMN IF NOT EXISTS source_family_key text,
  ADD COLUMN IF NOT EXISTS bucket_code text,
  ADD COLUMN IF NOT EXISTS component_key_type text,
  ADD COLUMN IF NOT EXISTS component_key_value text,
  ADD COLUMN IF NOT EXISTS component_state_json jsonb,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz;

UPDATE public.banking_pay_snapshot_case_component_state
SET component_state_json = COALESCE(component_state_json, '{}'::jsonb),
    created_at_utc = COALESCE(created_at_utc, now()),
    updated_at_utc = COALESCE(updated_at_utc, now())
WHERE component_state_json IS NULL
   OR created_at_utc IS NULL
   OR updated_at_utc IS NULL;

ALTER TABLE public.banking_pay_snapshot_case_component_state
  ALTER COLUMN id SET DEFAULT gen_random_uuid(),
  ALTER COLUMN component_state_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN updated_at_utc SET DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_case_component_state_pkey'
      AND conrelid = 'public.banking_pay_snapshot_case_component_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_case_component_state
      ADD CONSTRAINT banking_pay_snapshot_case_component_state_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_case_component_state_snapshot_run_id_fkey'
      AND conrelid = 'public.banking_pay_snapshot_case_component_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_case_component_state
      ADD CONSTRAINT banking_pay_snapshot_case_component_state_snapshot_run_id_fkey
      FOREIGN KEY (snapshot_run_id) REFERENCES public.banking_pay_snapshot_runs(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_case_component_state_candidate_id_fkey'
      AND conrelid = 'public.banking_pay_snapshot_case_component_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_case_component_state
      ADD CONSTRAINT banking_pay_snapshot_case_component_state_candidate_id_fkey
      FOREIGN KEY (candidate_id) REFERENCES public.candidates(id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_bpay_snapshot_case_component_run_candidate_case
  ON public.banking_pay_snapshot_case_component_state (snapshot_run_id, candidate_id, case_key);

CREATE INDEX IF NOT EXISTS idx_bpay_snapshot_case_component_run_candidate_timesheet
  ON public.banking_pay_snapshot_case_component_state (snapshot_run_id, candidate_id, timesheet_id);

CREATE INDEX IF NOT EXISTS idx_bpay_snapshot_case_component_match
  ON public.banking_pay_snapshot_case_component_state (
    snapshot_run_id,
    candidate_id,
    case_key,
    source_basis_fingerprint,
    source_family_key,
    bucket_code
  );

--------------------------------------------------------------------------------
-- 6) Create public.banking_pay_snapshot_line_state
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_snapshot_line_state (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  snapshot_run_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  preview_row_id text NOT NULL,
  timesheet_id uuid,
  case_key text,
  pay_channel text,
  component_key_type text,
  component_key_value text,
  canonical_line_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.banking_pay_snapshot_line_state
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS snapshot_run_id uuid,
  ADD COLUMN IF NOT EXISTS candidate_id uuid,
  ADD COLUMN IF NOT EXISTS preview_row_id text,
  ADD COLUMN IF NOT EXISTS timesheet_id uuid,
  ADD COLUMN IF NOT EXISTS case_key text,
  ADD COLUMN IF NOT EXISTS pay_channel text,
  ADD COLUMN IF NOT EXISTS component_key_type text,
  ADD COLUMN IF NOT EXISTS component_key_value text,
  ADD COLUMN IF NOT EXISTS canonical_line_json jsonb,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz;

UPDATE public.banking_pay_snapshot_line_state
SET canonical_line_json = COALESCE(canonical_line_json, '{}'::jsonb),
    created_at_utc = COALESCE(created_at_utc, now()),
    updated_at_utc = COALESCE(updated_at_utc, now())
WHERE canonical_line_json IS NULL
   OR created_at_utc IS NULL
   OR updated_at_utc IS NULL;

ALTER TABLE public.banking_pay_snapshot_line_state
  ALTER COLUMN id SET DEFAULT gen_random_uuid(),
  ALTER COLUMN canonical_line_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN updated_at_utc SET DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_line_state_pkey'
      AND conrelid = 'public.banking_pay_snapshot_line_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_line_state
      ADD CONSTRAINT banking_pay_snapshot_line_state_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_line_state_snapshot_run_id_fkey'
      AND conrelid = 'public.banking_pay_snapshot_line_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_line_state
      ADD CONSTRAINT banking_pay_snapshot_line_state_snapshot_run_id_fkey
      FOREIGN KEY (snapshot_run_id) REFERENCES public.banking_pay_snapshot_runs(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_snapshot_line_state_candidate_id_fkey'
      AND conrelid = 'public.banking_pay_snapshot_line_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_snapshot_line_state
      ADD CONSTRAINT banking_pay_snapshot_line_state_candidate_id_fkey
      FOREIGN KEY (candidate_id) REFERENCES public.candidates(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_snapshot_line_state_run_candidate_preview_row
  ON public.banking_pay_snapshot_line_state (snapshot_run_id, candidate_id, preview_row_id);

CREATE INDEX IF NOT EXISTS idx_bpay_snapshot_line_state_run_candidate
  ON public.banking_pay_snapshot_line_state (snapshot_run_id, candidate_id);

--------------------------------------------------------------------------------
-- 7) Create public.banking_pay_workbench_sessions
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_user_id uuid NOT NULL,
  pay_date date NOT NULL,
  week_ending_cutoff date NOT NULL,
  filters_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  scope_candidate_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
  session_signature text NOT NULL,
  source_snapshot_run_id uuid NOT NULL,
  status text NOT NULL DEFAULT 'OPEN',
  version bigint NOT NULL DEFAULT 1,
  server_selected_preview_row_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  discarded_at_utc timestamptz
);

DO $$
BEGIN
  IF (
    SELECT count(*)
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'banking_pay_workbench_sessions'
      AND column_name IN (
        'id',
        'actor_user_id',
        'pay_date',
        'week_ending_cutoff',
        'filters_json',
        'scope_candidate_ids',
        'session_signature',
        'source_snapshot_run_id',
        'status',
        'version',
        'server_selected_preview_row_ids',
        'created_at_utc',
        'updated_at_utc',
        'discarded_at_utc'
      )
  ) <> 14 THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
      ADD COLUMN IF NOT EXISTS actor_user_id uuid,
      ADD COLUMN IF NOT EXISTS pay_date date,
      ADD COLUMN IF NOT EXISTS week_ending_cutoff date,
      ADD COLUMN IF NOT EXISTS filters_json jsonb,
      ADD COLUMN IF NOT EXISTS scope_candidate_ids uuid[],
      ADD COLUMN IF NOT EXISTS session_signature text,
      ADD COLUMN IF NOT EXISTS source_snapshot_run_id uuid,
      ADD COLUMN IF NOT EXISTS status text,
      ADD COLUMN IF NOT EXISTS version bigint,
      ADD COLUMN IF NOT EXISTS server_selected_preview_row_ids jsonb,
      ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
      ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz,
      ADD COLUMN IF NOT EXISTS discarded_at_utc timestamptz;
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions
    WHERE filters_json IS NULL
       OR scope_candidate_ids IS NULL
       OR status IS NULL
       OR version IS NULL
       OR server_selected_preview_row_ids IS NULL
       OR created_at_utc IS NULL
       OR updated_at_utc IS NULL
  ) THEN
    UPDATE public.banking_pay_workbench_sessions
    SET filters_json = COALESCE(filters_json, '{}'::jsonb),
        scope_candidate_ids = COALESCE(scope_candidate_ids, '{}'::uuid[]),
        status = COALESCE(status, 'OPEN'),
        version = COALESCE(version, 1),
        server_selected_preview_row_ids = COALESCE(server_selected_preview_row_ids, '[]'::jsonb),
        created_at_utc = COALESCE(created_at_utc, now()),
        updated_at_utc = COALESCE(updated_at_utc, now())
    WHERE filters_json IS NULL
       OR scope_candidate_ids IS NULL
       OR status IS NULL
       OR version IS NULL
       OR server_selected_preview_row_ids IS NULL
       OR created_at_utc IS NULL
       OR updated_at_utc IS NULL;
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'banking_pay_workbench_sessions'
      AND (
        (column_name = 'id' AND column_default IS DISTINCT FROM 'gen_random_uuid()')
        OR (column_name = 'filters_json' AND column_default IS DISTINCT FROM '''{}''::jsonb')
        OR (column_name = 'scope_candidate_ids' AND column_default IS DISTINCT FROM '''{}''::uuid[]')
        OR (column_name = 'status' AND column_default IS DISTINCT FROM '''OPEN''::text')
        OR (column_name = 'version' AND column_default IS DISTINCT FROM '1')
        OR (column_name = 'server_selected_preview_row_ids' AND column_default IS DISTINCT FROM '''[]''::jsonb')
        OR (column_name = 'created_at_utc' AND column_default IS DISTINCT FROM 'now()')
        OR (column_name = 'updated_at_utc' AND column_default IS DISTINCT FROM 'now()')
      )
  ) THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ALTER COLUMN id SET DEFAULT gen_random_uuid(),
      ALTER COLUMN filters_json SET DEFAULT '{}'::jsonb,
      ALTER COLUMN scope_candidate_ids SET DEFAULT '{}'::uuid[],
      ALTER COLUMN status SET DEFAULT 'OPEN',
      ALTER COLUMN version SET DEFAULT 1,
      ALTER COLUMN server_selected_preview_row_ids SET DEFAULT '[]'::jsonb,
      ALTER COLUMN created_at_utc SET DEFAULT now(),
      ALTER COLUMN updated_at_utc SET DEFAULT now();
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_sessions_pkey'
      AND conrelid = 'public.banking_pay_workbench_sessions'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ADD CONSTRAINT banking_pay_workbench_sessions_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_sessions_status_chk'
      AND conrelid = 'public.banking_pay_workbench_sessions'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ADD CONSTRAINT banking_pay_workbench_sessions_status_chk
      CHECK (status IN ('OPEN', 'DISCARDED'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_sessions_source_snapshot_run_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_sessions'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ADD CONSTRAINT banking_pay_workbench_sessions_source_snapshot_run_id_fkey
      FOREIGN KEY (source_snapshot_run_id) REFERENCES public.banking_pay_snapshot_runs(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_workbench_sessions_actor_signature_open
  ON public.banking_pay_workbench_sessions (actor_user_id, session_signature)
  WHERE status = 'OPEN';

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_sessions_snapshot_status
  ON public.banking_pay_workbench_sessions (source_snapshot_run_id, status);

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_sessions_scope_candidates_gin
  ON public.banking_pay_workbench_sessions USING gin (scope_candidate_ids);

--------------------------------------------------------------------------------
-- 8) Create public.banking_pay_workbench_session_case_resolutions
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_session_case_resolutions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  case_key text NOT NULL,
  resolution_family text NOT NULL,
  resolution_identity_key text NOT NULL,
  timesheet_id uuid,
  source_basis_fingerprint text,
  source_family_key text,
  bucket_code text,
  component_key_type text,
  component_key_value text,
  payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.banking_pay_workbench_session_case_resolutions
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS session_id uuid,
  ADD COLUMN IF NOT EXISTS candidate_id uuid,
  ADD COLUMN IF NOT EXISTS case_key text,
  ADD COLUMN IF NOT EXISTS resolution_family text,
  ADD COLUMN IF NOT EXISTS resolution_identity_key text,
  ADD COLUMN IF NOT EXISTS timesheet_id uuid,
  ADD COLUMN IF NOT EXISTS source_basis_fingerprint text,
  ADD COLUMN IF NOT EXISTS source_family_key text,
  ADD COLUMN IF NOT EXISTS bucket_code text,
  ADD COLUMN IF NOT EXISTS component_key_type text,
  ADD COLUMN IF NOT EXISTS component_key_value text,
  ADD COLUMN IF NOT EXISTS payload_json jsonb,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz;

UPDATE public.banking_pay_workbench_session_case_resolutions
SET payload_json = COALESCE(payload_json, '{}'::jsonb),
    created_at_utc = COALESCE(created_at_utc, now()),
    updated_at_utc = COALESCE(updated_at_utc, now())
WHERE payload_json IS NULL
   OR created_at_utc IS NULL
   OR updated_at_utc IS NULL;

ALTER TABLE public.banking_pay_workbench_session_case_resolutions
  ALTER COLUMN id SET DEFAULT gen_random_uuid(),
  ALTER COLUMN payload_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN updated_at_utc SET DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_case_resolutions_pkey'
      AND conrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_case_resolutions
      ADD CONSTRAINT banking_pay_workbench_session_case_resolutions_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_case_resolutions_session_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_case_resolutions
      ADD CONSTRAINT banking_pay_workbench_session_case_resolutions_session_id_fkey
      FOREIGN KEY (session_id) REFERENCES public.banking_pay_workbench_sessions(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_case_resolutions_candidate_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_case_resolutions
      ADD CONSTRAINT banking_pay_workbench_session_case_resolutions_candidate_id_fkey
      FOREIGN KEY (candidate_id) REFERENCES public.candidates(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_session_case_resolutions_identity
  ON public.banking_pay_workbench_session_case_resolutions (session_id, resolution_identity_key);

CREATE INDEX IF NOT EXISTS idx_bpay_session_case_resolutions_session_candidate
  ON public.banking_pay_workbench_session_case_resolutions (session_id, candidate_id);

--------------------------------------------------------------------------------
-- 9) Create public.banking_pay_workbench_session_overrides
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_session_overrides (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  override_type text NOT NULL,
  override_identity_key text NOT NULL,
  timesheet_id uuid,
  payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.banking_pay_workbench_session_overrides
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS session_id uuid,
  ADD COLUMN IF NOT EXISTS candidate_id uuid,
  ADD COLUMN IF NOT EXISTS override_type text,
  ADD COLUMN IF NOT EXISTS override_identity_key text,
  ADD COLUMN IF NOT EXISTS timesheet_id uuid,
  ADD COLUMN IF NOT EXISTS payload_json jsonb,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz;

UPDATE public.banking_pay_workbench_session_overrides
SET payload_json = COALESCE(payload_json, '{}'::jsonb),
    created_at_utc = COALESCE(created_at_utc, now()),
    updated_at_utc = COALESCE(updated_at_utc, now())
WHERE payload_json IS NULL
   OR created_at_utc IS NULL
   OR updated_at_utc IS NULL;

ALTER TABLE public.banking_pay_workbench_session_overrides
  ALTER COLUMN id SET DEFAULT gen_random_uuid(),
  ALTER COLUMN payload_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN updated_at_utc SET DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_overrides_pkey'
      AND conrelid = 'public.banking_pay_workbench_session_overrides'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_overrides
      ADD CONSTRAINT banking_pay_workbench_session_overrides_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_overrides_session_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_session_overrides'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_overrides
      ADD CONSTRAINT banking_pay_workbench_session_overrides_session_id_fkey
      FOREIGN KEY (session_id) REFERENCES public.banking_pay_workbench_sessions(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_overrides_candidate_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_session_overrides'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_overrides
      ADD CONSTRAINT banking_pay_workbench_session_overrides_candidate_id_fkey
      FOREIGN KEY (candidate_id) REFERENCES public.candidates(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_session_overrides_identity
  ON public.banking_pay_workbench_session_overrides (session_id, override_identity_key);

CREATE INDEX IF NOT EXISTS idx_bpay_session_overrides_session_candidate_type
  ON public.banking_pay_workbench_session_overrides (session_id, candidate_id, override_type);

--------------------------------------------------------------------------------
-- 10) Create public.banking_pay_workbench_session_candidate_state
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_session_candidate_state (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  effective_candidate_fragment_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  effective_summary_fragment_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  effective_paye_candidate_json jsonb,
  effective_non_paye_payee_json jsonb,
  effective_payees_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  effective_case_resolution_states_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  effective_canonical_preview_lines_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  source_change_seq bigint NOT NULL DEFAULT 0,
  session_version bigint NOT NULL DEFAULT 1,
  pending_job_id uuid,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  last_recomputed_at_utc timestamptz,
  last_error_json jsonb
);

ALTER TABLE public.banking_pay_workbench_session_candidate_state
  ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS session_id uuid,
  ADD COLUMN IF NOT EXISTS candidate_id uuid,
  ADD COLUMN IF NOT EXISTS status text,
  ADD COLUMN IF NOT EXISTS effective_candidate_fragment_json jsonb,
  ADD COLUMN IF NOT EXISTS effective_summary_fragment_json jsonb,
  ADD COLUMN IF NOT EXISTS effective_paye_candidate_json jsonb,
  ADD COLUMN IF NOT EXISTS effective_non_paye_payee_json jsonb,
  ADD COLUMN IF NOT EXISTS effective_payees_json jsonb,
  ADD COLUMN IF NOT EXISTS effective_case_resolution_states_json jsonb,
  ADD COLUMN IF NOT EXISTS effective_canonical_preview_lines_json jsonb,
  ADD COLUMN IF NOT EXISTS source_change_seq bigint,
  ADD COLUMN IF NOT EXISTS session_version bigint,
  ADD COLUMN IF NOT EXISTS pending_job_id uuid,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS last_recomputed_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS last_error_json jsonb;

UPDATE public.banking_pay_workbench_session_candidate_state
SET status = COALESCE(status, 'PENDING'),
    effective_candidate_fragment_json = COALESCE(effective_candidate_fragment_json, '{}'::jsonb),
    effective_summary_fragment_json = COALESCE(effective_summary_fragment_json, '{}'::jsonb),
    effective_payees_json = COALESCE(effective_payees_json, '[]'::jsonb),
    effective_case_resolution_states_json = COALESCE(effective_case_resolution_states_json, '[]'::jsonb),
    effective_canonical_preview_lines_json = COALESCE(effective_canonical_preview_lines_json, '[]'::jsonb),
    source_change_seq = COALESCE(source_change_seq, 0),
    session_version = COALESCE(session_version, 1),
    created_at_utc = COALESCE(created_at_utc, now()),
    updated_at_utc = COALESCE(updated_at_utc, now())
WHERE status IS NULL
   OR effective_candidate_fragment_json IS NULL
   OR effective_summary_fragment_json IS NULL
   OR effective_payees_json IS NULL
   OR effective_case_resolution_states_json IS NULL
   OR effective_canonical_preview_lines_json IS NULL
   OR source_change_seq IS NULL
   OR session_version IS NULL
   OR created_at_utc IS NULL
   OR updated_at_utc IS NULL;

ALTER TABLE public.banking_pay_workbench_session_candidate_state
  ALTER COLUMN id SET DEFAULT gen_random_uuid(),
  ALTER COLUMN status SET DEFAULT 'PENDING',
  ALTER COLUMN effective_candidate_fragment_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN effective_summary_fragment_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN effective_payees_json SET DEFAULT '[]'::jsonb,
  ALTER COLUMN effective_case_resolution_states_json SET DEFAULT '[]'::jsonb,
  ALTER COLUMN effective_canonical_preview_lines_json SET DEFAULT '[]'::jsonb,
  ALTER COLUMN source_change_seq SET DEFAULT 0,
  ALTER COLUMN session_version SET DEFAULT 1,
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN updated_at_utc SET DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_candidate_state_pkey'
      AND conrelid = 'public.banking_pay_workbench_session_candidate_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_candidate_state
      ADD CONSTRAINT banking_pay_workbench_session_candidate_state_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_candidate_state_status_chk'
      AND conrelid = 'public.banking_pay_workbench_session_candidate_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_candidate_state
      ADD CONSTRAINT banking_pay_workbench_session_candidate_state_status_chk
      CHECK (status IN ('PENDING', 'READY', 'FAILED'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_candidate_state_session_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_session_candidate_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_candidate_state
      ADD CONSTRAINT banking_pay_workbench_session_candidate_state_session_id_fkey
      FOREIGN KEY (session_id) REFERENCES public.banking_pay_workbench_sessions(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_session_candidate_state_candidate_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_session_candidate_state'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_candidate_state
      ADD CONSTRAINT banking_pay_workbench_session_candidate_state_candidate_id_fkey
      FOREIGN KEY (candidate_id) REFERENCES public.candidates(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_session_candidate_state_session_candidate
  ON public.banking_pay_workbench_session_candidate_state (session_id, candidate_id);

CREATE INDEX IF NOT EXISTS idx_bpay_session_candidate_state_session_status
  ON public.banking_pay_workbench_session_candidate_state (session_id, status);

--------------------------------------------------------------------------------
-- 11) Create public.banking_pay_workbench_jobs
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_type text NOT NULL,
  status text NOT NULL DEFAULT 'QUEUED',
  priority integer NOT NULL DEFAULT 100,
  run_at_utc timestamptz NOT NULL DEFAULT now(),
  attempt_count integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 8,
  dedupe_key text NOT NULL,
  snapshot_run_id uuid,
  session_id uuid,
  candidate_id uuid,
  payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  started_at_utc timestamptz,
  completed_at_utc timestamptz,
  failed_at_utc timestamptz,
  last_error_json jsonb
);

DO $$
BEGIN
  IF (
    SELECT count(*)
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'banking_pay_workbench_jobs'
      AND column_name IN (
        'id',
        'job_type',
        'status',
        'priority',
        'run_at_utc',
        'attempt_count',
        'max_attempts',
        'dedupe_key',
        'snapshot_run_id',
        'session_id',
        'candidate_id',
        'payload_json',
        'created_at_utc',
        'updated_at_utc',
        'started_at_utc',
        'completed_at_utc',
        'failed_at_utc',
        'last_error_json'
      )
  ) <> 18 THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid(),
      ADD COLUMN IF NOT EXISTS job_type text,
      ADD COLUMN IF NOT EXISTS status text,
      ADD COLUMN IF NOT EXISTS priority integer,
      ADD COLUMN IF NOT EXISTS run_at_utc timestamptz,
      ADD COLUMN IF NOT EXISTS attempt_count integer,
      ADD COLUMN IF NOT EXISTS max_attempts integer,
      ADD COLUMN IF NOT EXISTS dedupe_key text,
      ADD COLUMN IF NOT EXISTS snapshot_run_id uuid,
      ADD COLUMN IF NOT EXISTS session_id uuid,
      ADD COLUMN IF NOT EXISTS candidate_id uuid,
      ADD COLUMN IF NOT EXISTS payload_json jsonb,
      ADD COLUMN IF NOT EXISTS created_at_utc timestamptz,
      ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz,
      ADD COLUMN IF NOT EXISTS started_at_utc timestamptz,
      ADD COLUMN IF NOT EXISTS completed_at_utc timestamptz,
      ADD COLUMN IF NOT EXISTS failed_at_utc timestamptz,
      ADD COLUMN IF NOT EXISTS last_error_json jsonb;
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_jobs
    WHERE status IS NULL
       OR priority IS NULL
       OR run_at_utc IS NULL
       OR attempt_count IS NULL
       OR max_attempts IS NULL
       OR payload_json IS NULL
       OR created_at_utc IS NULL
       OR updated_at_utc IS NULL
  ) THEN
    UPDATE public.banking_pay_workbench_jobs
    SET status = COALESCE(status, 'QUEUED'),
        priority = COALESCE(priority, 100),
        run_at_utc = COALESCE(run_at_utc, now()),
        attempt_count = COALESCE(attempt_count, 0),
        max_attempts = COALESCE(max_attempts, 8),
        payload_json = COALESCE(payload_json, '{}'::jsonb),
        created_at_utc = COALESCE(created_at_utc, now()),
        updated_at_utc = COALESCE(updated_at_utc, now())
    WHERE status IS NULL
       OR priority IS NULL
       OR run_at_utc IS NULL
       OR attempt_count IS NULL
       OR max_attempts IS NULL
       OR payload_json IS NULL
       OR created_at_utc IS NULL
       OR updated_at_utc IS NULL;
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'banking_pay_workbench_jobs'
      AND (
        (column_name = 'id' AND column_default IS DISTINCT FROM 'gen_random_uuid()')
        OR (column_name = 'status' AND column_default IS DISTINCT FROM '''QUEUED''::text')
        OR (column_name = 'priority' AND column_default IS DISTINCT FROM '100')
        OR (column_name = 'run_at_utc' AND column_default IS DISTINCT FROM 'now()')
        OR (column_name = 'attempt_count' AND column_default IS DISTINCT FROM '0')
        OR (column_name = 'max_attempts' AND column_default IS DISTINCT FROM '8')
        OR (column_name = 'payload_json' AND column_default IS DISTINCT FROM '''{}''::jsonb')
        OR (column_name = 'created_at_utc' AND column_default IS DISTINCT FROM 'now()')
        OR (column_name = 'updated_at_utc' AND column_default IS DISTINCT FROM 'now()')
      )
  ) THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ALTER COLUMN id SET DEFAULT gen_random_uuid(),
      ALTER COLUMN status SET DEFAULT 'QUEUED',
      ALTER COLUMN priority SET DEFAULT 100,
      ALTER COLUMN run_at_utc SET DEFAULT now(),
      ALTER COLUMN attempt_count SET DEFAULT 0,
      ALTER COLUMN max_attempts SET DEFAULT 8,
      ALTER COLUMN payload_json SET DEFAULT '{}'::jsonb,
      ALTER COLUMN created_at_utc SET DEFAULT now(),
      ALTER COLUMN updated_at_utc SET DEFAULT now();
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_jobs_pkey'
      AND conrelid = 'public.banking_pay_workbench_jobs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ADD CONSTRAINT banking_pay_workbench_jobs_pkey PRIMARY KEY (id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_jobs_status_chk'
      AND conrelid = 'public.banking_pay_workbench_jobs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ADD CONSTRAINT banking_pay_workbench_jobs_status_chk
      CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_jobs_snapshot_run_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_jobs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ADD CONSTRAINT banking_pay_workbench_jobs_snapshot_run_id_fkey
      FOREIGN KEY (snapshot_run_id) REFERENCES public.banking_pay_snapshot_runs(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_jobs_session_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_jobs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ADD CONSTRAINT banking_pay_workbench_jobs_session_id_fkey
      FOREIGN KEY (session_id) REFERENCES public.banking_pay_workbench_sessions(id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'banking_pay_workbench_jobs_candidate_id_fkey'
      AND conrelid = 'public.banking_pay_workbench_jobs'::regclass
  ) THEN
    ALTER TABLE public.banking_pay_workbench_jobs
      ADD CONSTRAINT banking_pay_workbench_jobs_candidate_id_fkey
      FOREIGN KEY (candidate_id) REFERENCES public.candidates(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_workbench_jobs_active_dedupe
  ON public.banking_pay_workbench_jobs (dedupe_key)
  WHERE status IN ('QUEUED', 'RUNNING');

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_jobs_status_due_priority
  ON public.banking_pay_workbench_jobs (status, run_at_utc, priority);

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_jobs_session_candidate_status
  ON public.banking_pay_workbench_jobs (session_id, candidate_id, status);

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_jobs_snapshot_candidate_status
  ON public.banking_pay_workbench_jobs (snapshot_run_id, candidate_id, status);

COMMIT;
