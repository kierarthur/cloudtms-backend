/* ============================================================================
   CloudTMS Banking Pay Workbench source-build schema/settings migration

   Safe to rerun.
   Scope:
   1) Create public.banking_pay_workbench_candidate_source_lines.
   2) Add per-stage Banking Pay workbench settings and rollover settings.
   3) Conditionally extend any job_type-only CHECK constraint, if one exists
      in another deployment variant. The attached schema has no job_type check.

   Policy X:
   This is pre-draft live-truth workbench staging only.
   It does not change payment economics, VAT, draft freeze, remittance,
   settlement, correction logic, bank export, or post-draft authority.
   ============================================================================ */


/* ============================================================================
   1. Row-level persisted candidate source table
   ============================================================================ */

CREATE TABLE IF NOT EXISTS public.banking_pay_workbench_candidate_source_lines (
  id uuid NOT NULL DEFAULT gen_random_uuid(),

  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,

  session_version bigint NOT NULL,
  source_change_seq bigint NOT NULL,
  source_build_run_id uuid NOT NULL,

  source_ordinal bigint NOT NULL,

  line_key text NOT NULL,
  parent_line_key text NULL,
  split_suffix text NULL,

  timesheet_id uuid NULL,

  section text NULL,

  source_row_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  economic_key_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  contract_json jsonb NOT NULL DEFAULT '{}'::jsonb,

  pay_channel_scope text NOT NULL DEFAULT 'ALL',
  refresh_scope_kind text NOT NULL DEFAULT 'CANDIDATE_FULL_LIVE',

  status text NOT NULL DEFAULT 'CURRENT',

  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT bpay_wb_candidate_source_lines_pkey
    PRIMARY KEY (id),

  CONSTRAINT bpay_wb_source_lines_status_chk
    CHECK (status = ANY (ARRAY[
      'CURRENT'::text,
      'SUPERSEDED'::text,
      'DIRTY'::text,
      'ERROR'::text
    ])),

  CONSTRAINT bpay_wb_source_lines_json_shape_chk
    CHECK (
      jsonb_typeof(source_row_json) = 'object'::text
      AND jsonb_typeof(economic_key_json) = 'object'::text
      AND jsonb_typeof(contract_json) = 'object'::text
    )
);


/* Add FKs idempotently. These mirror the existing workbench line-work pattern. */

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_candidate_source_lines'::regclass
      AND conname = 'bpay_wb_source_lines_session_fk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_candidate_source_lines
      ADD CONSTRAINT bpay_wb_source_lines_session_fk
      FOREIGN KEY (session_id)
      REFERENCES public.banking_pay_workbench_sessions(id)
      ON DELETE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_candidate_source_lines'::regclass
      AND conname = 'bpay_wb_source_lines_candidate_fk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_candidate_source_lines
      ADD CONSTRAINT bpay_wb_source_lines_candidate_fk
      FOREIGN KEY (candidate_id)
      REFERENCES public.candidates(id)
      ON DELETE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_candidate_source_lines'::regclass
      AND conname = 'bpay_wb_source_lines_timesheet_fk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_candidate_source_lines
      ADD CONSTRAINT bpay_wb_source_lines_timesheet_fk
      FOREIGN KEY (timesheet_id)
      REFERENCES public.timesheets(timesheet_id)
      ON DELETE SET NULL;
  END IF;
END $$;


/* Current source-row paging by session/candidate/version/source-change/ordinal/id. */

CREATE INDEX IF NOT EXISTS idx_bpay_wb_source_current_page
ON public.banking_pay_workbench_candidate_source_lines
USING btree (
  session_id,
  candidate_id,
  session_version,
  source_change_seq,
  source_ordinal,
  id
)
WHERE status = 'CURRENT';


/* Current source-row paging for the specific source-build run used by line seed. */

CREATE INDEX IF NOT EXISTS idx_bpay_wb_source_current_run_page
ON public.banking_pay_workbench_candidate_source_lines
USING btree (
  session_id,
  candidate_id,
  session_version,
  source_change_seq,
  source_build_run_id,
  source_ordinal,
  id
)
WHERE status = 'CURRENT';


/* Targeted invalidation by session/candidate/timesheet. */

CREATE INDEX IF NOT EXISTS idx_bpay_wb_source_targeted_timesheet
ON public.banking_pay_workbench_candidate_source_lines
USING btree (
  session_id,
  candidate_id,
  timesheet_id
)
WHERE timesheet_id IS NOT NULL
  AND status = ANY (ARRAY['CURRENT'::text, 'DIRTY'::text]);


/* Cleanup / retirement by session/candidate/status. */

CREATE INDEX IF NOT EXISTS idx_bpay_wb_source_status_cleanup
ON public.banking_pay_workbench_candidate_source_lines
USING btree (
  session_id,
  candidate_id,
  status,
  updated_at_utc
);


/* Cheap session-level status/progress counting. */

CREATE INDEX IF NOT EXISTS idx_bpay_wb_source_session_status
ON public.banking_pay_workbench_candidate_source_lines
USING btree (
  session_id,
  status
);


/* Duplicate prevention for current source rows only.
   Null-safe timesheet identity matches the existing line-work index style. */

CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_wb_source_current_identity
ON public.banking_pay_workbench_candidate_source_lines
USING btree (
  session_id,
  candidate_id,
  session_version,
  source_change_seq,
  source_build_run_id,
  COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid),
  line_key
)
WHERE status = 'CURRENT';


/* Keep updated_at_utc current, using the existing project trigger helper. */

DROP TRIGGER IF EXISTS trg_bpay_wb_source_lines_touch_updated_at_utc
ON public.banking_pay_workbench_candidate_source_lines;

CREATE TRIGGER trg_bpay_wb_source_lines_touch_updated_at_utc
BEFORE UPDATE ON public.banking_pay_workbench_candidate_source_lines
FOR EACH ROW
EXECUTE FUNCTION public._cloudtms_touch_updated_at_utc();


COMMENT ON TABLE public.banking_pay_workbench_candidate_source_lines IS
'Banking Pay workbench pre-draft live-truth row-level candidate source cache. Used to make candidate line-work seed a cheap indexed page read. Not a post-draft payment authority.';

COMMENT ON COLUMN public.banking_pay_workbench_candidate_source_lines.source_change_seq IS
'Source change sequence carried through Banking Pay workbench dirty/source-build/line-seed payloads. Existing code uses source_change_seq naming.';

COMMENT ON COLUMN public.banking_pay_workbench_candidate_source_lines.source_build_run_id IS
'Stable id for one bounded source-build run/chain. Used with source_change_seq/session_version to prevent duplicate current source rows.';

COMMENT ON COLUMN public.banking_pay_workbench_candidate_source_lines.source_row_json IS
'Persisted classified pre-draft source row payload for later indexed line-work seeding.';

COMMENT ON COLUMN public.banking_pay_workbench_candidate_source_lines.economic_key_json IS
'Economic-key parity payload where required by downstream pre-draft preview processing. Not post-draft authority.';


/* ============================================================================
   2. Per-stage Banking Pay workbench settings/defaults
   ============================================================================ */

/* Generic per-stage defaults. Short column names are intentional to stay safely
   under PostgreSQL's 63-byte identifier limit. */

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_scope_seed_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_source_build_units_per_job integer NOT NULL DEFAULT 10,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_line_seed_units_per_job integer NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_line_process_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_preview_mat_units_per_job integer NOT NULL DEFAULT 25,

  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_scope_seed_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_source_build_units_per_job integer NOT NULL DEFAULT 5,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_line_seed_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_line_process_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_preview_mat_units_per_job integer NOT NULL DEFAULT 25,

  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_scope_seed_units_per_job integer NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_source_build_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_line_seed_units_per_job integer NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_line_process_units_per_job integer NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_preview_mat_units_per_job integer NOT NULL DEFAULT 50,

  ADD COLUMN IF NOT EXISTS banking_pay_workbench_rollover_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_rollover_max_sessions_per_tick integer NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_rollover_nudge_after_create boolean NOT NULL DEFAULT true;


/* Backfill if the columns existed but were nullable/null in a previous partial migration. */

UPDATE public.settings_defaults
SET
  banking_pay_workbench_scope_seed_units_per_job =
    COALESCE(banking_pay_workbench_scope_seed_units_per_job, 25),
  banking_pay_workbench_source_build_units_per_job =
    COALESCE(banking_pay_workbench_source_build_units_per_job, 10),
  banking_pay_workbench_line_seed_units_per_job =
    COALESCE(banking_pay_workbench_line_seed_units_per_job, 50),
  banking_pay_workbench_line_process_units_per_job =
    COALESCE(banking_pay_workbench_line_process_units_per_job, 25),
  banking_pay_workbench_preview_mat_units_per_job =
    COALESCE(banking_pay_workbench_preview_mat_units_per_job, 25),

  banking_pay_workbench_nudge_scope_seed_units_per_job =
    COALESCE(banking_pay_workbench_nudge_scope_seed_units_per_job, 25),
  banking_pay_workbench_nudge_source_build_units_per_job =
    COALESCE(banking_pay_workbench_nudge_source_build_units_per_job, 5),
  banking_pay_workbench_nudge_line_seed_units_per_job =
    COALESCE(banking_pay_workbench_nudge_line_seed_units_per_job, 25),
  banking_pay_workbench_nudge_line_process_units_per_job =
    COALESCE(banking_pay_workbench_nudge_line_process_units_per_job, 25),
  banking_pay_workbench_nudge_preview_mat_units_per_job =
    COALESCE(banking_pay_workbench_nudge_preview_mat_units_per_job, 25),

  banking_pay_workbench_cron_scope_seed_units_per_job =
    COALESCE(banking_pay_workbench_cron_scope_seed_units_per_job, 50),
  banking_pay_workbench_cron_source_build_units_per_job =
    COALESCE(banking_pay_workbench_cron_source_build_units_per_job, 25),
  banking_pay_workbench_cron_line_seed_units_per_job =
    COALESCE(banking_pay_workbench_cron_line_seed_units_per_job, 50),
  banking_pay_workbench_cron_line_process_units_per_job =
    COALESCE(banking_pay_workbench_cron_line_process_units_per_job, 50),
  banking_pay_workbench_cron_preview_mat_units_per_job =
    COALESCE(banking_pay_workbench_cron_preview_mat_units_per_job, 50),

  banking_pay_workbench_rollover_enabled =
    COALESCE(banking_pay_workbench_rollover_enabled, true),
  banking_pay_workbench_rollover_max_sessions_per_tick =
    COALESCE(banking_pay_workbench_rollover_max_sessions_per_tick, 3),
  banking_pay_workbench_rollover_nudge_after_create =
    COALESCE(banking_pay_workbench_rollover_nudge_after_create, true);


/* Ensure defaults remain correct if the columns already existed. */

ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_workbench_scope_seed_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_source_build_units_per_job SET DEFAULT 10,
  ALTER COLUMN banking_pay_workbench_line_seed_units_per_job SET DEFAULT 50,
  ALTER COLUMN banking_pay_workbench_line_process_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_preview_mat_units_per_job SET DEFAULT 25,

  ALTER COLUMN banking_pay_workbench_nudge_scope_seed_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_units_per_job SET DEFAULT 5,
  ALTER COLUMN banking_pay_workbench_nudge_line_seed_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_nudge_line_process_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_nudge_preview_mat_units_per_job SET DEFAULT 25,

  ALTER COLUMN banking_pay_workbench_cron_scope_seed_units_per_job SET DEFAULT 50,
  ALTER COLUMN banking_pay_workbench_cron_source_build_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_cron_line_seed_units_per_job SET DEFAULT 50,
  ALTER COLUMN banking_pay_workbench_cron_line_process_units_per_job SET DEFAULT 50,
  ALTER COLUMN banking_pay_workbench_cron_preview_mat_units_per_job SET DEFAULT 50,

  ALTER COLUMN banking_pay_workbench_rollover_enabled SET DEFAULT true,
  ALTER COLUMN banking_pay_workbench_rollover_max_sessions_per_tick SET DEFAULT 3,
  ALTER COLUMN banking_pay_workbench_rollover_nudge_after_create SET DEFAULT true;


/* Ensure NOT NULL remains correct if the columns already existed as nullable. */

ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_workbench_scope_seed_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_source_build_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_line_seed_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_line_process_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_preview_mat_units_per_job SET NOT NULL,

  ALTER COLUMN banking_pay_workbench_nudge_scope_seed_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_nudge_line_seed_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_nudge_line_process_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_nudge_preview_mat_units_per_job SET NOT NULL,

  ALTER COLUMN banking_pay_workbench_cron_scope_seed_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_source_build_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_line_seed_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_line_process_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_preview_mat_units_per_job SET NOT NULL,

  ALTER COLUMN banking_pay_workbench_rollover_enabled SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_rollover_max_sessions_per_tick SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_rollover_nudge_after_create SET NOT NULL;


/* Add per-stage range checks idempotently. These mirror the existing
   banking_pay_workbench_stage_work_units_per_job 1..100 range. */

DO $$
DECLARE
  v_check record;
BEGIN
  FOR v_check IN
    SELECT *
    FROM (
      VALUES
        (
          'settings_defaults_bpw_scope_seed_units_chk',
          'banking_pay_workbench_scope_seed_units_per_job >= 1 AND banking_pay_workbench_scope_seed_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_source_build_units_chk',
          'banking_pay_workbench_source_build_units_per_job >= 1 AND banking_pay_workbench_source_build_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_line_seed_units_chk',
          'banking_pay_workbench_line_seed_units_per_job >= 1 AND banking_pay_workbench_line_seed_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_line_process_units_chk',
          'banking_pay_workbench_line_process_units_per_job >= 1 AND banking_pay_workbench_line_process_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_preview_mat_units_chk',
          'banking_pay_workbench_preview_mat_units_per_job >= 1 AND banking_pay_workbench_preview_mat_units_per_job <= 100'
        ),

        (
          'settings_defaults_bpw_nudge_scope_seed_units_chk',
          'banking_pay_workbench_nudge_scope_seed_units_per_job >= 1 AND banking_pay_workbench_nudge_scope_seed_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_nudge_source_build_units_chk',
          'banking_pay_workbench_nudge_source_build_units_per_job >= 1 AND banking_pay_workbench_nudge_source_build_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_nudge_line_seed_units_chk',
          'banking_pay_workbench_nudge_line_seed_units_per_job >= 1 AND banking_pay_workbench_nudge_line_seed_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_nudge_line_process_units_chk',
          'banking_pay_workbench_nudge_line_process_units_per_job >= 1 AND banking_pay_workbench_nudge_line_process_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_nudge_preview_mat_units_chk',
          'banking_pay_workbench_nudge_preview_mat_units_per_job >= 1 AND banking_pay_workbench_nudge_preview_mat_units_per_job <= 100'
        ),

        (
          'settings_defaults_bpw_cron_scope_seed_units_chk',
          'banking_pay_workbench_cron_scope_seed_units_per_job >= 1 AND banking_pay_workbench_cron_scope_seed_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_cron_source_build_units_chk',
          'banking_pay_workbench_cron_source_build_units_per_job >= 1 AND banking_pay_workbench_cron_source_build_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_cron_line_seed_units_chk',
          'banking_pay_workbench_cron_line_seed_units_per_job >= 1 AND banking_pay_workbench_cron_line_seed_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_cron_line_process_units_chk',
          'banking_pay_workbench_cron_line_process_units_per_job >= 1 AND banking_pay_workbench_cron_line_process_units_per_job <= 100'
        ),
        (
          'settings_defaults_bpw_cron_preview_mat_units_chk',
          'banking_pay_workbench_cron_preview_mat_units_per_job >= 1 AND banking_pay_workbench_cron_preview_mat_units_per_job <= 100'
        ),

        (
          'settings_defaults_bpw_rollover_max_sessions_chk',
          'banking_pay_workbench_rollover_max_sessions_per_tick >= 0 AND banking_pay_workbench_rollover_max_sessions_per_tick <= 50'
        )
    ) AS checks(constraint_name, check_sql)
  LOOP
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.settings_defaults'::regclass
        AND conname = v_check.constraint_name
    ) THEN
      EXECUTE format(
        'ALTER TABLE public.settings_defaults ADD CONSTRAINT %I CHECK (%s)',
        v_check.constraint_name,
        v_check.check_sql
      );
    END IF;
  END LOOP;
END $$;


COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_scope_seed_units_per_job IS
'Default work units per job for Banking Pay WORKBENCH_SESSION_SCOPE_SEED. Falls back from existing generic stage setting in Worker code if required.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_source_build_units_per_job IS
'Default work units per job for Banking Pay WORKBENCH_CANDIDATE_SOURCE_BUILD. Keeps source/classifier work bounded before line seed.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_line_seed_units_per_job IS
'Default work units per job for Banking Pay WORKBENCH_CANDIDATE_LINE_WORK_SEED after source rows are persisted.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_line_process_units_per_job IS
'Default work units per job for Banking Pay WORKBENCH_CANDIDATE_LINE_WORK_PROCESS.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_preview_mat_units_per_job IS
'Default work units per job for Banking Pay WORKBENCH_PREVIEW_ROWS_MATERIALISE.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_rollover_enabled IS
'Enables proactive Banking Pay shared-session creation/refresh after pay-date rollover once Worker rollover code is implemented.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_rollover_max_sessions_per_tick IS
'Maximum current-pay-date Banking Pay workbench sessions to create/reuse per scheduled rollover tick.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_rollover_nudge_after_create IS
'When true, session creation/reuse during rollover should nudge the workbench drain for that session.';


/* ============================================================================
   3. Conditional job_type CHECK extension for deployment variants

   The attached schema has no job_type check constraint on
   public.banking_pay_workbench_jobs. This block is inert there.

   If another deployment variant has a simple job_type-only CHECK, this safely
   extends that existing constraint to allow WORKBENCH_CANDIDATE_SOURCE_BUILD
   while preserving the original expression for all other job types.

   It deliberately does not rewrite mixed/multi-column constraints.
   ============================================================================ */

DO $$
DECLARE
  v_constraint record;
  v_expr text;
BEGIN
  FOR v_constraint IN
    SELECT
      c.oid,
      c.conname,
      pg_get_constraintdef(c.oid) AS constraint_def,
      array_agg(a.attname ORDER BY a.attname) AS constraint_columns
    FROM pg_constraint AS c
    JOIN pg_class AS rel
      ON rel.oid = c.conrelid
    JOIN pg_namespace AS nsp
      ON nsp.oid = rel.relnamespace
    JOIN LATERAL unnest(c.conkey) AS ck(attnum)
      ON true
    JOIN pg_attribute AS a
      ON a.attrelid = rel.oid
     AND a.attnum = ck.attnum
    WHERE nsp.nspname = 'public'
      AND rel.relname = 'banking_pay_workbench_jobs'
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) ILIKE '%job_type%'
    GROUP BY c.oid, c.conname
  LOOP
    IF v_constraint.constraint_def ILIKE '%WORKBENCH_CANDIDATE_SOURCE_BUILD%' THEN
      CONTINUE;
    END IF;

    IF v_constraint.constraint_columns = ARRAY['job_type']::text[] THEN
      v_expr := regexp_replace(
        v_constraint.constraint_def,
        '^CHECK \((.*)\)( NOT VALID)?$',
        '\1'
      );

      EXECUTE format(
        'ALTER TABLE public.banking_pay_workbench_jobs DROP CONSTRAINT %I',
        v_constraint.conname
      );

      EXECUTE format(
        'ALTER TABLE public.banking_pay_workbench_jobs ADD CONSTRAINT %I CHECK ((job_type = %L) OR (%s))',
        v_constraint.conname,
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        v_expr
      );
    ELSE
      RAISE NOTICE
        'Not rewriting mixed-column job_type CHECK constraint %. Please review manually if WORKBENCH_CANDIDATE_SOURCE_BUILD inserts are blocked. Definition: %',
        v_constraint.conname,
        v_constraint.constraint_def;
    END IF;
  END LOOP;
END $$;