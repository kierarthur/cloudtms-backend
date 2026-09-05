-- H2 P2 rollback-only current-V8 policy-owner proof for paired Timesheets.
-- Runtime authority is Miget TEST; the `supabase` path name is historical.
-- This fixture is for task-owned disposable PG17/PG18 databases only. It does
-- not execute payments, provider submission, settlement or remittance and it
-- does not reactivate the disabled legacy all-at-once Draft route.
\set ON_ERROR_STOP on

BEGIN;
SET LOCAL jit=off;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='1500ms';
SET LOCAL idle_in_transaction_session_timeout='30s';
SET LOCAL cloudtms.rollback_fixture_scope='BANKING_PAY_DRAFT_V8_PAIRED_TIMESHEET_PARITY_P2_V1';
SET LOCAL request.jwt.claim.role='service_role';

DO $guard$
BEGIN
  IF current_database()<>'banking_modal_v2_test' THEN
    RAISE EXCEPTION 'LOCAL_FIXTURE_ONLY';
  END IF;
  IF EXISTS (SELECT 1 FROM public.timesheets WHERE timesheet_id::text LIKE '92000000-0000-4000-8000-%')
     OR EXISTS (SELECT 1 FROM public.candidates WHERE id::text LIKE '92000000-0000-4000-8000-%')
     OR EXISTS (SELECT 1 FROM public.clients WHERE id::text LIKE '92000000-0000-4000-8000-%')
     OR EXISTS (SELECT 1 FROM public.settings_finance_windows WHERE id::text LIKE '92000000-0000-4000-8000-%')
     OR EXISTS (SELECT 1 FROM public.banking_pay_operations WHERE id::text LIKE '92000000-0000-4000-8000-%') THEN
    RAISE EXCEPTION 'ROLLBACK_FIXTURE_ID_COLLISION';
  END IF;
END;
$guard$;

INSERT INTO public.settings_finance_windows(
  id,date_from,date_to,vat_rate_pct,erni_pct,holiday_pay_pct,
  apply_holiday_to,apply_erni_to,margin_includes
) VALUES(
  '92000000-0000-4000-8000-000000000099','2026-01-01','2026-12-31',
  20,0,0,'NONE','PAYE_ONLY','[]'::jsonb
);

\ir fixtures/banking-pay-settled-certificate-v8-runtime-helpers.sql

CREATE TEMPORARY TABLE pg_temp.h2_p2_results(
  class_id text PRIMARY KEY,
  current_v8_status text NOT NULL,
  typed_artifact jsonb NOT NULL
) ON COMMIT DROP;

-- The production overpayment-sync owner first rewrites a complete correction
-- chain into this bounded candidate table.  Keep the exact table contract so
-- the reversal-only proof exercises that owner rather than guessing from the
-- sign of the residual.
CREATE TEMPORARY TABLE pg_temp.tmp_sync_timesheet_case_candidates(
  candidate_id uuid NOT NULL,
  timesheet_id uuid NOT NULL,
  client_id uuid NULL,
  linked_shift_date date NULL,
  corrected_amount_ex numeric(12,2) NOT NULL,
  baseline_signature text NULL,
  candidate_pay_method text NOT NULL,
  case_is_blocked boolean NOT NULL,
  needs_lifecycle_tracking boolean NOT NULL DEFAULT false,
  overpayment_amount_ex numeric(12,2) NOT NULL,
  underpayment_amount_ex numeric(12,2) NOT NULL,
  desired_case_type public.pay_finance_case_type_enum NULL,
  desired_advance_kind public.pay_advance_kind_enum NULL,
  desired_reason public.pay_advance_reason_enum NULL,
  source_original_paid_amount numeric(12,2) NULL,
  source_corrected_paid_amount numeric(12,2) NULL,
  components_sync_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  PRIMARY KEY(candidate_id,timesheet_id)
) ON COMMIT DROP;

CREATE TEMPORARY TABLE pg_temp.h2_p2_unrelated_candidate_baseline(
  candidate_id uuid PRIMARY KEY,
  row_digest_sha256 text NOT NULL
) ON COMMIT DROP;

CREATE OR REPLACE FUNCTION pg_temp.h2_p2_uuid(p_value integer)
RETURNS uuid
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path TO ''
AS $function$
  SELECT ('92000000-0000-4000-8000-'||pg_catalog.lpad(p_value::text,12,'0'))::uuid
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_p2_sha256(p_value jsonb)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path TO ''
AS $function$
  SELECT pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(p_value::text,'UTF8'),'sha256'),
    'hex'
  )
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_p2_assert(p_condition boolean,p_message text)
RETURNS void
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
BEGIN
  IF COALESCE(p_condition,false) IS NOT TRUE THEN
    RAISE EXCEPTION 'H2_P2_ASSERTION_FAILED: %',p_message;
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_p2_policy_envelope(
  p_root_id uuid,
  p_operation_id text,
  p_chain_id text,
  p_shape text,
  p_pay_channel text
)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
DECLARE
  v_pay_vat numeric := CASE WHEN upper(p_pay_channel)='UMBRELLA' THEN 20 ELSE 0 END;
  v_tsfin jsonb;
  v_invoice jsonb;
  v_reversal jsonb;
  v_replacement jsonb;
  v_envelope jsonb;
BEGIN
  v_tsfin:=pg_catalog.jsonb_build_object(
    'erni_pct',0,'apply_erni_to','PAYE_ONLY','applied_pay_vat_rate_pct',v_pay_vat,
    'pay_channel',upper(p_pay_channel),'materialisation_stage','TSFIN'
  );
  v_tsfin:=v_tsfin||pg_catalog.jsonb_build_object(
    'tsfin_policy_fingerprint',pg_temp.h2_p2_sha256(v_tsfin));
  v_invoice:=pg_catalog.jsonb_build_object(
    'applicable',true,'materialisation_stage','INVOICE_GENERATION',
    'final_invoice_vat_materialised',false,'source_vat_rate_pct',20,
    'applied_vat_rate_pct',20,'invoice_vat_chargeable',true,
    'invoice_stream','NORMAL'
  );
  v_invoice:=v_invoice||pg_catalog.jsonb_build_object(
    'invoice_policy_fingerprint',pg_temp.h2_p2_sha256(v_invoice));
  v_reversal:=pg_catalog.jsonb_build_object(
    'role','REVERSAL','tsfin_policy',v_tsfin,'invoice_policy',v_invoice);
  v_reversal:=v_reversal||pg_catalog.jsonb_build_object(
    'leg_fingerprint',pg_temp.h2_p2_sha256(v_reversal));
  v_replacement:=pg_catalog.jsonb_build_object(
    'role','REPLACEMENT','tsfin_policy',v_tsfin,'invoice_policy',v_invoice);
  v_replacement:=v_replacement||pg_catalog.jsonb_build_object(
    'leg_fingerprint',pg_temp.h2_p2_sha256(v_replacement));
  v_envelope:=pg_catalog.jsonb_build_object(
    'policy_schema_version','IMPORT_CORRECTION_FINANCIALS_POLICY_V2',
    'route_family','IMPORT_AUTHORITATIVE',
    'classification',pg_catalog.jsonb_build_object('canonical',true,'source_system','HEALTHROSTER'),
    'operation',pg_catalog.jsonb_build_object(
      'operation_id',p_operation_id,'correction_action','CHANGED_HOURS'),
    'correction_chain_id',p_chain_id,
    'root_timesheet_id',p_root_id::text,
    'correction_shape',upper(p_shape),
    'expected_member_count',CASE WHEN upper(p_shape)='REVERSAL_ONLY' THEN 1 ELSE 2 END,
    'expected_member_roles',CASE WHEN upper(p_shape)='REVERSAL_ONLY'
      THEN pg_catalog.jsonb_build_array('REVERSAL')
      ELSE pg_catalog.jsonb_build_array('REVERSAL','REPLACEMENT') END,
    'invoice_stream','NORMAL','reversal',v_reversal
  );
  IF upper(p_shape)='REVERSAL_REPLACEMENT' THEN
    v_envelope:=v_envelope||pg_catalog.jsonb_build_object('replacement',v_replacement);
  END IF;
  RETURN v_envelope||pg_catalog.jsonb_build_object(
    'envelope_fingerprint',pg_temp.h2_p2_sha256(v_envelope));
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_p2_seed_chain(
  p_base integer,
  p_pay_channel text,
  p_shape text DEFAULT 'REVERSAL_REPLACEMENT'
)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
DECLARE
  v_actor uuid:=pg_temp.h2_p2_uuid(1);
  v_candidate uuid:=pg_temp.h2_p2_uuid(p_base+10);
  v_client uuid:=pg_temp.h2_p2_uuid(p_base+11);
  v_contract uuid:=pg_temp.h2_p2_uuid(p_base+12);
  v_root uuid:=pg_temp.h2_p2_uuid(p_base+1);
  v_reversal uuid:=pg_temp.h2_p2_uuid(p_base+2);
  v_replacement uuid:=pg_temp.h2_p2_uuid(p_base+3);
  v_envelope jsonb;
  v_reversal_leg jsonb;
  v_replacement_leg jsonb;
  v_root_snapshot jsonb;
BEGIN
  v_envelope:=pg_temp.h2_p2_policy_envelope(
    v_root,'h2-p2-operation-'||p_base,'h2-p2-chain-'||p_base,p_shape,p_pay_channel);
  v_reversal_leg:=v_envelope->'reversal';
  v_replacement_leg:=v_envelope->'replacement';

  INSERT INTO public.tms_users(id,email,role,password_hash,is_active)
  VALUES(v_actor,'h2-p2-actor@example.invalid','admin','test-only',true)
  ON CONFLICT(id) DO NOTHING;
  INSERT INTO public.clients(id,cli_ref,name,vat_chargeable,payment_terms_days)
  VALUES(v_client,'H2-P2-'||p_base,'H2 P2 client '||p_base,true,30);
  INSERT INTO public.candidates(id,tms_ref,display_name,pay_method,active)
  VALUES(v_candidate,'H2-P2-'||p_base,'H2 P2 candidate '||p_base,upper(p_pay_channel),true);
  INSERT INTO public.client_settings(
    id,client_id,is_nhsp,autoprocess_hr,no_timesheet_required
  ) VALUES(pg_temp.h2_p2_uuid(p_base+13),v_client,true,true,true);
  INSERT INTO public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot
  ) VALUES(v_contract,v_candidate,v_client,'2026-01-01','2026-12-31',upper(p_pay_channel));

  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,contract_id,sheet_scope,worked_start_iso,worked_end_iso,
    actual_schedule_json,authorised_at_server,is_current,version,is_adjustment
  ) VALUES(
    v_root,'h2-p2-root-'||p_base,'h2-p2-'||p_base,'h2 hospital','h2 ward','h2 role',
    '2026-09-06',v_contract,'DAILY','2026-08-31 08:00+01','2026-08-31 18:00+01',
    '{"date":"2026-08-31","start":"08:00","end":"18:00","break_minutes":0}'::jsonb,
    '2026-09-01 09:00+01',true,1,false
  );
  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,contract_id,sheet_scope,worked_start_iso,worked_end_iso,
    actual_schedule_json,is_current,version,is_adjustment,parent_timesheet_id,
    correction_id,correction_kind,adjustment_origin,candidate_hint_text
  ) VALUES(
    v_reversal,'h2-p2-reversal-'||p_base,'h2-p2-'||p_base,'h2 hospital','h2 ward','h2 role',
    '2026-09-06',v_contract,'DAILY','2026-08-31 08:00+01','2026-08-31 18:00+01',
    '{"date":"2026-08-31","start":"08:00","end":"18:00","break_minutes":0}'::jsonb,
    true,1,true,v_root,'h2-p2-correction-'||p_base,'CHANGED_HOURS_REVERSAL',
    'IMPORT_CORRECTION',pg_catalog.jsonb_build_object('correction_financials_policy_envelope',v_envelope)
  );
  IF upper(p_shape)='REVERSAL_REPLACEMENT' THEN
    INSERT INTO public.timesheets(
      timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
      week_ending_date,contract_id,sheet_scope,worked_start_iso,worked_end_iso,
      actual_schedule_json,is_current,version,is_adjustment,parent_timesheet_id,
      correction_id,correction_kind,adjustment_origin,candidate_hint_text
    ) VALUES(
      v_replacement,'h2-p2-replacement-'||p_base,'h2-p2-'||p_base,'h2 hospital','h2 ward','h2 role',
      '2026-09-06',v_contract,'DAILY','2026-08-31 08:00+01','2026-08-31 20:00+01',
      '{"date":"2026-08-31","start":"08:00","end":"20:00","break_minutes":0}'::jsonb,
      true,1,true,v_root,'h2-p2-correction-'||p_base,'CHANGED_HOURS_REPLACEMENT',
      'IMPORT_CORRECTION',pg_catalog.jsonb_build_object('correction_financials_policy_envelope',v_envelope)
    );
  END IF;
  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','',true);

  INSERT INTO public.timesheets_financials(
    id,timesheet_id,timesheet_version,basis,is_current,is_stale,
    worked_start_iso,worked_end_iso,actual_schedule_json,candidate_id,client_id,
    role,pay_method,occupant_key_norm,candidate_assignment,processing_status,
    hours_day,pay_day,charge_day,total_hours,total_pay_ex_vat,total_charge_ex_vat,
    margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue,
    pay_vat_rate_pct_snapshot,policy_snapshot_json
  ) VALUES(
    pg_temp.h2_p2_uuid(p_base+21),v_root,1,'SELF_REPORTED',true,false,
    '2026-08-31 08:00+01','2026-08-31 18:00+01',
    '{"date":"2026-08-31","start":"08:00","end":"18:00","break_minutes":0}'::jsonb,
    v_candidate,v_client,'h2 role',upper(p_pay_channel),'h2-p2-'||p_base,
    'ASSIGNED','READY_FOR_INVOICE',10,10,15,10,100,150,50,false,false,false,
    CASE WHEN upper(p_pay_channel)='UMBRELLA' THEN 20 ELSE 0 END,'{}'::jsonb
  );
  INSERT INTO public.timesheets_financials(
    id,timesheet_id,timesheet_version,basis,is_current,is_stale,
    worked_start_iso,worked_end_iso,actual_schedule_json,candidate_id,client_id,
    role,pay_method,occupant_key_norm,candidate_assignment,processing_status,
    hours_day,pay_day,charge_day,total_hours,total_pay_ex_vat,total_charge_ex_vat,
    margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue,
    pay_vat_rate_pct_snapshot,policy_snapshot_json,rate_source_refs_json
  ) VALUES(
    pg_temp.h2_p2_uuid(p_base+22),v_reversal,1,'SELF_REPORTED',true,false,
    '2026-08-31 08:00+01','2026-08-31 18:00+01',
    '{"date":"2026-08-31","start":"08:00","end":"18:00","break_minutes":0}'::jsonb,
    v_candidate,v_client,'h2 role',upper(p_pay_channel),'h2-p2-'||p_base,
    'ASSIGNED','PENDING_AUTH',-10,-10,-15,-10,-100,-150,-50,false,false,false,
    CASE WHEN upper(p_pay_channel)='UMBRELLA' THEN 20 ELSE 0 END,
    pg_catalog.jsonb_build_object(
      'correction_financials_policy_envelope',v_envelope,
      'correction_financials_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
      'correction_leg_fingerprint',v_reversal_leg->>'leg_fingerprint',
      'correction_tsfin_policy',v_reversal_leg->'tsfin_policy',
      'correction_tsfin_policy_fingerprint',v_reversal_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}',
      'correction_invoice_policy',v_reversal_leg->'invoice_policy',
      'correction_invoice_policy_fingerprint',v_reversal_leg#>>'{invoice_policy,invoice_policy_fingerprint}',
      'erni_pct',0,'apply_erni_to','PAYE_ONLY','pay_vat_rate_pct',
      CASE WHEN upper(p_pay_channel)='UMBRELLA' THEN 20 ELSE 0 END
    ),'{}'::jsonb
  );
  IF upper(p_shape)='REVERSAL_REPLACEMENT' THEN
    INSERT INTO public.timesheets_financials(
      id,timesheet_id,timesheet_version,basis,is_current,is_stale,
      worked_start_iso,worked_end_iso,actual_schedule_json,candidate_id,client_id,
      role,pay_method,occupant_key_norm,candidate_assignment,processing_status,
      hours_day,pay_day,charge_day,total_hours,total_pay_ex_vat,total_charge_ex_vat,
      margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue,
      pay_vat_rate_pct_snapshot,policy_snapshot_json,rate_source_refs_json
    ) VALUES(
      pg_temp.h2_p2_uuid(p_base+23),v_replacement,1,'SELF_REPORTED',true,false,
      '2026-08-31 08:00+01','2026-08-31 20:00+01',
      '{"date":"2026-08-31","start":"08:00","end":"20:00","break_minutes":0}'::jsonb,
      v_candidate,v_client,'h2 role',upper(p_pay_channel),'h2-p2-'||p_base,
      'ASSIGNED','PENDING_AUTH',12,10,15,12,120,180,60,false,false,false,
      CASE WHEN upper(p_pay_channel)='UMBRELLA' THEN 20 ELSE 0 END,
      pg_catalog.jsonb_build_object(
        'correction_financials_policy_envelope',v_envelope,
        'correction_financials_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
        'correction_leg_fingerprint',v_replacement_leg->>'leg_fingerprint',
        'correction_tsfin_policy',v_replacement_leg->'tsfin_policy',
        'correction_tsfin_policy_fingerprint',v_replacement_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}',
        'correction_invoice_policy',v_replacement_leg->'invoice_policy',
        'correction_invoice_policy_fingerprint',v_replacement_leg#>>'{invoice_policy,invoice_policy_fingerprint}',
        'erni_pct',0,'apply_erni_to','PAYE_ONLY','pay_vat_rate_pct',
        CASE WHEN upper(p_pay_channel)='UMBRELLA' THEN 20 ELSE 0 END
      ),'{}'::jsonb
    );
  END IF;

  v_root_snapshot:=pg_catalog.jsonb_build_object(
    'segments',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'segment_id','h2-p2-root-'||p_base,'pay_amount',100,
      'exclude_from_pay',false,'date','2026-08-31')),
    'additional_units_json','{}'::jsonb,'additional_pay_ex_vat',0,
    'expenses','{}'::jsonb,'adjustments','[]'::jsonb
  );
  INSERT INTO public.timesheet_pay_state(
    timesheet_id,last_settled_snapshot_json,last_settled_signature,
    last_settled_at_utc,summary_net_delta_ex_vat
  ) VALUES(v_root,v_root_snapshot,'h2-p2-settled-'||p_base,'2026-08-25 12:00+01',0);

  RETURN pg_catalog.jsonb_build_object(
    'actor_id',v_actor::text,'candidate_id',v_candidate::text,
    'client_id',v_client::text,'contract_id',v_contract::text,
    'root_timesheet_id',v_root::text,'reversal_timesheet_id',v_reversal::text,
    'replacement_timesheet_id',CASE WHEN upper(p_shape)='REVERSAL_REPLACEMENT'
      THEN v_replacement::text ELSE NULL END,
    'correction_shape',upper(p_shape),'pay_channel',upper(p_pay_channel),
    'policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint'
  );
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_p2_authorise_chain(p_seed jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
DECLARE
  v_expected_count integer:=CASE
    WHEN p_seed->>'correction_shape'='REVERSAL_ONLY' THEN 1 ELSE 2 END;
  v_result jsonb;
BEGIN
  v_result:=public.timesheet_authorise_bulk_atomic(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'timesheet_id',p_seed->>'reversal_timesheet_id')),
    (p_seed->>'actor_id')::uuid,
    '2026-09-04 10:00+01'::timestamptz);
  PERFORM pg_temp.h2_p2_assert(
    (v_result->>'all_success')::boolean
      AND (v_result->>'success_count')::integer=v_expected_count
      AND (v_result->>'failure_count')::integer=0,
    'canonical correction lifecycle authorisation did not succeed atomically');
  RETURN v_result;
END;
$function$;

-- Canonical import-authoritative correction carrier.  Unlike h2_p2_seed_chain,
-- this helper never constructs a correction pair.  It seeds only the preceding
-- protected weekly Timesheet and immutable source/review evidence, then invokes
-- the current apply-envelope, operation-claim, phase-3 producer and correction
-- lifecycle owners.  The small TSFIN insert between source commit and lifecycle
-- validation represents the independently asynchronous TSFIN Worker boundary;
-- its values are taken from the frozen B/A evidence and the operation-owned
-- financial policy envelope, not from a second economic calculation.
CREATE OR REPLACE FUNCTION pg_temp.h2_p2_seed_canonical_import_pair(
  p_base integer,
  p_source_system text,
  p_pay_channel text
)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
DECLARE
  v_source text:=upper(pg_catalog.btrim(p_source_system));
  v_channel text:=upper(pg_catalog.btrim(p_pay_channel));
  v_actor uuid:=pg_temp.h2_p2_uuid(1);
  v_candidate uuid:=pg_temp.h2_p2_uuid(p_base+10);
  v_client uuid:=pg_temp.h2_p2_uuid(p_base+11);
  v_contract uuid:=pg_temp.h2_p2_uuid(p_base+12);
  v_settings uuid:=pg_temp.h2_p2_uuid(p_base+13);
  v_root uuid:=pg_temp.h2_p2_uuid(p_base+1);
  v_root_fin uuid:=pg_temp.h2_p2_uuid(p_base+21);
  v_root_week uuid:=pg_temp.h2_p2_uuid(p_base+31);
  v_invoice uuid:=pg_temp.h2_p2_uuid(p_base+81);
  v_invoice_line uuid:=pg_temp.h2_p2_uuid(p_base+82);
  v_import uuid:=pg_temp.h2_p2_uuid(p_base+50);
  v_hr_row uuid:=pg_temp.h2_p2_uuid(p_base+51);
  v_shift uuid:=pg_temp.h2_p2_uuid(p_base+52);
  v_operation uuid:=pg_temp.h2_p2_uuid(p_base+60);
  v_key text:='H2-P2-'||v_source||'-'||p_base::text;
  v_action text;
  v_evidence_fingerprint text;
  v_scope_fingerprint text;
  v_reconciliation_fingerprint text;
  v_policy_basis_fingerprint text;
  v_b_schedule jsonb;
  v_a_schedule jsonb;
  v_envelope jsonb;
  v_request_hash text;
  v_claim jsonb;
  v_prepare jsonb;
  v_phase3 jsonb;
  v_complete jsonb;
  v_validate jsonb;
  v_authorise jsonb;
  v_unit jsonb;
  v_policy jsonb;
  v_reversal uuid;
  v_replacement uuid;
  v_reconciliation_units jsonb;
  v_basis public.timesheet_fin_basis_enum;
BEGIN
  IF v_source NOT IN ('NHSP','HEALTHROSTER')
     OR v_channel NOT IN ('PAYE','UMBRELLA') THEN
    RAISE EXCEPTION 'H2_P2_CANONICAL_ROUTE_INPUT_INVALID' USING ERRCODE='22023';
  END IF;

  v_action:=pg_temp.h2_p2_sha256(pg_catalog.jsonb_build_object(
    'case','canonical-import-pair','source_system',v_source,'base',p_base));
  v_evidence_fingerprint:=pg_temp.h2_p2_sha256(pg_catalog.jsonb_build_object(
    'case','canonical-import-pair-evidence','action_id',v_action));
  v_scope_fingerprint:=pg_temp.h2_p2_sha256(pg_catalog.jsonb_build_object(
    'source_identity',v_key,'source_system',v_source,'source_shift_id',v_shift,
    'source_timesheet_id',v_root,'candidate_id',v_candidate,'client_id',v_client,
    'contract_id',v_contract,'week_ending_date','2026-09-06','invoice_stream','SELF_BILL'));
  v_reconciliation_fingerprint:=pg_temp.h2_p2_sha256(pg_catalog.jsonb_build_object(
    'case','canonical-import-reconciliation','source_scope_fingerprint',v_scope_fingerprint));
  v_policy_basis_fingerprint:=pg_temp.h2_p2_sha256(pg_catalog.jsonb_build_object(
    'case','canonical-import-policy-basis','source_scope_fingerprint',v_scope_fingerprint));
  v_b_schedule:=pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'date','2026-08-31','start_utc','2026-08-31T07:00:00Z',
    'end_utc','2026-08-31T17:00:00Z','break_mins',0,
    'shift_id',v_shift::text,'external_row_key',v_key,'import_id',v_import::text));
  v_a_schedule:=pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'date','2026-08-31','start_utc','2026-08-31T07:00:00Z',
    'end_utc','2026-08-31T19:00:00Z','break_mins',0,
    'shift_id',v_shift::text,'external_row_key',v_key,'import_id',v_import::text));

  INSERT INTO public.tms_users(id,email,role,password_hash,is_active)
  VALUES(v_actor,'h2-p2-actor@example.invalid','admin','test-only',true)
  ON CONFLICT(id) DO NOTHING;
  INSERT INTO public.clients(id,cli_ref,name,vat_chargeable,payment_terms_days)
  VALUES(v_client,'H2-P2-'||p_base,'H2 P2 canonical client '||p_base,true,30);
  INSERT INTO public.candidates(id,tms_ref,display_name,pay_method,active)
  VALUES(v_candidate,'H2-P2-'||p_base,'H2 P2 canonical Candidate '||p_base,v_channel,true);
  INSERT INTO public.client_settings(
    id,client_id,is_nhsp,autoprocess_hr,no_timesheet_required,
    reversal_complete_financials_date,reversal_replacement_financials_date,
    erni_pct,apply_erni_to,vat_rate_pct
  ) VALUES(
    v_settings,v_client,v_source='NHSP',v_source='HEALTHROSTER',true,
    'NOW','NOW',0,'PAYE_ONLY',
    CASE WHEN v_channel='UMBRELLA' THEN 20 ELSE 0 END
  );
  INSERT INTO public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,
    role,band,display_site,ward_hint,self_bill,rates_json
  ) VALUES(
    v_contract,v_candidate,v_client,'2026-01-01','2026-12-31',v_channel,
    'RMN','5','H2 Hospital','H2 Ward',true,
    pg_catalog.jsonb_build_object(
      'paye_day',10,'paye_night',10,'paye_sat',10,'paye_sun',10,'paye_bh',10,
      'umb_day',10,'umb_night',10,'umb_sat',10,'umb_sun',10,'umb_bh',10,
      'charge_day',15,'charge_night',15,'charge_sat',15,'charge_sun',15,'charge_bh',15)
  );
  INSERT INTO public.hr_imports(
    id,filename,source_system,client_id,import_scope,coverage_fingerprint,
    revision_group_id,revision_no
  ) VALUES(
    v_import,'h2-p2-'||lower(v_source)||'-'||p_base||'.csv',
    v_source::public.hr_source_enum,v_client,
    CASE WHEN v_source='NHSP' THEN 'NHSP' ELSE 'HR_WEEKLY' END,
    pg_catalog.repeat('c',64),v_import,1
  );
  INSERT INTO public.hr_rows(
    id,import_id,external_row_key,date_local,start_time_local,end_time_local,
    hours_worked,role_type,staff_norm,staff_raw,assignment_grade_norm,
    hr_request_id,payload_json
  ) VALUES(
    v_hr_row,v_import,v_key,'2026-08-31','08:00','20:00',12,'RMN','h2 candidate',
    'H2 Candidate','5','H2-REF-'||p_base,
    pg_catalog.jsonb_build_object(
      'start_utc','2026-08-31T07:00:00Z','end_utc','2026-08-31T19:00:00Z',
      'break_mins',0,'actual_break_mins',0,'ref_num','H2-REF-'||p_base,
      'reference_number','H2-REF-'||p_base,
      'trust','H2 P2 canonical client '||p_base)
  );
  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,contract_id,sheet_scope,submission_mode,worked_start_iso,worked_end_iso,
    actual_schedule_json,authorised_at_server,is_current,version,is_adjustment
  ) VALUES(
    v_root,'h2-p2-root-'||p_base,'h2 candidate','h2 hospital','h2 ward','rmn',
    '2026-09-06',v_contract,'WEEKLY','MANUAL',
    '2026-08-31T07:00:00Z','2026-08-31T17:00:00Z',v_b_schedule,
    '2026-09-01T08:00:00Z',true,1,false
  );
  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','',true);
  INSERT INTO public.nhsp_shifts(
    id,external_row_key,source_system,candidate_id,client_id,contract_id,
    start_utc,end_utc,break_mins,pay_minutes,work_date,week_ending_date,
    timesheet_id,latest_import_id,hr_request_id,ref_num,staff_name,staff_norm,
    ward,ward_norm
  ) VALUES(
    v_shift,v_key,v_source::public.hr_source_enum,v_candidate,v_client,v_contract,
    '2026-08-31T07:00:00Z','2026-08-31T17:00:00Z',0,600,'2026-08-31','2026-09-06',
    v_root,v_import,'H2-REF-'||p_base,'H2-REF-'||p_base,'H2 Candidate','h2 candidate',
    'H2 Ward','h2 ward'
  );
  INSERT INTO public.invoices(
    id,type,client_id,issued_at_utc,status,subtotal_ex_vat,vat_amount,total_inc_vat,
    issue_state,header_snapshot_json
  ) VALUES(
    v_invoice,'INVOICE',v_client,'2026-09-01T08:15:00Z','ISSUED',150,30,180,'ISSUED',
    pg_catalog.jsonb_build_object(
      'vat_chargeable',true,'applied_vat_rate_pct',20,
      'meta',pg_catalog.jsonb_build_object('self_bill',true))
  );
  v_basis:=CASE WHEN v_source='NHSP' THEN 'NHSP'::public.timesheet_fin_basis_enum
    ELSE 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum END;
  INSERT INTO public.timesheets_financials(
    id,timesheet_id,timesheet_version,basis,is_current,is_stale,
    worked_start_iso,worked_end_iso,actual_schedule_json,candidate_id,client_id,
    role,pay_method,occupant_key_norm,candidate_assignment,processing_status,
    hours_day,total_hours,pay_day,charge_day,total_pay_ex_vat,total_charge_ex_vat,
    margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue,
    pay_vat_rate_pct_snapshot,policy_snapshot_json,paid_at_utc,authorised_at_utc,
    locked_by_invoice_id,locked_at_utc,invoice_breakdown_json
  ) VALUES(
    v_root_fin,v_root,1,v_basis,true,false,
    '2026-08-31T07:00:00Z','2026-08-31T17:00:00Z',v_b_schedule,v_candidate,v_client,
    'RMN',v_channel,'h2 candidate','ASSIGNED','READY_FOR_INVOICE',
    10,10,10,15,100,150,50,false,false,false,
    CASE WHEN v_channel='UMBRELLA' THEN 20 ELSE 0 END,
    pg_catalog.jsonb_build_object(
      'erni_pct',0,'apply_erni_to','PAYE_ONLY','pay_vat_rate_pct',
      CASE WHEN v_channel='UMBRELLA' THEN 20 ELSE 0 END),
    '2026-09-01T08:30:00Z','2026-09-01T08:00:00Z',v_invoice,'2026-09-01T08:15:00Z',
    pg_catalog.jsonb_build_object('mode','SEGMENTS','segments',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'date','2026-08-31','start_utc','2026-08-31T07:00:00Z',
        'end_utc','2026-08-31T17:00:00Z','break_mins',0,
        'shift_id',v_shift::text,'external_row_key',v_key,'import_id',v_import::text,
        'hours_day',10,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
        'pay_amount',100,'charge_amount',150)))
  );
  INSERT INTO public.invoice_lines(
    id,invoice_id,timesheet_id,booking_id,description,
    hours_day,hours_night,hours_sat,hours_sun,hours_bh,
    pay_day,charge_day,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,
    vat_rate_pct,vat_amount,total_inc_vat,meta_json,source_key
  ) VALUES(
    v_invoice_line,v_invoice,v_root,'h2-p2-root-'||p_base,'H2 P2 frozen weekly hours',
    10,0,0,0,0,10,15,100,150,50,20,30,180,
    pg_catalog.jsonb_build_object('line_type','HOURS_WEEKLY','timesheet_id',v_root::text,'tsfin_id',v_root_fin::text),
    'H2-P2-FROZEN-'||p_base
  );
  INSERT INTO public.contract_weeks(
    id,timesheet_id,contract_id,week_ending_date,status,submission_mode_snapshot,is_adjustment
  ) VALUES(v_root_week,v_root,v_contract,'2026-09-06','AUTHORISED','ELECTRONIC',false);
  INSERT INTO public.import_review_states(
    import_id,status,state_version,preview_generation,preview_fingerprint,
    follow_up_status,created_by_user_id,updated_by_user_id
  ) VALUES(v_import,'READY',1,1,pg_catalog.repeat('p',64),'NOT_REQUIRED',v_actor,v_actor);
  INSERT INTO public.import_review_decisions(
    action_id,import_id,preview_generation,action_kind,action_category,target_key,
    source_identity,shift_id,candidate_id,client_id,contract_id,timesheet_id,hr_row_id,
    is_current,selected,default_selected,selectable,blocking,summary_json,evidence_fingerprint,
    selected_by_user_id,selected_at_utc
  ) VALUES(
    v_action,v_import,1,'APPLY_AMENDMENT','READY','hr-row:'||v_hr_row,v_key,v_shift,
    v_candidate,v_client,v_contract,v_root,v_hr_row,true,true,true,true,false,
    pg_catalog.jsonb_build_object(
      'authority_mode','AUTHORITATIVE','is_daily',false,'existing_shift_id',v_shift::text,
      'week_ending_date','2026-09-06','invoice_stream','SELF_BILL',
      'source_scope_fingerprint',v_scope_fingerprint,
      'amendment_route','CREATE_REVERSAL_REPLACEMENT',
      'reconciliation_mode','FROZEN_INVOICE_BALANCE',
      'effective_invoice_ids',pg_catalog.jsonb_build_array(v_invoice),
      'effective_invoice_line_ids',pg_catalog.jsonb_build_array(v_invoice_line),
      'B_hours',pg_catalog.jsonb_build_object(
        'hours_day',10,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,'total_hours',10),
      'B_financials',pg_catalog.jsonb_build_object(
        'pay_ex_vat',100,'charge_ex_vat',150,'margin_ex_vat',50),
      'B_standard_schedule_json',v_b_schedule,
      'active_mutable_member_ids','[]'::jsonb,
      'physically_missing_mutable_roles','[]'::jsonb,
      'M_hours',pg_catalog.jsonb_build_object(
        'hours_day',0,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,'total_hours',0),
      'A_schedule_json',v_a_schedule,
      'A_hours',pg_catalog.jsonb_build_object(
        'hours_day',12,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,'total_hours',12),
      'review_policy_basis_kind','IMPORT_AUTHORITATIVE_WEEKLY_V1',
      'review_policy_basis_fingerprint',v_policy_basis_fingerprint,
      'authoritative_evidence_fingerprint',pg_catalog.repeat('a',64),
      'reconciliation_fingerprint',v_reconciliation_fingerprint,
      'intended_authorisation_action','REAUTHORISE',
      'financial_validation_mode','CORRECTION_NEGATIVE_MUST_REVERSE_FROZEN_B_AND_POSITIVE_TSFIN_DEFINES_A'),
    v_evidence_fingerprint,v_actor,'2026-09-04T10:00:00Z'
  );

  v_envelope:=public._import_review_apply_envelope_core_v1(v_import);
  PERFORM pg_temp.h2_p2_assert(
    pg_catalog.jsonb_array_length(v_envelope->'reconciliation_units')=1
      AND v_envelope#>>'{reconciliation_units,0,route}'='CREATE_REVERSAL_REPLACEMENT'
      AND pg_catalog.jsonb_array_length(v_envelope->'correction_units')=1,
    v_source||' actual apply-envelope omitted the canonical protected correction unit');
  v_request_hash:=public._import_review_hash_v1(v_envelope::text);
  v_claim:=public._import_apply_operation_claim_core_v2(
    v_operation,v_import,v_source::public.hr_source_enum,v_import::text||':1',
    v_request_hash,v_actor,v_envelope);
  PERFORM pg_temp.h2_p2_assert(
    v_claim->>'state'='PREPARED'
      AND v_claim#>>'{response_json,correction_operation_contract,schema_version}'='IMPORT_CORRECTION_OPERATION_V2',
    v_source||' operation claim did not freeze the current policy owner');

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.import_review_reconciliation_units_v1(
    action_id text PRIMARY KEY,source_identity text NOT NULL,source_shift_id uuid,
    source_timesheet_id uuid,route text NOT NULL,unit_fingerprint text NOT NULL,
    unit_json jsonb NOT NULL
  ) ON COMMIT DROP;
  TRUNCATE pg_temp.import_review_reconciliation_units_v1;
  INSERT INTO pg_temp.import_review_reconciliation_units_v1(
    action_id,source_identity,source_shift_id,source_timesheet_id,route,unit_fingerprint,unit_json
  ) SELECT u->>'action_id',u->>'source_identity',nullif(u->>'source_shift_id','')::uuid,
      nullif(u->>'source_timesheet_id','')::uuid,u->>'route',u->>'unit_fingerprint',u
    FROM pg_catalog.jsonb_array_elements(v_envelope->'reconciliation_units') u;
  PERFORM pg_catalog.set_config('cloudtms.import_reconciliation_operation_id',v_operation::text,true);
  PERFORM pg_catalog.set_config('cloudtms.import_reconciliation_request_hash',v_request_hash,true);
  PERFORM pg_catalog.set_config('cloudtms.import_reconciliation_unit_fingerprints',
    v_envelope#>>'{reconciliation_units,0,unit_fingerprint}',true);
  UPDATE public.import_review_states
  SET status='APPLYING',state_version=state_version+1,last_operation_id=v_operation
  WHERE import_id=v_import;

  v_prepare:=public.import_review_correction_generation_transition_v1(
    v_import,v_operation,v_request_hash,'PREPARE',v_actor,ARRAY[v_action],'2026-09-04T10:00:00Z');
  -- Each real import apply owns a transaction. The fixture deliberately runs
  -- several isolated import cases inside one outer rollback, so release the
  -- current owner's transaction-local scratch relation between those cases.
  DROP TABLE IF EXISTS pg_temp.tmp_phase3_by_key;
  v_phase3:=CASE WHEN v_source='NHSP'
    THEN public.nhsp_weekly_phase3_apply_adjustment_truth(v_import,ARRAY[v_key],v_actor)
    ELSE public.hr_weekly_phase3_apply_adjustment_truth(v_import,ARRAY[v_key],v_actor) END;
  SELECT unit_json INTO STRICT v_unit
  FROM pg_temp.import_review_reconciliation_units_v1 WHERE action_id=v_action;
  v_reversal:=(v_unit->>'reversal_timesheet_id')::uuid;
  v_replacement:=(v_unit->>'replacement_timesheet_id')::uuid;
  PERFORM pg_temp.h2_p2_assert(
    v_reversal IS NOT NULL AND v_replacement IS NOT NULL
      AND v_reversal IS DISTINCT FROM v_replacement
      AND (SELECT count(*) FROM public.timesheets
        WHERE timesheet_id IN (v_reversal,v_replacement)
          AND parent_timesheet_id=v_root AND is_current AND archived_at_utc IS NULL)=2,
    v_source||' actual phase-3 producer did not create one exact pair');

  SELECT policy_unit->'policy_envelope' INTO STRICT v_policy
  FROM pg_catalog.jsonb_array_elements(
    v_claim#>'{response_json,correction_operation_contract,correction_units}') policy_unit
  WHERE policy_unit->>'action_id'=v_action;

  INSERT INTO public.timesheets_financials(
    id,timesheet_id,timesheet_version,basis,is_current,is_stale,
    worked_start_iso,worked_end_iso,actual_schedule_json,candidate_id,client_id,
    role,pay_method,occupant_key_norm,candidate_assignment,processing_status,
    hours_day,total_hours,pay_day,charge_day,total_pay_ex_vat,total_charge_ex_vat,
    margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue,
    pay_vat_rate_pct_snapshot,policy_snapshot_json,rate_source_refs_json,
    invoice_breakdown_json
  )
  SELECT
    pg_temp.h2_p2_uuid(p_base+CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL' THEN 22 ELSE 23 END),
    t.timesheet_id,t.version,
    CASE WHEN v_source='NHSP' THEN 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum
      ELSE 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum END,
    true,false,
    (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz,
    (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz,t.actual_schedule_json,
    v_candidate,v_client,'RMN',v_channel,'h2 candidate','ASSIGNED','PENDING_AUTH',
    CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL' THEN -10 ELSE 12 END,
    CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL' THEN -10 ELSE 12 END,
    10,15,
    CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL' THEN -100 ELSE 120 END,
    CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL' THEN -150 ELSE 180 END,
    CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL' THEN -50 ELSE 60 END,
    false,false,false,CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL'
      THEN (v_policy#>>'{reversal,tsfin_policy,applied_pay_vat_rate_pct}')::numeric
      ELSE (v_policy#>>'{replacement,tsfin_policy,applied_pay_vat_rate_pct}')::numeric END,
    pg_catalog.jsonb_build_object(
      'correction_financials_policy_envelope',v_policy,
      'correction_financials_policy_envelope_fingerprint',v_policy->>'envelope_fingerprint',
      'correction_leg_fingerprint',CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL'
        THEN v_policy#>>'{reversal,leg_fingerprint}' ELSE v_policy#>>'{replacement,leg_fingerprint}' END,
      'correction_tsfin_policy',CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL'
        THEN v_policy#>'{reversal,tsfin_policy}' ELSE v_policy#>'{replacement,tsfin_policy}' END,
      'correction_tsfin_policy_fingerprint',CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL'
        THEN v_policy#>>'{reversal,tsfin_policy,tsfin_policy_fingerprint}'
        ELSE v_policy#>>'{replacement,tsfin_policy,tsfin_policy_fingerprint}' END,
      'correction_invoice_policy',CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL'
        THEN v_policy#>'{reversal,invoice_policy}' ELSE v_policy#>'{replacement,invoice_policy}' END,
      'correction_invoice_policy_fingerprint',CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL'
        THEN v_policy#>>'{reversal,invoice_policy,invoice_policy_fingerprint}'
        ELSE v_policy#>>'{replacement,invoice_policy,invoice_policy_fingerprint}' END,
      'erni_pct',0,'apply_erni_to','PAYE_ONLY','pay_vat_rate_pct',
      CASE WHEN v_channel='UMBRELLA' THEN 20 ELSE 0 END),
    pg_catalog.jsonb_build_object('h2_fixture_boundary','ASYNCHRONOUS_TSFIN_CARRIER_ONLY'),
    pg_catalog.jsonb_build_object(
      'mode','SEGMENTS',
      'segments',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'segment_id','h2-p2-correction-'||t.timesheet_id::text,
        'segment_key','h2-p2-correction-'||t.timesheet_id::text,
        'segment_stable_key','correction-timesheet:'||t.timesheet_id::text,
        'date','2026-08-31',
        'pay_amount',CASE WHEN t.correction_kind='CHANGED_HOURS_REVERSAL' THEN -100 ELSE 120 END,
        'exclude_from_pay',false,
        'start_utc',t.actual_schedule_json#>>'{0,start_utc}',
        'end_utc',t.actual_schedule_json#>>'{0,end_utc}',
        'break_mins',0))
    )
  FROM public.timesheets t WHERE t.timesheet_id IN (v_reversal,v_replacement);

  SELECT pg_catalog.jsonb_agg(unit_json ORDER BY action_id)
  INTO v_reconciliation_units FROM pg_temp.import_review_reconciliation_units_v1;
  v_complete:=public._import_review_apply_complete_core_v1(
    v_import,v_operation,v_actor,
    pg_catalog.jsonb_build_object(
      'schema_version','H2_P2_CANONICAL_IMPORT_PAIR_V1',
      'affected_timesheet_ids',pg_catalog.jsonb_build_array(v_reversal,v_replacement),
      'reconciliation_units',v_reconciliation_units,
      'operation_bound_correction_action_ids',pg_catalog.jsonb_build_array(v_action),
      'operation_bound_correction_timesheet_ids',pg_catalog.jsonb_build_array(v_reversal,v_replacement),
      'post_commit_email_action_ids','[]'::jsonb),true);
  v_validate:=public.import_review_correction_generation_transition_v1(
    v_import,v_operation,v_request_hash,'VALIDATE',v_actor,ARRAY[v_action],'2026-09-04T10:00:00Z');
  v_authorise:=public.import_review_correction_generation_transition_v1(
    v_import,v_operation,v_request_hash,'AUTHORISE',v_actor,ARRAY[v_action],'2026-09-04T10:00:00Z');
  PERFORM pg_temp.h2_p2_assert(
    v_validate->>'action'='VALIDATE' AND v_authorise->>'action'='AUTHORISE'
      AND (SELECT count(*) FROM public.timesheets t
        JOIN public.timesheets_financials tf ON tf.timesheet_id=t.timesheet_id AND tf.is_current
        JOIN public.contract_weeks cw ON cw.timesheet_id=t.timesheet_id
        WHERE t.timesheet_id IN (v_reversal,v_replacement)
          AND t.authorised_at_server IS NOT NULL AND tf.authorised_at_utc IS NOT NULL
          AND cw.status='AUTHORISED')=2,
    v_source||' actual correction lifecycle did not atomically authorise the pair');

  RETURN pg_catalog.jsonb_build_object(
    'canonical_route','_import_review_apply_envelope_core_v1>'||
      CASE WHEN v_source='NHSP' THEN 'nhsp_weekly_phase3_apply_adjustment_truth'
        ELSE 'hr_weekly_phase3_apply_adjustment_truth' END||
      '>import_review_correction_generation_transition_v1',
    'source_system',v_source,'pay_channel',v_channel,'actor_id',v_actor::text,
    'candidate_id',v_candidate::text,'client_id',v_client::text,'contract_id',v_contract::text,
    'import_id',v_import::text,'operation_id',v_operation::text,
    'root_timesheet_id',v_root::text,'reversal_timesheet_id',v_reversal::text,
    'replacement_timesheet_id',v_replacement::text,'correction_shape','REVERSAL_REPLACEMENT',
    'request_hash',v_request_hash,'policy_envelope_fingerprint',v_policy->>'envelope_fingerprint',
    'prepare',v_prepare,'phase3',v_phase3,'complete',v_complete,
    'validate',v_validate,'authorise',v_authorise);
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_p2_v8_transport(
  p_base integer,
  p_session_id uuid,
  p_seed jsonb,
  p_residual jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
DECLARE
  v_operation uuid:=pg_temp.h2_p2_uuid(p_base+40);
  v_certificate uuid:=pg_temp.h2_p2_uuid(p_base+41);
  v_scope uuid:=pg_temp.h2_p2_uuid(p_base+42);
  v_preview uuid:=pg_temp.h2_p2_uuid(p_base+43);
  v_candidate uuid:=(p_seed->>'candidate_id')::uuid;
  v_timesheet uuid:=(p_residual->>'latest_positive_timesheet_id')::uuid;
  v_channel text:=p_residual->>'target_pay_method';
  v_component jsonb:=p_residual#>'{components,0}';
  v_amount numeric:=(p_residual->>'total_target_outstanding_ex_vat')::numeric;
  v_amount_text text:=pg_catalog.to_char(v_amount,'FM999999999999990.00');
  v_session_signature text:=pg_temp.h2_p2_sha256(pg_catalog.jsonb_build_object(
    'session_id',p_session_id::text));
  v_scope_digest text:=pg_catalog.repeat(substr(pg_catalog.md5('scope-'||p_base),1,1),64);
  v_identity_digest text:=pg_temp.h2_p2_sha256(pg_catalog.jsonb_build_object(
    'candidate_id',p_seed->>'candidate_id','root_timesheet_id',p_seed->>'root_timesheet_id',
    'ordered_member_timesheet_ids',p_residual->'ordered_member_timesheet_ids',
    'component_key_type',v_component->>'component_key_type',
    'component_key_value',v_component->>'component_key_value'));
  v_line jsonb;
  v_payload_digest text;
  v_first jsonb;
  v_second jsonb;
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM public.banking_pay_workbench_sessions WHERE id=p_session_id
  ) THEN
    PERFORM pg_temp.h2_seed_workbench_context_v8(
      (p_seed->>'actor_id')::uuid,p_session_id,pg_temp.h2_p2_uuid(3),
      v_session_signature,1,1,1);
  END IF;
  v_line:=pg_catalog.jsonb_build_object(
    'line_type','SEGMENT_DELTA','timesheet_id',v_timesheet::text,
    'candidate_id',v_candidate::text,'pay_channel',v_channel,
    'amount_ex_vat',v_amount,'amount_inc_vat',
      (v_component->>'effective_source_outstanding_inc_vat')::numeric,
    'key_type',v_component->>'component_key_type',
    'key_value',v_component->>'component_key_value',
    'canonical_correction_key',v_component->>'canonical_correction_key',
    'correction_identity_version',p_residual->>'correction_identity_version',
    'correction_root_id',p_residual->>'root_timesheet_id',
    'ordered_member_timesheet_ids',p_residual->'ordered_member_timesheet_ids',
    'component_lineage_fingerprint',v_component->>'component_lineage_fingerprint',
    'chain_fingerprint',p_residual->>'chain_fingerprint',
    'source_basis_fingerprint',v_component->>'source_basis_fingerprint',
    'correction_financials_policy_envelope_fingerprint',
      p_residual->>'correction_financials_policy_envelope_fingerprint',
    'workbench_settled_certificate_binding_v8',pg_catalog.jsonb_build_object(
      'binding_contract_version','WORKBENCH_SETTLED_CERTIFICATE_BINDING_V8',
      'certificate_uuid',v_certificate::text,'constituent_ordinal',0,
      'source_identity_digest_sha256',v_identity_digest)
  );
  v_payload_digest:=pg_temp.h2_p2_sha256(v_line);

  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,idempotency_key,actor_user_id,
    workbench_session_id,input_json
  ) VALUES(
    v_operation,'DRAFT_CREATE','RUNNING','SEED_CANDIDATE_SCOPE',
    'h2-p2-v8-'||p_base,(p_seed->>'actor_id')::uuid,p_session_id,'{}'::jsonb
  );
  PERFORM pg_temp.h2_seed_certificate_v8(
    v_certificate,p_session_id,(p_seed->>'actor_id')::uuid,
    pg_temp.h2_p2_uuid(3),v_session_signature,1,1,1,1,1,1,
    v_amount_text,pg_temp.h2_p2_sha256(pg_catalog.jsonb_build_object(
      'certificate_uuid',v_certificate::text)));
  PERFORM pg_temp.h2_seed_certificate_entry_v8(
    v_certificate,p_session_id,0,p_base,v_candidate,v_preview,v_channel,
    v_amount_text,v_identity_digest,1);
  UPDATE public.banking_pay_workbench_preview_rows
  SET row_key='correction:'||p_base,
      row_json=v_line,
      timesheet_id=v_timesheet,
      key_type=v_component->>'component_key_type',
      key_value=v_component->>'component_key_value'
  WHERE id=v_preview;
  UPDATE private.banking_pay_workbench_settled_certificate_entries_v8
  SET row_key='correction:'||p_base,
      source_kind='TIMESHEET_CORRECTION_CHAIN',
      source_row_key=v_component->>'canonical_correction_key',
      timesheet_id=v_timesheet,
      economic_key_timesheet_id=v_timesheet,
      semantic_kind='WORKED_TIME',
      economic_key_type=v_component->>'component_key_type',
      economic_key_value=v_component->>'component_key_value',
      canonical_amount_ex_vat=v_amount_text,
      source_identity_digest_sha256=v_identity_digest,
      constituent_digest_sha256=v_identity_digest
  WHERE certificate_uuid=v_certificate AND constituent_ordinal=0;
  INSERT INTO private.banking_pay_workbench_settled_certificate_partitions_v8(
    certificate_uuid,partition_ordinal,candidate_id,resolved_pay_channel,
    constituent_count,canonical_amount_ex_vat_total,partition_digest_sha256
  ) VALUES(v_certificate,0,v_candidate,v_channel,1,v_amount_text,v_scope_digest);
  INSERT INTO private.banking_pay_workbench_settled_certificate_partition_members_v8(
    certificate_uuid,stream_ordinal,partition_ordinal,member_ordinal,
    constituent_ordinal,stable_identity_digest_sha256
  ) VALUES(v_certificate,0,0,0,0,v_identity_digest);
  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id,certificate_uuid,pay_channel_scope,constituent_count,partition_count,
    canonical_amount_ex_vat_total,selected_constituents_digest_sha256,
    selected_partitions_digest_sha256,manifest_digest_sha256,freeze_state,frozen_at_utc
  ) VALUES(
    v_operation,v_certificate,v_channel,1,1,v_amount_text,
    v_identity_digest,v_scope_digest,pg_catalog.repeat('c',64),'FROZEN',pg_catalog.clock_timestamp());
  INSERT INTO private.banking_pay_draft_frozen_constituent_refs_v8(
    operation_id,certificate_uuid,constituent_ordinal,staged_page_sequence
  ) VALUES(v_operation,v_certificate,0,0);
  INSERT INTO private.banking_pay_draft_frozen_constituent_payloads_v8(
    operation_id,constituent_ordinal,certificate_uuid,preview_row_id,candidate_id,
    resolved_pay_channel,timesheet_id,row_key,row_ordinal,payload_json,payload_digest_sha256
  ) VALUES(
    v_operation,0,v_certificate,v_preview,v_candidate,v_channel,v_timesheet,
    'correction:'||p_base,0,v_line,v_payload_digest);
  INSERT INTO private.banking_pay_draft_frozen_candidate_scopes_v8(
    operation_id,candidate_scope_ordinal,certificate_uuid,partition_ordinal,
    candidate_id,resolved_pay_channel,constituent_count,canonical_amount_ex_vat_total,
    scope_digest_sha256,scope_state
  ) VALUES(v_operation,0,v_certificate,0,v_candidate,v_channel,1,v_amount_text,v_scope_digest,'FROZEN');
  INSERT INTO private.banking_pay_draft_frozen_candidate_scope_members_v8(
    operation_id,candidate_scope_ordinal,member_ordinal,certificate_uuid,
    partition_ordinal,constituent_ordinal,stable_identity_digest_sha256
  ) VALUES(v_operation,0,0,v_certificate,0,0,v_identity_digest);
  INSERT INTO public.banking_pay_operation_candidate_scope(
    id,operation_id,workbench_session_id,source_snapshot_run_id,
    source_session_version,candidate_id,pay_channel,scope_hash,status
  ) VALUES(v_scope,v_operation,p_session_id,pg_temp.h2_p2_uuid(3),1,
    v_candidate,v_channel,v_scope_digest,'DRAFTED');

  SELECT COALESCE(pg_catalog.jsonb_agg(row.value ORDER BY row.ordinality),'[]'::jsonb)
  INTO v_first
  FROM private.pay_workbench_draft_scope_line_rows_v8(v_scope,'[]'::jsonb,'[]'::jsonb) AS row;
  SELECT COALESCE(pg_catalog.jsonb_agg(row.value ORDER BY row.ordinality),'[]'::jsonb)
  INTO v_second
  FROM private.pay_workbench_draft_scope_line_rows_v8(v_scope,'[]'::jsonb,'[]'::jsonb) AS row;

  PERFORM pg_temp.h2_p2_assert(v_first=pg_catalog.jsonb_build_array(v_line),
    'V8 row-backed payload differs from certified correction line');
  PERFORM pg_temp.h2_p2_assert(v_second=v_first,
    'V8 response-loss replay changed the correction payload');
  PERFORM pg_temp.h2_p2_assert((SELECT count(*) FROM private.banking_pay_draft_frozen_constituent_payloads_v8 WHERE operation_id=v_operation)=1,
    'V8 response-loss replay duplicated payload state');
  RETURN pg_catalog.jsonb_build_object(
    'operation_id_role','DRAFT_CREATE_OPERATION','candidate_scope_id_role','CANDIDATE_SCOPE',
    'payload_digest_sha256',v_payload_digest,'row_backed_payload',v_line,
    'row_backed_replay_equal',true,'payload_row_count',1);
END;
$function$;

-- Shared V8 fixture context. Each scenario has a separate Candidate and source
-- chain; the single session is only a container for exact certified rows.
SELECT pg_temp.h2_seed_workbench_context_v8(
  pg_temp.h2_p2_uuid(1),pg_temp.h2_p2_uuid(2),pg_temp.h2_p2_uuid(3),
  pg_catalog.repeat('a',64),1,1,1);
INSERT INTO public.candidates(id,tms_ref,display_name,pay_method,active)
VALUES(pg_temp.h2_p2_uuid(4),'H2-P2-UNRELATED','H2 P2 unrelated Candidate','PAYE',true);
INSERT INTO pg_temp.h2_p2_unrelated_candidate_baseline(candidate_id,row_digest_sha256)
SELECT id,pg_temp.h2_p2_sha256(pg_catalog.to_jsonb(candidate_row))
FROM public.candidates AS candidate_row
WHERE id=pg_temp.h2_p2_uuid(4);

-- 1. PAYE reversal + replacement.
DO $case$
DECLARE v_seed jsonb; v_chain jsonb; v_transition jsonb; v_preview jsonb; v_residual jsonb; v_transport jsonb;
BEGIN
  v_seed:=pg_temp.h2_p2_seed_canonical_import_pair(10000,'HEALTHROSTER','PAYE');
  v_chain:=public.timesheet_correction_chain_scope_v1((v_seed->>'reversal_timesheet_id')::uuid,false,32,100);
  v_transition:=public.timesheet_correction_pair_transition_v1(
    (v_seed->>'reversal_timesheet_id')::uuid,'AUTHORISE',(v_seed->>'actor_id')::uuid,
    NULL,v_chain->>'chain_fingerprint',false,100);
  v_preview:=public.timesheet_correction_pair_lifecycle_preview_v1(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('timesheet_id',v_seed->>'reversal_timesheet_id')),
    'AUTHORISE',(v_seed->>'actor_id')::uuid,100);
  v_residual:=public.pay_correction_chain_residual_v1(
    (v_seed->>'reversal_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,'PAYE',NULL,NULL,100);
  PERFORM pg_temp.h2_p2_assert((v_chain->>'valid')::boolean AND (v_chain->>'member_count')::integer=3,
    'PAYE pair chain is not root plus two exact legs');
  PERFORM pg_temp.h2_p2_assert((v_transition->>'action_ready')::boolean
    AND (v_transition->>'expected_member_count')::integer=2
    AND (v_preview->>'correction_pair_count')::integer=1,
    'PAYE pair lifecycle plan is not atomic and ready');
  PERFORM pg_temp.h2_p2_assert((v_residual->>'draftable')::boolean
    AND (v_residual->>'component_count')::integer=1
    AND v_residual#>>'{components,0,component_key_type}'='TS_DAY'
    AND (v_residual->>'total_target_outstanding_ex_vat')::numeric=20,
    'PAYE pair residual is not exact TS_DAY 20.00');
  v_transport:=pg_temp.h2_p2_v8_transport(10000,pg_temp.h2_p2_uuid(10002),v_seed,v_residual);
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_reversal_replacement_paye','PASS_CURRENT_V8',
    pg_catalog.jsonb_build_object('seed',v_seed,'chain',v_chain,'transition',v_transition,
      'preview',v_preview,'residual',v_residual,'v8_transport',v_transport));
END;
$case$;

-- 2. Umbrella reversal + replacement.
DO $case$
DECLARE v_seed jsonb; v_chain jsonb; v_residual jsonb; v_transport jsonb;
BEGIN
  v_seed:=pg_temp.h2_p2_seed_canonical_import_pair(20000,'NHSP','UMBRELLA');
  v_chain:=public.timesheet_correction_chain_scope_v1((v_seed->>'replacement_timesheet_id')::uuid,false,32,100);
  v_residual:=public.pay_correction_chain_residual_v1(
    (v_seed->>'replacement_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,'UMBRELLA',NULL,NULL,100);
  PERFORM pg_temp.h2_p2_assert((v_chain->>'valid')::boolean
    AND v_chain#>>'{requested_correction_unit,correction_shape}'='REVERSAL_REPLACEMENT',
    'Umbrella pair shape is not exact');
  PERFORM pg_temp.h2_p2_assert((v_residual->>'draftable')::boolean
    AND (v_residual->>'total_target_outstanding_ex_vat')::numeric=20
    AND v_residual#>>'{correction_financials_policy_envelope,correction_units,0,policy_envelope,reversal,tsfin_policy,source_pay_vat_rate_pct}'='20.00'
    AND v_residual#>>'{correction_financials_policy_envelope,correction_units,0,policy_envelope,reversal,tsfin_policy,applied_pay_vat_rate_pct}'='0'
    AND v_residual#>>'{correction_financials_policy_envelope,correction_units,0,policy_envelope,reversal,invoice_policy,applied_vat_rate_pct}'='20.00',
    'Umbrella pair VAT/ex-VAT owner evidence changed');
  v_transport:=pg_temp.h2_p2_v8_transport(20000,pg_temp.h2_p2_uuid(20002),v_seed,v_residual);
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_reversal_replacement_umbrella','PASS_CURRENT_V8',
    pg_catalog.jsonb_build_object('seed',v_seed,'chain',v_chain,
      'residual',v_residual,'v8_transport',v_transport));
END;
$case$;

-- 3. PAYE source to Umbrella target: first unresolved, then exactly resolved.
DO $case$
DECLARE
  v_seed jsonb; v_unresolved jsonb; v_resolved jsonb; v_component jsonb;
  v_bucket jsonb; v_buckets jsonb; v_payload jsonb;
BEGIN
  v_seed:=pg_temp.h2_p2_seed_chain(30000,'PAYE','REVERSAL_REPLACEMENT');
  PERFORM pg_temp.h2_p2_authorise_chain(v_seed);
  v_unresolved:=public.pay_correction_chain_residual_v1(
    (v_seed->>'replacement_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,
    'UMBRELLA',pg_temp.h2_p2_uuid(2),NULL,100);
  PERFORM pg_temp.h2_p2_assert((v_unresolved->>'draftable')::boolean=false
    AND v_unresolved->>'block_code'='CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED'
    AND (v_unresolved->>'unresolved_count')::integer=1,
    'cross-channel correction did not first fail closed');
  v_component:=v_unresolved#>'{components,0}';
  v_bucket:=pg_catalog.jsonb_build_object(
    'target_pay_method','UMBRELLA',
    'target_amount_ex_vat',pg_catalog.abs((v_component->>'effective_source_outstanding_ex_vat')::numeric),
    'resolution_economic_fingerprint',v_component->>'resolution_economic_fingerprint',
    'saved_resolution_payload_json',pg_catalog.jsonb_build_object(
      'target_pay_method','UMBRELLA',
      'resolution_economic_fingerprint',v_component->>'resolution_economic_fingerprint'));
  v_buckets:=pg_catalog.jsonb_build_array(v_bucket);
  v_payload:=pg_catalog.jsonb_build_object(
    'correction_resolution_aggregate_version','CORRECTION_CHAIN_BUCKET_SET_V1',
    'bucket_resolutions',v_buckets,'physical_decision_count',1,
    'distinct_projected_target_count',1,
    'physical_decision_set_digest',pg_temp.h2_p2_sha256(v_buckets),
    'correction_financials_policy_envelope_fingerprint',
      v_unresolved->>'correction_financials_policy_envelope_fingerprint',
    'resolution_economic_fingerprint',v_component->>'resolution_economic_fingerprint',
    'target_pay_method','UMBRELLA',
    'target_amount_ex_vat',pg_catalog.abs((v_component->>'effective_source_outstanding_ex_vat')::numeric),
    'is_resolution_stale',false);
  INSERT INTO public.banking_pay_workbench_session_case_resolutions(
    id,session_id,candidate_id,case_key,resolution_family,resolution_identity_key,
    timesheet_id,source_basis_fingerprint,source_family_key,bucket_code,
    component_key_type,component_key_value,payload_json,
    resolution_origin_session_id,resolution_origin_pay_date,
    resolution_origin_source_basis_fingerprint
  ) VALUES(
    pg_temp.h2_p2_uuid(30030),pg_temp.h2_p2_uuid(2),(v_seed->>'candidate_id')::uuid,
    'h2-p2-cross-channel','CORRECTION_CHAIN','h2-p2-cross-channel-ts-day',
    (v_seed->>'replacement_timesheet_id')::uuid,v_component->>'source_basis_fingerprint',
    v_unresolved->>'source_family_key','TS_DAY:2026-08-31','TS_DAY','2026-08-31',
    v_payload,pg_temp.h2_p2_uuid(2),'2026-09-04',v_component->>'source_basis_fingerprint');
  v_resolved:=public.pay_correction_chain_residual_v1(
    (v_seed->>'replacement_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,
    'UMBRELLA',pg_temp.h2_p2_uuid(2),NULL,100);
  PERFORM pg_temp.h2_p2_assert((v_resolved->>'draftable')::boolean
    AND (v_resolved->>'unresolved_count')::integer=0
    AND v_resolved#>>'{components,0,resolution_complete}'='true'
    AND v_resolved#>>'{components,0,source_basis_matches}'='true'
    AND v_resolved#>>'{components,0,historical_anchor_matches}'='true',
    'exact saved cross-channel resolution was not accepted');
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_cross_channel_resolution','PASS_CURRENT_V8',
    pg_catalog.jsonb_build_object('seed',v_seed,'unresolved',v_unresolved,'resolved',v_resolved));
END;
$case$;

-- 4. Broken and duplicate legs are rejected before Draft state.
DO $case$
DECLARE
  v_missing jsonb;
  v_positive jsonb;
  v_duplicate jsonb;
  v_ordinary jsonb;
  v_unrelated jsonb;
  v_unrelated_timesheet uuid:=pg_temp.h2_p2_uuid(40404);
  v_chain jsonb;
  v_duplicate_rejected boolean:=false;
  v_ordinary_overlap_rejected boolean:=false;
  v_unrelated_overlap_rejected boolean:=false;
  v_constraint_name text;
BEGIN
  v_missing:=pg_temp.h2_p2_seed_chain(40000,'PAYE','REVERSAL_REPLACEMENT');
  DELETE FROM public.timesheets_financials WHERE timesheet_id=(v_missing->>'replacement_timesheet_id')::uuid;
  DELETE FROM public.timesheets WHERE timesheet_id=(v_missing->>'replacement_timesheet_id')::uuid;
  v_chain:=public.timesheet_correction_chain_scope_v1((v_missing->>'reversal_timesheet_id')::uuid,false,32,100);
  PERFORM pg_temp.h2_p2_assert((v_chain->>'valid')::boolean=false
    AND v_chain->'errors' @> '[{"code":"CORRECTION_UNIT_INVALID"}]'::jsonb,
    'missing replacement was not typed invalid');

  -- The opposite partial deletion is equally invalid: a replacement without
  -- its exact reversal can never become a positive-only correction unit.
  v_positive:=pg_temp.h2_p2_seed_chain(40050,'PAYE','REVERSAL_REPLACEMENT');
  DELETE FROM public.timesheets_financials
  WHERE timesheet_id=(v_positive->>'reversal_timesheet_id')::uuid;
  DELETE FROM public.timesheets
  WHERE timesheet_id=(v_positive->>'reversal_timesheet_id')::uuid;
  v_chain:=public.timesheet_correction_chain_scope_v1(
    (v_positive->>'replacement_timesheet_id')::uuid,false,32,100);
  PERFORM pg_temp.h2_p2_assert((v_chain->>'valid')::boolean=false
    AND v_chain->'errors' @> '[{"code":"CORRECTION_UNIT_INVALID"}]'::jsonb,
    'positive-only replacement was not typed invalid');
  v_duplicate:=pg_temp.h2_p2_seed_chain(40100,'PAYE','REVERSAL_REPLACEMENT');
  BEGIN
    PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
    INSERT INTO public.timesheets(
      timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
      week_ending_date,contract_id,sheet_scope,is_current,version,is_adjustment,
      parent_timesheet_id,correction_id,correction_kind,adjustment_origin,candidate_hint_text
    ) SELECT pg_temp.h2_p2_uuid(40104),'h2-p2-duplicate-40100',occupant_key_norm,hospital_norm,
      ward_norm,job_title_norm,week_ending_date,contract_id,sheet_scope,true,1,true,
      parent_timesheet_id,correction_id,correction_kind,adjustment_origin,candidate_hint_text
    FROM public.timesheets WHERE timesheet_id=(v_duplicate->>'replacement_timesheet_id')::uuid;
  EXCEPTION WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS v_constraint_name=CONSTRAINT_NAME;
    v_duplicate_rejected:=v_constraint_name='uq_timesheets_correction_id_kind';
  END;
  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','',true);
  PERFORM pg_temp.h2_p2_assert(v_duplicate_rejected,
    'duplicate replacement did not fail at the established correction identity constraint');

  -- The correction-family compatibility must not weaken the ordinary overlap
  -- rule.  A row that is not a certified correction member still conflicts
  -- with the existing accepted root Timesheet.
  v_ordinary:=pg_temp.h2_p2_seed_chain(40300,'PAYE','REVERSAL_REPLACEMENT');
  BEGIN
    PERFORM private._timesheet_cross_record_overlap_assert_v1(
      (v_ordinary->>'candidate_id')::uuid,'{}'::jsonb,
      '[{"date":"2026-08-31","start":"08:00","end":"18:00","break_minutes":0}]'::jsonb,
      NULL,NULL,NULL,pg_temp.h2_p2_uuid(40399),'h2-p2-unrelated-booking-40399');
  EXCEPTION WHEN SQLSTATE 'PT409' THEN
    v_ordinary_overlap_rejected:=SQLERRM='TIMESHEET_WORK_INTERVAL_OVERLAP';
  END;
  PERFORM pg_temp.h2_p2_assert(v_ordinary_overlap_rejected,
    'ordinary overlapping Timesheet was incorrectly admitted');

  -- Even an exact valid correction family must still conflict with a third
  -- accepted Timesheet that is not in its certified root/member set.
  v_unrelated:=pg_temp.h2_p2_seed_chain(40400,'PAYE','REVERSAL_REPLACEMENT');
  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,contract_id,sheet_scope,worked_start_iso,worked_end_iso,
    actual_schedule_json,is_current,version,is_adjustment
  ) SELECT
    v_unrelated_timesheet,'h2-p2-third-unrelated-40400',occupant_key_norm,
    hospital_norm,ward_norm,job_title_norm,week_ending_date,contract_id,sheet_scope,
    worked_start_iso,worked_end_iso,actual_schedule_json,true,1,false
  FROM public.timesheets
  WHERE timesheet_id=(v_unrelated->>'root_timesheet_id')::uuid;
  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','',true);
  INSERT INTO public.timesheets_financials(
    id,timesheet_id,timesheet_version,basis,is_current,is_stale,
    worked_start_iso,worked_end_iso,actual_schedule_json,candidate_id,client_id,
    role,pay_method,occupant_key_norm,candidate_assignment,processing_status,
    hours_day,pay_day,charge_day,total_hours,total_pay_ex_vat,total_charge_ex_vat,
    margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue,
    pay_vat_rate_pct_snapshot,policy_snapshot_json,authorised_at_utc
  ) SELECT
    pg_temp.h2_p2_uuid(40424),v_unrelated_timesheet,1,basis,true,false,
    worked_start_iso,worked_end_iso,actual_schedule_json,candidate_id,client_id,
    role,pay_method,occupant_key_norm,candidate_assignment,processing_status,
    hours_day,pay_day,charge_day,total_hours,total_pay_ex_vat,total_charge_ex_vat,
    margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue,
    pay_vat_rate_pct_snapshot,policy_snapshot_json,'2026-09-01 10:00+01'
  FROM public.timesheets_financials
  WHERE timesheet_id=(v_unrelated->>'root_timesheet_id')::uuid AND is_current=true;
  BEGIN
    PERFORM private._timesheet_cross_record_overlap_assert_v1(
      (v_unrelated->>'candidate_id')::uuid,'{}'::jsonb,
      '[{"date":"2026-08-31","start":"08:00","end":"18:00","break_minutes":0}]'::jsonb,
      NULL,NULL,NULL,(v_unrelated->>'reversal_timesheet_id')::uuid,
      'h2-p2-reversal-40400');
  EXCEPTION WHEN SQLSTATE 'PT409' THEN
    v_unrelated_overlap_rejected:=SQLERRM='TIMESHEET_WORK_INTERVAL_OVERLAP';
  END;
  PERFORM pg_temp.h2_p2_assert(v_unrelated_overlap_rejected,
    'unrelated third overlapping Timesheet was incorrectly hidden by family exclusion');

  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_broken_or_duplicate_leg','PASS_TYPED_ZERO_WRITE',
    pg_catalog.jsonb_build_object('missing_replacement_code','CORRECTION_UNIT_INVALID',
      'positive_only_code','CORRECTION_UNIT_INVALID',
      'duplicate_replacement_code','uq_timesheets_correction_id_kind',
      'ordinary_overlap_code','TIMESHEET_WORK_INTERVAL_OVERLAP',
      'unrelated_third_overlap_code','TIMESHEET_WORK_INTERVAL_OVERLAP'));
END;
$case$;

-- 5. A stale chain fingerprint and a stale stored leg policy both fail closed.
DO $case$
DECLARE v_seed jsonb; v_chain jsonb; v_out jsonb;
BEGIN
  v_seed:=pg_temp.h2_p2_seed_chain(50000,'PAYE','REVERSAL_REPLACEMENT');
  v_chain:=public.timesheet_correction_chain_scope_v1((v_seed->>'reversal_timesheet_id')::uuid,false,32,100);
  BEGIN
    PERFORM public.timesheet_correction_pair_transition_v1(
      (v_seed->>'reversal_timesheet_id')::uuid,'AUTHORISE',(v_seed->>'actor_id')::uuid,
      NULL,pg_catalog.repeat('f',64),false,100);
    RAISE EXCEPTION 'EXPECTED_STALE_CHAIN_REJECTION';
  EXCEPTION WHEN SQLSTATE '40001' THEN
    IF SQLERRM<>'CORRECTION_TRANSITION_CHAIN_STALE' THEN RAISE; END IF;
  END;
  UPDATE public.timesheets_financials
  SET policy_snapshot_json=pg_catalog.jsonb_set(
    policy_snapshot_json,'{correction_leg_fingerprint}',pg_catalog.to_jsonb(pg_catalog.repeat('0',64)),false)
  WHERE timesheet_id=(v_seed->>'replacement_timesheet_id')::uuid;
  v_out:=public.timesheet_correction_pair_transition_v1(
    (v_seed->>'reversal_timesheet_id')::uuid,'AUTHORISE',(v_seed->>'actor_id')::uuid,
    NULL,NULL,false,100);
  PERFORM pg_temp.h2_p2_assert((v_out->>'valid')::boolean=false
    AND v_out->'errors' @> '[{"code":"CORRECTION_LEG_POLICY_NOT_FROZEN"}]'::jsonb,
    'stale stored leg fingerprint was not typed invalid');
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_stale_fingerprint','PASS_TYPED_ZERO_WRITE',
    pg_catalog.jsonb_build_object('chain_error','CORRECTION_TRANSITION_CHAIN_STALE',
      'leg_error','CORRECTION_LEG_POLICY_NOT_FROZEN'));
END;
$case$;

-- 6. Candidate and client identities may not differ between pair members.
DO $case$
DECLARE v_seed jsonb; v_other_candidate uuid:=pg_temp.h2_p2_uuid(60090); v_other_client uuid:=pg_temp.h2_p2_uuid(60091);
BEGIN
  v_seed:=pg_temp.h2_p2_seed_chain(60000,'PAYE','REVERSAL_REPLACEMENT');
  INSERT INTO public.candidates(id,pay_method) VALUES(v_other_candidate,'PAYE');
  UPDATE public.timesheets_financials SET candidate_id=v_other_candidate
  WHERE timesheet_id=(v_seed->>'replacement_timesheet_id')::uuid;
  BEGIN
    PERFORM public.pay_correction_chain_residual_v1(
      (v_seed->>'reversal_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,'PAYE',NULL,NULL,100);
    RAISE EXCEPTION 'EXPECTED_MIXED_CANDIDATE_REJECTION';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM<>'CORRECTION_RESIDUAL_CHAIN_IDENTITY_MISMATCH' THEN RAISE; END IF;
  END;
  UPDATE public.timesheets_financials SET candidate_id=(v_seed->>'candidate_id')::uuid
  WHERE timesheet_id=(v_seed->>'replacement_timesheet_id')::uuid;
  INSERT INTO public.clients(id,name) VALUES(v_other_client,'H2 P2 other client');
  UPDATE public.timesheets_financials SET client_id=v_other_client
  WHERE timesheet_id=(v_seed->>'replacement_timesheet_id')::uuid;
  BEGIN
    PERFORM public.pay_correction_chain_residual_v1(
      (v_seed->>'reversal_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,'PAYE',NULL,NULL,100);
    RAISE EXCEPTION 'EXPECTED_MIXED_CLIENT_REJECTION';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM<>'CORRECTION_RESIDUAL_CHAIN_IDENTITY_MISMATCH' THEN RAISE; END IF;
  END;
  -- This is an expected negative inside a larger rollback-only matrix.  Restore
  -- the deliberately corrupted identity after proving the typed rejection so a
  -- later summary-view scan cannot observe invalid fixture state.
  UPDATE public.timesheets_financials
  SET client_id=(v_seed->>'client_id')::uuid
  WHERE timesheet_id=(v_seed->>'replacement_timesheet_id')::uuid;
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_mixed_candidate_or_client','PASS_TYPED_ZERO_WRITE',
    pg_catalog.jsonb_build_object('candidate_error','CORRECTION_RESIDUAL_CHAIN_IDENTITY_MISMATCH',
      'client_error','CORRECTION_RESIDUAL_CHAIN_IDENTITY_MISMATCH'));
END;
$case$;

-- 7. Realistic paid and invoice-locked lifecycle states are blocked by the
-- unchanged atomic bulk owner and propagated to both legs.
DO $case$
DECLARE v_paid jsonb; v_invoiced jsonb; v_result jsonb; v_invoice uuid:=pg_temp.h2_p2_uuid(70100);
BEGIN
  v_paid:=pg_temp.h2_p2_seed_chain(70000,'PAYE','REVERSAL_REPLACEMENT');
  UPDATE public.timesheets_financials
  SET paid_at_utc='2026-09-02 12:00+01',processing_status='READY_FOR_INVOICE'
  WHERE timesheet_id=(v_paid->>'reversal_timesheet_id')::uuid;
  v_result:=public.timesheet_authorise_bulk_atomic(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('timesheet_id',v_paid->>'reversal_timesheet_id')),
    (v_paid->>'actor_id')::uuid,'2026-09-04 10:00+01');
  PERFORM pg_temp.h2_p2_assert((v_result->>'all_success')::boolean=false
    AND (v_result->>'success_count')::integer=0
    AND (v_result->>'failure_count')::integer=2
    AND v_result->'failed_items' @> '[{"error_code":"CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED"}]'::jsonb,
    'paid pair was not atomically blocked');
  v_invoiced:=pg_temp.h2_p2_seed_chain(70200,'PAYE','REVERSAL_REPLACEMENT');
  INSERT INTO public.invoices(id,client_id,type,status,issued_at_utc,issue_state)
  VALUES(v_invoice,(v_invoiced->>'client_id')::uuid,'INVOICE','ISSUED','2026-09-02 12:00+01','ISSUED');
  UPDATE public.timesheets_financials
  SET locked_by_invoice_id=v_invoice,locked_at_utc='2026-09-02 12:00+01'
  WHERE timesheet_id=(v_invoiced->>'replacement_timesheet_id')::uuid;
  v_result:=public.timesheet_authorise_bulk_atomic(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('timesheet_id',v_invoiced->>'reversal_timesheet_id')),
    (v_invoiced->>'actor_id')::uuid,'2026-09-04 10:00+01');
  PERFORM pg_temp.h2_p2_assert((v_result->>'all_success')::boolean=false
    AND (v_result->>'success_count')::integer=0
    AND (v_result->>'failure_count')::integer=2
    AND v_result->'failed_items' @> '[{"error_code":"CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED"}]'::jsonb,
    'invoice-locked pair was not atomically blocked');
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_paid_or_invoiced_conflict','PASS_TYPED_ZERO_WRITE',
    pg_catalog.jsonb_build_object('paid_error','CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED',
      'invoiced_error','CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED','pair_failure_count',2));
END;
$case$;

-- 8. Repeating the same read-only atomic transition request is identical.
DO $case$
DECLARE v_seed jsonb; v_chain jsonb; v_first jsonb; v_second jsonb;
BEGIN
  v_seed:=pg_temp.h2_p2_seed_chain(80000,'PAYE','REVERSAL_REPLACEMENT');
  v_chain:=public.timesheet_correction_chain_scope_v1((v_seed->>'reversal_timesheet_id')::uuid,false,32,100);
  v_first:=public.timesheet_correction_pair_transition_v1(
    (v_seed->>'reversal_timesheet_id')::uuid,'AUTHORISE',(v_seed->>'actor_id')::uuid,
    NULL,v_chain->>'chain_fingerprint',false,100);
  v_second:=public.timesheet_correction_pair_transition_v1(
    (v_seed->>'reversal_timesheet_id')::uuid,'AUTHORISE',(v_seed->>'actor_id')::uuid,
    NULL,v_chain->>'chain_fingerprint',false,100);
  PERFORM pg_temp.h2_p2_assert(v_second=v_first AND (v_first->>'action_ready')::boolean,
    'transition replay changed plan or readiness');
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_transition_replay','PASS_CURRENT_V8',
    pg_catalog.jsonb_build_object('transition_digest_sha256',pg_temp.h2_p2_sha256(v_first),
      'replay_equal',true));
END;
$case$;

-- 9. Frozen row-backed response-loss replay is exact and non-duplicating.
DO $case$
DECLARE v_seed jsonb; v_authorise jsonb; v_residual jsonb; v_transport jsonb;
BEGIN
  v_seed:=pg_temp.h2_p2_seed_chain(90000,'PAYE','REVERSAL_REPLACEMENT');
  v_authorise:=pg_temp.h2_p2_authorise_chain(v_seed);
  v_residual:=public.pay_correction_chain_residual_v1(
    (v_seed->>'replacement_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,'PAYE',NULL,NULL,100);
  v_transport:=pg_temp.h2_p2_v8_transport(90000,pg_temp.h2_p2_uuid(90002),v_seed,v_residual);
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_draft_response_loss_replay','PASS_CURRENT_V8',
    v_transport||pg_catalog.jsonb_build_object('authorise',v_authorise));
END;
$case$;

-- 10-11. Reversal-only preserves one physical leg and a signed residual. It is
-- not directly reclassified as a payable Draft line by this fixture.
DO $case$
DECLARE
  v_seed jsonb;
  v_chain jsonb;
  v_authorise jsonb;
  v_residual jsonb;
  v_handoff jsonb;
  v_handoff_row jsonb;
BEGIN
  v_seed:=pg_temp.h2_p2_seed_chain(100000,'PAYE','REVERSAL_ONLY');
  v_chain:=public.timesheet_correction_chain_scope_v1((v_seed->>'reversal_timesheet_id')::uuid,false,32,100);
  v_authorise:=pg_temp.h2_p2_authorise_chain(v_seed);
  v_residual:=public.pay_correction_chain_residual_v1(
    (v_seed->>'reversal_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,'PAYE',NULL,NULL,100);
  PERFORM pg_temp.h2_p2_assert((v_chain->>'valid')::boolean
    AND v_chain#>>'{requested_correction_unit,correction_shape}'='REVERSAL_ONLY'
    AND (v_chain#>>'{requested_correction_unit,expected_member_count}')::integer=1
    AND v_seed->>'replacement_timesheet_id' IS NULL,
    'PAYE reversal-only shape invented or lost a member');
  PERFORM pg_temp.h2_p2_assert((v_residual->>'total_target_outstanding_ex_vat')::numeric=-100,
    'PAYE reversal-only signed residual changed');
  TRUNCATE TABLE pg_temp.tmp_sync_timesheet_case_candidates;
  v_handoff:=public._ctms_rewrite_sync_correction_cases_v1(
    pg_temp.h2_p2_uuid(2),
    ARRAY[(v_seed->>'candidate_id')::uuid],
    ARRAY[(v_seed->>'root_timesheet_id')::uuid]
  );
  SELECT pg_catalog.to_jsonb(candidate_row)
  INTO STRICT v_handoff_row
  FROM pg_temp.tmp_sync_timesheet_case_candidates AS candidate_row
  WHERE candidate_row.candidate_id=(v_seed->>'candidate_id')::uuid
    AND candidate_row.timesheet_id=(v_seed->>'root_timesheet_id')::uuid;
  PERFORM pg_temp.h2_p2_assert(
    (v_handoff->>'rewritten_chain_count')::integer=1
      AND v_handoff_row->>'desired_case_type'='OVERPAYMENT'
      AND v_handoff_row->>'desired_advance_kind'='OVERPAYMENT'
      AND v_handoff_row->>'desired_reason'='OVERPAYMENT'
      AND (v_handoff_row->>'overpayment_amount_ex')::numeric=100
      AND (v_handoff_row->>'underpayment_amount_ex')::numeric=0
      AND (v_handoff_row->>'source_original_paid_amount')::numeric=100
      AND (v_handoff_row->>'source_corrected_paid_amount')::numeric=0
      AND v_handoff_row->>'candidate_pay_method'='PAYE'
      AND v_handoff_row#>>'{components_sync_json,0,source_family_key}'
        LIKE 'correction-chain:%'
      AND v_handoff_row#>>'{components_sync_json,0,current_target_pay_method}'='PAYE',
    'PAYE reversal-only residual did not enter the exact existing overpayment recovery handoff');
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_reversal_only_paye','PASS_CURRENT_V8_RECOVERY_HANDOFF',
    pg_catalog.jsonb_build_object('seed',v_seed,'chain',v_chain,'authorise',v_authorise,
      'residual',v_residual,'recovery_handoff',v_handoff,
      'recovery_handoff_row',v_handoff_row,
      'direct_draft_materialisation_claimed',false));

  v_seed:=pg_temp.h2_p2_seed_chain(110000,'UMBRELLA','REVERSAL_ONLY');
  v_chain:=public.timesheet_correction_chain_scope_v1((v_seed->>'reversal_timesheet_id')::uuid,false,32,100);
  v_authorise:=pg_temp.h2_p2_authorise_chain(v_seed);
  v_residual:=public.pay_correction_chain_residual_v1(
    (v_seed->>'reversal_timesheet_id')::uuid,(v_seed->>'candidate_id')::uuid,'UMBRELLA',NULL,NULL,100);
  PERFORM pg_temp.h2_p2_assert((v_chain->>'valid')::boolean
    AND (v_chain#>>'{requested_correction_unit,expected_member_count}')::integer=1
    AND (v_residual->>'total_target_outstanding_ex_vat')::numeric=-100
    AND v_chain#>>'{requested_correction_unit,policy_envelope,reversal,tsfin_policy,applied_pay_vat_rate_pct}'='20',
    'Umbrella reversal-only signed residual or VAT evidence changed');
  TRUNCATE TABLE pg_temp.tmp_sync_timesheet_case_candidates;
  v_handoff:=public._ctms_rewrite_sync_correction_cases_v1(
    pg_temp.h2_p2_uuid(2),
    ARRAY[(v_seed->>'candidate_id')::uuid],
    ARRAY[(v_seed->>'root_timesheet_id')::uuid]
  );
  SELECT pg_catalog.to_jsonb(candidate_row)
  INTO STRICT v_handoff_row
  FROM pg_temp.tmp_sync_timesheet_case_candidates AS candidate_row
  WHERE candidate_row.candidate_id=(v_seed->>'candidate_id')::uuid
    AND candidate_row.timesheet_id=(v_seed->>'root_timesheet_id')::uuid;
  PERFORM pg_temp.h2_p2_assert(
    (v_handoff->>'rewritten_chain_count')::integer=1
      AND v_handoff_row->>'desired_case_type'='OVERPAYMENT'
      AND v_handoff_row->>'desired_advance_kind'='OVERPAYMENT'
      AND v_handoff_row->>'desired_reason'='OVERPAYMENT'
      AND (v_handoff_row->>'overpayment_amount_ex')::numeric=100
      AND (v_handoff_row->>'underpayment_amount_ex')::numeric=0
      AND (v_handoff_row->>'source_original_paid_amount')::numeric=100
      AND (v_handoff_row->>'source_corrected_paid_amount')::numeric=0
      AND v_handoff_row->>'candidate_pay_method'='UMBRELLA'
      AND v_handoff_row#>>'{components_sync_json,0,source_family_key}'
        LIKE 'correction-chain:%'
      AND v_handoff_row#>>'{components_sync_json,0,current_target_pay_method}'='UMBRELLA',
    'Umbrella reversal-only residual did not enter the exact existing overpayment recovery handoff');
  INSERT INTO pg_temp.h2_p2_results VALUES(
    'paired_reversal_only_umbrella','PASS_CURRENT_V8_RECOVERY_HANDOFF',
    pg_catalog.jsonb_build_object('seed',v_seed,'chain',v_chain,'authorise',v_authorise,
      'residual',v_residual,'recovery_handoff',v_handoff,
      'recovery_handoff_row',v_handoff_row,
      'direct_draft_materialisation_claimed',false));
END;
$case$;

DO $final$
DECLARE v_unrelated_before text; v_unrelated_after text; v_result_digest text;
BEGIN
  SELECT row_digest_sha256 INTO v_unrelated_before
  FROM pg_temp.h2_p2_unrelated_candidate_baseline
  WHERE candidate_id=pg_temp.h2_p2_uuid(4);
  SELECT pg_temp.h2_p2_sha256(pg_catalog.to_jsonb(candidate_row)) INTO v_unrelated_after
  FROM public.candidates AS candidate_row WHERE id=pg_temp.h2_p2_uuid(4);
  PERFORM pg_temp.h2_p2_assert(v_unrelated_after=v_unrelated_before,
    'unrelated Candidate changed');
  PERFORM pg_temp.h2_p2_assert((SELECT count(*) FROM pg_temp.h2_p2_results)=11,
    'not all 11 paired-Timesheet classes produced a result');
  PERFORM pg_temp.h2_p2_assert(NOT EXISTS(
    SELECT 1 FROM pg_temp.h2_p2_results WHERE current_v8_status NOT LIKE 'PASS%'),
    'one or more paired-Timesheet current-V8 checks failed');
  PERFORM pg_temp.h2_p2_assert(NOT EXISTS(
    SELECT 1 FROM public.banking_pay_operation_provider_attempts
    WHERE operation_id::text LIKE '92000000-0000-4000-8000-%'),
    'provider attempt created');
  PERFORM pg_temp.h2_p2_assert(NOT EXISTS(
    SELECT 1 FROM public.banking_pay_operation_settlement_scope
    WHERE operation_id::text LIKE '92000000-0000-4000-8000-%'),
    'settlement scope created');
  PERFORM pg_temp.h2_p2_assert(NOT EXISTS(
    SELECT 1 FROM public.banking_pay_operation_remittance_scope
    WHERE operation_id::text LIKE '92000000-0000-4000-8000-%'),
    'remittance scope created');
  -- Generated UUIDs and clock timestamps are deliberately excluded from this
  -- cross-engine digest.  The executable assertions above still compare every
  -- policy-bearing value (shape, membership, channel, sign, amount, VAT,
  -- fingerprints, readiness and replay).  Only the stable class verdict is
  -- hashed here so PG17 and PG18 must emit one byte-identical summary.
  SELECT pg_temp.h2_p2_sha256(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object('class_id',class_id,'status',current_v8_status)
    ORDER BY class_id))
  INTO v_result_digest FROM pg_temp.h2_p2_results;
  RAISE NOTICE 'H2_P2_PAIRED_TIMESHEET_CURRENT_V8_PASS=11 RESULT_DIGEST=% V1_V8_PARITY=OPEN',v_result_digest;
END;
$final$;

ROLLBACK;
