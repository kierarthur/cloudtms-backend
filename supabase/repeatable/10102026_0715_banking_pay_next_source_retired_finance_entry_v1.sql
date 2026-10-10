-- SP-02: exact retired entry admission and NEXT informational absence boundary.
-- Shared finalisation, TSFIN, local protected staging and factual Source owners retained.
\set ON_ERROR_STOP on
begin;
create or replace function public.bpay_next_source_legacy_finance_admit_v1()
returns boolean language plpgsql volatile security definer
set search_path=pg_catalog,private
as $function$
declare v_claim text;v_owner text;
begin
  begin
    v_claim:=coalesce(nullif(pg_catalog.current_setting('request.jwt.claim.role',true),''),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','');
  exception when invalid_text_representation then
    raise exception using errcode='42501',message='BPAY_NEXT_SERVICE_REQUIRED';
  end;
  if v_claim<>'service_role' then raise exception using errcode='42501',message='BPAY_NEXT_SERVICE_REQUIRED';end if;
  if private.bpay_next_legacy_callback_enabled_v1() then return true;end if;
  -- The existing callback holds this module row FOR SHARE through this transaction.
  select m.active_owner into strict v_owner from private.bpay_next_module_control m where m.id=1;
  if v_owner='NEXT' then
    raise exception using errcode='PT410',message='BPAY_NEXT_LEGACY_WRITER_RETIRED';
  end if;
  raise exception using errcode='PT503',message='BPAY_NEXT_LEGACY_WRITER_DISABLED';
end;
$function$;
alter function public.bpay_next_source_legacy_finance_admit_v1() owner to postgres;
revoke all on function public.bpay_next_source_legacy_finance_admit_v1() from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_source_legacy_finance_admit_v1() to service_role;

-- Exact retained authority supabase/repeatable/17092026_0100_weekly_source_banking_pay_absence_v1.sql; positive NEXT branch only.
create or replace function private.weekly_source_banking_pay_integration_arrived_v1()
returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_shape integer;
  v_pre record;
  v_overlay record;
begin
  perform private.bpay_next_legacy_callback_enabled_v1();
  if (select m.active_owner from private.bpay_next_module_control m where m.id=1)='NEXT' then
    return pg_catalog.jsonb_build_object('arrived',true,'integration','BANKING_PAY_NEXT','changes_no_outcome',true);
  end if;
  select pg_catalog.count(*)::integer into v_shape
  from pg_catalog.pg_proc as classifier
  cross join lateral pg_catalog.unnest(classifier.proargnames) as argument(name)
  where classifier.oid=pg_catalog.to_regprocedure(
          'public._pay_rail_state_money_movement_classify(text,text,jsonb,jsonb)')
    and argument.name in ('cash_state','is_terminal_no_money','is_pending_non_final',
                          'is_final_money_moved','reason');
  if coalesce(v_shape,0)<>5 then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_TYPED_SHAPE_LOST'
      using errcode='55000',
            detail='The installed Banking Pay classifier must return a TYPED result carrying '
                   ||'cash_state, is_terminal_no_money, is_pending_non_final, '
                   ||'is_final_money_moved and reason; found '||coalesce(v_shape,0)||' of 5. '
                   ||'Anything that cannot read it proves nothing and must fail loudly.';
  end if;

  select * into v_pre
  from private.weekly_source_banking_pay_branch_probe_v1('PRE_BANK_CANCEL_VOIDED');
  select * into v_overlay
  from private.weekly_source_banking_pay_branch_probe_v1(
    'CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED');

  return pg_catalog.jsonb_build_object(
    'arrived',coalesce(v_pre.is_terminal_no_money,false)
              or coalesce(v_overlay.is_terminal_no_money,false),
    'typed_shape_columns',v_shape,
    'branches',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'branch','PRE_BANK_CANCEL_VOIDED',
        'cash_state',v_pre.cash_state,
        'is_terminal_no_money',coalesce(v_pre.is_terminal_no_money,false),
        'reason',v_pre.reason),
      pg_catalog.jsonb_build_object(
        'branch','CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED',
        'cash_state',v_overlay.cash_state,
        'is_terminal_no_money',coalesce(v_overlay.is_terminal_no_money,false),
        'reason',v_overlay.reason)));
end;
$function$;
alter function private.weekly_source_banking_pay_integration_arrived_v1() owner to postgres;
revoke all on function private.weekly_source_banking_pay_integration_arrived_v1() from public,anon,authenticated,service_role;

-- Exact retained authority supabase/repeatable/17092026_0100_weekly_source_banking_pay_absence_v1.sql; positive NEXT branch only.
create or replace function private.weekly_source_banking_pay_absence_v1()
returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_rows integer;
  v_setting private.weekly_source_banking_pay_absence%rowtype;
  v_environment text;
  v_identity_rows integer;
  v_arrived jsonb;
begin
  perform private.bpay_next_legacy_callback_enabled_v1();
  if (select m.active_owner from private.bpay_next_module_control m where m.id=1)='NEXT' then
    return pg_catalog.jsonb_build_object('setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','BANKING_PAY_NEXT_ACTIVE');
  end if;
  -- Cardinality, never `limit 1`. Absent is off.
  select pg_catalog.count(*)::integer into v_rows
  from private.weekly_source_banking_pay_absence;
  if v_rows=0 then
    return pg_catalog.jsonb_build_object(
      'setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','SETTING_ABSENT');
  end if;
  if v_rows<>1 then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_ABSENCE_AMBIGUOUS'
      using errcode='55000',
            detail='The absence declaration is a singleton; found '||v_rows||' rows.';
  end if;

  select * into v_setting from private.weekly_source_banking_pay_absence;
  if coalesce(v_setting.declared,false) is not true then
    return pg_catalog.jsonb_build_object(
      'setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','SETTING_OFF');
  end if;

  -- The declaration's OWN environment check, repeated at read time so that a
  -- row which somehow survived a restore, a clone or a promotion into a live
  -- database still reads as off. Fails closed on anything but TEST.
  select pg_catalog.count(*)::integer into v_identity_rows
  from private.cloudtms_database_identity;
  if v_identity_rows<>1 then
    return pg_catalog.jsonb_build_object(
      'setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','ENVIRONMENT_UNKNOWN',
      'identity_rows',v_identity_rows);
  end if;
  select identity_row.environment into v_environment
  from private.cloudtms_database_identity as identity_row;
  if coalesce(v_environment,'')<>'TEST' then
    return pg_catalog.jsonb_build_object(
      'setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','ENVIRONMENT_NOT_TEST',
      'environment',v_environment);
  end if;

  -- The declaration cannot outlive its own truth.
  v_arrived:=private.weekly_source_banking_pay_integration_arrived_v1();
  if coalesce((v_arrived->>'arrived')::boolean,false) then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_ABSENCE_NO_LONGER_TRUE'
      using errcode='55000',
            detail='The Banking Pay money-movement classifier now answers TERMINAL_NO_MONEY for '
                   ||'at least one of its two reason-specific branches, so the integration this '
                   ||'setting declares absent has arrived and the setting must be REMOVED rather '
                   ||'than left on. The WP-07c future-expectation tripwire fires on the same '
                   ||'event and independently; both are intended. Branch probes: '
                   ||v_arrived::text;
  end if;

  return pg_catalog.jsonb_build_object(
    'setting','weekly_source.banking_pay_integration_absent',
    'declared',true,
    'reason_code','DECLARED_ABSENT',
    'environment',v_environment,
    'declared_by',v_setting.declared_by,
    'declared_at_utc',v_setting.declared_at_utc,
    'reason',v_setting.reason,
    'classifier_branches',v_arrived->'branches');
end;
$function$;
alter function private.weekly_source_banking_pay_absence_v1() owner to postgres;
revoke all on function private.weekly_source_banking_pay_absence_v1() from public,anon,authenticated,service_role;

-- Exact retained authority supabase/repeatable/17092026_0100_weekly_source_banking_pay_absence_v1.sql; positive NEXT branch only.
create or replace function private.weekly_source_banking_pay_boundary_notice_v1()
returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_state jsonb;
begin
  perform private.bpay_next_legacy_callback_enabled_v1();
  if (select m.active_owner from private.bpay_next_module_control m where m.id=1)='NEXT' then
    return pg_catalog.jsonb_build_object('kind','BANKING_PAY_NEXT','severity','INFORMATIONAL',
      'headline','Pay is published through Banking Pay NEXT',
      'message','Pay is published through Banking Pay NEXT; there is nothing to recover here',
      'changes_no_outcome',true);
  end if;
  v_state:=private.weekly_source_banking_pay_absence_v1();
  if coalesce((v_state->>'declared')::boolean,false) is not true then
    return null;
  end if;
  return pg_catalog.jsonb_build_object(
    'kind','BANKING_PAY_INTEGRATION_ABSENT',
    'severity','RELEASE_BLOCKED_UNTIL_ACTIVATION_GATE',
    'headline','Banking Pay activation gate required',
    'message','Banking Pay is not connected yet. This declaration is informational and does not '
      ||'stop an existing Workbench claim route. Weekly Source authorisation is safe to test only '
      ||'after the HANDOVER 2 server-side activation gate is installed and verified with its '
      ||'switch off. Until then, test import, review, finalisation, self-billing and presentation, '
      ||'but do not treat this notice as a payment stop.',
    'declared_by',v_state->>'declared_by',
    'declared_at_utc',v_state->>'declared_at_utc',
    'reason',v_state->>'reason',
    'changes_no_outcome',true);
end;
$function$;
alter function private.weekly_source_banking_pay_boundary_notice_v1() owner to postgres;
revoke all on function private.weekly_source_banking_pay_boundary_notice_v1() from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';
commit;
