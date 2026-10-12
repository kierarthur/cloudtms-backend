-- WB-29 / BD155: complete native treatment publication; original earnings retained.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_revision_guard_v1()
returns trigger
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_missing_detail boolean;
  v_line_count bigint;
  v_total_source_ex numeric;
  v_daily_work private.bpay_next_work%rowtype;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_financial public.timesheets_financials%rowtype;
begin
  if tg_op = 'INSERT' then
    if new.approved_at_utc is not null or new.sealed_at_utc is not null then
      raise exception using errcode='23514', message='BPAY_NEXT_DIRECT_SEALED_INSERT_FORBIDDEN';
    end if;
    return new;
  end if;
  if tg_op = 'DELETE' then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_DELETE_FORBIDDEN';
  end if;
  if old.sealed_at_utc is not null then
    raise exception using errcode='23514', message='BPAY_NEXT_SEALED_REVISION_IMMUTABLE';
  end if;
  if (to_jsonb(new) - 'approved_at_utc' - 'sealed_at_utc')
     is distinct from (to_jsonb(old) - 'approved_at_utc' - 'sealed_at_utc')
     or new.approved_at_utc is null or new.sealed_at_utc is null then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_SEAL_ONLY';
  end if;
  -- WB-29: direct original approval; staged payload has no publication yet.
  if new.payment_treatment_approval_revision_id is not null then
    perform private.bpay_next_treatment_payload_assert_v1(old.id);
  end if;
  select count(*),coalesce(sum(source_pay_ex_vat),0)
    into v_line_count,v_total_source_ex
    from private.bpay_next_approved_line l where l.revision_id=old.id;
  if v_line_count<>new.expected_line_count
     or v_total_source_ex<>new.approved_source_ex_vat then
    raise exception using errcode='23514', message='BPAY_NEXT_REVISION_INCOMPLETE_OR_TOTAL_MISMATCH';
  end if;
  if new.financial_snapshot_id is not null and not exists (
    select 1 from public.timesheets_financials tf
     where tf.id=new.financial_snapshot_id
       and tf.timesheet_id=new.physical_timesheet_id
       and tf.timesheet_version=new.physical_timesheet_version
  ) then
    raise exception using errcode='23514', message='BPAY_NEXT_FINANCIAL_SNAPSHOT_IDENTITY_MISMATCH';
  end if;
  if exists (
    select 1 from private.bpay_next_approved_line l
    left join private.bpay_next_rate_detail rd on rd.approved_line_id=l.id
    where l.revision_id=old.id
    group by l.id,l.expected_rate_detail_count
    having count(rd.id)<>l.expected_rate_detail_count
  ) then
    raise exception using errcode='23514', message='BPAY_NEXT_RATE_DETAIL_COUNT_MISMATCH';
  end if;
  if (select count(*) from private.bpay_next_rate_schedule s
      where s.revision_id=old.id)<>new.expected_rate_schedule_count then
    raise exception using errcode='23514', message='BPAY_NEXT_RATE_SCHEDULE_COUNT_MISMATCH';
  end if;
  select w.* into strict v_daily_work from private.bpay_next_work w where w.id=new.work_id;
  if v_daily_work.origin_kind='DAILY' or new.daily_approval_evidence_json is not null then
    select t.* into strict v_daily_timesheet from public.timesheets t where t.timesheet_id=new.physical_timesheet_id;
    if v_daily_timesheet.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum then
      raise exception using errcode='23514',message='BPAY_NEXT_CONTRACT_REVISION_DAILY_EVIDENCE_FORBIDDEN';
    end if;
    if new.source_kind is distinct from 'ORDINARY' or new.financial_snapshot_id is null
       or v_daily_timesheet.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum
       or v_daily_timesheet.line_type::text is distinct from 'HOURS'
       or v_daily_timesheet.booking_id is distinct from v_daily_work.booking_id
       or v_daily_timesheet.contract_id is distinct from v_daily_work.contract_id
       or v_daily_timesheet.version is distinct from new.physical_timesheet_version
       or jsonb_typeof(new.daily_approval_evidence_json) is distinct from 'object'
       or new.daily_approval_evidence_json->>'contract_version' is distinct from 'BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
       or new.daily_approval_evidence_json->>'sheet_scope' is distinct from 'DAILY'
       or new.daily_approval_evidence_json->>'timesheet_id' is distinct from new.physical_timesheet_id::text
       or new.daily_approval_evidence_json->>'timesheet_version' is distinct from new.physical_timesheet_version::text
       or new.daily_approval_evidence_json->>'financial_id' is distinct from new.financial_snapshot_id::text
       or new.daily_approval_evidence_json->>'candidate_id' is distinct from v_daily_work.candidate_id::text
       or new.daily_approval_evidence_json->>'booking_id' is distinct from v_daily_work.booking_id
       or new.daily_approval_evidence_json->'contract_id' is distinct from coalesce(to_jsonb(v_daily_work.contract_id),'null'::jsonb)
       or new.daily_approval_evidence_json->>'work_date' is distinct from ((v_daily_timesheet.worked_start_iso at time zone 'Europe/London')::date)::text
       or (v_daily_work.origin_kind='DAILY' and new.daily_approval_evidence_json->>'work_date' is distinct from v_daily_work.origin_work_date::text)
       or new.daily_approval_evidence_json->'settings_authority_json' is distinct from v_daily_timesheet.settings_authority_json
       or new.daily_approval_evidence_json->'timesheet_authorised_at_utc' is distinct from to_jsonb(v_daily_timesheet.authorised_at_server) then
      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_REVISION_EVIDENCE_REQUIRED';
    end if;
    select tf.* into strict v_daily_financial from public.timesheets_financials tf where tf.id=new.financial_snapshot_id;
    if v_daily_financial.is_current is distinct from true or v_daily_financial.is_stale is distinct from false
       or v_daily_financial.basis::text is distinct from 'SELF_REPORTED'
       or v_daily_financial.authorised_at_utc is null or v_daily_timesheet.authorised_at_server is null
       or v_daily_financial.candidate_id is distinct from v_daily_work.candidate_id
       or new.daily_approval_evidence_json->>'client_id' is distinct from v_daily_financial.client_id::text
       or new.daily_approval_evidence_json->'financial_authorised_at_utc' is distinct from to_jsonb(v_daily_financial.authorised_at_utc)
       or new.daily_approval_evidence_json->>'role' is distinct from v_daily_financial.role
       or new.daily_approval_evidence_json->>'band' is distinct from v_daily_financial.band
       or new.job_title is distinct from v_daily_financial.role or new.band_label is distinct from v_daily_financial.band then
      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_REVISION_EVIDENCE_REQUIRED';
    end if;
  end if;
  if new.detail_kind='SHIFT' then
    select exists (
      select 1 from private.bpay_next_approved_line l
      where l.revision_id=old.id
        and l.component_kind in ('WORK','PROTECTED_WORK')
        and not exists (
          select 1 from private.bpay_next_shift_detail d
          where d.approved_line_id=l.id
            and d.work_date is not null
            and (d.approved_minutes is not null or d.approved_hours is not null
                 or d.shift_start_at is not null)
        )
    ) into v_missing_detail;
    if v_missing_detail then
      raise exception using errcode='23514', message='BPAY_NEXT_REVISION_WITHOUT_SHIFT_DETAIL';
    end if;
  end if;
  return new;
end
$function$;


alter function private.bpay_next_revision_guard_v1() owner to postgres;
revoke all on function private.bpay_next_revision_guard_v1() from public,anon,authenticated,service_role;
commit;
