-- WB29: saved decisions remain after unsnooze; exact opening membership only.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_snooze_context_v1(p_actor_id uuid,p_args jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
  v_physical_matches uuid[];v_physical_revisions uuid[];v_candidate uuid; v_work_id uuid; v_revision_id uuid; v_scope text; v_key text;
  v_case_id uuid; v_case_component_id uuid; v_kind text; v_identity text; v_basis bytea;
  v_revision private.bpay_next_work_revision%rowtype; v_work private.bpay_next_work%rowtype;
  v_segment private.bpay_next_approved_segment%rowtype; v_line private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_case_component%rowtype; v_snooze private.bpay_next_snooze%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;
  v_module_epoch bigint; v_until date; v_date_context jsonb; v_warning text; v_phase text;
  v_conflict text; v_business_date date; v_indefinite boolean; v_note text; v_financial_revision text;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_id is null or not exists(select 1 from public.tms_users u
      where u.id=p_actor_id and u.is_active and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_FORBIDDEN';
  end if;
  select m.owner_epoch into v_module_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE'; end if;
  if p_args is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>4096
    or exists(select 1 from jsonb_object_keys(p_args) k where k not in ('request_id','candidate_id','work_id','physical_timesheet_id','physical_timesheet_version',
      'expected_revision_id','scope_kind','segment_key','component_key','case_id','case_component_id','snooze_kind',
      'until_date','indefinite','note','expected_version','worker_pay_basis','acknowledgement_token','phase','expected_case_revision')) then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_REQUEST_INVALID';
  end if;
  -- Timesheet entry uses the actual viewed physical row, never an origin fallback.
  if p_args ? 'physical_timesheet_id' then
    if p_args->>'phase' is distinct from 'PRE_OPEN' or p_args ? 'work_id' or p_args ? 'expected_revision_id'
      or p_args->>'scope_kind' is distinct from 'WHOLE_TIMESHEET' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_FOREIGN_SCOPE_FIELD';end if;
    if p_args->>'physical_timesheet_version' is null or p_args->>'physical_timesheet_version'!~'^[1-9][0-9]{0,9}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_REQUEST_INVALID';end if;
    select array_agg(x.work_id),array_agg(x.revision_id) into v_physical_matches,v_physical_revisions from
      (select w.id work_id,r.id revision_id from private.bpay_next_work_revision r join private.bpay_next_work w
        on w.current_revision_id=r.id and r.work_id=w.id where w.candidate_id=(p_args->>'candidate_id')::uuid
          and r.physical_timesheet_id=(p_args->>'physical_timesheet_id')::uuid
          and r.physical_timesheet_version=(p_args->>'physical_timesheet_version')::integer
          and r.sealed_at_utc is not null order by w.id limit 2) x;
    if cardinality(v_physical_matches) is distinct from 1 then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REVISION_STALE';end if;
    v_work_id:=v_physical_matches[1];v_revision_id:=v_physical_revisions[1];
    p_args:=(p_args-array['physical_timesheet_id','physical_timesheet_version'])||jsonb_build_object('work_id',v_work_id,'expected_revision_id',v_revision_id);
  end if;
  v_candidate:=(p_args->>'candidate_id')::uuid; v_work_id:=nullif(p_args->>'work_id','')::uuid;
  v_revision_id:=nullif(p_args->>'expected_revision_id','')::uuid;
  v_case_id:=nullif(p_args->>'case_id','')::uuid; v_case_component_id:=nullif(p_args->>'case_component_id','')::uuid;
  v_scope:=p_args->>'scope_kind'; v_key:=case v_scope when 'SEGMENT' then p_args->>'segment_key'
    when 'EXPENSE_LINE' then p_args->>'component_key' end;
  if (v_scope<>'SEGMENT' and p_args ? 'segment_key')
    or (v_scope<>'EXPENSE_LINE' and p_args ? 'component_key')
    or (v_scope<>'CASE' and (p_args ? 'case_id' or p_args ? 'case_component_id' or p_args ? 'expected_case_revision'))
    or (v_scope='CASE' and (p_args ? 'work_id' or p_args ? 'expected_revision_id')) then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_FOREIGN_SCOPE_FIELD';
  end if;
  v_kind:=case upper(btrim(p_args->>'snooze_kind')) when 'BLOCKED' then 'BLOCKED_TIMESHEET'
    when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE_REPAYMENT' else upper(btrim(p_args->>'snooze_kind')) end;
  v_phase:=coalesce(p_args->>'phase','PRE_SAVE'); v_note:=coalesce(p_args->>'note','');
  v_until:=nullif(p_args->>'until_date','')::date;
  if v_scope is null or v_scope not in ('WHOLE_TIMESHEET','SEGMENT','EXPENSE_LINE','EXPENSES_ALL','CASE')
    or v_kind is null or v_kind not in ('DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT',
      'OVERPAYMENT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','MANUAL_DEBT_RECOVERY')
    or v_phase not in ('PRE_OPEN','PRE_SAVE') or v_candidate is null or length(v_note)>500
    or jsonb_typeof(p_args->'indefinite') is distinct from 'boolean'
    or (v_phase='PRE_SAVE' and (p_args->>'indefinite')::boolean is distinct from (v_until is null))
    or (p_args ? 'until_date' and jsonb_typeof(p_args->'until_date') not in ('string','null'))
    or (v_until is not null and p_args->>'until_date' !~ '^\d{4}-\d{2}-\d{2}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_SCOPE_OR_VALUE_INVALID';
  end if;
  if v_scope='CASE' then
    select x.* into v_case from private.bpay_next_case_component x
      where x.id=v_case_component_id and x.case_id=v_case_id and x.candidate_id=v_candidate;
    if not found or v_work_id is not null or v_revision_id is not null
      or not (case v_kind when 'OVERPAYMENT_RECOVERY' then v_case.case_kind='OVERPAYMENT'
        when 'PAYMENT_ADVANCE_REPAYMENT' then v_case.case_kind in ('LOAN','ADVANCE')
        when 'MANUAL_DEBT_RECOVERY' then v_case.case_kind='MANUAL_DEBT' else false end)
      or v_case.instruction_kind<>'RECOVERY' then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_CASE_AUTHORITY_MISMATCH';
    end if;
    v_identity:=v_case.id::text; v_financial_revision:=v_case.component_revision::text;
    select r.* into strict v_rule from private.bpay_next_case_rule r
      where r.id=v_case.rule_id and r.case_id=v_case.case_id and r.candidate_id=v_candidate;
    v_basis:=pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array(v_candidate,v_case.case_id,v_case.id,
      v_case.case_kind,v_case.case_subtype,v_case.tax_treatment,v_case.instruction_kind,v_case.direction,
      v_case.payroll_stage,v_case.source_pay_channel,v_case.approved_source_ex_vat,v_case.funded_source_ex_vat,
      v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,v_rule.weekly_due_source_ex_vat,
      v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)::text,'UTF8'));
  else
    select w.* into v_work from private.bpay_next_work w where w.id=v_work_id and w.candidate_id=v_candidate;
    if not found or v_case_id is not null or v_case_component_id is not null or v_kind not in
      ('DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT') then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WORK_AUTHORITY_MISMATCH';
    end if;
    select r.* into v_revision from private.bpay_next_work_revision r where r.id=v_work.current_revision_id
      and r.work_id=v_work.id and r.sealed_at_utc is not null;
    if not found or v_revision_id is distinct from v_revision.id or v_work.approval_state<>'APPROVED' then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REVISION_STALE';
    end if;
    v_financial_revision:=v_revision.revision_no::text;
    v_identity:=v_work.id::text;
    if v_scope='SEGMENT' then
      select s.* into v_segment from private.bpay_next_approved_segment s
        where s.revision_id=v_revision.id and s.segment_key=v_key;
      if not found then
        if exists(select 1 from private.bpay_next_approved_segment s where s.revision_id=v_revision.id) then
          raise exception using errcode='23514',message='SNOOZE_SEGMENT_STALE';
        end if;
        raise exception using errcode='23514',message='SNOOZE_SEGMENT_SCOPE_UNSUPPORTED';
      end if;
      v_identity:=v_identity||':'||v_key; v_basis:=private.bpay_next_segment_pay_basis_v1(v_segment.id);
    elsif v_scope='EXPENSE_LINE' then
      select a.* into v_line from private.bpay_next_approved_line a
        where a.revision_id=v_revision.id and a.component_key=v_key and a.component_kind in ('EXPENSE','MILEAGE');
      if not found or v_kind<>'DO_NOT_PAY' then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EXPENSE_IDENTITY_INVALID';
      end if;
      v_identity:=v_identity||':'||v_key; v_basis:=private.bpay_next_line_pay_basis_v1(v_line.id);
    else
      if v_scope='EXPENSES_ALL' and (v_kind<>'DO_NOT_PAY' or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_kind in ('EXPENSE','MILEAGE'))) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EXPENSE_SCOPE_UNSUPPORTED';
      end if;
      select case when v_scope='EXPENSES_ALL' then b.expense_basis else b.whole_basis end into v_basis
        from private.bpay_next_revision_worker_basis b where b.revision_id=v_revision.id;
    end if;
    if v_scope='WHOLE_TIMESHEET' and exists(select 1 from private.bpay_next_snooze s
      where s.work_id=v_work.id and s.active and s.scope_kind='SEGMENT'
        and (s.until_date is null or s.until_date>=(current_timestamp at time zone 'Europe/London')::date)
        and exists(select 1 from private.bpay_next_approved_segment a where a.revision_id=v_revision.id
          and a.segment_key=s.segment_key and private.bpay_next_segment_pay_basis_v1(a.id)=s.worker_pay_basis)) then
      v_conflict:='SEGMENT_SNOOZES_ALREADY_EXIST';
    elsif v_scope='SEGMENT' and exists(select 1 from private.bpay_next_snooze s
      join private.bpay_next_revision_worker_basis b on b.revision_id=v_revision.id and b.whole_basis=s.worker_pay_basis
      where s.work_id=v_work.id and s.active and s.scope_kind='WHOLE_TIMESHEET'
        and (s.until_date is null or s.until_date>=(current_timestamp at time zone 'Europe/London')::date)) then
      v_conflict:='WHOLE_TIMESHEET_ALREADY_SNOOZED';
    end if;
  end if;
  if v_basis is null then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WORKER_BASIS_UNAVAILABLE'; end if;
  select s.* into v_snooze from private.bpay_next_snooze s where s.candidate_id=v_candidate
    and s.scope_kind=v_scope and s.scope_identity=v_identity and s.active;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  v_business_date:=(v_date_context->>'business_date')::date;
  if v_phase='PRE_SAVE' and v_until<v_business_date then raise exception using errcode='22023',message='SNOOZE_DATE_IN_PAST'; end if;
  -- WB29: keyed actual mapped owners, including incompatible pay-week variants.
  -- Only this actual taxable Work/positive shift bucket can require the notice.
  if v_phase='PRE_OPEN' and v_scope in ('WHOLE_TIMESHEET','SEGMENT') and (
    exists(select 1 from private.bpay_next_component_resolution d join private.bpay_next_approved_line a on a.id=d.approved_line_id where d.work_id=v_work.id and d.candidate_id=v_candidate and d.state='ACTIVE' and d.based_on_revision_id=v_revision.id
      and a.id=d.approved_line_id and a.revision_id=v_revision.id and a.component_key=d.component_key and a.tax_treatment='TAXABLE'
      and ((d.rate_family='STANDARD' and a.component_kind in ('WORK','PROTECTED_WORK')
        and exists(select 1 from private.bpay_next_rate_detail rd where rd.approved_line_id=a.id and rd.bucket=d.rate_code))
        or (d.rate_family='ADDITIONAL' and a.component_kind='ADDITIONAL' and a.component_key='ADDITIONAL:'||d.rate_code)
        or (d.rate_family='ADJUSTMENT' and a.component_kind='ADJUSTMENT' and a.component_key=d.rate_code))
      and (v_scope='WHOLE_TIMESHEET' or (a.id=v_segment.approved_line_id and d.component_key=v_segment.component_key and d.rate_family='STANDARD'
        and case d.rate_code when 'DAY' then v_segment.hours_day>0 when 'NIGHT' then v_segment.hours_night>0
          when 'SAT' then v_segment.hours_sat>0 when 'SUN' then v_segment.hours_sun>0 when 'BH' then v_segment.hours_bh>0 else false end)))
    or exists(select 1 from private.bpay_next_component_resolution d join private.bpay_next_approved_line a on a.id=d.published_line_id where d.work_id=v_work.id and d.candidate_id=v_candidate and d.state='ACTIVE' and d.published_revision_id=v_revision.id
      and a.id=d.published_line_id and a.revision_id=v_revision.id and a.component_key=d.component_key and a.tax_treatment='TAXABLE'
      and ((d.rate_family='STANDARD' and a.component_kind in ('WORK','PROTECTED_WORK')
        and exists(select 1 from private.bpay_next_rate_detail rd where rd.approved_line_id=a.id and rd.bucket=d.rate_code))
        or (d.rate_family='ADDITIONAL' and a.component_kind='ADDITIONAL' and a.component_key='ADDITIONAL:'||d.rate_code)
        or (d.rate_family='ADJUSTMENT' and a.component_kind='ADJUSTMENT' and a.component_key=d.rate_code))
      and (v_scope='WHOLE_TIMESHEET' or (a.id=v_segment.approved_line_id and d.component_key=v_segment.component_key and d.rate_family='STANDARD'
        and case d.rate_code when 'DAY' then v_segment.hours_day>0 when 'NIGHT' then v_segment.hours_night>0
          when 'SAT' then v_segment.hours_sat>0 when 'SUN' then v_segment.hours_sun>0 when 'BH' then v_segment.hours_bh>0 else false end)))
  ) then v_warning:='RATE_RESOLUTION_REQUIRES_REVIEW_AFTER_UNSNOOZE';end if;
  if v_phase='PRE_SAVE' and v_until is not null then
    v_warning:=case when v_until<(v_date_context->>'next_official_pay_date')::date
      then 'SNOOZE_DATE_BEFORE_NEXT_PAY_RUN' else 'SNOOZE_INCLUDES_NEXT_PAY_RUN' end;
  end if;
  v_indefinite:=exists(select 1 from private.bpay_next_snooze s where s.candidate_id=v_candidate and s.active
    and s.until_date is null and ((s.scope_kind=v_scope and s.scope_identity=v_identity and s.worker_pay_basis=v_basis)
      or (v_work_id is not null and s.work_id=v_work_id and ((s.scope_kind='WHOLE_TIMESHEET'
        and s.worker_pay_basis=(select b.whole_basis from private.bpay_next_revision_worker_basis b where b.revision_id=v_revision.id))
        or (v_scope='EXPENSE_LINE' and s.scope_kind='EXPENSES_ALL'
          and s.worker_pay_basis=(select b.expense_basis from private.bpay_next_revision_worker_basis b where b.revision_id=v_revision.id))))));
  return jsonb_build_object('agency_id',(select d.installation_id from private.cloudtms_database_identity d where d.singleton),'candidate_id',v_candidate,'work_id',v_work_id,'expected_revision_id',v_revision_id,
    'physical_timesheet_id',v_revision.physical_timesheet_id,'physical_timesheet_version',v_revision.physical_timesheet_version::text,'scope_kind',v_scope,'scope_identity',v_identity,
    'segment_key',v_segment.segment_key,'approved_segment_id',v_segment.id,
    'component_key',coalesce(v_segment.component_key,v_line.component_key),'expense_approved_line_id',v_line.id,
    'case_id',v_case.case_id,'case_component_id',v_case.id,'case_kind',v_case.case_kind,'snooze_kind',v_kind,
    'worker_pay_basis',encode(v_basis,'hex'),'snooze_id',v_snooze.id,'expected_version',coalesce(v_snooze.version,0)::text,
    'financial_revision',v_financial_revision,'stale_control',v_snooze.id is not null and v_snooze.worker_pay_basis<>v_basis,
    'module_epoch',v_module_epoch::text,
    'control_cut',(select c.last_sequence::text from private.bpay_next_command_clock c where c.id=1),
    'expense_count',case when v_scope<>'EXPENSES_ALL' then '0' else (select count(*)::text from private.bpay_next_approved_line a
      where a.revision_id=v_revision.id and a.component_kind in ('EXPENSE','MILEAGE')) end,'conflict',v_conflict,'indefinite_wins',v_indefinite,'until_date',case when v_phase='PRE_OPEN' then v_snooze.until_date else v_until end,
    'note',case when v_phase='PRE_OPEN' then coalesce(v_snooze.note,'') else v_note end,
    'active',v_snooze.id is not null,'business_date',v_business_date,
    'warning_context',jsonb_build_object('phase',v_phase,'until_date',v_until,'business_date',v_business_date,
      'next_official_pay_date',v_date_context->'next_official_pay_date',
      'configuration_fingerprint',v_date_context->'configuration_fingerprint','warning_code',v_warning,
      'warning_required',v_warning is not null));
end
$function$;
create or replace function private.bpay_next_snooze_action_v1(p_actor_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare
  v_request uuid; v_receipt private.bpay_next_snooze_request%rowtype;
  v_command private.bpay_next_command%rowtype; v_snooze private.bpay_next_snooze%rowtype;
  v_ack private.bpay_next_snooze_warning_ack%rowtype;
  v_sequence bigint;
  v_context jsonb; v_result jsonb; v_candidate uuid; v_work uuid; v_before jsonb; v_event text;
  v_expected bigint; v_token uuid; v_now timestamptz; v_epoch bigint;
begin
  if p_action in ('SNOOZE_CHECK','SNOOZE_CONTEXT') then
    return private.bpay_next_snooze_context_v1(p_actor_id,p_args);
  end if;
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_id is null or not exists(select 1 from public.tms_users u
      where u.id=p_actor_id and u.is_active and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_FORBIDDEN';
  end if;
  if p_action is null or p_action not in ('SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR')
    or p_args is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>4096 then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_ACTION_INVALID';
  end if;
  v_request:=(p_args->>'request_id')::uuid;
  -- Exact replay is read before token consumption or any new control write.
  select r.* into v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
  if found then
    if (v_receipt.actor_id,v_receipt.action,v_receipt.request_body) is distinct from (p_actor_id,p_action,p_args) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';
    end if;
    return v_receipt.result_body;
  end if;
  if p_action='SNOOZE_CLEAR' then
    if exists(select 1 from jsonb_object_keys(p_args) k where k not in ('request_id','snooze_id','candidate_id','expected_version')) then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_CLEAR_ARGS_INVALID';
    end if;
    select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=(p_args->>'snooze_id')::uuid;
    v_candidate:=v_snooze.candidate_id; v_work:=v_snooze.work_id;
    if v_candidate is distinct from (p_args->>'candidate_id')::uuid then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_CANDIDATE_MISMATCH';
    end if;
  else
    v_candidate:=(p_args->>'candidate_id')::uuid; v_work:=nullif(p_args->>'work_id','')::uuid;
  end if;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  if v_work is not null then perform 1 from private.bpay_next_work w where w.id=v_work and w.candidate_id=v_candidate for update; end if;
  if p_action<>'SNOOZE_CLEAR' and p_args->>'scope_kind'='CASE' then
    perform 1 from private.bpay_next_case_component x where x.id=(p_args->>'case_component_id')::uuid
      and x.candidate_id=v_candidate for update;
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(v_request,p_action);
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_request and c.agency_sequence=v_sequence;
  -- A concurrent identical retry may have completed while this call waited
  -- on the Candidate lock. Recheck its original body before any token write.
  select r.* into v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
  if found then
    if (v_receipt.actor_id,v_receipt.action,v_receipt.request_body) is distinct from (p_actor_id,p_action,p_args) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';
    end if;
    return v_receipt.result_body;
  end if;
  v_expected:=(p_args->>'expected_version')::bigint;
  if v_expected is null or v_expected<0 then raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_VERSION_REQUIRED'; end if;
  if p_action='SNOOZE_CLEAR' then
    select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=(p_args->>'snooze_id')::uuid for update;
    if not v_snooze.active or v_snooze.version<>v_expected then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_VERSION_STALE';
    end if;
    v_before:=to_jsonb(v_snooze);
    update private.bpay_next_snooze s set active=false,version=s.version+1,control_sequence=v_command.agency_sequence,
      updated_at_utc=clock_timestamp(),updated_by=p_actor_id,last_request_id=v_request
      where s.id=v_snooze.id returning s.* into v_snooze;
    v_event:='CLEARED';
    -- CLEAR remains available on stale financial facts. This pure point read
    -- only reports valid remaining dominance; it does not readmit the old scope.
    select jsonb_build_object('indefinite_wins',exists(select 1 from private.bpay_next_snooze s
      join private.bpay_next_work w on w.id=s.work_id
      join private.bpay_next_revision_worker_basis b on b.revision_id=w.current_revision_id
      left join private.bpay_next_approved_segment seg on seg.revision_id=w.current_revision_id and seg.segment_key=s.segment_key
      left join private.bpay_next_approved_line line on line.revision_id=w.current_revision_id and line.component_key=s.component_key
      where s.work_id=v_snooze.work_id and s.active and s.until_date is null and
        ((s.scope_kind=v_snooze.scope_kind and s.scope_identity=v_snooze.scope_identity
          and s.worker_pay_basis=case s.scope_kind when 'WHOLE_TIMESHEET' then b.whole_basis when 'EXPENSES_ALL' then b.expense_basis
            when 'SEGMENT' then private.bpay_next_segment_pay_basis_v1(seg.id) when 'EXPENSE_LINE' then private.bpay_next_line_pay_basis_v1(line.id) end)
          or (s.scope_kind='WHOLE_TIMESHEET' and s.worker_pay_basis=b.whole_basis)
          or (s.scope_kind='EXPENSES_ALL' and v_snooze.scope_kind='EXPENSE_LINE' and s.worker_pay_basis=b.expense_basis)))) into v_context;
  else
    -- CASE authority is locked separately; no due/pause/schedule is rewritten.
    if p_args->>'scope_kind'='CASE' then
      perform 1 from private.bpay_next_case_component x where x.id=(p_args->>'case_component_id')::uuid
        and x.candidate_id=v_candidate for update;
    end if;
    v_context:=private.bpay_next_snooze_context_v1(p_actor_id,p_args);
    if p_action='SNOOZE_SET' and v_context#>>'{warning_context,phase}'<>'PRE_SAVE' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_SET_REQUIRES_PRE_SAVE';
    end if;
    if v_context->>'conflict' is not null then raise exception using errcode='23514',message=v_context->>'conflict'; end if;
    if v_expected is distinct from (v_context->>'expected_version')::bigint
      or p_args->>'worker_pay_basis' is distinct from v_context->>'worker_pay_basis'
      or (p_args->>'scope_kind'='CASE' and p_args->>'expected_case_revision' is distinct from v_context->>'financial_revision') then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_FINANCIAL_VERSION_STALE';
    end if;
    if p_action='SNOOZE_WARNING_ACK_ISSUE' then
      -- Original immutable replay above remains first. Fresh tokens are for
      -- existing PRE_SAVE date consent only; the opening notice is local.
      if v_context#>>'{warning_context,phase}' is distinct from 'PRE_SAVE'
        or (v_context#>>'{warning_context,warning_code}' is not null
          and v_context#>>'{warning_context,warning_code}' not in ('SNOOZE_DATE_BEFORE_NEXT_PAY_RUN','SNOOZE_INCLUDES_NEXT_PAY_RUN')) then
        raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_SET_REQUIRES_PRE_SAVE';end if;
      v_now:=clock_timestamp();
      insert into private.bpay_next_snooze_warning_ack(request_id,actor_id,candidate_id,module_epoch,
        scope_kind,scope_identity,expected_revision_id,expected_version,worker_pay_basis,warning_context,issued_at_utc,expires_at_utc)
      values(v_request,p_actor_id,v_candidate,v_command.module_epoch,v_context->>'scope_kind',v_context->>'scope_identity',
        nullif(v_context->>'expected_revision_id','')::uuid,v_expected,decode(v_context->>'worker_pay_basis','hex'),
        v_context->'warning_context',v_now,v_now+interval '30 minutes') returning token into v_token;
      v_result:=v_context||jsonb_build_object('acknowledgement_token',v_token,'acknowledgement_expires_at_utc',v_now+interval '30 minutes');
    else
      if coalesce((v_context#>>'{warning_context,warning_required}')::boolean,false) or p_args ? 'acknowledgement_token' then
        select a.* into v_ack from private.bpay_next_snooze_warning_ack a
          where a.token=nullif(p_args->>'acknowledgement_token','')::uuid for update;
        if not found or v_ack.consumed_by_request_id is not null or v_ack.expires_at_utc<=clock_timestamp()
          or (v_ack.actor_id,v_ack.candidate_id,v_ack.module_epoch,v_ack.scope_kind,v_ack.scope_identity,
              v_ack.expected_revision_id,v_ack.expected_version,v_ack.worker_pay_basis,v_ack.warning_context)
            is distinct from (p_actor_id,v_candidate,v_command.module_epoch,v_context->>'scope_kind',v_context->>'scope_identity',
              nullif(v_context->>'expected_revision_id','')::uuid,v_expected,decode(v_context->>'worker_pay_basis','hex'),v_context->'warning_context') then
          raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WARNING_ACK_REQUIRED_OR_STALE';
        end if;
        update private.bpay_next_snooze_warning_ack set consumed_by_request_id=v_request,consumed_at_utc=clock_timestamp() where token=v_ack.token;
      end if;
      select s.* into v_snooze from private.bpay_next_snooze s where s.id=nullif(v_context->>'snooze_id','')::uuid for update;
      if found then
        v_before:=to_jsonb(v_snooze); v_event:='AMENDED';
        update private.bpay_next_snooze s set expected_revision_id=nullif(v_context->>'expected_revision_id','')::uuid,
          physical_timesheet_id=nullif(v_context->>'physical_timesheet_id','')::uuid,
          approved_segment_id=nullif(v_context->>'approved_segment_id','')::uuid,
          expense_approved_line_id=nullif(v_context->>'expense_approved_line_id','')::uuid,
          component_key=v_context->>'component_key',snooze_kind=v_context->>'snooze_kind',
          worker_pay_basis=decode(v_context->>'worker_pay_basis','hex'),until_date=nullif(v_context->>'until_date','')::date,
          note=v_context->>'note',version=s.version+1,control_sequence=v_command.agency_sequence,
          updated_at_utc=clock_timestamp(),updated_by=p_actor_id,last_request_id=v_request
          where s.id=v_snooze.id returning s.* into v_snooze;
      else
        v_event:='SET';
        insert into private.bpay_next_snooze(candidate_id,work_id,expected_revision_id,physical_timesheet_id,scope_kind,scope_identity,
          component_key,segment_key,approved_segment_id,expense_approved_line_id,case_id,case_component_id,case_kind,
          snooze_kind,worker_pay_basis,until_date,note,control_sequence,created_by,updated_by,last_request_id)
        values(v_candidate,v_work,nullif(v_context->>'expected_revision_id','')::uuid,nullif(v_context->>'physical_timesheet_id','')::uuid,
          v_context->>'scope_kind',v_context->>'scope_identity',v_context->>'component_key',v_context->>'segment_key',
          nullif(v_context->>'approved_segment_id','')::uuid,nullif(v_context->>'expense_approved_line_id','')::uuid,
          nullif(v_context->>'case_id','')::uuid,nullif(v_context->>'case_component_id','')::uuid,v_context->>'case_kind',
          v_context->>'snooze_kind',decode(v_context->>'worker_pay_basis','hex'),nullif(v_context->>'until_date','')::date,
          v_context->>'note',v_command.agency_sequence,p_actor_id,p_actor_id,v_request) returning * into v_snooze;
      end if;
    end if;
  end if;
  if v_event is not null then
    insert into private.bpay_next_snooze_event(snooze_id,candidate_id,event_kind,version,control_sequence,actor_id,request_id,reason,before_state,after_state)
      values(v_snooze.id,v_candidate,v_event,v_snooze.version,v_command.agency_sequence,p_actor_id,v_request,v_event,v_before,to_jsonb(v_snooze));
    if p_action='SNOOZE_SET' then v_context:=private.bpay_next_snooze_context_v1(p_actor_id,p_args); end if;
    v_result:=jsonb_build_object('snooze_id',v_snooze.id,'version',v_snooze.version,'active',v_snooze.active,
      'control_sequence',v_command.agency_sequence,'indefinite_wins',coalesce((v_context->>'indefinite_wins')::boolean,false));
  end if;
  insert into private.bpay_next_snooze_request(request_id,actor_id,action,request_body,result_body)
    values(v_request,p_actor_id,p_action,p_args,v_result);
  update private.bpay_next_command set status='COMPLETE',expected_member_count=0,sealed_at_utc=clock_timestamp() where id=v_request;
  return v_result;
end
$function$;
alter function private.bpay_next_snooze_context_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_snooze_context_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_snooze_action_v1(uuid,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_snooze_action_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
commit;
