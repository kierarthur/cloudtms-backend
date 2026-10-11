-- WB-38 native shared-window context. Strict PRE_SAVE/SET authority retained.
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
  -- WB-45 adds its genuine resolved-rate context here at its chain turn.
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
alter function private.bpay_next_snooze_context_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_snooze_context_v1(uuid,jsonb) from public,anon,authenticated,service_role;
commit;
