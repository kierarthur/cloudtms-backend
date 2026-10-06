-- A8: one maintained financial writer. LEGACY remains the installed default.
-- No owner/GUC exemptions, no old-history scan, no activation or data rewrite.
-- Shared scope-change tokens are factual Source transaction evidence, not
-- legacy financial rows. Their exact zero-effect NEXT proof belongs to0556;
-- this fence must be accepted/activated only with0555+0556 joined proof.
-- FOR SHARE serialises each admitted legacy DB transaction with owner UPDATE.
-- External provider/email work still requires the separately reviewed quiet
-- window and reconciliation before switching the singleton to NEXT.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_assert_legacy_writer_v1()
returns void language plpgsql volatile security definer
set search_path=pg_catalog,private
as $function$
declare
  v_owner text;
  v_epoch bigint;
begin
  select m.active_owner,m.owner_epoch into v_owner,v_epoch
    from private.bpay_next_module_control m where m.id=1 for share;
  if not found or v_owner is null or v_owner not in ('LEGACY','NEXT','DISABLED')
     or v_epoch is null or v_epoch<=0 then
    raise exception using errcode='55000',message='BPAY_NEXT_OWNER_STATUS_UNAVAILABLE';
  end if;
  if v_owner<>'LEGACY' then
    raise exception using errcode='55000',message='BPAY_NEXT_LEGACY_WRITER_RETIRED';
  end if;
end;
$function$;

create or replace function private.bpay_next_legacy_writer_fence_v1()
returns trigger language plpgsql volatile security definer
set search_path=pg_catalog,private
as $function$
begin
  if tg_table_schema<>'public' or not (tg_table_name=any(array[
    -- BEGIN EXACT LEGACY RELATIONS
    'pay_advance_patches','pay_advance_reservations','pay_advances',
    'pay_bank_transfer_events','pay_bank_transfers','pay_batch_auth_actions',
    'pay_batch_auth_requests','pay_batch_auth_tokens','pay_batch_candidates',
    'pay_batch_display_summary','pay_batch_item_breakdowns','pay_batch_items',
    'pay_batch_paye_net_inputs','pay_batch_timesheet_snapshots','pay_batches',
    'pay_finance_case_components','pay_finance_case_events',
    'pay_finance_case_oneoff_payout_bank_details','pay_item_snoozes',
    'pay_manual_adjustment_carry_forwards','pay_payment_correction_actions',
    'pay_payment_correction_items','pay_payment_correction_request_candidates',
    'pay_payment_correction_requests','pay_payment_correction_work_items',
    'pay_payment_return_notice_groups','pay_snooze_warning_acknowledgements',
    'timesheet_pay_state','timesheet_pay_state_history',
    'timesheet_payment_overrides','ts_pay_adjustments',
    'banking_pay_operations','banking_pay_workbench_jobs'
    -- END EXACT LEGACY RELATIONS
  ]::text[])) or tg_when<>'BEFORE' or not (
      (tg_level='ROW' and tg_op in ('INSERT','UPDATE','DELETE'))
      or (tg_level='STATEMENT' and tg_op='TRUNCATE')) then
    raise exception using errcode='55000',message='BPAY_NEXT_LEGACY_FENCE_SCOPE_INVALID';
  end if;
  perform private.bpay_next_assert_legacy_writer_v1();
  if tg_level='STATEMENT' then return null; end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end;
$function$;

create or replace function public.bpay_next_owner_status_v1()
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private
as $function$
declare
  v_claim text;
  v_owner text;
  v_epoch bigint;
begin
  begin
    v_claim:=coalesce(nullif(pg_catalog.current_setting('request.jwt.claim.role',true),''),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','');
  exception when invalid_text_representation then
    raise exception using errcode='42501',message='BPAY_NEXT_OWNER_STATUS_FORBIDDEN';
  end;
  if v_claim<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_OWNER_STATUS_FORBIDDEN';
  end if;
  select m.active_owner,m.owner_epoch into v_owner,v_epoch
    from private.bpay_next_module_control m where m.id=1;
  if not found or v_owner is null or v_owner not in ('LEGACY','NEXT','DISABLED')
     or v_epoch is null or v_epoch<=0 then
    raise exception using errcode='55000',message='BPAY_NEXT_OWNER_STATUS_UNAVAILABLE';
  end if;
  return pg_catalog.jsonb_build_object('active_owner',v_owner,'owner_epoch',v_epoch::text);
end;
$function$;

alter function private.bpay_next_assert_legacy_writer_v1() owner to postgres;
alter function private.bpay_next_legacy_writer_fence_v1() owner to postgres;
alter function public.bpay_next_owner_status_v1() owner to postgres;
revoke all on function private.bpay_next_assert_legacy_writer_v1() from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_legacy_writer_fence_v1() from public,anon,authenticated,service_role;
revoke all on function public.bpay_next_owner_status_v1() from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_owner_status_v1() to service_role;

do $install$
declare
  v_tables constant text[]:=array[
    -- BEGIN EXACT LEGACY RELATIONS
    'pay_advance_patches','pay_advance_reservations','pay_advances',
    'pay_bank_transfer_events','pay_bank_transfers','pay_batch_auth_actions',
    'pay_batch_auth_requests','pay_batch_auth_tokens','pay_batch_candidates',
    'pay_batch_display_summary','pay_batch_item_breakdowns','pay_batch_items',
    'pay_batch_paye_net_inputs','pay_batch_timesheet_snapshots','pay_batches',
    'pay_finance_case_components','pay_finance_case_events',
    'pay_finance_case_oneoff_payout_bank_details','pay_item_snoozes',
    'pay_manual_adjustment_carry_forwards','pay_payment_correction_actions',
    'pay_payment_correction_items','pay_payment_correction_request_candidates',
    'pay_payment_correction_requests','pay_payment_correction_work_items',
    'pay_payment_return_notice_groups','pay_snooze_warning_acknowledgements',
    'timesheet_pay_state','timesheet_pay_state_history',
    'timesheet_payment_overrides','ts_pay_adjustments',
    'banking_pay_operations','banking_pay_workbench_jobs'
    -- END EXACT LEGACY RELATIONS
  ];
  v_table text;
  v_rel oid;
  v_trigger record;
begin
  if pg_catalog.cardinality(v_tables)<>33
     or (select count(distinct x) from pg_catalog.unnest(v_tables) x)<>33 then
    raise exception using errcode='55000',message='BPAY_NEXT_LEGACY_FENCE_INVENTORY_INVALID';
  end if;
  -- Retire only our two previously proposed token fences. Never drop another
  -- Source guard or an unexpected trigger with a coincidentally equal name.
  for v_trigger in select t.* from pg_catalog.pg_trigger t
    where t.tgrelid='public.banking_pay_scope_change_transactions'::regclass
      and t.tgname in ('bpay_next_legacy_writer_fence_row','bpay_next_legacy_writer_fence_truncate') loop
    if v_trigger.tgisinternal or v_trigger.tgfoid<>'private.bpay_next_legacy_writer_fence_v1()'::regprocedure
      or v_trigger.tgnargs<>0 or v_trigger.tgqual is not null
      or v_trigger.tgtype<>(case when v_trigger.tgname='bpay_next_legacy_writer_fence_row' then 31 else 34 end) then
      raise exception using errcode='55000',message='BPAY_NEXT_LEGACY_FENCE_TRIGGER_CONFLICT';
    end if;
    execute pg_catalog.format('drop trigger %I on public.banking_pay_scope_change_transactions',v_trigger.tgname);
  end loop;
  foreach v_table in array v_tables loop
    v_rel:=pg_catalog.to_regclass(pg_catalog.format('public.%I',v_table));
    if v_rel is null or not exists(select 1 from pg_catalog.pg_class c
      join pg_catalog.pg_namespace n on n.oid=c.relnamespace
      where c.oid=v_rel and c.relkind='r' and n.nspname='public' and c.relname=v_table) then
      raise exception using errcode='55000',message='BPAY_NEXT_LEGACY_FENCE_RELATION_MISSING';
    end if;
    for v_trigger in select t.* from pg_catalog.pg_trigger t where t.tgrelid=v_rel
      and t.tgname in ('bpay_next_legacy_writer_fence_row','bpay_next_legacy_writer_fence_truncate') loop
      if v_trigger.tgisinternal or v_trigger.tgfoid<>'private.bpay_next_legacy_writer_fence_v1()'::regprocedure
         or v_trigger.tgnargs<>0 or v_trigger.tgqual is not null
         or v_trigger.tgtype<>(case when v_trigger.tgname='bpay_next_legacy_writer_fence_row' then 31 else 34 end) then
        raise exception using errcode='55000',message='BPAY_NEXT_LEGACY_FENCE_TRIGGER_CONFLICT';
      end if;
    end loop;
    execute pg_catalog.format('drop trigger if exists bpay_next_legacy_writer_fence_row on public.%I',v_table);
    execute pg_catalog.format('drop trigger if exists bpay_next_legacy_writer_fence_truncate on public.%I',v_table);
    execute pg_catalog.format('create trigger bpay_next_legacy_writer_fence_row before insert or update or delete on public.%I for each row execute function private.bpay_next_legacy_writer_fence_v1()',v_table);
    execute pg_catalog.format('create trigger bpay_next_legacy_writer_fence_truncate before truncate on public.%I for each statement execute function private.bpay_next_legacy_writer_fence_v1()',v_table);
    execute pg_catalog.format('alter table public.%I enable always trigger bpay_next_legacy_writer_fence_row',v_table);
    execute pg_catalog.format('alter table public.%I enable always trigger bpay_next_legacy_writer_fence_truncate',v_table);
    if (select count(*) from pg_catalog.pg_trigger t where t.tgrelid=v_rel
        and t.tgfoid='private.bpay_next_legacy_writer_fence_v1()'::regprocedure
        and not t.tgisinternal and t.tgenabled='A' and t.tgnargs=0 and t.tgqual is null
        and ((t.tgname='bpay_next_legacy_writer_fence_row' and t.tgtype=31)
          or (t.tgname='bpay_next_legacy_writer_fence_truncate' and t.tgtype=34)))<>2 then
      raise exception using errcode='55000',message='BPAY_NEXT_LEGACY_FENCE_INSTALL_INVALID';
    end if;
  end loop;
  if (select count(*) from pg_catalog.pg_trigger t
      where t.tgfoid='private.bpay_next_legacy_writer_fence_v1()'::regprocedure)<>66 then
    raise exception using errcode='55000',message='BPAY_NEXT_LEGACY_FENCE_INSTALL_INVALID';
  end if;
end;
$install$;

notify pgrst,'reload schema';
commit;
